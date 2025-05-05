
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from '@upstash/redis';

// Initialize Redis client for distributed locks and rate limiting
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const { queue_name, message_id } = await req.json();
    
    if (!queue_name || !message_id) {
      throw new Error("queue_name and message_id are required");
    }
    
    console.log(`Attempting to delete message ${message_id} from queue ${queue_name}`);
    
    // Rate limiting to prevent overloading the database
    const rateLimitKey = `ratelimit:deleteFromQueue`;
    let requestCount = 1;
    
    try {
      requestCount = await redis.incr(rateLimitKey);
      if (requestCount === 1) {
        await redis.expire(rateLimitKey, 1); // 1 second TTL
      }
      
      if (requestCount > 100) {
        console.warn(`Rate limit exceeded for deleteFromQueue: ${requestCount} requests in 1 second`);
        return new Response(
          JSON.stringify({ 
            error: "Rate limit exceeded, please try again later",
            retry_after: 1
          }),
          { 
            status: 429, 
            headers: { 
              ...corsHeaders, 
              'Content-Type': 'application/json',
              'Retry-After': '1'
            } 
          }
        );
      }
    } catch (redisError) {
      // Continue if Redis fails, rate limiting is not critical
      console.warn("Redis rate limiting failed:", redisError);
    }
    
    // Check idempotency - if we've already deleted this message successfully
    try {
      const deletedKey = `deleted:${queue_name}:${message_id}`;
      const alreadyDeleted = await redis.exists(deletedKey);
      
      if (alreadyDeleted === 1) {
        console.log(`Message ${message_id} was already deleted previously, returning success`);
        return new Response(
          JSON.stringify({ 
            success: true, 
            message: `Message ${message_id} already handled`, 
            idempotent: true 
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    } catch (redisError) {
      // Continue if Redis fails, idempotency check is not critical
      console.warn("Redis idempotency check failed:", redisError);
    }
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    // Add distributed lock to prevent concurrent attempts for the same message
    const lockKey = `delete_lock:${queue_name}:${message_id}`;
    let lockAcquired = false;
    
    try {
      lockAcquired = await redis.set(lockKey, "true", {
        nx: true, // Only set if not exists
        ex: 30 // 30 second expiration
      }) === 'OK';
      
      if (!lockAcquired) {
        console.log(`Another process is already trying to delete message ${message_id}, skipping`);
        return new Response(
          JSON.stringify({ 
            success: true, 
            message: `Message ${message_id} is being handled by another process` 
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    } catch (redisError) {
      // Continue if Redis fails, locking is not critical
      console.warn("Redis lock acquisition failed:", redisError);
    }
    
    // Try multiple approaches with retries to ensure deletion works
    let success = false;
    let attempts = 0;
    const maxAttempts = 3;
    let deletionMethod = 'none';
    
    while (!success && attempts < maxAttempts) {
      attempts++;
      
      // Try different deletion methods in sequence
      try {
        // Attempt 1: Try using pg_delete_message RPC with message_id as numeric
        if (!success && attempts <= maxAttempts) {
          try {
            const numericId = parseInt(message_id.toString(), 10);
            
            if (!isNaN(numericId)) {
              const { data, error } = await supabase.rpc(
                'pg_delete_message',
                { 
                  queue_name,
                  message_id: numericId.toString() 
                }
              );
              
              if (!error && data === true) {
                console.log(`Successfully deleted message ${message_id} using numeric ID (attempt ${attempts})`);
                success = true;
                deletionMethod = 'numeric_id';
                break;
              } else if (error) {
                console.error(`Numeric ID deletion error (attempt ${attempts}):`, error);
              }
            }
          } catch (e) {
            console.error(`Exception in numeric deletion attempt ${attempts}:`, e);
          }
        }
        
        // Attempt 2: Try using pg_delete_message RPC with message_id as string
        if (!success && attempts <= maxAttempts) {
          try {
            const { data, error } = await supabase.rpc(
              'pg_delete_message',
              { 
                queue_name,
                message_id: message_id.toString() 
              }
            );
            
            if (!error && data === true) {
              console.log(`Successfully deleted message ${message_id} using string ID (attempt ${attempts})`);
              success = true;
              deletionMethod = 'string_id';
              break;
            } else if (error) {
              console.error(`String ID deletion error (attempt ${attempts}):`, error);
            }
          } catch (e) {
            console.error(`Exception in string deletion attempt ${attempts}:`, e);
          }
        }
        
        // Attempt 3: Try cross-schema operation as last resort
        if (!success && attempts === maxAttempts) {
          try {
            const { data, error } = await supabase.rpc('cross_schema_queue_op', {
              p_queue_name: queue_name,
              p_message_id: message_id.toString(),
              p_operation: 'delete'
            });
            
            if (!error && data === true) {
              console.log(`Successfully deleted message ${message_id} using cross-schema operation`);
              success = true;
              deletionMethod = 'cross_schema';
              break;
            } else if (error) {
              console.error(`Cross-schema deletion error:`, error);
            }
          } catch (e) {
            console.error(`Exception in cross-schema deletion:`, e);
          }
        }
      } catch (attemptError) {
        console.error(`General error in deletion attempt ${attempts}:`, attemptError);
      }
      
      // Wait before retrying with exponential backoff
      if (!success && attempts < maxAttempts) {
        const backoffMs = Math.min(100 * Math.pow(2, attempts), 2000);
        const jitterMs = Math.floor(Math.random() * 100);
        await new Promise(resolve => setTimeout(resolve, backoffMs + jitterMs));
      }
    }
    
    // If we couldn't delete, try to reset visibility timeout as a fallback
    if (!success) {
      try {
        console.log(`Failed to delete message ${message_id} after ${maxAttempts} attempts, trying visibility reset`);
        
        const { data: resetData, error: resetError } = await supabase.rpc(
          'reset_stuck_message',
          { 
            queue_name,
            message_id: message_id.toString()
          }
        );
        
        if (resetError) {
          console.error("Error resetting message visibility:", resetError);
        } else if (resetData === true) {
          console.log(`Reset visibility timeout for message ${message_id}`);
          success = true;
          deletionMethod = 'visibility_reset';
        }
      } catch (resetError) {
        console.error("Error trying to reset visibility timeout:", resetError);
      }
    }
    
    // Release the lock regardless of outcome
    try {
      if (lockAcquired) {
        await redis.del(lockKey);
      }
    } catch (redisError) {
      console.warn("Failed to release Redis lock:", redisError);
    }
    
    if (success) {
      // Track successful deletion in Redis for idempotency
      try {
        const deletedKey = `deleted:${queue_name}:${message_id}`;
        await redis.set(deletedKey, 'true', { ex: 86400 }); // 24 hour TTL
        
        // Also set a metadata entry with details about how it was deleted
        await redis.set(
          `deleted_meta:${queue_name}:${message_id}`, 
          JSON.stringify({
            method: deletionMethod,
            timestamp: Date.now(),
            attempts
          }), 
          { ex: 86400 }
        );
      } catch (redisError) {
        console.warn("Failed to track deleted message in Redis:", redisError);
      }
      
      return new Response(
        JSON.stringify({ 
          success: true, 
          message: `Message ${message_id} deleted successfully`,
          method: deletionMethod,
          attempts
        }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } else {
      console.error(`Failed to delete or reset message ${message_id} after ${maxAttempts} attempts`);
      
      // Try the nuclear option with forceDeleteMessage
      try {
        const forceDeleteResponse = await fetch(
          `${Deno.env.get("SUPABASE_URL")}/functions/v1/forceDeleteMessage`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "Authorization": `Bearer ${Deno.env.get("SUPABASE_ANON_KEY")}`
            },
            body: JSON.stringify({ 
              queue_name, 
              message_id,
              bypass_checks: true 
            })
          }
        );
        
        if (forceDeleteResponse.ok) {
          const forceResult = await forceDeleteResponse.json();
          if (forceResult.success) {
            console.log(`Successfully force-deleted message ${message_id}`);
            
            // Track successful force deletion in Redis for idempotency
            try {
              const deletedKey = `deleted:${queue_name}:${message_id}`;
              await redis.set(deletedKey, 'true', { ex: 86400 }); // 24 hour TTL
              await redis.set(
                `deleted_meta:${queue_name}:${message_id}`, 
                JSON.stringify({
                  method: 'force_delete',
                  timestamp: Date.now(),
                  attempts: attempts + 1
                }), 
                { ex: 86400 }
              );
            } catch (redisError) {
              console.warn("Failed to track force-deleted message in Redis:", redisError);
            }
            
            return new Response(
              JSON.stringify({ 
                success: true, 
                message: `Message ${message_id} force-deleted successfully`,
                force_delete: true
              }),
              { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
            );
          }
        }
      } catch (forceError) {
        console.error("Force delete attempt failed:", forceError);
      }
      
      return new Response(
        JSON.stringify({ 
          success: false, 
          message: `Failed to handle message ${message_id} after all attempts` 
        }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
  } catch (error) {
    console.error("Error in deleteFromQueue:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
