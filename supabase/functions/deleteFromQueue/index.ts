
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
    try {
      const requestCount = await redis.incr(rateLimitKey);
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
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    // Try multiple approaches to ensure deletion works
    let success = false;
    let attempts = 0;
    const maxAttempts = 3;
    
    // Add distributed lock to prevent concurrent attempts for the same message
    const lockKey = `delete_lock:${queue_name}:${message_id}`;
    try {
      const lockAcquired = await redis.set(lockKey, "true", {
        nx: true, // Only set if not exists
        ex: 30 // 30 second expiration
      });
      
      if (lockAcquired !== 'OK') {
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
    
    while (!success && attempts < maxAttempts) {
      attempts++;
      
      // Attempt 1: Try as numeric ID
      if (attempts === 1) {
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
              console.log(`Successfully deleted message ${message_id} using numeric ID`);
              success = true;
              break;
            }
          }
        } catch (e) {
          console.error("Error with numeric ID deletion:", e);
        }
      }
      
      // Attempt 2: Try as string ID
      if (attempts === 2) {
        try {
          const { data, error } = await supabase.rpc(
            'pg_delete_message',
            { 
              queue_name,
              message_id: message_id.toString() 
            }
          );
          
          if (!error && data === true) {
            console.log(`Successfully deleted message ${message_id} using string ID`);
            success = true;
            break;
          }
        } catch (e) {
          console.error("Error with string ID deletion:", e);
        }
      }
      
      // Attempt 3: Try direct SQL as a last resort
      if (attempts === 3) {
        try {
          // Try to delete directly using SQL
          const { data, error } = await supabase.rpc('cross_schema_queue_op', {
            p_queue_name: queue_name,
            p_message_id: message_id.toString(),
            p_operation: 'delete'
          });
          
          if (!error && data === true) {
            console.log(`Successfully deleted message ${message_id} using cross-schema operation`);
            success = true;
            break;
          }
        } catch (e) {
          console.error("Error with cross-schema deletion:", e);
        }
      }
      
      // Wait before retrying
      if (!success && attempts < maxAttempts) {
        await new Promise(resolve => setTimeout(resolve, 100 * attempts));
      }
    }
    
    // If we couldn't delete, try to reset visibility timeout as a fallback
    if (!success) {
      try {
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
        }
      } catch (resetError) {
        console.error("Error trying to reset visibility timeout:", resetError);
      }
    }
    
    // Release the lock regardless of outcome
    try {
      await redis.del(lockKey);
    } catch (redisError) {
      console.warn("Failed to release Redis lock:", redisError);
    }
    
    if (success) {
      // Track successful deletion in Redis for idempotency
      try {
        const deletedKey = `deleted:${queue_name}:${message_id}`;
        await redis.set(deletedKey, 'true', { ex: 86400 }); // 24 hour TTL
      } catch (redisError) {
        console.warn("Failed to track deleted message in Redis:", redisError);
      }
      
      return new Response(
        JSON.stringify({ success: true, message: `Message ${message_id} handled successfully` }),
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
          message: `Failed to handle message ${message_id} after ${maxAttempts} attempts` 
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
