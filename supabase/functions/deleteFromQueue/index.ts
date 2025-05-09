
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

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
    
    // Try using ensure_message_deleted RPC first - most reliable approach
    try {
      const { data, error } = await supabase.rpc(
        'ensure_message_deleted',
        { 
          queue_name,
          message_id: message_id.toString(),
          max_attempts: 3
        }
      );
      
      if (!error && data === true) {
        console.log(`Successfully deleted message ${message_id} using ensure_message_deleted`);
        
        // Record successful deletion
        try {
          const deletedKey = `deleted:${queue_name}:${message_id}`;
          await redis.set(deletedKey, 'true', { ex: 86400 }); // 24 hour TTL
        } catch (redisError) {
          console.warn("Failed to record deleted message in Redis:", redisError);
        }
        
        // Release the lock
        try {
          if (lockAcquired) {
            await redis.del(lockKey);
          }
        } catch (redisError) {
          console.warn("Failed to release Redis lock:", redisError);
        }
        
        return new Response(
          JSON.stringify({ 
            success: true, 
            message: `Message ${message_id} deleted successfully`,
            method: "ensure_message_deleted"
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    } catch (rpcError) {
      console.warn(`ensure_message_deleted failed:`, rpcError);
    }
    
    // Fall back to pg_delete_message RPC
    try {
      const { data, error } = await supabase.rpc(
        'pg_delete_message',
        { 
          queue_name,
          message_id: message_id.toString()
        }
      );
      
      if (!error && data === true) {
        console.log(`Successfully deleted message ${message_id} using pg_delete_message`);
        
        // Record successful deletion
        try {
          const deletedKey = `deleted:${queue_name}:${message_id}`;
          await redis.set(deletedKey, 'true', { ex: 86400 }); // 24 hour TTL
        } catch (redisError) {
          console.warn("Failed to record deleted message in Redis:", redisError);
        }
        
        // Release the lock
        try {
          if (lockAcquired) {
            await redis.del(lockKey);
          }
        } catch (redisError) {
          console.warn("Failed to release Redis lock:", redisError);
        }
        
        return new Response(
          JSON.stringify({ 
            success: true, 
            message: `Message ${message_id} deleted successfully`,
            method: "pg_delete_message"
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    } catch (pgError) {
      console.warn(`pg_delete_message failed:`, pgError);
    }
    
    // If message couldn't be deleted, try to reset its visibility timeout as a fallback
    try {
      const { data, error } = await supabase.rpc(
        'reset_stuck_message',
        { 
          queue_name,
          message_id: message_id.toString()
        }
      );
      
      if (!error && data === true) {
        console.log(`Reset visibility timeout for message ${message_id}`);
        
        // Release the lock
        try {
          if (lockAcquired) {
            await redis.del(lockKey);
          }
        } catch (redisError) {
          console.warn("Failed to release Redis lock:", redisError);
        }
        
        return new Response(
          JSON.stringify({ 
            success: true, 
            message: `Message ${message_id} visibility timeout reset`,
            reset: true
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    } catch (resetError) {
      console.error("Error trying to reset visibility timeout:", resetError);
    }
    
    // Release the lock regardless of outcome
    try {
      if (lockAcquired) {
        await redis.del(lockKey);
      }
    } catch (redisError) {
      console.warn("Failed to release Redis lock:", redisError);
    }
    
    // Nothing worked, return failure
    return new Response(
      JSON.stringify({ 
        success: false, 
        message: `Failed to handle message ${message_id} after all attempts` 
      }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error("Error in deleteFromQueue:", error);
    
    return new Response(
      JSON.stringify({ 
        error: error.message,
        stack: error.stack
      }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
