
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
    const { queue_name, message_id, force_option } = await req.json();
    
    if (!queue_name || !message_id) {
      throw new Error("queue_name and message_id are required");
    }
    
    console.log(`Attempting to force delete message ${message_id} from queue ${queue_name} with option ${force_option || 'default'}`);
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    // Add distributed lock to prevent concurrent attempts for the same message
    const lockKey = `force_delete_lock:${queue_name}:${message_id}`;
    let lockAcquired = false;
    
    try {
      lockAcquired = await redis.set(lockKey, "true", {
        nx: true, // Only set if not exists
        ex: 30 // 30 second expiration
      }) === 'OK';
      
      if (!lockAcquired) {
        console.log(`Another process is already trying to force delete message ${message_id}, skipping`);
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
    
    let deleteResult;
    
    try {
      // Use the new force_delete_message function for comprehensive deletion
      const { data, error } = await supabase.rpc(
        'force_delete_message',
        { 
          p_queue_name: queue_name,
          p_message_id: message_id
        }
      );
      
      if (error) {
        console.error("Error in force_delete_message RPC:", error);
        
        // Fallback to direct cross-schema operation in case the RPC fails
        const { data: crossData, error: crossError } = await supabase.rpc(
          'cross_schema_queue_op',
          {
            p_operation: force_option === 'reset' ? 'reset' : 'delete',
            p_queue_name: queue_name,
            p_message_id: message_id
          }
        );
        
        deleteResult = crossError ? 
          { success: false, error: crossError.message } : 
          crossData;
      } else {
        deleteResult = data;
      }
    } catch (deleteError) {
      console.error("Error during force deletion:", deleteError);
      deleteResult = { 
        success: false, 
        error: deleteError.message,
        suggestion: "Try using the 'reset' force_option to reset visibility timeout instead"
      };
    }
    
    // Release the lock regardless of outcome
    try {
      if (lockAcquired) {
        await redis.del(lockKey);
      }
    } catch (redisError) {
      console.warn("Failed to release Redis lock:", redisError);
    }
    
    // Record the problematic message for monitoring
    try {
      await supabase.rpc('record_problematic_message', {
        p_queue_name: queue_name,
        p_message_id: message_id,
        p_message_body: null, // We don't have the body here
        p_error_type: 'needed_force_delete',
        p_error_details: JSON.stringify(deleteResult)
      });
    } catch (recordError) {
      // Non-critical operation
      console.warn("Failed to record problematic message:", recordError);
    }
    
    return new Response(
      JSON.stringify({ 
        result: deleteResult,
        message: deleteResult.success ? 
          `Successfully handled message ${message_id}` : 
          `Failed to handle message ${message_id}` 
      }),
      { 
        status: deleteResult.success ? 200 : 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      }
    );
  } catch (error) {
    console.error("Error in forceDeleteMessage:", error);
    
    return new Response(
      JSON.stringify({ 
        error: error.message,
        stack: error.stack
      }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
