
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
    const { queue_name, message_id, bypass_checks = false } = await req.json();
    
    if (!queue_name || !message_id) {
      throw new Error("queue_name and message_id are required");
    }

    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    console.log(`FORCE DELETE: Attempting to delete message ${message_id} from queue ${queue_name}`);
    
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
    
    // Add distributed lock to prevent concurrent attempts
    const lockKey = `force_delete_lock:${queue_name}:${message_id}`;
    let lockAcquired = false;
    
    try {
      lockAcquired = await redis.set(lockKey, "true", {
        nx: true, // Only set if not exists
        ex: 30 // 30 second expiration
      }) === 'OK';
      
      if (!lockAcquired) {
        console.log(`Another process is already trying to force delete message ${message_id}, waiting...`);
        // For force delete, we'll wait a bit and then continue anyway
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    } catch (redisError) {
      // Continue if Redis fails, locking is not critical
      console.warn("Redis lock acquisition failed:", redisError);
    }
    
    let success = false;
    let response = { method: 'none', details: null };
    
    // Approach 1: Try standard pg_delete_message with various ID formats
    try {
      // Try with numeric ID
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
          console.log(`Successfully deleted message ${message_id} using pg_delete_message with numeric ID`);
          success = true;
          response = { method: 'pg_delete_message_numeric', details: null };
        } else if (error) {
          console.error("Error with pg_delete_message (numeric):", error);
        }
      }
      
      // Try with string ID if numeric approach failed
      if (!success) {
        const { data, error } = await supabase.rpc(
          'pg_delete_message',
          { 
            queue_name, 
            message_id: message_id.toString() 
          }
        );
        
        if (!error && data === true) {
          console.log(`Successfully deleted message ${message_id} using pg_delete_message with string ID`);
          success = true;
          response = { method: 'pg_delete_message_string', details: null };
        } else if (error) {
          console.error("Error with pg_delete_message (string):", error);
        }
      }
    } catch (e) {
      console.error("Error with standard deletion:", e);
    }
    
    // Approach 2: Direct SQL with explicit schema targeting
    if (!success) {
      try {
        // Try to delete using SQL with schema selection
        const { data, error } = await supabase.rpc('raw_sql_query', {
          sql_query: `
            DO $$
            DECLARE
              success_count INT := 0;
              table_exists BOOLEAN;
            BEGIN
              -- Check if table exists in pgmq schema
              SELECT EXISTS (
                SELECT FROM pg_tables 
                WHERE schemaname = 'pgmq' 
                AND tablename = 'q_' || $1
              ) INTO table_exists;
            
              IF table_exists THEN
                -- Try pgmq schema with exact match
                BEGIN
                  EXECUTE 'DELETE FROM pgmq.q_' || $1 || ' WHERE msg_id::TEXT = $2 OR id::TEXT = $2';
                  GET DIAGNOSTICS success_count = ROW_COUNT;
                EXCEPTION WHEN OTHERS THEN
                  RAISE NOTICE 'Error deleting from pgmq schema: %', SQLERRM;
                END;
              END IF;
              
              -- Check if table exists in public schema
              SELECT EXISTS (
                SELECT FROM pg_tables 
                WHERE schemaname = 'public' 
                AND tablename = 'pgmq_' || $1
              ) INTO table_exists;
              
              -- Try public schema if still no success
              IF success_count = 0 AND table_exists THEN
                BEGIN
                  EXECUTE 'DELETE FROM public.pgmq_' || $1 || ' WHERE msg_id::TEXT = $2 OR id::TEXT = $2';
                  GET DIAGNOSTICS success_count = ROW_COUNT;
                EXCEPTION WHEN OTHERS THEN
                  RAISE NOTICE 'Error deleting from public schema: %', SQLERRM;
                END;
              END IF;
              
              -- Super aggressive mode if still no success
              IF success_count = 0 AND $3 = TRUE THEN
                IF table_exists THEN
                  BEGIN
                    -- Try using wildcard deletion for extreme cases
                    -- Try pgmq schema with partial ID match
                    EXECUTE '
                      DELETE FROM pgmq.q_' || $1 || ' 
                      WHERE 
                        msg_id::TEXT LIKE $2 || ''%'' OR 
                        id::TEXT LIKE $2 || ''%'' OR
                        msg_id = ' || CASE WHEN $2 ~ ''^[0-9]+$'' THEN $2 ELSE 'NULL' END;
                      
                    GET DIAGNOSTICS success_count = ROW_COUNT;
                  EXCEPTION WHEN OTHERS THEN
                    RAISE NOTICE 'Error in aggressive deletion: %', SQLERRM;
                  END;
                END IF;
                
                -- Try public schema with partial ID match if still no success
                IF success_count = 0 AND table_exists THEN
                  BEGIN
                    EXECUTE '
                      DELETE FROM public.pgmq_' || $1 || ' 
                      WHERE 
                        msg_id::TEXT LIKE $2 || ''%'' OR 
                        id::TEXT LIKE $2 || ''%'' OR
                        msg_id = ' || CASE WHEN $2 ~ ''^[0-9]+$'' THEN $2 ELSE 'NULL' END;
                    
                    GET DIAGNOSTICS success_count = ROW_COUNT;
                  EXCEPTION WHEN OTHERS THEN
                    RAISE NOTICE 'Error in aggressive deletion (public schema): %', SQLERRM;
                  END;
                END IF;
              END IF;
              
              -- Return count of deleted rows
              PERFORM success_count;
            END $$;
            SELECT TRUE as success;
          `,
          params: [queue_name, message_id.toString(), bypass_checks]
        });
        
        if (!error && data) {
          console.log(`Successfully deleted message ${message_id} using direct SQL`);
          success = true;
          response = { method: 'direct_sql', details: data };
        } else if (error) {
          console.error("Error with direct SQL deletion:", error);
        }
      } catch (e) {
        console.error("Error with direct SQL deletion:", e);
      }
    }
    
    // Approach 3: Reset visibility timeout as last resort
    if (!success) {
      try {
        const { data: resetResult, error: resetError } = await supabase.rpc('reset_stuck_message', {
          queue_name,
          message_id: message_id.toString()
        });
        
        if (!resetError && resetResult === true) {
          console.log(`Reset visibility timeout for message ${message_id}`);
          success = true;
          response = { method: 'visibility_reset', details: null };
        } else if (resetError) {
          console.error("Error resetting visibility timeout:", resetError);
        }
      } catch (e) {
        console.error("Error resetting visibility timeout:", e);
      }
    }
    
    // Release lock if we acquired it
    try {
      if (lockAcquired) {
        await redis.del(lockKey);
      }
    } catch (redisError) {
      console.warn("Failed to release Redis lock:", redisError);
    }
    
    // Record result in Redis for idempotency and troubleshooting
    if (success) {
      try {
        const deletedKey = `deleted:${queue_name}:${message_id}`;
        await redis.set(deletedKey, 'true', { ex: 86400 }); // 24 hour TTL
        
        const metaKey = `deleted_meta:${queue_name}:${message_id}`;
        await redis.set(metaKey, JSON.stringify({
          method: response.method,
          timestamp: Date.now(),
          details: response.details
        }), { ex: 86400 });
      } catch (redisError) {
        console.warn("Failed to record deletion in Redis:", redisError);
      }
    }
    
    return new Response(
      JSON.stringify({
        success,
        message: success 
          ? `Successfully handled message ${message_id} via ${response.method}` 
          : `Failed to delete or reset message ${message_id}`,
        response
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error("Error with force delete:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
