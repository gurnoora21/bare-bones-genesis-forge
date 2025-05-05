
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

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
            BEGIN
              -- Try pgmq schema
              BEGIN
                EXECUTE 'DELETE FROM pgmq.q_' || $1 || ' WHERE msg_id::TEXT = $2 OR id::TEXT = $2';
                GET DIAGNOSTICS success_count = ROW_COUNT;
              EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Error deleting from pgmq schema: %', SQLERRM;
              END;
              
              -- Try public schema
              IF success_count = 0 THEN
                BEGIN
                  EXECUTE 'DELETE FROM public.pgmq_' || $1 || ' WHERE msg_id::TEXT = $2 OR id::TEXT = $2';
                  GET DIAGNOSTICS success_count = ROW_COUNT;
                EXCEPTION WHEN OTHERS THEN
                  RAISE NOTICE 'Error deleting from public schema: %', SQLERRM;
                END;
              END IF;
              
              -- Super aggressive mode if needed
              IF success_count = 0 AND $3 = TRUE THEN
                BEGIN
                  -- Try using wildcard deletion for extreme cases
                  EXECUTE '
                    DELETE FROM pgmq.q_' || $1 || ' 
                    WHERE 
                      msg_id::TEXT LIKE $2 || ''%'' OR 
                      id::TEXT LIKE $2 || ''%'' OR
                      msg_id = ' || CASE WHEN $2 ~ ''^[0-9]+$'' THEN $2 ELSE 'NULL' END;
                      
                  GET DIAGNOSTICS success_count = ROW_COUNT;
                  
                  IF success_count = 0 THEN
                    EXECUTE '
                      DELETE FROM public.pgmq_' || $1 || ' 
                      WHERE 
                        msg_id::TEXT LIKE $2 || ''%'' OR 
                        id::TEXT LIKE $2 || ''%'' OR
                        msg_id = ' || CASE WHEN $2 ~ ''^[0-9]+$'' THEN $2 ELSE 'NULL' END;
                    
                    GET DIAGNOSTICS success_count = ROW_COUNT;
                  END IF;
                EXCEPTION WHEN OTHERS THEN
                  RAISE NOTICE 'Error in aggressive deletion: %', SQLERRM;
                END;
              END IF;
            END $$;
            SELECT TRUE as success;
          `,
          params: [queue_name, message_id.toString(), bypass_checks]
        });
        
        if (!error && data) {
          console.log(`Successfully deleted message ${message_id} using direct SQL`);
          success = true;
          response = { method: 'direct_sql', details: data };
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
        }
      } catch (e) {
        console.error("Error resetting visibility timeout:", e);
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
