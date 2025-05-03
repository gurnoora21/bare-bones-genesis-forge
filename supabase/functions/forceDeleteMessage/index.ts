
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
    
    // Use the cross_schema_queue_op function for reliable operation across schemas
    const { data: deleteResult, error: deleteError } = await supabase.rpc(
      'cross_schema_queue_op',
      { 
        p_queue_name: queue_name, 
        p_message_id: message_id.toString(),
        p_operation: 'delete'
      }
    );
    
    if (deleteError) {
      console.error("Error with cross schema operation:", deleteError);
    }
    
    // Direct SQL approach as backup
    const { data: directResult, error: directError } = await supabase.rpc('raw_sql_query', {
      sql_query: `
        DO $$
        DECLARE
          pgmq_exists BOOLEAN;
          public_exists BOOLEAN;
          success_count INT := 0;
        BEGIN
          -- Check for tables in both schemas
          SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'pgmq' AND table_name = 'q_${queue_name}'
          ) INTO pgmq_exists;
          
          SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'pgmq_${queue_name}'
          ) INTO public_exists;
          
          -- Try in pgmq schema
          IF pgmq_exists THEN
            BEGIN
              EXECUTE 'DELETE FROM pgmq.q_${queue_name} WHERE msg_id::TEXT = $1 OR id::TEXT = $1'
                USING '${message_id}';
              GET DIAGNOSTICS success_count = ROW_COUNT;
              
              IF success_count > 0 THEN
                RAISE NOTICE 'Successfully deleted message from pgmq schema';
              END IF;
            EXCEPTION WHEN OTHERS THEN
              RAISE NOTICE 'Error with pgmq delete: %', SQLERRM;
            END;
          END IF;
          
          -- Try in public schema if not already successful
          IF public_exists AND success_count = 0 THEN
            BEGIN
              EXECUTE 'DELETE FROM public.pgmq_${queue_name} WHERE msg_id::TEXT = $1 OR id::TEXT = $1'
                USING '${message_id}';
              GET DIAGNOSTICS success_count = ROW_COUNT;
              
              IF success_count > 0 THEN
                RAISE NOTICE 'Successfully deleted message from public schema';
              END IF;
            EXCEPTION WHEN OTHERS THEN
              RAISE NOTICE 'Error with public delete: %', SQLERRM;
            END;
          END IF;
          
          -- Super aggressive mode for stubborn messages
          IF success_count = 0 AND ${bypass_checks ? 'TRUE' : 'FALSE'} THEN
            BEGIN
              -- Try to delete by any means necessary in both schemas
              IF pgmq_exists THEN
                EXECUTE 'DELETE FROM pgmq.q_${queue_name} WHERE 
                  msg_id = ${message_id} OR 
                  id = ${message_id} OR 
                  msg_id::TEXT = ''${message_id}'' OR 
                  id::TEXT = ''${message_id}''';
              END IF;
              
              IF public_exists THEN
                EXECUTE 'DELETE FROM public.pgmq_${queue_name} WHERE 
                  msg_id = ${message_id} OR 
                  id = ${message_id} OR 
                  msg_id::TEXT = ''${message_id}'' OR 
                  id::TEXT = ''${message_id}''';
              END IF;
            EXCEPTION WHEN OTHERS THEN
              RAISE NOTICE 'Error with aggressive deletion: %', SQLERRM;
            END;
          END IF;
        END $$;
        SELECT TRUE AS success;
      `
    });
    
    if (directError) {
      console.error("Error with direct SQL deletion:", directError);
    }
    
    // Try visibility timeout reset if deletion didn't work
    const { data: resetResult, error: resetError } = await supabase.rpc(
      'cross_schema_queue_op',
      { 
        p_queue_name: queue_name, 
        p_message_id: message_id.toString(),
        p_operation: 'reset'
      }
    );
    
    if (resetError) {
      console.error("Error with visibility reset:", resetError);
    }
    
    // Final check to see if the message still exists
    let messageExists = true;
    try {
      const { data: checkResult, error: checkError } = await supabase.rpc('raw_sql_query', {
        sql_query: `
          SELECT EXISTS (
            SELECT 1 
            FROM pgmq.q_${queue_name}
            WHERE msg_id::TEXT = '${message_id}' OR id::TEXT = '${message_id}'
          ) OR EXISTS (
            SELECT 1 
            FROM public.pgmq_${queue_name}
            WHERE msg_id::TEXT = '${message_id}' OR id::TEXT = '${message_id}'
          ) AS still_exists;
        `
      });
      
      if (!checkError && checkResult) {
        messageExists = checkResult.still_exists;
      }
    } catch (checkError) {
      console.error("Error checking if message still exists:", checkError);
    }
    
    return new Response(
      JSON.stringify({
        success: deleteResult || directResult?.success,
        message: `Attempted deletion of message ${message_id} from queue ${queue_name}`,
        reset: resetResult,
        message_still_exists: messageExists,
        direct_result: directResult
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
