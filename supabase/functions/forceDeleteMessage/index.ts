
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
      return new Response(
        JSON.stringify({ error: "queue_name and message_id are required" }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    console.log(`FORCE DELETE: Attempting to delete message ${message_id} from queue ${queue_name}`);
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    // Try direct raw SQL deletion first on the correct table pattern
    try {
      const { data: rawSqlResult } = await supabase.rpc(
        'raw_sql_query',
        {
          sql_query: `
            DO $$
            DECLARE
              success BOOLEAN := FALSE;
            BEGIN
              -- Try deletion from pgmq.q_ pattern first
              DELETE FROM pgmq.q_${queue_name} 
              WHERE msg_id::TEXT = '${message_id}' OR id::TEXT = '${message_id}';
              
              GET DIAGNOSTICS success = ROW_COUNT;
              
              IF success THEN
                RAISE NOTICE 'Successfully deleted message ${message_id} from pgmq.q_${queue_name}';
              ELSE
                -- Try fallback to public.pgmq_ pattern
                DELETE FROM public.pgmq_${queue_name} 
                WHERE msg_id::TEXT = '${message_id}' OR id::TEXT = '${message_id}';
                
                GET DIAGNOSTICS success = ROW_COUNT;
                
                IF success THEN
                  RAISE NOTICE 'Successfully deleted message ${message_id} from public.pgmq_${queue_name}';
                ELSE
                  RAISE NOTICE 'Message ${message_id} not found in any queue tables';
                END IF;
              END IF;
            END $$;
            SELECT 'Direct SQL deletion attempted' as result;
          `
        }
      );
      
      console.log("Raw SQL deletion result:", rawSqlResult);
    } catch (rawSqlError) {
      console.error("Raw SQL deletion error:", rawSqlError);
    }

    // Use our new cross-schema queue operation function for reliable deletion
    const { data: crossSchemaResult, error: crossSchemaError } = await supabase.rpc(
      'cross_schema_queue_op',
      { 
        p_operation: 'delete',
        p_queue_name: queue_name,
        p_message_id: message_id
      }
    );
    
    console.log("Cross-schema deletion result:", crossSchemaResult);
    
    if (crossSchemaError) {
      console.error("Cross-schema operation error:", crossSchemaError);
    }
    
    if (crossSchemaResult && crossSchemaResult.success === true) {
      console.log(`SUCCESS: Message ${message_id} has been force-deleted from ${crossSchemaResult.table}`);
      
      // Record the success in worker_issues for audit
      await supabase.from('worker_issues').insert({
        worker_name: "forceDeleteMessage",
        issue_type: "success",
        message: `Successfully force-deleted message ${message_id} from queue ${queue_name} (table: ${crossSchemaResult.table})`,
        details: { queue_name, message_id, bypass_checks, table: crossSchemaResult.table },
        resolved: true
      });
      
      return new Response(
        JSON.stringify({ 
          success: true, 
          message: `Successfully force-deleted message ${message_id} from queue ${queue_name}`,
          details: crossSchemaResult
        }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    // If direct deletion failed, try cross-schema reset
    const { data: resetResult, error: resetError } = await supabase.rpc(
      'cross_schema_queue_op',
      { 
        p_operation: 'reset',
        p_queue_name: queue_name,
        p_message_id: message_id
      }
    );
    
    if (resetError) {
      console.error("Cross-schema reset error:", resetError);
    }
    
    if (resetResult && resetResult.success === true) {
      console.log(`Message visibility timeout reset for ${message_id} in ${resetResult.table}`);
      
      await supabase.from('worker_issues').insert({
        worker_name: "forceDeleteMessage",
        issue_type: "partial_success",
        message: `Reset visibility timeout for message ${message_id} in queue ${queue_name} (table: ${resetResult.table})`,
        details: { queue_name, message_id, bypass_checks, table: resetResult.table },
        resolved: true
      });
      
      return new Response(
        JSON.stringify({ 
          success: false, 
          reset: true, 
          message: `Could not delete message ${message_id}, but successfully reset visibility timeout`,
          details: resetResult
        }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    // Create a direct_pgmq_delete function if it doesn't exist yet
    try {
      const { data: createFunctionResult } = await supabase.rpc(
        'raw_sql_query',
        {
          sql_query: `
            CREATE OR REPLACE FUNCTION public.direct_pgmq_delete(
              p_queue_name TEXT,
              p_message_id TEXT
            ) RETURNS BOOLEAN AS $$
            BEGIN
              -- Call pgmq.delete directly
              RETURN pgmq.delete(p_queue_name, p_message_id::UUID);
            EXCEPTION WHEN OTHERS THEN
              RAISE NOTICE 'direct_pgmq_delete error: %', SQLERRM;
              RETURN FALSE;
            END;
            $$ LANGUAGE plpgsql SECURITY DEFINER;
            
            SELECT 'direct_pgmq_delete function created' as result;
          `
        }
      );
      
      console.log("Created direct_pgmq_delete function:", createFunctionResult);
    } catch (createFunctionError) {
      console.error("Error creating direct_pgmq_delete function:", createFunctionError);
    }
    
    // Try direct PGMQ deletion
    try {
      const { data: pgmqResult } = await supabase.rpc(
        'direct_pgmq_delete',
        { 
          p_queue_name: queue_name,
          p_message_id: message_id
        }
      );
      
      if (pgmqResult === true) {
        console.log(`Message ${message_id} successfully deleted using direct PGMQ call`);
        
        return new Response(
          JSON.stringify({ 
            success: true, 
            message: `Successfully deleted message ${message_id} from queue ${queue_name} using direct PGMQ call`
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    } catch (pgmqError) {
      console.error("Direct PGMQ deletion error:", pgmqError);
    }
    
    // If all else fails, try directly running SQL with aggressive patterns for the correct table
    if (bypass_checks) {
      try {
        const { data: aggressiveResult } = await supabase.rpc(
          'raw_sql_query',
          {
            sql_query: `
              DO $$
              DECLARE
                success BOOLEAN := FALSE;
              BEGIN
                -- Aggressive fix on the correct table pattern
                BEGIN
                  -- Try numeric ID with different operators
                  EXECUTE 'DELETE FROM pgmq.q_${queue_name} WHERE 
                    msg_id = ${message_id} OR
                    id = ${message_id} OR
                    msg_id::TEXT = ''${message_id}'' OR
                    id::TEXT = ''${message_id}''';
                    
                  GET DIAGNOSTICS success = ROW_COUNT;
                  
                  IF success THEN
                    RAISE NOTICE 'Aggressive numeric deletion succeeded';
                  END IF;
                EXCEPTION WHEN OTHERS THEN
                  RAISE NOTICE 'Aggressive numeric deletion failed: %', SQLERRM;
                END;
                
                -- If still no success, try UUID casting
                IF NOT success THEN
                  BEGIN
                    EXECUTE 'DELETE FROM pgmq.q_${queue_name} WHERE 
                      id = ''${message_id}''::UUID';
                      
                    GET DIAGNOSTICS success = ROW_COUNT;
                    
                    IF success THEN
                      RAISE NOTICE 'UUID deletion succeeded';
                    END IF;
                  EXCEPTION WHEN OTHERS THEN
                    RAISE NOTICE 'UUID deletion failed: %', SQLERRM;
                  END;
                END IF;
                
                -- Last resort - visibility timeout reset
                IF NOT success THEN
                  BEGIN
                    EXECUTE 'UPDATE pgmq.q_${queue_name} SET vt = NULL WHERE 
                      msg_id = ${message_id} OR
                      id = ${message_id} OR
                      msg_id::TEXT = ''${message_id}'' OR
                      id::TEXT = ''${message_id}''';
                      
                    GET DIAGNOSTICS success = ROW_COUNT;
                    
                    IF success THEN
                      RAISE NOTICE 'Visibility timeout reset succeeded';
                    END IF;
                  EXCEPTION WHEN OTHERS THEN
                    RAISE NOTICE 'Visibility timeout reset failed: %', SQLERRM;
                  END;
                END IF;
              END $$;
              SELECT 'Aggressive fix attempted' as result;
            `
          }
        );
        
        console.log("Aggressive fix result:", aggressiveResult);
        
        // Check if message still exists
        const { data: checkResult } = await supabase.rpc(
          'raw_sql_query',
          {
            sql_query: `
              SELECT EXISTS (
                SELECT 1 FROM pgmq.q_${queue_name} 
                WHERE msg_id::TEXT = '${message_id}' OR id::TEXT = '${message_id}'
              ) as message_exists;
            `
          }
        );
        
        if (checkResult && checkResult.message_exists === false) {
          return new Response(
            JSON.stringify({
              success: true,
              message: `Message ${message_id} successfully removed from queue ${queue_name} with aggressive deletion`
            }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
      } catch (aggressiveError) {
        console.error("Aggressive fix error:", aggressiveError);
      }
    }
    
    // Get diagnostic info about queue tables
    const { data: diagData } = await supabase.rpc(
      'diagnose_queue_tables',
      { queue_name }
    );
    
    // Record failure with diagnostic info
    await supabase.from('worker_issues').insert({
      worker_name: "forceDeleteMessage",
      issue_type: "failure",
      message: `Failed to force-delete or reset message ${message_id} from queue ${queue_name}`,
      details: { 
        queue_name, 
        message_id, 
        bypass_checks,
        diagnostic: diagData
      },
      resolved: false
    });
    
    return new Response(
      JSON.stringify({ 
        error: `Failed to manage message ${message_id} in queue ${queue_name} after multiple attempts`,
        diagnostic: diagData
      }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error("Error in forceDeleteMessage:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
