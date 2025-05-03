
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
    const requestBody = await req.json();
    const { 
      queue_name,
      operation, // One of: 'purge', 'reset_visibility', 'list_stuck', 'delete_by_id', 'fix_stuck'
      message_id,
      max_age_minutes,
      visibility_timeout
    } = requestBody;
    
    if (!queue_name || !operation) {
      throw new Error("queue_name and operation are required");
    }
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    console.log(`Queue operation: ${operation} on queue ${queue_name}`);
    
    // Get queue table name
    let queueTable = '';
    try {
      const { data: tableData, error: tableError } = await supabase.rpc(
        'raw_sql_query',
        { 
          sql_query: `SELECT pgmq.get_queue_table_name($1) as table_name`,
          params: [queue_name]
        }
      );
      
      if (!tableError && tableData && tableData.length > 0 && tableData[0].table_name) {
        queueTable = tableData[0].table_name;
      } else {
        queueTable = `pgmq_${queue_name}`;
      }
    } catch (tableError) {
      queueTable = `pgmq_${queue_name}`;
    }
    
    let result;
    
    switch (operation) {
      case 'purge':
        // Purge all messages from queue
        const { data: purgeData, error: purgeError } = await supabase.rpc(
          'raw_sql_query',
          {
            sql_query: `
              DO $$
              BEGIN
                EXECUTE format('TRUNCATE TABLE %I', $1);
              END $$;
              SELECT 'Queue purged' as result;
            `,
            params: [queueTable]
          }
        );
        
        if (purgeError) throw purgeError;
        result = { operation: 'purge', status: 'success', data: purgeData };
        break;
        
      case 'reset_visibility': 
        // Reset visibility timeout for all messages or a specific message
        let resetQuery = message_id ?
          `UPDATE ${queueTable} SET vt = NULL WHERE id::TEXT = $1 OR msg_id::TEXT = $1` :
          `UPDATE ${queueTable} SET vt = NULL WHERE vt IS NOT NULL`;
        
        const resetParams = message_id ? [message_id.toString()] : [];
        
        const { data: resetData, error: resetError } = await supabase.rpc(
          'raw_sql_query',
          {
            sql_query: `
              WITH reset_result AS (
                ${resetQuery}
                RETURNING *
              )
              SELECT COUNT(*) as reset_count FROM reset_result
            `,
            params: resetParams
          }
        );
        
        if (resetError) throw resetError;
        result = { operation: 'reset_visibility', status: 'success', data: resetData };
        break;
        
      case 'list_stuck':
        // List messages that appear to be stuck (visibility timeout expired)
        const maxAgeMinutes = max_age_minutes || 10; // Default to 10 minutes
        
        const { data: stuckData, error: stuckError } = await supabase.rpc(
          'raw_sql_query',
          {
            sql_query: `
              SELECT 
                id, 
                msg_id, 
                message, 
                vt, 
                read_ct,
                created_at,
                EXTRACT(EPOCH FROM (NOW() - vt))/60 AS minutes_since_locked
              FROM ${queueTable}
              WHERE 
                vt IS NOT NULL 
                AND vt < NOW() - INTERVAL '${maxAgeMinutes} minutes'
              ORDER BY vt ASC
            `,
            params: []
          }
        );
        
        if (stuckError) throw stuckError;
        result = { operation: 'list_stuck', status: 'success', data: stuckData };
        break;
        
      case 'delete_by_id':
        // Delete a specific message by ID
        if (!message_id) {
          throw new Error("message_id is required for delete_by_id operation");
        }
        
        const { data: deleteData, error: deleteError } = await supabase.rpc(
          'raw_sql_query',
          {
            sql_query: `
              WITH deleted AS (
                DELETE FROM ${queueTable}
                WHERE id::TEXT = $1 OR msg_id::TEXT = $1
                RETURNING *
              )
              SELECT COUNT(*) as deleted_count FROM deleted
            `,
            params: [message_id.toString()]
          }
        );
        
        if (deleteError) throw deleteError;
        result = { operation: 'delete_by_id', status: 'success', data: deleteData };
        break;
        
      case 'fix_stuck':
        // Automatically fix stuck messages (reset visibility timeout for stuck messages)
        const fixTimeout = visibility_timeout || 60; // Default to 60 seconds
        const fixMaxAge = max_age_minutes || 10; // Default to 10 minutes
        
        const { data: fixData, error: fixError } = await supabase.rpc(
          'raw_sql_query',
          {
            sql_query: `
              WITH fixed AS (
                UPDATE ${queueTable}
                SET vt = NULL
                WHERE 
                  vt IS NOT NULL 
                  AND vt < NOW() - INTERVAL '${fixMaxAge} minutes'
                RETURNING *
              )
              SELECT COUNT(*) as fixed_count FROM fixed
            `,
            params: []
          }
        );
        
        if (fixError) throw fixError;
        result = { operation: 'fix_stuck', status: 'success', data: fixData };
        break;
        
      default:
        throw new Error(`Unknown operation: ${operation}`);
    }
    
    return new Response(
      JSON.stringify(result),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Error managing queue:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
