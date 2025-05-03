
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

    // Determine if ID is numeric
    const isNumeric = !isNaN(Number(message_id)) && message_id.toString().trim() !== '';
    
    // Get the actual queue table name
    const { data: tableData, error: tableError } = await supabase.rpc(
      'raw_sql_query',
      { 
        sql_query: `SELECT pgmq.get_queue_table_name($1) as table_name`,
        params: [queue_name]
      }
    );
    
    if (tableError) {
      console.error("Error getting queue table name:", tableError);
    }
    
    const queueTable = tableData?.[0]?.table_name || `pgmq_${queue_name}`;
    console.log(`Using queue table: ${queueTable}`);
    
    // Transaction with multiple deletion attempts
    const { data: deleteResult, error: deleteError } = await supabase.rpc(
      'executeRawSql',
      {
        transaction: true,
        statements: [
          // 1. Direct delete by ID (both numeric and text forms)
          {
            sql: `DELETE FROM ${queueTable} WHERE ${isNumeric ? 
              `msg_id = ${Number(message_id)} OR id = ${Number(message_id)}` : 
              `id = '${message_id}'::UUID`}`
          },
          // 2. Delete by text comparison
          {
            sql: `DELETE FROM ${queueTable} WHERE msg_id::TEXT = '${message_id}' OR id::TEXT = '${message_id}'`
          },
          // 3. If all else fails and bypass_checks is true, use brutal direct deletion
          ...(bypass_checks ? [{
            sql: `DELETE FROM ${queueTable} WHERE (msg_id IS NOT NULL AND msg_id::TEXT LIKE '${message_id}%') 
                  OR (id IS NOT NULL AND id::TEXT LIKE '${message_id}%')`
          }] : []),
          // 4. Check if message still exists
          {
            sql: `SELECT NOT EXISTS(
                    SELECT 1 FROM ${queueTable}
                    WHERE msg_id::TEXT = '${message_id}' 
                    OR id::TEXT = '${message_id}'
                    ${bypass_checks ? `OR (msg_id IS NOT NULL AND msg_id::TEXT LIKE '${message_id}%') 
                    OR (id IS NOT NULL AND id::TEXT LIKE '${message_id}%')` : ''}
                  ) AS deleted`
          }
        ]
      }
    );
    
    if (deleteError) {
      console.error("Transaction error:", deleteError);
      throw deleteError;
    }
    
    const isDeleted = deleteResult?.data?.[3]?.[0]?.deleted === true;
    
    if (isDeleted) {
      console.log(`SUCCESS: Message ${message_id} has been force-deleted from ${queue_name}`);
      
      // Record the success in worker_issues for audit
      await supabase.from('worker_issues').insert({
        worker_name: "forceDeleteMessage",
        issue_type: "success",
        message: `Successfully force-deleted message ${message_id} from queue ${queue_name}`,
        details: { queue_name, message_id, bypass_checks },
        resolved: true
      });
      
      return new Response(
        JSON.stringify({ 
          success: true, 
          message: `Successfully force-deleted message ${message_id} from queue ${queue_name}` 
        }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // If direct deletion failed, try emergency visibility timeout reset
    console.log("Direct deletion failed, attempting visibility timeout reset...");
    
    const { data: resetResult, error: resetError } = await supabase.rpc(
      'raw_sql_query',
      {
        sql_query: `
          UPDATE ${queueTable} SET vt = NULL 
          WHERE msg_id::TEXT = '${message_id}' OR id::TEXT = '${message_id}'
          RETURNING TRUE AS reset
        `,
        params: []
      }
    );
    
    if (resetError) {
      console.error("Reset error:", resetError);
      throw resetError;
    }
    
    const isReset = resetResult?.[0]?.reset === true;
    
    if (isReset) {
      console.log(`Message visibility timeout reset for ${message_id}`);
      
      await supabase.from('worker_issues').insert({
        worker_name: "forceDeleteMessage",
        issue_type: "partial_success",
        message: `Reset visibility timeout for message ${message_id} in queue ${queue_name}`,
        details: { queue_name, message_id, bypass_checks },
        resolved: true
      });
      
      return new Response(
        JSON.stringify({ 
          success: false, 
          reset: true, 
          message: `Could not delete message ${message_id}, but successfully reset visibility timeout` 
        }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    // If both deletion and reset failed, record the issue
    await supabase.from('worker_issues').insert({
      worker_name: "forceDeleteMessage",
      issue_type: "failure",
      message: `Failed to force-delete or reset message ${message_id} from queue ${queue_name}`,
      details: { queue_name, message_id, bypass_checks },
      resolved: false
    });
    
    throw new Error(`Failed to force-delete or reset message ${message_id} from queue ${queue_name}`);
  } catch (error) {
    console.error("Error in forceDeleteMessage:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
