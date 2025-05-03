
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
    
    // If cross-schema delete failed, try our enhanced delete method
    const { data: enhancedData, error: enhancedError } = await supabase.rpc(
      'enhanced_delete_message',
      { 
        p_queue_name: queue_name,
        p_message_id: message_id.toString(),
        p_is_numeric: isNumeric
      }
    );
    
    if (enhancedError) {
      console.error("Enhanced delete error:", enhancedError);
    } else if (enhancedData === true) {
      console.log(`SUCCESS: Message ${message_id} has been deleted using enhanced_delete_message`);
      
      await supabase.from('worker_issues').insert({
        worker_name: "forceDeleteMessage",
        issue_type: "success",
        message: `Successfully deleted message ${message_id} from queue ${queue_name} using enhanced_delete_message`,
        details: { queue_name, message_id, bypass_checks },
        resolved: true
      });
      
      return new Response(
        JSON.stringify({ 
          success: true, 
          message: `Successfully deleted message ${message_id} from queue ${queue_name}` 
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
    
    // If all schema-aware operations failed, try direct brutal table access as last resort
    if (bypass_checks) {
      // Try direct deletion on known patterns for PGMQ tables
      const schemas = ['pgmq', 'public'];
      const tablePatterns = [`q_${queue_name}`, `pgmq_${queue_name}`];
      
      for (const schema of schemas) {
        for (const pattern of tablePatterns) {
          const tableName = `${schema}.${pattern}`;
          
          try {
            const { data: directResult } = await supabase.rpc(
              'raw_sql_query',
              {
                sql_query: `
                  BEGIN;
                  DELETE FROM ${tableName} WHERE msg_id::TEXT = '${message_id}' OR id::TEXT = '${message_id}';
                  COMMIT;
                  SELECT 'Attempted direct deletion on ${tableName}' as result;
                `,
                params: []
              }
            );
            
            console.log(`Direct deletion result on ${tableName}:`, directResult);
          } catch (directError) {
            console.error(`Direct deletion error on ${tableName}:`, directError);
          }
        }
      }
      
      // Record the attempt
      await supabase.from('worker_issues').insert({
        worker_name: "forceDeleteMessage",
        issue_type: "bypass_attempt",
        message: `Attempted direct table deletion for message ${message_id} in queue ${queue_name}`,
        details: { queue_name, message_id, bypass_checks },
        resolved: false
      });
    }
    
    // Get diagnostic info about queue tables to help troubleshooting
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
    
    throw new Error(`Failed to manage message ${message_id} in queue ${queue_name} after multiple attempts`);
  } catch (error) {
    console.error("Error in forceDeleteMessage:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
