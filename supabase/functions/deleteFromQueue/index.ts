
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
    const { queue_name, message_id } = requestBody;
    
    if (!queue_name || !message_id) {
      console.error("Missing required parameters:", { queue_name, message_id });
      throw new Error("queue_name and message_id are required");
    }
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    console.log(`Attempting to delete message ${message_id} from queue ${queue_name}`);
    
    // Determine message ID type and format
    const isNumeric = !isNaN(Number(message_id)) && message_id.toString().trim() !== '';
    let messageIdFormatted = isNumeric ? Number(message_id) : message_id;
    
    // For numeric IDs, especially in artist_discovery queue
    if (isNumeric) {
      try {
        // Use enhanced_delete_message RPC for numeric IDs
        const { data: enhancedData, error: enhancedError } = await supabase.rpc(
          'enhanced_delete_message',
          { 
            p_queue_name: queue_name,
            p_message_id: message_id.toString(),
            p_is_numeric: true
          }
        );
        
        if (!enhancedError && enhancedData === true) {
          return new Response(
            JSON.stringify({ 
              success: true,
              message: `Successfully deleted numeric message ${message_id} from queue ${queue_name}`
            }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
      } catch (enhancedError) {
        console.error(`Enhanced delete procedure failed:`, enhancedError);
        // Continue to fallback methods
      }
    }
    
    // Initialize variables for tracking attempts
    let success = false;
    let attempts = 0;
    const maxAttempts = 3;
    
    // Standard deletion methods with retries
    while (!success && attempts < maxAttempts) {
      attempts++;
      
      try {
        // Add exponential backoff for retries with jitter
        if (attempts > 1) {
          const delayMs = Math.pow(2, attempts - 1) * 100 + Math.random() * 100;
          await new Promise(resolve => setTimeout(resolve, delayMs));
        }
        
        // METHOD 1: Try pgmq.delete with the appropriate ID format
        try {
          const { data: pgmqData, error: pgmqError } = await supabase.rpc(
            'pgmq.delete',
            {
              queue_name,
              msg_id: messageIdFormatted
            }
          );
          
          if (!pgmqError && pgmqData === true) {
            success = true;
            break;
          }
        } catch (pgmqError) {
          console.error(`pgmq.delete error (attempt ${attempts}):`, pgmqError);
        }
        
        // METHOD 2: Try our enhanced delete function
        if (!success) {
          try {
            const { data: enhancedData, error: enhancedError } = await supabase.rpc(
              'enhanced_delete_message',
              { 
                p_queue_name: queue_name,
                p_message_id: message_id.toString(),
                p_is_numeric: isNumeric
              }
            );
            
            if (!enhancedError && enhancedData === true) {
              success = true;
              break;
            }
          } catch (enhancedError) {
            console.error(`Enhanced delete error (attempt ${attempts}):`, enhancedError);
          }
        }
        
        // METHOD 3: Try with raw SQL direct approach as last resort
        if (!success && attempts === maxAttempts - 1) {
          // Get actual queue table name
          let queueTable = '';
          try {
            const { data: tableData, error: tableError } = await supabase.rpc(
              'raw_sql_query',
              { 
                sql_query: `SELECT pgmq.get_queue_table_name($1) as table_name`,
                params: [queue_name]
              }
            );
            
            if (!tableError && tableData && tableData.length > 0) {
              queueTable = tableData[0].table_name;
            } else {
              queueTable = `pgmq_${queue_name}`;
            }
          } catch (tableError) {
            queueTable = `pgmq_${queue_name}`;
          }
          
          const sqlQuery = isNumeric
            ? `DELETE FROM ${queueTable} WHERE msg_id = ${Number(message_id)} OR id = ${Number(message_id)}`
            : `DELETE FROM ${queueTable} WHERE id = '${message_id}'::UUID`;
            
          try {
            const { data: rawData, error: rawError } = await supabase.rpc(
              'raw_sql_query',
              { sql_query, params: [] }
            );
            
            if (!rawError) {
              // Verify deletion
              const verifyQuery = isNumeric
                ? `SELECT NOT EXISTS(SELECT 1 FROM ${queueTable} WHERE msg_id = ${Number(message_id)} OR id = ${Number(message_id)}) AS deleted`
                : `SELECT NOT EXISTS(SELECT 1 FROM ${queueTable} WHERE id = '${message_id}'::UUID) AS deleted`;
              
              const { data: verifyData, error: verifyError } = await supabase.rpc(
                'raw_sql_query',
                { sql_query: verifyQuery, params: [] }
              );
              
              if (!verifyError && verifyData && verifyData.length > 0 && verifyData[0].deleted === true) {
                success = true;
                break;
              }
            }
          } catch (rawError) {
            console.error(`Raw SQL deletion error (attempt ${attempts}):`, rawError);
          }
        }
      } catch (attemptError) {
        console.error(`General error during deletion attempt ${attempts}:`, attemptError);
      }
    }
    
    // Final verification
    if (!success) {
      try {
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
          
          if (!tableError && tableData && tableData.length > 0) {
            queueTable = tableData[0].table_name;
          } else {
            queueTable = `pgmq_${queue_name}`;
          }
        } catch (tableError) {
          queueTable = `pgmq_${queue_name}`;
        }
        
        const verifyQuery = `
          SELECT NOT EXISTS(
            SELECT 1 FROM ${queueTable}
            WHERE msg_id::TEXT = '${message_id}' OR id::TEXT = '${message_id}'
          ) AS deleted
        `;
        
        const { data: verifyData, error: verifyError } = await supabase.rpc(
          'raw_sql_query',
          { sql_query: verifyQuery, params: [] }
        );
        
        if (!verifyError && verifyData && verifyData.length > 0 && verifyData[0].deleted === true) {
          success = true;
        }
      } catch (verifyError) {
        console.error(`Final verification error:`, verifyError);
      }
    }
    
    if (success) {
      return new Response(
        JSON.stringify({ 
          success: true,
          message: `Successfully deleted message ${message_id} from queue ${queue_name}`
        }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } else {
      // If all deletion methods fail, try to reset visibility timeout
      try {
        const { data: resetData, error: resetError } = await supabase.rpc(
          'emergency_reset_message',
          { 
            p_queue_name: queue_name,
            p_message_id: message_id.toString(),
            p_is_numeric: isNumeric
          }
        );
        
        if (!resetError && resetData === true) {
          return new Response(
            JSON.stringify({ 
              success: false,
              reset: true,
              message: `Could not delete message ${message_id}, but successfully reset visibility timeout`
            }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
      } catch (resetError) {
        console.error(`Emergency reset failed:`, resetError);
      }
      
      throw new Error(`Failed to delete message ${message_id} from queue ${queue_name} after ${maxAttempts} attempts`);
    }
    
  } catch (error) {
    console.error("Error deleting message from queue:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
