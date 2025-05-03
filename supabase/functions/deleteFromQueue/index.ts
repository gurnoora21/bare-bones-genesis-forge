
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
    
    console.log(`Deleting message ${message_id} from queue ${queue_name}`);
    
    // Check if the message_id is numeric
    const isNumeric = !isNaN(Number(message_id)) && message_id.toString().trim() !== '';
    
    // Initialize tracking variables
    let success = false;
    let error = null;
    
    // For numeric message IDs, try a different approach since PGMQ uses UUID
    if (isNumeric) {
      try {
        console.log(`Message ID is numeric: ${message_id}, using direct SQL approach`);
        const tableName = `pgmq_${queue_name}`;
        
        // Try different table format possibilities (schema.table combinations)
        const tablePossibilities = [
          tableName,
          `public.${tableName}`,
          queue_name,
          `public.${queue_name}`
        ];
        
        for (const table of tablePossibilities) {
          try {
            console.log(`Trying to delete from table ${table} where msg_id = ${message_id}`);
            const { data, error: deleteError } = await supabase.rpc(
              'raw_sql_query',
              { 
                sql_query: `DELETE FROM ${table} WHERE msg_id = ${message_id} RETURNING TRUE AS deleted`
              }
            );
            
            if (!deleteError && data && data.length > 0) {
              console.log(`Successfully deleted message ${message_id} from ${table}`);
              success = true;
              break;
            }
          } catch (tableError) {
            console.log(`Table ${table} delete attempt failed:`, tableError.message);
            // Continue to next table
          }
        }
        
        if (!success) {
          // Try more combinations with different column names
          for (const table of tablePossibilities) {
            try {
              console.log(`Trying to delete from table ${table} where id = ${message_id}`);
              const { data, error: deleteError } = await supabase.rpc(
                'raw_sql_query',
                { 
                  sql_query: `DELETE FROM ${table} WHERE id = ${message_id} RETURNING TRUE AS deleted`
                }
              );
              
              if (!deleteError && data && data.length > 0) {
                console.log(`Successfully deleted message ${message_id} from ${table} using id column`);
                success = true;
                break;
              }
            } catch (tableError) {
              console.log(`Table ${table} delete attempt (id column) failed:`, tableError.message);
            }
          }
        }
      } catch (directError) {
        console.error(`Direct numeric ID delete approach failed for ${message_id}:`, directError);
        error = directError;
      }
    } else {
      // Standard approach for UUID message IDs
      // Try direct pgmq.delete approach first
      try {
        const { data, error: deleteError } = await supabase.rpc('pgmq.delete', {
          queue_name,
          msg_id: message_id
        });
        
        if (!deleteError) {
          success = true;
          console.log(`Successfully deleted message ${message_id} using pgmq.delete directly`);
        } else {
          console.error(`Direct pgmq.delete failed for ${message_id}:`, deleteError);
          error = deleteError;
          // Will try fallback methods
        }
      } catch (pgmqError) {
        console.error(`pgmq.delete attempt failed for ${message_id}:`, pgmqError);
        error = pgmqError;
      }
      
      // If the direct approach failed, try using the pg_delete_message function
      if (!success) {
        try {
          console.log(`Trying pg_delete_message for message ${message_id}`);
          const { data, error: rpcError } = await supabase.rpc('pg_delete_message', {
            queue_name,
            message_id
          });
          
          if (!rpcError && data === true) {
            success = true;
            console.log(`Successfully deleted message ${message_id} using pg_delete_message`);
          } else {
            error = rpcError;
            console.error(`pg_delete_message failed for ${message_id}:`, rpcError || "Function returned false");
          }
        } catch (rpcError) {
          console.error(`Error calling pg_delete_message for ${message_id}:`, rpcError);
          error = error || rpcError;
        }
      }
      
      // Last resort: try a direct SQL delete using any potential table naming convention
      if (!success) {
        try {
          const possibleTableNames = [
            `pgmq_${queue_name}`, // Standard format
            `pgmq.${queue_name}`, // Schema qualified
            `public.pgmq_${queue_name}`, // Public schema qualified
            queue_name, // Just the queue name
            `public.${queue_name}` // Public schema with queue name
          ];
          
          for (const tableName of possibleTableNames) {
            console.log(`Trying direct delete from table ${tableName} for message ${message_id}`);
            
            try {
              // Safe SQL using parameterized queries
              const { error: deleteError } = await supabase
                .from(tableName)
                .delete()
                .eq('id', message_id);
              
              if (!deleteError) {
                success = true;
                console.log(`Successfully deleted message ${message_id} from ${tableName}`);
                break; // Exit the loop if successful
              }
            } catch (tableError) {
              console.log(`Table ${tableName} delete attempt failed:`, tableError.message);
              // Continue to next table name
            }
          }
        } catch (fallbackError) {
          console.error(`All fallback delete attempts failed for message ${message_id}:`, fallbackError);
          error = error || fallbackError;
        }
      }
    }
    
    // Verify if the message is actually gone (check in all possible tables)
    if (!success) {
      try {
        console.log(`Checking if message ${message_id} was deleted despite errors`);
        
        // Add a helper function to check if message exists in a table
        const checkMessageExistsInTable = async (tableName, columnName) => {
          try {
            const { data, error } = await supabase.rpc(
              'raw_sql_query',
              { 
                sql_query: `SELECT EXISTS(SELECT 1 FROM ${tableName} WHERE ${columnName} = ${isNumeric ? message_id : `'${message_id}'`}) AS exists`
              }
            );
            
            if (!error && data && data.length > 0) {
              return data[0].exists;
            }
            return true; // Assume it exists if there's an error
          } catch (e) {
            console.log(`Error checking in ${tableName}:`, e.message);
            return true; // Assume it exists if there's an error
          }
        };
        
        // Check all possible table combinations
        const tablesToCheck = [
          { table: `pgmq_${queue_name}`, column: 'msg_id' },
          { table: `pgmq_${queue_name}`, column: 'id' },
          { table: `pgmq.${queue_name}`, column: 'msg_id' },
          { table: `pgmq.${queue_name}`, column: 'id' },
          { table: queue_name, column: 'msg_id' },
          { table: queue_name, column: 'id' }
        ];
        
        let exists = false;
        for (const { table, column } of tablesToCheck) {
          exists = await checkMessageExistsInTable(table, column);
          if (exists) {
            break;
          }
        }
        
        if (!exists) {
          success = true;
          console.log(`Message ${message_id} verified as not found in any tables, considering it deleted`);
        }
      } catch (verifyError) {
        console.error(`Error verifying message deletion:`, verifyError);
      }
    }
    
    if (success) {
      return new Response(
        JSON.stringify({ success: true }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } else {
      throw new Error(error?.message || `Failed to delete message ${message_id} from queue ${queue_name}`);
    }
    
  } catch (error) {
    console.error("Error deleting message from queue:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
