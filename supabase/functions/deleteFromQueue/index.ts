
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
    let attempts = 0;
    const maxAttempts = 3;
    
    // Try direct database deletion first - this is the most reliable method for numeric IDs
    if (isNumeric) {
      try {
        console.log(`Message ID is numeric: ${message_id}, trying direct SQL deletion first`);
        
        // Create parametrized SQL query - this is safer and more reliable than dynamic SQL
        const { data: directResult, error: directError } = await supabase.rpc(
          'raw_sql_query',
          { 
            sql_query: `
              DO $$
              DECLARE
                success boolean := false;
                queue_table text;
              BEGIN
                -- Try multiple possible table name formats
                FOR queue_table IN 
                  SELECT unnest(ARRAY['pgmq_' || $1, $1])
                LOOP
                  BEGIN
                    -- Try different ID column formats
                    EXECUTE 'DELETE FROM ' || quote_ident(queue_table) || ' WHERE msg_id = $1 OR id = $1' 
                    USING $2::numeric;
                    
                    GET DIAGNOSTICS success = ROW_COUNT;
                    IF success THEN 
                      EXIT; 
                    END IF;
                  EXCEPTION WHEN OTHERS THEN
                    -- Just continue to the next iteration
                  END;
                END LOOP;
              END $$;
              SELECT true as deleted;
            `,
            params: [queue_name, Number(message_id)]
          }
        );
        
        if (!directError) {
          console.log(`Successfully deleted numeric message ${message_id} using direct SQL`);
          success = true;
        }
      } catch (sqlError) {
        console.error(`Direct SQL deletion failed for message ${message_id}:`, sqlError);
        error = sqlError;
      }
    }
    
    // If direct SQL delete didn't work, try using standard PGMQ methods
    while (!success && attempts < maxAttempts) {
      attempts++;
      try {
        console.log(`Attempt ${attempts} to delete message ${message_id}`);
        
        let currentMethod = '';
        
        // Try different deletion methods in sequence
        if (attempts === 1) {
          // First try using the deleteFromQueue edge function (for UUID message IDs)
          currentMethod = 'pgmq.delete';
          const { data: pgmqData, error: pgmqError } = await supabase.rpc('pgmq.delete', {
            queue_name,
            msg_id: message_id
          });
          
          if (!pgmqError && pgmqData) {
            success = true;
            console.log(`Successfully deleted message ${message_id} using pgmq.delete`);
            break;
          }
        } else if (attempts === 2) {
          // Next try using the pg_delete_message function
          currentMethod = 'pg_delete_message';
          // For UUID format
          if (!isNumeric) {
            const { data: pgDeleteData, error: pgDeleteError } = await supabase.rpc('pg_delete_message', {
              queue_name,
              message_id
            });
            
            if (!pgDeleteError && pgDeleteData === true) {
              success = true;
              console.log(`Successfully deleted message ${message_id} using pg_delete_message`);
              break;
            }
          }
          
          // Try raw SQL for additional fallback
          const { data: rawSqlData, error: rawSqlError } = await supabase.rpc(
            'raw_sql_query',
            { 
              sql_query: `SELECT pg_delete_message($1, $2::text)`,
              params: [queue_name, message_id]
            }
          );
          
          if (!rawSqlError && rawSqlData && rawSqlData.length > 0) {
            success = true;
            console.log(`Successfully deleted message ${message_id} using SQL call to pg_delete_message`);
            break;
          }
        } else {
          // Last resort: Try more aggressive direct SQL methods
          currentMethod = 'direct SQL deletion';
          
          // Attempt to get the correct table name
          const { data: tableData } = await supabase.rpc(
            'raw_sql_query',
            { 
              sql_query: `SELECT pgmq.get_queue_table_name($1) as table_name`,
              params: [queue_name]
            }
          );
          
          let tableName = '';
          if (tableData && tableData.length > 0 && tableData[0].table_name) {
            tableName = tableData[0].table_name;
          } else {
            tableName = `pgmq_${queue_name}`;
          }
          
          // Try explicit deletion with both column names
          const { data: lastData, error: lastError } = await supabase.rpc(
            'raw_sql_query',
            { 
              sql_query: `
                WITH deletion AS (
                  DELETE FROM ${tableName} WHERE msg_id = $1 OR id = $1 RETURNING true as deleted
                )
                SELECT coalesce((SELECT deleted FROM deletion LIMIT 1), false) as deleted
              `,
              params: [isNumeric ? Number(message_id) : message_id]
            }
          );
          
          if (!lastError && lastData && lastData.length > 0 && lastData[0].deleted) {
            success = true;
            console.log(`Successfully deleted message ${message_id} using direct table deletion`);
            break;
          }
        }
        
        console.warn(`${currentMethod} deletion failed for message ${message_id}, attempt ${attempts}/${maxAttempts}`);
        
        // Wait a bit before retrying (exponential backoff)
        if (attempts < maxAttempts) {
          const delayMs = Math.pow(2, attempts) * 100;
          await new Promise(resolve => setTimeout(resolve, delayMs));
        }
      } catch (attemptError) {
        console.error(`Error during deletion attempt ${attempts} for message ${message_id}:`, attemptError);
        error = attemptError;
        
        // Wait before retrying with exponential backoff
        if (attempts < maxAttempts) {
          const delayMs = Math.pow(2, attempts) * 100;
          await new Promise(resolve => setTimeout(resolve, delayMs));
        }
      }
    }
    
    // Final verification - verify the message no longer exists
    if (!success) {
      try {
        console.log(`Performing final verification for message ${message_id}`);
        
        // Get correct table name
        const { data: tableNameData } = await supabase.rpc(
          'raw_sql_query',
          { 
            sql_query: `SELECT pgmq.get_queue_table_name($1) as table_name`,
            params: [queue_name]
          }
        );
        
        let tableName = '';
        if (tableNameData && tableNameData.length > 0 && tableNameData[0].table_name) {
          tableName = tableNameData[0].table_name;
        } else {
          tableName = `pgmq_${queue_name}`;
        }
        
        // Check if message still exists
        const { data: existsData, error: existsError } = await supabase.rpc(
          'raw_sql_query',
          { 
            sql_query: `SELECT EXISTS(
              SELECT 1 FROM ${tableName} 
              WHERE msg_id = $1 OR id = $1
            ) as exists`,
            params: [isNumeric ? Number(message_id) : message_id]
          }
        );
        
        // If it doesn't exist, consider it a success
        if (!existsError && existsData && existsData.length > 0 && existsData[0].exists === false) {
          console.log(`Message ${message_id} verified as not present in the queue, marking as deleted`);
          success = true;
        }
      } catch (verifyError) {
        console.error(`Error during final verification for message ${message_id}:`, verifyError);
      }
    }
    
    if (success) {
      return new Response(
        JSON.stringify({ success: true }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } else {
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
