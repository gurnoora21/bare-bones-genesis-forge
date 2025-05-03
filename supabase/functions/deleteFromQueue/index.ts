
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
    
    // Determine if the message_id is numeric or UUID
    const isNumeric = !isNaN(Number(message_id)) && message_id.toString().trim() !== '';
    const messageIdFormatted = isNumeric ? Number(message_id) : message_id;
    
    // Initialize variables for tracking attempts
    let success = false;
    let attempts = 0;
    const maxAttempts = 3;
    
    // First, try to get the correct queue table name
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
        console.log(`Resolved queue table name: ${queueTable}`);
      } else {
        // Fallback to default naming convention
        queueTable = `pgmq_${queue_name}`;
        console.log(`Using default queue table name: ${queueTable}`);
      }
    } catch (tableError) {
      // Fallback to default naming convention on error
      queueTable = `pgmq_${queue_name}`;
      console.log(`Error getting queue table name, using default: ${queueTable}`);
    }
    
    // METHOD 1: Direct SQL deletion (most reliable for both numeric and UUID types)
    try {
      console.log(`Attempting direct SQL deletion with ${isNumeric ? 'numeric' : 'UUID'} ID: ${messageIdFormatted}`);
      
      // Use a transaction to ensure atomicity and better error handling
      const { data: txData, error: txError } = await supabase.rpc(
        'raw_sql_query',
        { 
          sql_query: `
            DO $$
            DECLARE
              deleted_count INT := 0;
              msg_id_text TEXT := $1;
              is_numeric BOOLEAN := ${isNumeric};
              queue_table_name TEXT := $2;
            BEGIN
              -- For numeric IDs
              IF is_numeric THEN
                -- Try with msg_id column (new format)
                BEGIN
                  EXECUTE format('DELETE FROM %I WHERE msg_id = $1::BIGINT', queue_table_name)
                  USING msg_id_text::BIGINT;
                  GET DIAGNOSTICS deleted_count = ROW_COUNT;
                EXCEPTION WHEN OTHERS THEN
                  -- Log and continue to next attempt
                  RAISE NOTICE 'Error deleting by msg_id with numeric: %', SQLERRM;
                END;
                
                -- If not found, try with id column (old/alternate format)
                IF deleted_count = 0 THEN
                  BEGIN
                    EXECUTE format('DELETE FROM %I WHERE id = $1::BIGINT', queue_table_name)
                    USING msg_id_text::BIGINT;
                    GET DIAGNOSTICS deleted_count = ROW_COUNT;
                  EXCEPTION WHEN OTHERS THEN
                    -- Log and continue
                    RAISE NOTICE 'Error deleting by id with numeric: %', SQLERRM;
                  END;
                END IF;
              -- For UUID IDs
              ELSE
                -- Try with id column (UUID format)
                BEGIN
                  EXECUTE format('DELETE FROM %I WHERE id = $1::UUID', queue_table_name)
                  USING msg_id_text;
                  GET DIAGNOSTICS deleted_count = ROW_COUNT;
                EXCEPTION WHEN OTHERS THEN
                  -- Log and continue
                  RAISE NOTICE 'Error deleting by id with UUID: %', SQLERRM;
                END;
                
                -- If not found, try with msg_id column (alternate format)
                IF deleted_count = 0 THEN
                  BEGIN
                    EXECUTE format('DELETE FROM %I WHERE msg_id::TEXT = $1', queue_table_name)
                    USING msg_id_text;
                    GET DIAGNOSTICS deleted_count = ROW_COUNT;
                  EXCEPTION WHEN OTHERS THEN
                    -- Log and continue
                    RAISE NOTICE 'Error deleting by msg_id with text cast: %', SQLERRM;
                  END;
                END IF;
              END IF;
              
              -- Final check with text comparison (most flexible, but less efficient)
              IF deleted_count = 0 THEN
                BEGIN
                  EXECUTE format('DELETE FROM %I WHERE msg_id::TEXT = $1 OR id::TEXT = $1', queue_table_name)
                  USING msg_id_text;
                  GET DIAGNOSTICS deleted_count = ROW_COUNT;
                EXCEPTION WHEN OTHERS THEN
                  RAISE NOTICE 'Error deleting with text comparison: %', SQLERRM;
                END;
              END IF;
              
              -- Return the deletion status
              IF deleted_count > 0 THEN
                RAISE NOTICE 'Successfully deleted message % from %', msg_id_text, queue_table_name;
              ELSE
                RAISE NOTICE 'Message % not found in %', msg_id_text, queue_table_name;
              END IF;
            END $$;
            SELECT EXISTS(
              SELECT 1 FROM ${queueTable}
              WHERE (msg_id::TEXT = $1 OR id::TEXT = $1)
            ) AS still_exists
          `,
          params: [message_id.toString(), queueTable]
        }
      );
      
      // Check if the message is still there
      if (!txError && txData && txData.length > 0 && txData[0].still_exists === false) {
        console.log(`Direct SQL deletion successful for message ${messageIdFormatted}`);
        success = true;
      } else {
        console.log(`Direct SQL deletion did not remove the message (it may already be gone)`);
      }
    } catch (directSqlError) {
      console.error(`Direct SQL deletion failed:`, directSqlError);
    }
    
    // If direct SQL didn't work, try standard methods with retries
    while (!success && attempts < maxAttempts) {
      attempts++;
      
      try {
        // Add exponential backoff for retries with some jitter
        if (attempts > 1) {
          const delayMs = Math.pow(2, attempts - 1) * 100 + Math.random() * 100;
          console.log(`Retry attempt ${attempts}, waiting ${delayMs}ms before next attempt...`);
          await new Promise(resolve => setTimeout(resolve, delayMs));
        }
        
        console.log(`Deletion attempt ${attempts}/${maxAttempts} for message ${messageIdFormatted}`);
        
        // METHOD 2: Try using pgmq.delete RPC
        if (attempts === 1) {
          try {
            const { data: pgmqData, error: pgmqError } = await supabase.rpc('pgmq.delete', {
              queue_name,
              msg_id: messageIdFormatted
            });
            
            if (!pgmqError && pgmqData === true) {
              console.log(`Successfully deleted message using pgmq.delete`);
              success = true;
              break;
            } else {
              console.log(`pgmq.delete method failed:`, pgmqError || "No error but returned false");
            }
          } catch (pgmqDeleteError) {
            console.error(`Error calling pgmq.delete:`, pgmqDeleteError);
          }
        }
        
        // METHOD 3: Use the pg_delete_message function
        if (!success && attempts === 2) {
          try {
            const { data: pgDeleteData, error: pgDeleteError } = await supabase.rpc('pg_delete_message', {
              queue_name,
              message_id: messageIdFormatted
            });
            
            if (!pgDeleteError && pgDeleteData === true) {
              console.log(`Successfully deleted message using pg_delete_message`);
              success = true;
              break;
            } else {
              console.log(`pg_delete_message method failed:`, pgDeleteError || "No error but returned false");
            }
          } catch (pgDeleteError) {
            console.error(`Error calling pg_delete_message:`, pgDeleteError);
          }
        }
        
        // METHOD 4: Try alternative approaches if previous methods failed
        if (!success && attempts === 3) {
          // Try with raw SQL with explicit casts depending on type
          try {
            const sqlQuery = isNumeric ? 
              `SELECT pgmq.delete($1, $2::BIGINT)` :
              `SELECT pgmq.delete($1, $2::UUID)`;
              
            const { data: rawData, error: rawError } = await supabase.rpc('raw_sql_query', {
              sql_query: sqlQuery, 
              params: [queue_name, messageIdFormatted]
            });
            
            if (!rawError && rawData && rawData.length > 0 && rawData[0].delete === true) {
              console.log(`Successfully deleted message with raw SQL pgmq.delete call`);
              success = true;
              break;
            } else {
              console.log(`Raw SQL pgmq.delete failed:`, rawError || "Returned false or null");
            }
          } catch (rawSqlError) {
            console.error(`Error with raw SQL pgmq.delete:`, rawSqlError);
          }
        }
      } catch (attemptError) {
        console.error(`General error during deletion attempt ${attempts}:`, attemptError);
      }
    }
    
    // Final verification - confirm the message no longer exists
    if (!success) {
      try {
        console.log(`Performing final verification for message ${messageIdFormatted}`);
        
        const { data: existsData, error: existsError } = await supabase.rpc(
          'raw_sql_query',
          {
            sql_query: `
              SELECT NOT EXISTS(
                SELECT 1 FROM ${queueTable}
                WHERE msg_id::TEXT = $1 OR id::TEXT = $1
              ) AS deleted
            `,
            params: [message_id.toString()]
          }
        );
        
        if (!existsError && existsData && existsData.length > 0 && existsData[0].deleted === true) {
          console.log(`Final verification confirms message ${messageIdFormatted} is not in the queue`);
          success = true;
        } else {
          console.log(`Message ${messageIdFormatted} still exists in the queue after all attempts`);
        }
      } catch (verifyError) {
        console.error(`Error during final verification:`, verifyError);
      }
    }
    
    if (success) {
      return new Response(
        JSON.stringify({ 
          success: true,
          message: `Successfully deleted message ${messageIdFormatted} from queue ${queue_name}`
        }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } else {
      throw new Error(`Failed to delete message ${messageIdFormatted} from queue ${queue_name} after ${maxAttempts} attempts`);
    }
    
  } catch (error) {
    console.error("Error deleting message from queue:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
