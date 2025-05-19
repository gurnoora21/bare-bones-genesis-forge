
/**
 * PGMQ Bridge Functions
 * 
 * Simple wrapper functions for interacting with PGMQ queues
 * Uses direct SQL operations for reliability
 */
import { SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { logDebug, logError } from "./debugHelper.ts";

const MODULE_NAME = "PGMQBridge";

/**
 * Execute SQL directly for queue operations
 */
export async function executeQueueSql(
  supabase: SupabaseClient,
  sql: string,
  params: any[] = []
): Promise<any> {
  try {
    logDebug(MODULE_NAME, `Executing SQL: ${sql.substring(0, 100)}...`);
    
    const { data, error } = await supabase.rpc('raw_sql_query', {
      sql_query: sql,
      params
    });
    
    if (error) {
      logError(MODULE_NAME, `Error executing SQL: ${error.message}`);
      return null;
    }
    
    return data;
  } catch (error) {
    logError(MODULE_NAME, `Fatal error executing SQL: ${error.message}`);
    return null;
  }
}

/**
 * Read messages from a PGMQ queue
 * Uses direct SQL operations for reliability
 */
export async function readQueueMessages(
  supabase: SupabaseClient,
  queueName: string,
  batchSize: number = 10,
  visibilityTimeout: number = 30
): Promise<any[]> {
  try {
    logDebug(MODULE_NAME, `Reading messages from queue ${queueName} with batch size ${batchSize}`);

    // First try using the pg_dequeue function
    const { data, error } = await supabase.rpc('pg_dequeue', {
      queue_name: queueName,
      batch_size: batchSize,
      visibility_timeout: visibilityTimeout
    });

    if (!error) {
      // Parse the result if it's a string
      const messages = Array.isArray(data) ? data : 
                      (typeof data === 'string' ? JSON.parse(data) : []);
      
      logDebug(MODULE_NAME, `Successfully read ${messages.length} messages from queue ${queueName} using pg_dequeue`);
      return messages;
    }
    
    // If pg_dequeue fails, use direct SQL
    logDebug(MODULE_NAME, `pg_dequeue failed, using direct SQL: ${error.message}`);
    
    // Direct SQL approach to read messages
    const sql = `
      WITH next_messages AS (
        SELECT 
          id,
          msg_id,
          message,
          created_at,
          visible_at,
          NOW() + INTERVAL '${visibilityTimeout} seconds' AS new_visible_at
        FROM pgmq.q_${queueName}
        WHERE visible_at <= NOW()
        ORDER BY created_at
        LIMIT ${batchSize}
        FOR UPDATE SKIP LOCKED
      ),
      updated AS (
        UPDATE pgmq.q_${queueName} q
        SET visible_at = nm.new_visible_at
        FROM next_messages nm
        WHERE q.id = nm.id
        RETURNING q.id, q.msg_id, q.message, q.created_at
      )
      SELECT 
        id,
        msg_id AS "msgId",
        message,
        created_at AS "createdAt"
      FROM updated
    `;
    
    const result = await executeQueueSql(supabase, sql);
    
    if (!result || !Array.isArray(result)) {
      logError(MODULE_NAME, `Direct SQL read failed for queue ${queueName}`);
      return [];
    }
    
    // Process the messages to match the expected format
    const messages = result.map(msg => {
      // Parse the message if it's a string
      let parsedMessage;
      try {
        parsedMessage = typeof msg.message === 'string' ? JSON.parse(msg.message) : msg.message;
      } catch (e) {
        parsedMessage = msg.message;
      }
      
      return {
        id: msg.msg_id || msg.msgId || msg.id,
        message: parsedMessage,
        created_at: msg.createdAt || msg.created_at
      };
    });
    
    logDebug(MODULE_NAME, `Successfully read ${messages.length} messages from queue ${queueName} using direct SQL`);
    return messages;
  } catch (error) {
    logError(MODULE_NAME, `Fatal error in readQueueMessages for ${queueName}: ${error.message}`);
    return [];
  }
}

/**
 * Mark a message as processed/delete from queue
 * Uses the robust pg_delete_message function for reliability
 */
export async function deleteQueueMessage(
  supabase: SupabaseClient, 
  queueName: string, 
  messageId: string | number | undefined
): Promise<boolean> {
  // Handle undefined or null message IDs
  if (messageId === undefined || messageId === null) {
    logError(MODULE_NAME, `Cannot delete message: messageId is ${messageId} from queue ${queueName}`);
    return false;
  }
  
  // Convert to string if it's a number
  const messageIdStr = String(messageId);
  
  try {
    logDebug(MODULE_NAME, `Deleting message ${messageIdStr} from queue ${queueName} using pg_delete_message`);
    
    // Use the pg_delete_message function which now uses our robust implementation
    const { data, error } = await supabase.rpc('pg_delete_message', {
      queue_name: queueName,
      message_id: messageIdStr
    });
    
    if (error) {
      logError(MODULE_NAME, `Error deleting message ${messageIdStr} from queue ${queueName}: ${error.message}`);
      
      // Fallback to direct SQL if the function call fails
      logDebug(MODULE_NAME, `Falling back to direct SQL deletion for message ${messageIdStr}`);
      
      const sql = `
        SELECT pgmq.delete_message_robust($1, $2) as deleted
      `;
      
      const result = await executeQueueSql(supabase, sql, [queueName, messageIdStr]);
      
      if (result && result.length > 0 && result[0].deleted) {
        logDebug(MODULE_NAME, `Successfully deleted message ${messageIdStr} using fallback method`);
        return true;
      }
      
      logError(MODULE_NAME, `Failed to delete message ${messageIdStr} from queue ${queueName} using fallback method`);
      return false;
    }
    
    // If data is true, the message was successfully deleted
    if (data === true) {
      logDebug(MODULE_NAME, `Successfully deleted message ${messageIdStr} from queue ${queueName}`);
      return true;
    }
    
    // If we get here, the message wasn't deleted but no error was returned
    logDebug(MODULE_NAME, `Message ${messageIdStr} not found in queue ${queueName}, considering as deleted`);
    return true;
  } catch (error) {
    logError(MODULE_NAME, `Fatal error in deleteQueueMessage for ${queueName}: ${error.message}`);
    return false;
  }
}
