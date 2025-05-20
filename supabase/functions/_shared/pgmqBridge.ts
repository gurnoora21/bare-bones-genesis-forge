
/**
 * PGMQ Bridge Functions
 * 
 * Simple wrapper functions for interacting with PGMQ queues
 * Uses direct SQL operations for reliability
 */
import { SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2.7.1";
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
 * Updated for PGMQ 1.4.4 schema compatibility
 */
export async function readQueueMessages(
  supabase: SupabaseClient,
  queueName: string,
  batchSize: number = 10,
  visibilityTimeout: number = 30
): Promise<any[]> {
  try {
    logDebug(MODULE_NAME, `Reading messages from queue ${queueName} with batch size ${batchSize}`);
    
    // Updated SQL to use correct column names for PGMQ 1.4.4: msg_id, enqueued_at, vt
    const sql = `
      WITH next_messages AS (
        SELECT 
          msg_id,
          message,
          enqueued_at,
          vt,
          NOW() + INTERVAL '${visibilityTimeout} seconds' AS new_vt
        FROM pgmq.q_${queueName}
        WHERE vt IS NULL OR vt <= NOW()
        ORDER BY enqueued_at
        LIMIT ${batchSize}
        FOR UPDATE SKIP LOCKED
      ),
      updated AS (
        UPDATE pgmq.q_${queueName} q
        SET vt = nm.new_vt
        FROM next_messages nm
        WHERE q.msg_id = nm.msg_id
        RETURNING q.msg_id, q.message, q.enqueued_at
      )
      SELECT 
        msg_id,
        msg_id AS "msgId",
        message,
        enqueued_at AS "createdAt"
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
        id: msg.msg_id || msg.msgId || msg.id, // Keep existing order for backward compatibility
        msg_id: msg.msg_id, // Add explicit msg_id field for direct access
        message: parsedMessage,
        created_at: msg.createdAt || msg.created_at
      };
    });
    
    logDebug(MODULE_NAME, `Successfully read ${messages.length} messages from queue ${queueName}`);
    return messages;
  } catch (error) {
    logError(MODULE_NAME, `Fatal error in readQueueMessages for ${queueName}: ${error.message}`);
    return [];
  }
}

/**
 * Mark a message as processed/delete from queue
 * Uses pg_delete_message for simplicity and reliability
 */
export async function deleteQueueMessage(
  supabase: SupabaseClient, 
  queueName: string, 
  messageId: string | number | undefined,
  message?: any // Optional full message object
): Promise<boolean> {
  // Handle undefined or null message IDs
  if (messageId === undefined || messageId === null) {
    logError(MODULE_NAME, `Cannot delete message: messageId is ${messageId} from queue ${queueName}`);
    return false;
  }
  
  // If we have the full message object, try to get msg_id from it
  let finalMessageId = messageId;
  if (message && message.msg_id) {
    finalMessageId = message.msg_id;
    logDebug(MODULE_NAME, `Using msg_id ${finalMessageId} from message object`);
  }
  
  // Convert to string if it's a number
  const messageIdStr = String(finalMessageId);
  
  try {
    logDebug(MODULE_NAME, `Deleting message ${messageIdStr} from queue ${queueName}`);
    
    // Use the pg_delete_message function which is simpler and more reliable
    const { data, error } = await supabase.rpc('pg_delete_message', {
      queue_name: queueName,
      message_id: messageIdStr
    });
    
    if (error) {
      logError(MODULE_NAME, `Error deleting message ${messageIdStr} from queue ${queueName}: ${error.message}`);
      return false;
    }
    
    logDebug(MODULE_NAME, `Successfully deleted message ${messageIdStr} from queue ${queueName}`);
    return true;
  } catch (error) {
    logError(MODULE_NAME, `Error in deleteQueueMessage for ${queueName}: ${error.message}`);
    return false;
  }
}
