
/**
 * PGMQ Bridge Functions
 * 
 * Simple wrapper functions for interacting with PGMQ queues
 */
import { SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2.7.1";
import { logDebug, logError } from "./debugHelper.ts";

const MODULE_NAME = "PGMQBridge";

/**
 * Read messages from a PGMQ queue
 * Uses pg_dequeue for compatibility
 */
export async function readQueueMessages(
  supabase: SupabaseClient,
  queueName: string,
  batchSize: number = 10,
  visibilityTimeout: number = 30
): Promise<any[]> {
  try {
    logDebug(MODULE_NAME, `Reading messages from queue ${queueName} with batch size ${batchSize}`);
    
    // Use the pg_dequeue function which is more reliable
    const { data, error } = await supabase.rpc('pg_dequeue', {
      queue_name: queueName,
      batch_size: batchSize,
      visibility_timeout: visibilityTimeout
    });
    
    if (error) {
      logError(MODULE_NAME, `Error reading from queue ${queueName}: ${error.message}`);
      return [];
    }
    
    if (!data || !Array.isArray(data)) {
      logDebug(MODULE_NAME, `No messages found in queue ${queueName}`);
      return [];
    }
    
    // Process the messages to match the expected format
    const messages = data.map(msg => {
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
        created_at: msg.created_at || msg.createdAt
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
