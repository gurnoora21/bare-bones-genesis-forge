
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
 */
export async function readQueueMessages(
  supabase: SupabaseClient,
  queueName: string,
  batchSize: number = 10,
  visibilityTimeout: number = 30
): Promise<any[]> {
  try {
    logDebug(MODULE_NAME, `Reading messages from queue ${queueName} with batch size ${batchSize}`);
    
    // Use pg_dequeue RPC function which is more reliable
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
    
    // Process the messages to ensure consistent format
    const messages = data.map(msg => {
      // Parse the message if it's a string
      let parsedMessage;
      try {
        parsedMessage = typeof msg.message === 'string' ? JSON.parse(msg.message) : msg.message;
      } catch (e) {
        parsedMessage = msg.message;
      }
      
      return {
        id: msg.msg_id, // Use msg_id consistently
        msg_id: msg.msg_id, // Explicit msg_id field for direct access
        message: parsedMessage,
        created_at: msg.enqueued_at || msg.created_at // Handle both field names
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
 * Delete a message from a queue
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
  
  // Convert to string if it's a number
  const messageIdStr = String(messageId);
  
  try {
    logDebug(MODULE_NAME, `Deleting message ${messageIdStr} from queue ${queueName}`);
    
    // Use pg_delete_message RPC function
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
