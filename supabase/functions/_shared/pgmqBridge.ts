
/**
 * PGMQ Bridge Functions
 * 
 * Simple wrapper functions for interacting with PGMQ queues
 */
import { SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
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

    // Use pg_dequeue function - the standard way to read messages
    const { data, error } = await supabase.rpc('pg_dequeue', {
      queue_name: queueName,
      batch_size: batchSize,
      visibility_timeout: visibilityTimeout
    });

    if (error) {
      logError(MODULE_NAME, `Error reading from queue ${queueName}: ${error.message}`);
      return [];
    }

    // Parse the result if it's a string
    const messages = Array.isArray(data) ? data : 
                    (typeof data === 'string' ? JSON.parse(data) : []);
    
    logDebug(MODULE_NAME, `Successfully read ${messages.length} messages from queue ${queueName}`);
    return messages;
  } catch (error) {
    logError(MODULE_NAME, `Fatal error in readQueueMessages for ${queueName}: ${error.message}`);
    return [];
  }
}

/**
 * Mark a message as processed/delete from queue
 */
export async function deleteQueueMessage(
  supabase: SupabaseClient, 
  queueName: string, 
  messageId: string
): Promise<boolean> {
  try {
    logDebug(MODULE_NAME, `Deleting message ${messageId} from queue ${queueName}`);

    // Use pg_delete_message function - the standard way to delete messages
    const { data, error } = await supabase.rpc('pg_delete_message', {
      queue_name: queueName,
      message_id: messageId
    });
    
    if (error) {
      logError(MODULE_NAME, `Error deleting message ${messageId} from queue ${queueName}: ${error.message}`);
      return false;
    }
    
    logDebug(MODULE_NAME, `Successfully deleted message ${messageId} from queue ${queueName}`);
    return true;
  } catch (error) {
    logError(MODULE_NAME, `Fatal error in deleteQueueMessage for ${queueName}: ${error.message}`);
    return false;
  }
}
