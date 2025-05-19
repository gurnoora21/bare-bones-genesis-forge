
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
 * Uses direct SQL DELETE statement for reliability
 */
export async function deleteQueueMessage(
  supabase: SupabaseClient, 
  queueName: string, 
  messageId: string
): Promise<boolean> {
  try {
    logDebug(MODULE_NAME, `Deleting message ${messageId} from queue ${queueName} using direct SQL`);
    
    // Use direct SQL DELETE statement instead of function call
    // Target the pgmq.q_[queue_name] table directly
    const sql = `
      DELETE FROM pgmq.q_${queueName} 
      WHERE id = $1 OR msg_id = $1 
      RETURNING true AS deleted
    `;
    
    const result = await executeQueueSql(supabase, sql, [messageId]);
    
    if (!result || !result.length) {
      logError(MODULE_NAME, `No rows deleted for message ${messageId} from queue ${queueName}`);
      
      // Try an alternative approach - check if the message exists first
      const checkSql = `
        SELECT EXISTS(
          SELECT 1 FROM pgmq.q_${queueName} 
          WHERE id = $1 OR msg_id = $1
        ) AS exists
      `;
      
      const checkResult = await executeQueueSql(supabase, checkSql, [messageId]);
      
      if (checkResult && checkResult.length > 0 && checkResult[0].exists) {
        // Message exists but couldn't be deleted - try again with a different approach
        const forceSql = `
          DELETE FROM pgmq.q_${queueName} 
          WHERE id::text = $1::text OR msg_id::text = $1::text
          RETURNING true AS deleted
        `;
        
        const forceResult = await executeQueueSql(supabase, forceSql, [messageId]);
        
        if (forceResult && forceResult.length > 0) {
          logDebug(MODULE_NAME, `Successfully deleted message ${messageId} using text comparison`);
          return true;
        }
      } else {
        // Message doesn't exist - might have been already processed
        logDebug(MODULE_NAME, `Message ${messageId} not found in queue ${queueName}, considering as deleted`);
        return true;
      }
      
      return false;
    }
    
    logDebug(MODULE_NAME, `Successfully deleted message ${messageId} from queue ${queueName}`);
    return true;
  } catch (error) {
    logError(MODULE_NAME, `Fatal error in deleteQueueMessage for ${queueName}: ${error.message}`);
    
    // Last resort - try with a more permissive approach
    try {
      const safeSql = `
        BEGIN;
        DELETE FROM pgmq.q_${queueName} WHERE id::text = '${messageId}'::text;
        DELETE FROM pgmq.q_${queueName} WHERE msg_id::text = '${messageId}'::text;
        COMMIT;
      `;
      
      await executeQueueSql(supabase, safeSql);
      logDebug(MODULE_NAME, `Attempted safe deletion for message ${messageId} from queue ${queueName}`);
    } catch (safeError) {
      logError(MODULE_NAME, `Safe deletion also failed: ${safeError.message}`);
    }
    
    return false;
  }
}
