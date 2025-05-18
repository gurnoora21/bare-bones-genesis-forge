
/**
 * PGMQ Bridge Functions
 * 
 * Provides fallback mechanisms when the standard pgmq functions are not available
 * or accessible in the schema cache. This resolves the "PGRST202" error where
 * public.pgmq_read function could not be found.
 */
import { SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { logDebug, logError, logWarning } from "./debugHelper.ts";

const MODULE_NAME = "PGMQBridge";

/**
 * Read messages from a PGMQ queue using multiple fallback methods
 * when pgmq_read RPC function isn't accessible
 */
export async function readQueueMessages(
  supabase: SupabaseClient,
  queueName: string,
  batchSize: number = 10,
  visibilityTimeout: number = 30
): Promise<any[]> {
  try {
    logDebug(MODULE_NAME, `Reading messages from queue ${queueName} with batch size ${batchSize}`);

    // First try: Use the pgmq_read_safe function which is our safe wrapper
    try {
      logDebug(MODULE_NAME, "Attempting to use pgmq_read_safe function...");
      const { data: safeData, error: safeError } = await supabase.rpc('pgmq_read_safe', {
        queue_name: queueName,
        max_messages: batchSize,
        visibility_timeout: visibilityTimeout
      });

      if (!safeError && safeData) {
        logDebug(MODULE_NAME, `Successfully read ${Array.isArray(safeData) ? safeData.length : 0} messages using pgmq_read_safe`);
        return Array.isArray(safeData) ? safeData : 
               (typeof safeData === 'string' ? JSON.parse(safeData) : []);
      }
      
      if (safeError) {
        logWarning(MODULE_NAME, `pgmq_read_safe error: ${safeError.message}. Trying alternate methods...`);
      }
    } catch (readSafeErr) {
      logWarning(MODULE_NAME, `pgmq_read_safe exception: ${readSafeErr.message || readSafeErr}`);
    }
    
    // Second try: Use pg_dequeue function
    try {
      logDebug(MODULE_NAME, "Attempting to use pg_dequeue function...");
      const { data: pgDequeueData, error: pgDequeueError } = await supabase.rpc('pg_dequeue', {
        queue_name: queueName,
        batch_size: batchSize,
        visibility_timeout: visibilityTimeout
      });

      if (!pgDequeueError && pgDequeueData) {
        logDebug(MODULE_NAME, `Successfully read ${Array.isArray(pgDequeueData) ? pgDequeueData.length : 0} messages using pg_dequeue`);
        return Array.isArray(pgDequeueData) ? pgDequeueData : 
               (typeof pgDequeueData === 'string' ? JSON.parse(pgDequeueData) : []);
      }
      
      if (pgDequeueError) {
        logWarning(MODULE_NAME, `pg_dequeue error: ${pgDequeueError.message}. Trying alternate methods...`);
      }
    } catch (pgDequeueErr) {
      logWarning(MODULE_NAME, `pg_dequeue exception: ${pgDequeueErr.message || pgDequeueErr}`);
    }

    // Third try: Use direct SQL via raw_sql_query function
    try {
      logDebug(MODULE_NAME, `Attempting direct SQL query for queue ${queueName}`);
      
      // Get the actual queue table name using a helper function
      const { data: queueTableResult, error: tableError } = await supabase.rpc('get_queue_table_name_safe', {
        p_queue_name: queueName
      });
      
      if (tableError || !queueTableResult) {
        logWarning(MODULE_NAME, `Could not determine queue table name: ${tableError?.message || 'No result'}`);
        // Fallback to standard pattern
        const queueTable = `pgmq.q_${queueName}`;
        logDebug(MODULE_NAME, `Falling back to assumed table name: ${queueTable}`);
        
        // Build SQL that reads messages and sets visibility timeout
        // Updated to use msg_id instead of id
        const sql = `
          WITH visible_msgs AS (
            SELECT *
            FROM ${queueTable}
            WHERE vt IS NULL
            ORDER BY msg_id
            LIMIT ${batchSize}
            FOR UPDATE SKIP LOCKED
          ),
          updated AS (
            UPDATE ${queueTable} t
            SET vt = now() + interval '${visibilityTimeout} seconds',
                read_ct = COALESCE(read_ct, 0) + 1
            FROM visible_msgs
            WHERE t.msg_id = visible_msgs.msg_id
            RETURNING t.*
          )
          SELECT msg_id, message, enqueued_at as created_at, vt, read_ct FROM updated`;
        
        const { data: sqlResult, error: sqlError } = await supabase.rpc('raw_sql_query', {
          sql_query: sql
        });
        
        if (sqlError) {
          logError(MODULE_NAME, `Direct SQL error: ${sqlError.message}`);
          throw sqlError;
        }
        
        logDebug(MODULE_NAME, `Successfully read ${Array.isArray(sqlResult) ? sqlResult.length : 0} messages using direct SQL`);
        return sqlResult || [];
      } else {
        // We got the table name, now use it
        const queueTable = queueTableResult;
        logDebug(MODULE_NAME, `Determined queue table name: ${queueTable}`);
        
        // Build SQL that reads messages and sets visibility timeout
        // Updated to use msg_id instead of id
        const sql = `
          WITH visible_msgs AS (
            SELECT *
            FROM ${queueTable}
            WHERE vt IS NULL
            ORDER BY msg_id
            LIMIT ${batchSize}
            FOR UPDATE SKIP LOCKED
          ),
          updated AS (
            UPDATE ${queueTable} t
            SET vt = now() + interval '${visibilityTimeout} seconds',
                read_ct = COALESCE(read_ct, 0) + 1
            FROM visible_msgs
            WHERE t.msg_id = visible_msgs.msg_id
            RETURNING t.*
          )
          SELECT msg_id, message, enqueued_at as created_at, vt, read_ct FROM updated`;
        
        const { data: sqlResult, error: sqlError } = await supabase.rpc('raw_sql_query', {
          sql_query: sql
        });
        
        if (sqlError) {
          logError(MODULE_NAME, `Direct SQL error: ${sqlError.message}`);
          throw sqlError;
        }
        
        logDebug(MODULE_NAME, `Successfully read ${Array.isArray(sqlResult) ? sqlResult.length : 0} messages using direct SQL`);
        return sqlResult || [];
      }
    } catch (directErr) {
      logError(MODULE_NAME, `Direct SQL access error: ${directErr.message || directErr}`);
    }

    // Fourth try: Use the readQueue edge function if available
    try {
      logDebug(MODULE_NAME, `Attempting to call readQueue edge function as fallback`);
      const { data: functionData, error: functionError } = await supabase.functions.invoke('readQueue', {
        body: { 
          queue_name: queueName, 
          batch_size: batchSize,
          visibility_timeout: visibilityTimeout
        }
      });
      
      if (functionError) {
        logError(MODULE_NAME, `readQueue function error: ${functionError.message}`);
        throw functionError;
      }
      
      logDebug(MODULE_NAME, `Successfully read messages using readQueue function`);
      return functionData || [];
    } catch (funcErr) {
      logError(MODULE_NAME, `readQueue function error: ${funcErr.message || funcErr}`);
    }
    
    // All methods failed
    logError(MODULE_NAME, `All queue reading methods failed for ${queueName}`);
    return [];
    
  } catch (error) {
    logError(MODULE_NAME, `Fatal error in readQueueMessages for ${queueName}: ${error.message}`);
    return [];
  }
}

/**
 * Mark a message as processed/delete from queue with fallback methods
 */
export async function deleteQueueMessage(
  supabase: SupabaseClient, 
  queueName: string, 
  messageId: string
): Promise<boolean> {
  try {
    logDebug(MODULE_NAME, `Attempting to delete message ${messageId} from queue ${queueName}`);

    // First try: Use the ensure_message_deleted function which has multiple fallbacks built in
    try {
      const { data: ensureData, error: ensureError } = await supabase.rpc('ensure_message_deleted', {
        queue_name: queueName,
        message_id: messageId,
        max_attempts: 3
      });
      
      if (!ensureError && ensureData === true) {
        logDebug(MODULE_NAME, `Successfully deleted message ${messageId} using ensure_message_deleted`);
        return true;
      }
      
      if (ensureError) {
        logWarning(MODULE_NAME, `ensure_message_deleted error: ${ensureError.message}. Trying alternate methods...`);
      }
    } catch (ensureErr) {
      logWarning(MODULE_NAME, `ensure_message_deleted exception: ${ensureErr.message || ensureErr}`);
    }

    // Second try: Use pg_delete_message function
    try {
      const { data: deleteData, error: deleteError } = await supabase.rpc('pg_delete_message', {
        queue_name: queueName,
        message_id: messageId
      });
      
      if (!deleteError && deleteData === true) {
        logDebug(MODULE_NAME, `Successfully deleted message ${messageId} using pg_delete_message`);
        return true;
      }
      
      if (deleteError) {
        logWarning(MODULE_NAME, `pg_delete_message error: ${deleteError.message}. Trying alternate methods...`);
      }
    } catch (deleteErr) {
      logWarning(MODULE_NAME, `pg_delete_message exception: ${deleteErr.message || deleteErr}`);
    }

    // Third try: Use direct_pgmq_delete function
    try {
      const { data: directData, error: directError } = await supabase.rpc('direct_pgmq_delete', {
        p_queue_name: queueName,
        p_message_id: messageId
      });
      
      if (!directError && directData === true) {
        logDebug(MODULE_NAME, `Successfully deleted message ${messageId} using direct_pgmq_delete`);
        return true;
      }
      
      if (directError) {
        logWarning(MODULE_NAME, `direct_pgmq_delete error: ${directError.message}. Trying final method...`);
      }
    } catch (directErr) {
      logWarning(MODULE_NAME, `direct_pgmq_delete exception: ${directErr.message || directErr}`);
    }

    // All methods failed
    logError(MODULE_NAME, `All queue deletion methods failed for message ${messageId} in queue ${queueName}`);
    return false;
    
  } catch (error) {
    logError(MODULE_NAME, `Fatal error in deleteQueueMessage for ${queueName}: ${error.message}`);
    return false;
  }
}
