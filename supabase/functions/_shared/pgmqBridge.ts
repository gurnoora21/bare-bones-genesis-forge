
/**
 * PGMQ Bridge Functions
 * 
 * Provides fallback mechanisms when the standard pgmq functions are not available
 * or accessible in the schema cache. This resolves the "PGRST202" error where
 * public.pgmq_read function could not be found.
 */
import { SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

/**
 * Read messages from a PGMQ queue using direct table access as a fallback
 * when pgmq_read RPC function isn't accessible
 */
export async function readQueueMessages(
  supabase: SupabaseClient,
  queueName: string,
  batchSize: number = 10,
  visibilityTimeout: number = 30
): Promise<any[]> {
  try {
    console.log(`Attempting to read from queue ${queueName} using reliable method`);

    // First try: Use the pg_dequeue function which is our custom wrapper
    try {
      const { data: pgDequeueData, error: pgDequeueError } = await supabase.rpc('pg_dequeue', {
        queue_name: queueName,
        batch_size: batchSize,
        visibility_timeout: visibilityTimeout
      });

      if (!pgDequeueError && pgDequeueData) {
        console.log(`Successfully read ${Array.isArray(pgDequeueData) ? pgDequeueData.length : 0} messages using pg_dequeue`);
        return Array.isArray(pgDequeueData) ? pgDequeueData : 
               (typeof pgDequeueData === 'string' ? JSON.parse(pgDequeueData) : []);
      }
      
      if (pgDequeueError) {
        console.warn(`pg_dequeue error: ${pgDequeueError.message}. Trying alternate methods...`);
      }
    } catch (pgDequeueErr) {
      console.warn(`pg_dequeue exception: ${pgDequeueErr.message || pgDequeueErr}`);
    }

    // Second try: Use direct SQL via raw_sql_query function
    try {
      console.log(`Attempting direct SQL query for queue ${queueName}`);
      
      // Get the actual queue table name using a helper function
      const { data: queueTableResult, error: tableError } = await supabase.rpc('get_queue_table_name_safe', {
        p_queue_name: queueName
      });
      
      if (tableError || !queueTableResult) {
        console.warn(`Could not determine queue table name: ${tableError?.message || 'No result'}`);
        // Fallback to standard pattern
        const queueTable = `pgmq.q_${queueName}`;
        console.log(`Falling back to assumed table name: ${queueTable}`);
        
        // Build SQL that reads messages and sets visibility timeout
        const sql = `
          WITH visible_msgs AS (
            SELECT *
            FROM ${queueTable}
            WHERE vt IS NULL
            ORDER BY id
            LIMIT ${batchSize}
            FOR UPDATE SKIP LOCKED
          ),
          updated AS (
            UPDATE ${queueTable} t
            SET vt = now() + interval '${visibilityTimeout} seconds',
                read_ct = COALESCE(read_ct, 0) + 1
            FROM visible_msgs
            WHERE t.id = visible_msgs.id
            RETURNING t.*
          )
          SELECT id, msg_id, message, created_at, vt, read_ct FROM updated`;
        
        const { data: sqlResult, error: sqlError } = await supabase.rpc('raw_sql_query', {
          sql_query: sql
        });
        
        if (sqlError) {
          console.error(`Direct SQL error: ${sqlError.message}`);
          throw sqlError;
        }
        
        console.log(`Successfully read ${Array.isArray(sqlResult) ? sqlResult.length : 0} messages using direct SQL`);
        return sqlResult || [];
      } else {
        // We got the table name, now use it
        const queueTable = queueTableResult;
        console.log(`Determined queue table name: ${queueTable}`);
        
        // Build SQL that reads messages and sets visibility timeout
        const sql = `
          WITH visible_msgs AS (
            SELECT *
            FROM ${queueTable}
            WHERE vt IS NULL
            ORDER BY id
            LIMIT ${batchSize}
            FOR UPDATE SKIP LOCKED
          ),
          updated AS (
            UPDATE ${queueTable} t
            SET vt = now() + interval '${visibilityTimeout} seconds',
                read_ct = COALESCE(read_ct, 0) + 1
            FROM visible_msgs
            WHERE t.id = visible_msgs.id
            RETURNING t.*
          )
          SELECT id, msg_id, message, created_at, vt, read_ct FROM updated`;
        
        const { data: sqlResult, error: sqlError } = await supabase.rpc('raw_sql_query', {
          sql_query: sql
        });
        
        if (sqlError) {
          console.error(`Direct SQL error: ${sqlError.message}`);
          throw sqlError;
        }
        
        console.log(`Successfully read ${Array.isArray(sqlResult) ? sqlResult.length : 0} messages using direct SQL`);
        return sqlResult || [];
      }
    } catch (directErr) {
      console.error(`Direct SQL access error: ${directErr.message || directErr}`);
    }

    // Third try: Use the readQueue edge function if available
    try {
      console.log(`Attempting to call readQueue edge function as fallback`);
      const { data: functionData, error: functionError } = await supabase.functions.invoke('readQueue', {
        body: { 
          queue_name: queueName, 
          batch_size: batchSize,
          visibility_timeout: visibilityTimeout
        }
      });
      
      if (functionError) {
        console.error(`readQueue function error: ${functionError.message}`);
        throw functionError;
      }
      
      console.log(`Successfully read messages using readQueue function`);
      return functionData || [];
    } catch (funcErr) {
      console.error(`readQueue function error: ${funcErr.message || funcErr}`);
    }
    
    // All methods failed
    console.error(`All queue reading methods failed for ${queueName}`);
    return [];
    
  } catch (error) {
    console.error(`Fatal error in readQueueMessages for ${queueName}: ${error.message}`);
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
    console.log(`Attempting to delete message ${messageId} from queue ${queueName}`);

    // First try: Use the ensure_message_deleted function which has multiple fallbacks built in
    try {
      const { data: ensureData, error: ensureError } = await supabase.rpc('ensure_message_deleted', {
        queue_name: queueName,
        message_id: messageId,
        max_attempts: 3
      });
      
      if (!ensureError && ensureData === true) {
        console.log(`Successfully deleted message ${messageId} using ensure_message_deleted`);
        return true;
      }
      
      if (ensureError) {
        console.warn(`ensure_message_deleted error: ${ensureError.message}. Trying alternate methods...`);
      }
    } catch (ensureErr) {
      console.warn(`ensure_message_deleted exception: ${ensureErr.message || ensureErr}`);
    }

    // Second try: Use pg_delete_message function
    try {
      const { data: deleteData, error: deleteError } = await supabase.rpc('pg_delete_message', {
        queue_name: queueName,
        message_id: messageId
      });
      
      if (!deleteError && deleteData === true) {
        console.log(`Successfully deleted message ${messageId} using pg_delete_message`);
        return true;
      }
      
      if (deleteError) {
        console.warn(`pg_delete_message error: ${deleteError.message}. Trying alternate methods...`);
      }
    } catch (deleteErr) {
      console.warn(`pg_delete_message exception: ${deleteErr.message || deleteErr}`);
    }

    // Third try: Use direct_pgmq_delete function
    try {
      const { data: directData, error: directError } = await supabase.rpc('direct_pgmq_delete', {
        p_queue_name: queueName,
        p_message_id: messageId
      });
      
      if (!directError && directData === true) {
        console.log(`Successfully deleted message ${messageId} using direct_pgmq_delete`);
        return true;
      }
      
      if (directError) {
        console.warn(`direct_pgmq_delete error: ${directError.message}. Trying final method...`);
      }
    } catch (directErr) {
      console.warn(`direct_pgmq_delete exception: ${directErr.message || directErr}`);
    }

    // All methods failed
    console.error(`All queue deletion methods failed for message ${messageId} in queue ${queueName}`);
    return false;
    
  } catch (error) {
    console.error(`Fatal error in deleteQueueMessage for ${queueName}: ${error.message}`);
    return false;
  }
}
