
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { DeduplicationService } from "./deduplication.ts";

/**
 * Logs worker issues to the database for monitoring and debugging
 */
export async function logWorkerIssue(
  supabase: any,
  workerName: string,
  issueType: string,
  message: string,
  details: any = {}
): Promise<void> {
  try {
    const { error } = await supabase.from('worker_issues').insert({
      worker_name: workerName,
      issue_type: issueType,
      message: message,
      details: details,
      created_at: new Date().toISOString()
    });

    if (error) {
      console.error("Failed to log worker issue to database:", error);
    }
  } catch (error) {
    console.error("Error logging worker issue:", error);
  }
}

/**
 * Deletes a message from the queue
 */
export async function deleteMessageWithRetries(
  supabase: any,
  queueName: string,
  messageId: string,
  maxRetries: number = 3,
  delayMs: number = 500
): Promise<boolean> {
  let attempt = 0;
  let deleted = false;

  console.log(`Attempting to delete message ${messageId} from queue ${queueName}`);

  while (attempt < maxRetries && !deleted) {
    try {
      // Use database function to delete message
      const { data, error } = await supabase.rpc(
        'pg_delete_message',
        { 
          queue_name: queueName, 
          message_id: messageId.toString()
        }
      );
      
      if (!error && data === true) {
        console.log(`Successfully deleted message ${messageId}`);
        return true;
      } else if (error) {
        console.warn(`Database deletion failed for message ${messageId}:`, error);
      }

      // If failed, try the more reliable function
      try {
        const { data: resetData, error: resetError } = await supabase.rpc(
          'reset_stuck_message',
          { 
            queue_name: queueName, 
            message_id: messageId.toString()
          }
        );
        
        if (!resetError && resetData === true) {
          console.log(`Reset visibility timeout for message ${messageId}`);
          // Consider this a success
          return true;
        }
      } catch (resetError) {
        console.warn(`Error trying to reset message ${messageId}:`, resetError);
      }

      // Increment attempt counter and apply backoff
      attempt++;
      console.log(`Delete attempt ${attempt} failed for message ${messageId}, will retry in ${delayMs}ms`);
      await new Promise(resolve => setTimeout(resolve, delayMs));
      delayMs *= 2; // Exponential backoff
      
    } catch (error) {
      console.error(`Exception during attempt ${attempt + 1} to delete message ${messageId}:`, error);
      attempt++;
      await new Promise(resolve => setTimeout(resolve, delayMs));
      delayMs *= 2; // Exponential backoff
    }
  }

  // If we get here, we've exhausted our retries
  console.error(`Failed to delete message ${messageId} after ${maxRetries} attempts`);
  
  // Log this issue
  try {
    await logWorkerIssue(
      supabase,
      queueName,
      "message_deletion_failure",
      `Failed to delete message ${messageId} after ${maxRetries} attempts`,
      { messageId, queueName, attempts: attempt }
    );
  } catch (logError) {
    console.error("Failed to log deletion issue:", logError);
  }
  
  return false;
}

/**
 * Checks if an entity has already been processed
 */
export async function checkEntityProcessed(
  supabase: any,
  tableName: string,
  fieldName: string,
  fieldValue: string
): Promise<boolean> {
  try {
    const { data, error } = await supabase
      .from(tableName)
      .select('id')
      .eq(fieldName, fieldValue)
      .maybeSingle();

    if (error) {
      console.error(`Error checking if ${tableName} exists:`, error);
      return false;
    }

    return !!data;
  } catch (error) {
    console.error(`Error checking if ${tableName} exists:`, error);
    return false;
  }
}

/**
 * Safely processes a queue message with idempotency
 */
export async function processQueueMessageSafely(
  supabase: any,
  queueName: string,
  messageId: string,
  processMessage: () => Promise<any>,
  idempotencyKey: string,
  isAlreadyProcessed: () => Promise<boolean>,
  options: any = {}
): Promise<any> {
  const { 
    maxRetries = 3, 
    retryDelayMs = 1000,
    deduplication = { enabled: true, redis: null, ttlSeconds: 3600 }
  } = options;
  
  let attempt = 0;
  let lastError: any = null;
  
  // Initialize deduplication service if enabled
  const deduplicationService = deduplication.enabled && deduplication.redis
    ? new DeduplicationService(deduplication.redis)
    : null;
  
  while (attempt <= maxRetries) {
    try {
      // Check if message was already processed using the provided function
      if (await isAlreadyProcessed()) {
        console.log(`Message ${messageId} (key: ${idempotencyKey}) already processed, skipping`);
        
        // Delete message from queue to prevent reprocessing
        await deleteMessageWithRetries(supabase, queueName, messageId);
        
        return { deduplication: true, skipped: true, reason: "already_processed" };
      }
      
      // Check for deduplication
      if (deduplicationService) {
        const isDuplicate = await deduplicationService.isDuplicate(queueName, idempotencyKey);
        if (isDuplicate) {
          console.log(`Message ${messageId} (key: ${idempotencyKey}) deduplicated, skipping`);
          
          // Delete message from queue to prevent reprocessing
          await deleteMessageWithRetries(supabase, queueName, messageId);
          
          return { deduplication: true, skipped: true, reason: "deduplicated" };
        } else {
          // Mark as processed once we confirm it's not a duplicate
          await deduplicationService.markAsProcessed(queueName, idempotencyKey);
        }
      }
      
      // Process the message
      const result = await processMessage();
      
      // If successful, delete message from queue
      await deleteMessageWithRetries(supabase, queueName, messageId);
      
      return result;
    } catch (error) {
      lastError = error;
      console.error(`Attempt ${attempt + 1} to process message ${messageId} failed:`, error);
      
      attempt++;
      await new Promise(resolve => setTimeout(resolve, retryDelayMs));
    }
  }
  
  console.error(`Failed to process message ${messageId} after ${maxRetries} attempts`);
  
  // Log the final error
  await logWorkerIssue(
    supabase,
    queueName,
    "processing_failure",
    `Failed to process message ${messageId} after multiple retries`,
    {
      messageId: messageId,
      idempotencyKey: idempotencyKey,
      error: lastError ? lastError.message : "Unknown error",
      stack: lastError ? lastError.stack : null
    }
  );
  
  throw lastError; // Re-throw the last error after all retries have failed
}

/**
 * Enqueues a message to a Supabase queue with proper deduplication
 * Returns true if enqueued successfully, false if deduplicated or error
 */
export async function enqueueMessage(
  supabase: any,
  queueName: string,
  message: any,
  deduplicationKey?: string,
  deduplicationService?: DeduplicationService
): Promise<boolean> {
  try {
    // Check if we should deduplicate this message
    if (deduplicationKey && deduplicationService) {
      try {
        const isDuplicate = await deduplicationService.isDuplicate(queueName, deduplicationKey);
        if (isDuplicate) {
          console.log(`Message with key ${deduplicationKey} already queued in ${queueName}, skipping`);
          return false;
        }
        
        // Since not a duplicate, mark it as being processed
        await deduplicationService.markAsProcessed(queueName, deduplicationKey);
      } catch (dedupError) {
        // Log but continue - deduplication errors shouldn't block queueing
        console.warn(`Deduplication error for ${queueName}:${deduplicationKey}, proceeding anyway:`, dedupError);
      }
    }
    
    // Use the most reliable direct database function
    try {
      // Try specialized function first if available
      if (queueName === 'album_discovery' && message.artistId) {
        const { data, error } = await supabase.rpc(
          'start_album_discovery',
          { artist_id: message.artistId, offset_val: message.offset || 0 }
        );
        
        if (error) {
          throw new Error(`Error calling start_album_discovery: ${error.message}`);
        }
        
        console.log(`Successfully enqueued album discovery for artist ${message.artistId}, message ID: ${data}`);
        return true;
      }
      
      if (queueName === 'track_discovery' && message.albumId) {
        const { data, error } = await supabase.rpc(
          'start_track_discovery',
          { album_id: message.albumId, offset_val: message.offset || 0 }
        );
        
        if (error) {
          throw new Error(`Error calling start_track_discovery: ${error.message}`);
        }
        
        console.log(`Successfully enqueued track discovery for album ${message.albumId}, message ID: ${data}`);
        return true;
      }
      
      if (queueName === 'artist_discovery' && message.artistName) {
        const { data, error } = await supabase.rpc(
          'start_artist_discovery',
          { artist_name: message.artistName }
        );
        
        if (error) {
          throw new Error(`Error calling start_artist_discovery: ${error.message}`);
        }
        
        console.log(`Successfully enqueued artist discovery for ${message.artistName}, message ID: ${data}`);
        return true;
      }
      
      if (queueName === 'producer_identification' && message.trackId) {
        const { data, error } = await supabase.rpc(
          'start_producer_identification',
          { track_id: message.trackId }
        );
        
        if (error) {
          throw new Error(`Error calling start_producer_identification: ${error.message}`);
        }
        
        console.log(`Successfully enqueued producer identification for track ${message.trackId}, message ID: ${data}`);
        return true;
      }
      
      // General case - use generic pg_enqueue
      const { data, error } = await supabase.rpc(
        'pg_enqueue',
        { 
          queue_name: queueName, 
          message_body: {
            ...message,
            _idempotencyKey: deduplicationKey || `${queueName}:${Date.now()}:${Math.random().toString(36).substring(2, 10)}`
          }
        }
      );
      
      if (error) {
        throw new Error(`Error enqueueing to ${queueName}: ${error.message}`);
      }
      
      console.log(`Successfully enqueued message to ${queueName}, message ID: ${data}`);
      return true;
    } catch (rpcError) {
      console.error(`Failed to enqueue message to ${queueName} using database function:`, rpcError);
      
      // Fall back to Edge Function if database RPC failed
      try {
        const { error } = await supabase.functions.invoke("sendToQueue", {
          body: {
            queue_name: queueName,
            message: message
          }
        });

        if (error) {
          console.error(`Error from sendToQueue Edge Function:`, error);
          return false;
        }

        console.log(`Successfully enqueued message to ${queueName} via Edge Function`);
        return true;
      } catch (fnError) {
        console.error(`Failed to call sendToQueue Edge Function:`, fnError);
        return false;
      }
    }
  } catch (error) {
    console.error(`Exception enqueueing message to queue ${queueName}:`, error);
    return false;
  }
}
