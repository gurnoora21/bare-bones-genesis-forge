
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

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

  console.log(`Attempting to delete message ${messageId} from queue ${queueName}`);

  while (attempt < maxRetries) {
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

      // If failed, try the reset function as a fallback
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
      let isDuplicate = false;
      if (deduplicationService) {
        try {
          isDuplicate = await deduplicationService.isDuplicate(queueName, idempotencyKey);
          if (isDuplicate) {
            console.log(`Message ${messageId} (key: ${idempotencyKey}) deduplicated, skipping`);
            
            // Delete message from queue to prevent reprocessing
            await deleteMessageWithRetries(supabase, queueName, messageId);
            
            return { deduplication: true, skipped: true, reason: "deduplicated" };
          }
        } catch (dedupError) {
          // If deduplication check fails, log but continue processing
          // This is safer than failing the whole job
          console.error(`Deduplication check failed for ${messageId}: ${dedupError.message}`);
        }
      }
      
      // Process the message
      const result = await processMessage();
      
      // If successful, mark as processed in deduplication service
      if (deduplicationService && !isDuplicate) {
        try {
          await deduplicationService.markAsProcessed(queueName, idempotencyKey);
        } catch (markError) {
          // Non-fatal: Log but continue
          console.warn(`Failed to mark ${idempotencyKey} as processed: ${markError.message}`);
        }
      }
      
      // If successful, delete message from queue
      await deleteMessageWithRetries(supabase, queueName, messageId);
      
      return result;
    } catch (error) {
      lastError = error;
      console.error(`Attempt ${attempt + 1} to process message ${messageId} failed:`, error);
      
      attempt++;
      if (attempt <= maxRetries) {
        await new Promise(resolve => setTimeout(resolve, retryDelayMs * attempt));
      }
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
 * Enqueues a message to a Supabase queue using direct SQL functions
 * @returns true if enqueued successfully, false if error
 */
export async function enqueueMessage(
  supabase: any,
  queueName: string,
  message: any,
  deduplicationKey?: string,
  redis?: Redis
): Promise<boolean> {
  try {
    // Basic validation
    if (!queueName) {
      console.error("Queue name is required");
      return false;
    }
    
    // Check for deduplication if Redis and key are provided
    if (deduplicationKey && redis) {
      try {
        const dedupKey = `enqueued:${queueName}:${deduplicationKey}`;
        const exists = await redis.exists(dedupKey);
        
        if (exists === 1) {
          console.log(`Duplicate job skipped: ${queueName} with key ${deduplicationKey}`);
          return false; // Already enqueued, not an error
        }
        
        // Set deduplication key with 24h TTL
        await redis.set(dedupKey, new Date().toISOString(), { ex: 86400 });
      } catch (redisError) {
        // If Redis fails, log but continue with enqueueing
        console.warn(`Redis deduplication check failed for ${queueName}:${deduplicationKey}, proceeding anyway:`, redisError.message);
      }
    }
    
    // Use specialized function based on queue type
    if (queueName === 'album_discovery' && message.artistId) {
      const { data, error } = await supabase.rpc(
        'start_album_discovery',
        { artist_id: message.artistId, offset_val: message.offset || 0 }
      );
      
      if (error) {
        console.error(`Error calling start_album_discovery: ${error.message}`);
        return false;
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
        console.error(`Error calling start_track_discovery: ${error.message}`);
        return false;
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
        console.error(`Error calling start_artist_discovery: ${error.message}`);
        return false;
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
        console.error(`Error calling start_producer_identification: ${error.message}`);
        return false;
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
      console.error(`Error enqueueing to ${queueName}: ${error.message}`);
      return false;
    }
    
    console.log(`Successfully enqueued message to ${queueName}, message ID: ${data}`);
    return true;
  } catch (error) {
    console.error(`Exception enqueueing message to queue ${queueName}:`, error);
    return false;
  }
}

/**
 * Acquire a processing lock for distributed processing
 */
export async function acquireProcessingLock(
  entityType: string,
  entityId: string,
  ttlSeconds: number = 300
): Promise<boolean> {
  try {
    // Use a simple Redis check for brevity
    const redis = new Redis({
      url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
      token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
    });
    
    const lockKey = `lock:${entityType}:${entityId}`;
    const now = new Date().toISOString();
    
    // Try to set the lock with NX (only set if not exists)
    const result = await redis.set(lockKey, now, {
      nx: true,
      ex: ttlSeconds
    });
    
    return result === "OK";
  } catch (error) {
    // On Redis error, log but continue (safer to allow processing)
    console.error(`Failed to acquire processing lock for ${entityType}:${entityId}: ${error.message}`);
    return true;
  }
}

// Import from local module for use in function above
import { DeduplicationService } from "./deduplication.ts";
