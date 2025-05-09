
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { StateTransitionResult, ProcessingState } from "./stateManager.ts";
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
 * Deletes a message from the queue with retries
 */
export async function deleteMessageWithRetries(
  supabase: any,
  queueName: string,
  messageId: string,
  maxRetries: number = 3,
  delayMs: number = 500
): Promise<boolean> {
  let attempt = 0;

  while (attempt < maxRetries) {
    try {
      const { error } = await supabase.functions.invoke("deleteMessage", {
        body: {
          queue_name: queueName,
          msg_id: messageId
        }
      });

      if (error) {
        console.error(`Attempt ${attempt + 1} to delete message ${messageId} failed:`, error);
        attempt++;
        await new Promise(resolve => setTimeout(resolve, delayMs));
      } else {
        console.log(`Message ${messageId} deleted successfully from queue ${queueName}`);
        return true;
      }
    } catch (error) {
      console.error(`Exception during attempt ${attempt + 1} to delete message ${messageId}:`, error);
      attempt++;
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  }

  console.error(`Failed to delete message ${messageId} after ${maxRetries} attempts`);
  return false;
}

/**
 * Checks if a track has already been processed
 */
export async function checkTrackProcessed(
  supabase: any,
  trackSpotifyId: string
): Promise<boolean> {
  try {
    const { data, error } = await supabase
      .from('tracks')
      .select('id')
      .eq('spotify_id', trackSpotifyId)
      .maybeSingle();

    if (error) {
      console.error("Error checking if track exists:", error);
      return false;
    }

    return !!data;
  } catch (error) {
    console.error("Error checking if track exists:", error);
    return false;
  }
}

/**
 * Safely processes a queue message with idempotency and deduplication
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
    circuitBreaker = false,
    deduplication = { enabled: false, redis: null, ttlSeconds: 3600, strictMatching: true }
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
        // Use the correct method: isDuplicate instead of checkAndRegister
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
      
      // Implement circuit breaker pattern
      if (circuitBreaker && attempt >= 3) {
        console.warn(`Circuit breaker triggered for message ${messageId} after multiple failures`);
        throw error; // Stop retrying
      }
      
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
 * Acquires a processing lock for an entity to prevent duplicate processing
 */
export async function acquireProcessingLock(
  entityType: string,
  entityId: string,
  options: any = {}
): Promise<boolean> {
  const supabase = createClient(
    Deno.env.get("SUPABASE_URL") || "",
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || ""
  );

  try {
    // Use the lockManager edge function to acquire a lock
    const { data, error } = await supabase.functions.invoke("lockManager", {
      body: {
        operation: "acquire",
        entityType,
        entityId,
        options: {
          timeoutMinutes: options.timeoutMinutes || 30,
          ttlSeconds: options.ttlSeconds || 1800,
          workerId: Deno.env.get("WORKER_ID") || `worker_${Math.random().toString(36).substring(2, 10)}`,
          correlationId: options.correlationId || `lock_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`,
        }
      }
    });
    
    if (error) {
      console.error(`Error acquiring lock for ${entityType}:${entityId}:`, error);
      return false;
    }
    
    if (data && data.acquired) {
      console.log(`Successfully acquired lock for ${entityType}:${entityId} via ${data.method}`);
      return true;
    } else {
      const reason = data?.reason || "Unknown reason";
      console.log(`Failed to acquire lock for ${entityType}:${entityId}: ${reason}`);
      return false;
    }
  } catch (error) {
    console.error(`Exception acquiring lock for ${entityType}:${entityId}:`, error);
    return false;
  }
}

/**
 * Releases a processing lock
 */
export async function releaseProcessingLock(
  entityType: string,
  entityId: string,
  workerId?: string
): Promise<boolean> {
  const supabase = createClient(
    Deno.env.get("SUPABASE_URL") || "",
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || ""
  );

  try {
    // Use the lockManager edge function to release a lock
    const { data, error } = await supabase.functions.invoke("lockManager", {
      body: {
        operation: "release",
        entityType,
        entityId,
        options: {
          workerId: workerId || Deno.env.get("WORKER_ID") || undefined,
        }
      }
    });
    
    if (error) {
      console.error(`Error releasing lock for ${entityType}:${entityId}:`, error);
      return false;
    }
    
    if (data && data.released) {
      console.log(`Successfully released lock for ${entityType}:${entityId}`);
      return true;
    } else {
      const errors = data?.errors ? JSON.stringify(data.errors) : "Unknown reason";
      console.log(`Failed to release lock for ${entityType}:${entityId}: ${errors}`);
      return false;
    }
  } catch (error) {
    console.error(`Exception releasing lock for ${entityType}:${entityId}:`, error);
    return false;
  }
}

/**
 * Enqueues a message to a Supabase queue
 */
export async function enqueueMessage(
  supabase: any,
  queueName: string,
  message: any
): Promise<boolean> {
  try {
    const { error } = await supabase.functions.invoke("sendToQueue", {
      body: {
        queue_name: queueName,
        message: message
      }
    });

    if (error) {
      console.error(`Error enqueueing message to queue ${queueName}:`, error);
      return false;
    }

    console.log(`Message successfully enqueued to queue ${queueName}`);
    return true;
  } catch (error) {
    console.error(`Exception enqueueing message to queue ${queueName}:`, error);
    return false;
  }
}
