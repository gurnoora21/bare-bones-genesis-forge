// Adding acquireProcessingLock to queueHelper.ts exports
// (This is to fix the error in trackDiscovery.ts)

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getDeduplicationMetrics } from "./metrics.ts";

/**
 * Queue Helper Interface
 * Provides standard methods for queue operations
 */
export interface QueueHelper {
  enqueue(queueName: string, message: any, idempotencyKey?: string): Promise<boolean>;
  dequeue(queueName: string, batchSize?: number, visibilityTimeoutSeconds?: number): Promise<any[]>;
  deleteMessage(queueName: string, messageId: string): Promise<boolean>;
  resetVisibility(queueName: string, messageId: string): Promise<boolean>;
  moveToDeadLetterQueue(sourceQueue: string, dlqName: string, messageId: string, reason: string): Promise<boolean>;
  sendToDLQ(queueName: string, messageId: string, message: any, reason: string, metadata?: Record<string, any>): Promise<boolean>;
}

/**
 * Create a standardized QueueHelper instance
 */
export function getQueueHelper(supabase: any, redis: Redis): QueueHelper {
  const metrics = getDeduplicationMetrics(redis);
  
  return {
    /**
     * Enqueue a message with optional idempotency checking
     */
    enqueue: async (queueName: string, message: any, idempotencyKey?: string): Promise<boolean> => {
      try {
        // If an idempotency key is provided, check if message was already sent
        if (idempotencyKey && redis) {
          try {
            const existingKey = await redis.exists(`enqueued:${queueName}:${idempotencyKey}`);
            if (existingKey === 1) {
              console.log(`Message with key ${idempotencyKey} already sent to queue ${queueName}, skipping`);
              await metrics.recordDeduplicated(queueName, "producer");
              return true; // Already sent, consider it a success
            }
          } catch (redisError) {
            console.warn(`Redis check failed for idempotency, proceeding with send: ${redisError.message}`);
          }
        }
        
        // Enqueue the message through the dedicated Edge Function
        const { error } = await supabase.functions.invoke("sendToQueue", {
          body: { queue_name: queueName, message }
        });
        
        if (error) {
          console.error(`Failed to enqueue message to ${queueName}:`, error);
          return false;
        }
        
        // If successful and idempotency key provided, mark as sent in Redis
        if (idempotencyKey && redis) {
          try {
            // Set with 24 hour expiry to avoid infinite growth
            await redis.set(`enqueued:${queueName}:${idempotencyKey}`, "true", { ex: 86400 });
          } catch (redisError) {
            console.warn(`Failed to set Redis idempotency key: ${redisError.message}`);
          }
        }
        
        // Record metric
        await metrics.recordQueueOperation(queueName, "enqueue", true);
        return true;
      } catch (error) {
        console.error(`Error in enqueue operation for ${queueName}:`, error);
        await metrics.recordQueueOperation(queueName, "enqueue", false);
        return false;
      }
    },
    
    /**
     * Dequeue messages from a queue
     */
    dequeue: async (queueName: string, batchSize = 5, visibilityTimeoutSeconds = 300): Promise<any[]> => {
      try {
        const { data, error } = await supabase.functions.invoke("readQueue", {
          body: {
            queue_name: queueName,
            batch_size: batchSize,
            visibility_timeout: visibilityTimeoutSeconds
          }
        });
        
        if (error) {
          console.error(`Error reading from queue ${queueName}:`, error);
          await metrics.recordQueueOperation(queueName, "dequeue", false);
          return [];
        }
        
        await metrics.recordQueueOperation(queueName, "dequeue", true, Array.isArray(data) ? data.length : 0);
        return data || [];
      } catch (error) {
        console.error(`Error in dequeue operation for ${queueName}:`, error);
        await metrics.recordQueueOperation(queueName, "dequeue", false);
        return [];
      }
    },
    
    /**
     * Delete a message from a queue
     */
    deleteMessage: async (queueName: string, messageId: string): Promise<boolean> => {
      try {
        const { error } = await supabase.functions.invoke("deleteFromQueue", {
          body: { queue_name: queueName, msg_id: messageId }
        });
        
        if (error) {
          console.error(`Failed to delete message ${messageId} from ${queueName}:`, error);
          await metrics.recordQueueOperation(queueName, "delete", false);
          return false;
        }
        
        await metrics.recordQueueOperation(queueName, "delete", true);
        return true;
      } catch (error) {
        console.error(`Error in delete operation for ${queueName}:`, error);
        await metrics.recordQueueOperation(queueName, "delete", false);
        return false;
      }
    },
    
    /**
     * Reset visibility timeout for a message
     */
    resetVisibility: async (queueName: string, messageId: string): Promise<boolean> => {
      try {
        const { error } = await supabase.rpc('reset_stuck_message', {
          queue_name: queueName,
          message_id: messageId
        });
        
        if (error) {
          console.error(`Failed to reset visibility for ${messageId} in ${queueName}:`, error);
          await metrics.recordQueueOperation(queueName, "reset", false);
          return false;
        }
        
        await metrics.recordQueueOperation(queueName, "reset", true);
        return true;
      } catch (error) {
        console.error(`Error in reset visibility operation for ${queueName}:`, error);
        await metrics.recordQueueOperation(queueName, "reset", false);
        return false;
      }
    },
    
    /**
     * Move a message to a dead letter queue
     */
    moveToDeadLetterQueue: async (sourceQueue: string, dlqName: string, messageId: string, reason: string): Promise<boolean> => {
      try {
        const { data, error } = await supabase.rpc('move_to_dead_letter_queue', {
          source_queue: sourceQueue,
          dlq_name: dlqName,
          message_id: messageId,
          failure_reason: reason,
          metadata: {
            moved_at: new Date().toISOString()
          }
        });
        
        if (error) {
          console.error(`Failed to move message ${messageId} to DLQ ${dlqName}:`, error);
          await metrics.recordQueueOperation(dlqName, "move_to_dlq", false);
          return false;
        }
        
        await metrics.recordQueueOperation(dlqName, "move_to_dlq", true);
        return true;
      } catch (error) {
        console.error(`Error in move to DLQ operation for ${dlqName}:`, error);
        await metrics.recordQueueOperation(dlqName, "move_to_dlq", false);
        return false;
      }
    },
    
    /**
     * Enhanced version of moveToDeadLetterQueue that handles both PGMQ format and
     * provides standardized DLQ format with error details
     */
    sendToDLQ: async (queueName: string, messageId: string, message: any, reason: string, metadata: Record<string, any> = {}): Promise<boolean> => {
      try {
        // Format for DLQ is standardized across all workers
        const dlqName = `${queueName}_dlq`;
        const dlqPayload = {
          original_queue: queueName,
          original_message_id: messageId,
          original_message: message,
          error_reason: reason,
          failure_timestamp: new Date().toISOString(),
          attempts: metadata.attempts || 1,
          metadata: {
            ...metadata,
            worker_id: Deno.env.get("WORKER_ID") || "unknown",
            moved_to_dlq_at: new Date().toISOString()
          }
        };
        
        // Enqueue to DLQ through the dedicated Edge Function
        const { data, error } = await supabase.functions.invoke("sendToQueue", {
          body: { queue_name: dlqName, message: dlqPayload }
        });
        
        if (error) {
          console.error(`Failed to send message ${messageId} to DLQ ${dlqName}:`, error);
          await metrics.recordQueueOperation(dlqName, "send_to_dlq", false);
          return false;
        }
        
        // Delete the original message
        await deleteMessageWithRetries(supabase, queueName, messageId);
        
        // Record DLQ metric
        await metrics.recordQueueOperation(dlqName, "send_to_dlq", true);
        
        // Record in worker_issues table for visibility
        await logWorkerIssue(
          supabase,
          queueName,
          "sent_to_dlq",
          `Message ${messageId} sent to DLQ ${dlqName}: ${reason}`,
          { 
            messageId, 
            originalQueue: queueName,
            dlqName,
            reason,
            timestamp: new Date().toISOString()
          }
        );
        
        return true;
      } catch (error) {
        console.error(`Error sending message ${messageId} to DLQ:`, error);
        await metrics.recordQueueOperation(`${queueName}_dlq`, "send_to_dlq", false);
        return false;
      }
    }
  };
}

// Helper for consistent worker issue logging
export async function logWorkerIssue(
  supabase: any,
  workerName: string,
  issueType: string,
  message: string,
  details?: Record<string, any>
): Promise<void> {
  try {
    await supabase.from('worker_issues').insert({
      worker_name: workerName,
      issue_type: issueType,
      message: message,
      details: details || {},
      created_at: new Date().toISOString()
    });
  } catch (error) {
    console.error(`Failed to log worker issue: ${error.message}`);
  }
}

// Helper function to delete messages with retries
export async function deleteMessageWithRetries(
  supabase: any,
  queueName: string,
  messageId: string,
  maxRetries = 3
): Promise<boolean> {
  let attempts = 0;
  while (attempts < maxRetries) {
    attempts++;
    try {
      const { data, error } = await supabase.rpc('ensure_message_deleted', {
        queue_name: queueName,
        message_id: messageId,
        max_attempts: 3
      });
      
      if (!error) {
        return true;
      }
      
      console.warn(`Delete attempt ${attempts} failed: ${error.message}`);
    } catch (err) {
      console.warn(`Delete attempt ${attempts} error: ${err.message}`);
    }
    
    // Wait with exponential backoff before retry
    if (attempts < maxRetries) {
      await new Promise(r => setTimeout(r, Math.pow(2, attempts) * 100));
    }
  }
  
  return false;
}

// Process a queue message with consistent error handling and idempotency
export async function processQueueMessageSafely(
  supabase: any,
  queueName: string,
  messageId: string,
  processorFunction: () => Promise<any>,
  idempotencyKey?: string | null,
  isDuplicateCheck?: (() => Promise<boolean>) | null,
  options: {
    maxRetries?: number;
    circuitBreaker?: boolean;
    deduplication?: {
      enabled: boolean;
      redis: Redis;
      ttlSeconds: number;
      strictMatching?: boolean;
    };
    sendToDlqOnMaxRetries?: boolean;
    dlqName?: string;
  } = {}
): Promise<any | { deduplication: true, result: any } | false> {
  const { 
    maxRetries = 1, 
    circuitBreaker = false, 
    deduplication,
    sendToDlqOnMaxRetries = false,
    dlqName
  } = options;
  
  let attempts = 0;
  
  // Check for duplication if function provided
  if (isDuplicateCheck) {
    try {
      if (await isDuplicateCheck()) {
        console.log(`Message ${messageId} already processed, skipping`);
        
        // Still delete it from the queue
        await deleteMessageWithRetries(supabase, queueName, messageId);
        
        return { deduplication: true, result: { skipped: true } };
      }
    } catch (checkError) {
      console.warn(`Error checking duplication status: ${checkError.message}`);
      // Continue processing as a fail-safe
    }
  }
  
  // Redis-based deduplication option
  if (deduplication?.enabled && idempotencyKey) {
    try {
      const key = `processed:${queueName}:${idempotencyKey}`;
      const exists = await deduplication.redis.exists(key);
      
      if (exists === 1) {
        console.log(`Redis indicates message ${messageId} already processed, skipping`);
        
        // Still delete it from the queue
        await deleteMessageWithRetries(supabase, queueName, messageId);
        
        return { deduplication: true, result: { skipped: true } };
      }
    } catch (redisError) {
      console.warn(`Redis deduplication check failed: ${redisError.message}`);
      // Continue processing as a fail-safe
    }
  }
  
  // Process with retries
  let lastError;
  while (attempts < maxRetries + 1) {
    attempts++;
    try {
      const result = await processorFunction();
      
      // Mark as processed in Redis if deduplication is enabled
      if (deduplication?.enabled && idempotencyKey) {
        try {
          const key = `processed:${queueName}:${idempotencyKey}`;
          await deduplication.redis.set(key, JSON.stringify({
            timestamp: new Date().toISOString(),
            result,
            messageId
          }), { ex: deduplication.ttlSeconds });
        } catch (redisError) {
          console.warn(`Failed to mark as processed in Redis: ${redisError.message}`);
          // Non-fatal - continue
        }
      }
      
      // Delete the message from the queue
      await deleteMessageWithRetries(supabase, queueName, messageId);
      
      return result;
    } catch (error) {
      lastError = error;
      console.error(`Processing attempt ${attempts} failed: ${error.message}`);
      
      // Check if we should open the circuit breaker
      if (circuitBreaker) {
        // Here we'd implement actual circuit breaker logic
        // For now, just log a warning
        console.warn("Circuit breaker would trigger here in production");
      }
      
      // Wait with exponential backoff before retry
      if (attempts <= maxRetries) {
        const backoffMs = Math.min(Math.pow(2, attempts) * 100, 3000);
        await new Promise(r => setTimeout(r, backoffMs));
      }
    }
  }
  
  // All retries failed
  console.error(`All ${maxRetries + 1} processing attempts failed for message ${messageId}`);
  
  // Move to DLQ if enabled and all retries failed
  if (sendToDlqOnMaxRetries) {
    const actualDlqName = dlqName || `${queueName}_dlq`;
    try {
      // Get the message data to send to DLQ
      const { data: messageData } = await supabase.rpc('get_queue_message', {
        queue_name: queueName,
        message_id: messageId
      });
      
      if (messageData) {
        // Parse the message body if it's a string
        let messageBody;
        try {
          messageBody = typeof messageData.message === 'string' ? 
            JSON.parse(messageData.message) : messageData.message;
        } catch (parseErr) {
          messageBody = messageData.message;
        }
        
        // Send to DLQ
        await supabase.functions.invoke("sendToQueue", {
          body: { 
            queue_name: actualDlqName,
            message: {
              original_queue: queueName,
              original_message_id: messageId,
              original_message: messageBody,
              error_reason: lastError?.message || "Max retries exceeded",
              failure_timestamp: new Date().toISOString(),
              attempts: maxRetries + 1,
              metadata: {
                worker_id: Deno.env.get("WORKER_ID") || "unknown",
                moved_to_dlq_at: new Date().toISOString(),
                last_error: lastError?.message,
                last_error_stack: lastError?.stack
              }
            }
          }
        });
        
        // Delete the original message after sending to DLQ
        await deleteMessageWithRetries(supabase, queueName, messageId);
        
        // Log the issue
        await logWorkerIssue(supabase, queueName, "sent_to_dlq", 
          `Message ${messageId} sent to DLQ ${actualDlqName} after ${maxRetries + 1} attempts`, 
          { lastError: lastError?.message, stack: lastError?.stack }
        );
        
        return { sentToDlq: true, queue: actualDlqName };
      }
    } catch (dlqError) {
      console.error(`Failed to send message ${messageId} to DLQ: ${dlqError.message}`);
      // Don't rethrow, we still want to log the original issue below
    }
  }
  
  await logWorkerIssue(supabase, queueName, "processing_failed", 
    `Failed to process message ${messageId} after ${maxRetries + 1} attempts`, 
    { lastError: lastError?.message, stack: lastError?.stack }
  );
  
  return false;
}

// Function to check if a track has already been processed
export async function checkTrackProcessed(redis: Redis, trackId: string): Promise<boolean> {
  try {
    return await redis.exists(`processed:track:${trackId}`) === 1;
  } catch (error) {
    console.warn(`Redis check failed for track ${trackId}:`, error);
    return false;
  }
}

// Export acquireProcessingLock function to fix the error in trackDiscovery.ts
export async function acquireProcessingLock(entityType: string, entityId: string): Promise<boolean> {
  try {
    // This is a simplified version - in a real implementation, 
    // this would use PG advisory locks or a distributed lock manager
    const lockKey = `lock:${entityType}:${entityId}`;
    
    // For now we'll return true as a placeholder
    // In a real implementation, we'd do proper locking
    return true;
  } catch (error) {
    console.error(`Error acquiring processing lock: ${error.message}`);
    return false;
  }
}
