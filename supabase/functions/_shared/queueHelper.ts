
/**
 * Queue Helper Functions
 * Provides utilities for safe queue message processing with idempotency
 */

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { DeduplicationService, getDeduplicationService } from "./deduplication.ts";
import { CoordinatedStateManager, getStateManager } from "./coordinatedStateManager.ts";
import { ProcessingState, ErrorCategory, classifyError, generateCorrelationId } from "./stateManager.ts";
import { DeduplicationMetrics, getDeduplicationMetrics } from "./metrics.ts";

// Helper function to safely format message ID as string
export function safeMessageIdString(messageId: any): string | null {
  if (messageId === undefined || messageId === null) {
    return null;
  }
  return messageId.toString();
}

// Function to safely delete a message from a queue with retries
export async function deleteMessageWithRetries(
  supabase: any,
  queueName: string,
  messageId: any,
  maxRetries = 3
): Promise<boolean> {
  // Validate messageId
  if (messageId === undefined || messageId === null) {
    console.error(`Cannot delete message with undefined or null ID from ${queueName}`);
    return false;
  }

  const messageIdStr = messageId.toString();
  let retries = 0;
  let success = false;

  while (retries < maxRetries && !success) {
    try {
      const { data, error } = await supabase.functions.invoke("deleteFromQueue", {
        body: {
          queue_name: queueName,
          message_id: messageIdStr
        }
      });

      if (error) {
        console.error(`Error deleting message ${messageIdStr} (attempt ${retries + 1}):`, error);
      } else if (data && data.success) {
        success = true;
        console.log(`Successfully deleted message ${messageIdStr} from ${queueName}`);
        break;
      } else {
        console.warn(`Failed to delete message ${messageIdStr} (attempt ${retries + 1})`);
      }
    } catch (err) {
      console.error(`Exception deleting message ${messageIdStr} (attempt ${retries + 1}):`, err);
    }

    retries++;
    if (retries < maxRetries) {
      // Exponential backoff with jitter
      const delay = Math.floor(100 * Math.pow(2, retries) * (0.8 + Math.random() * 0.4));
      console.log(`Retrying deletion in ${delay}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }

  return success;
}

// Function to log worker issues to the database
export async function logWorkerIssue(
  supabase: any,
  workerName: string,
  issueType: string,
  message: string,
  details: any = {}
): Promise<void> {
  try {
    const now = new Date().toISOString();
    const logEntry = {
      worker_name: workerName,
      issue_type: issueType,
      message: message,
      details: details,
      created_at: now,
      updated_at: now
    };

    const { error } = await supabase
      .from('worker_issues')
      .insert(logEntry);

    if (error) {
      console.error("Failed to log worker issue to database:", error);
    } else {
      console.log(`Logged worker issue: ${message}`);
    }
  } catch (error) {
    console.error("Unexpected error logging worker issue:", error);
  }
}

// Enhanced options for queue processing
export interface ProcessQueueOptions {
  maxRetries?: number;
  circuitBreaker?: boolean;
  timeoutMs?: number;
  deduplication?: {
    enabled: boolean;
    redis?: Redis;
    ttlSeconds?: number;
    strictMatching?: boolean;
  };
  stateManagement?: {
    enabled: boolean;
    entityType?: string;
    entityId?: string;
    timeoutMinutes?: number;
  };
  deadLetter?: {
    enabled: boolean;
    queue?: string;
    errorCategories?: ErrorCategory[];
  };
  correlationId?: string;
  metrics?: {
    enabled: boolean;
    redis?: Redis;
  };
  retryStrategy?: {
    baseDelayMs?: number;
    maxDelayMs?: number;
    jitterFactor?: number;
  };
}

// Return type for process queue
export interface ProcessResult {
  success: boolean;
  deduplication?: boolean;
  stateManaged?: boolean;
  deadLettered?: boolean;
  error?: string;
  errorCategory?: ErrorCategory;
  correlationId?: string;
  processingTimeMs?: number;
  retryCount?: number;
}

// Function to calculate retry delay with exponential backoff and jitter
function calculateRetryDelay(
  attempt: number, 
  baseDelay: number = 100, 
  maxDelay: number = 30000, 
  jitterFactor: number = 0.2
): number {
  // Calculate exponential backoff
  const expBackoff = Math.min(maxDelay, baseDelay * Math.pow(2, attempt));
  
  // Add jitter
  const jitter = expBackoff * jitterFactor;
  const jitterRange = jitter * 2; // Total range is +/- jitter
  const randomJitter = Math.random() * jitterRange - jitter; // Random value between -jitter and +jitter
  
  return Math.max(baseDelay, Math.floor(expBackoff + randomJitter));
}

// Create a processing lock with heartbeat
export async function acquireProcessingLock(
  entityType: string, 
  entityId: string,
  correlationId?: string,
  options: { 
    timeoutMinutes?: number;
    heartbeatIntervalSeconds?: number;
  } = {}
): Promise<boolean> {
  try {
    // Use PostgreSQL advisory lock
    const result = await fetch('pg_lock_acquire', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        entity_type: entityType,
        entity_id: entityId,
        timeout_minutes: options.timeoutMinutes || 30
      })
    });
    
    if (result.ok) {
      const data = await result.json();
      return data.acquired === true;
    }
    
    return false;
  } catch (error) {
    console.error(`Error acquiring lock for ${entityType}:${entityId}: ${error.message}`);
    return false;
  }
}

// Check if a track has been processed
export async function checkTrackProcessed(
  redis: Redis,
  trackId: string,
  processingType: string
): Promise<boolean> {
  try {
    const processedKey = `processed:${processingType}:${trackId}`;
    return await redis.exists(processedKey) === 1;
  } catch (error) {
    console.warn(`Redis check failed for ${trackId}: ${error.message}`);
    return false;
  }
}

// Send a message to a dead-letter queue
async function sendToDeadLetterQueue(
  supabase: any,
  queueName: string,
  originalMessage: any,
  error: Error,
  correlationId: string,
  originalMessageId?: string
): Promise<boolean> {
  try {
    const deadLetterQueueName = `${queueName}_dlq`;
    const errorCategory = classifyError(error);
    const deadLetterMessage = {
      originalMessage,
      error: {
        message: error.message,
        stack: error.stack,
        name: error.name,
        category: errorCategory
      },
      metadata: {
        failedAt: new Date().toISOString(),
        correlationId,
        originalQueue: queueName,
        originalMessageId
      }
    };
    
    const { error: enqueueError } = await supabase.functions.invoke("sendToQueue", {
      body: {
        queue_name: deadLetterQueueName,
        message: deadLetterMessage
      }
    });
    
    if (enqueueError) {
      console.error(`Error sending to dead-letter queue ${deadLetterQueueName}:`, enqueueError);
      return false;
    }
    
    console.log(`Message sent to dead-letter queue ${deadLetterQueueName} with correlationId ${correlationId}`);
    return true;
  } catch (enqueueError) {
    console.error(`Failed to send to dead-letter queue:`, enqueueError);
    return false;
  }
}

// Function to process a queue message with safety checks, idempotency, and deduplication
export async function processQueueMessageSafely(
  supabase: any,
  queueName: string,
  messageId: string | null,
  processingFunction: () => Promise<any>,
  idempotencyKey: string,
  alreadyProcessedCheck?: () => Promise<boolean>,
  options: ProcessQueueOptions = {}
): Promise<ProcessResult> {
  // Handle null or undefined messageId
  if (!messageId) {
    console.error(`Cannot process message with null or undefined ID for queue ${queueName}`);
    return { success: false, error: "Missing messageId" };
  }

  // Generate correlation ID if not provided
  const correlationId = options.correlationId || 
    generateCorrelationId(`corr`);
  
  console.log(`[${correlationId}] Processing message ${messageId} from queue ${queueName}`);
  
  const startTime = Date.now();
  let stateManager: CoordinatedStateManager | undefined;
  let deduplicationService: DeduplicationService | undefined;
  let metrics: DeduplicationMetrics | undefined;
  let retryCount = 0;
  
  // Result object to track what happened
  const result: ProcessResult = {
    success: false,
    correlationId,
    retryCount
  };

  // Calculate retry parameters from options
  const retryStrategy = options.retryStrategy || {};
  const baseDelayMs = retryStrategy.baseDelayMs || 100;
  const maxDelayMs = retryStrategy.maxDelayMs || 30000;
  const jitterFactor = retryStrategy.jitterFactor || 0.2;
  
  // Setup max retries
  const maxRetries = options.maxRetries || 2;

  try {
    // Initialize metrics if enabled
    if (options.metrics?.enabled && options.metrics?.redis) {
      metrics = getDeduplicationMetrics(options.metrics.redis);
    }
    
    // Initialize state management if enabled
    if (options.stateManagement?.enabled) {
      console.log(`[${correlationId}] Using state management`);
      let redisClient: Redis | undefined;
      
      if (options.deduplication?.enabled && options.deduplication.redis) {
        redisClient = options.deduplication.redis;
      }
      
      stateManager = getStateManager(supabase, redisClient);
      
      const entityType = options.stateManagement.entityType || queueName;
      const entityId = options.stateManagement.entityId || idempotencyKey;
      const timeoutMinutes = options.stateManagement.timeoutMinutes || 30;
      
      // Try to acquire a processing lock
      const lockAcquired = await stateManager.acquireProcessingLock(
        entityType, 
        entityId,
        {
          timeoutMinutes,
          correlationId
        }
      );
      
      if (!lockAcquired) {
        console.log(`[${correlationId}] Failed to acquire processing lock, skipping message ${messageId}`);
        
        // Delete the message since we're skipping it (already being processed elsewhere)
        await deleteMessageWithRetries(supabase, queueName, messageId);
        
        result.stateManaged = true;
        result.success = true; // We consider this successful since it's being handled elsewhere
        return result;
      }
      
      result.stateManaged = true;
    }
  
    // Check deduplication if enabled
    if (options.deduplication?.enabled && options.deduplication.redis) {
      console.log(`[${correlationId}] Using deduplication service`);
      const redis = options.deduplication.redis;
      deduplicationService = getDeduplicationService(redis);
      
      // Use idempotency key for deduplication
      const isDuplicate = await deduplicationService.isDuplicate(
        queueName, 
        idempotencyKey,
        { 
          ttlSeconds: options.deduplication.ttlSeconds,
          useStrictPayloadMatch: options.deduplication.strictMatching ?? true,
          logDetails: true
        },
        { correlationId }
      );
      
      if (isDuplicate) {
        console.log(`[${correlationId}] Deduplication: Skipping duplicate message ${messageId} with key ${idempotencyKey} in queue ${queueName}`);
        
        // Delete the message since it's being skipped as a duplicate
        await deleteMessageWithRetries(supabase, queueName, messageId);
        
        // Record deduplication metric if enabled
        if (metrics) {
          await metrics.recordDeduplicated(queueName, "consumer", correlationId);
        }
        
        result.deduplication = true;
        result.success = true; // We consider this successful since it's a duplicate
        return result;
      }
    }

    // Check if already processed (if check function provided)
    if (alreadyProcessedCheck) {
      try {
        const alreadyProcessed = await alreadyProcessedCheck();
        if (alreadyProcessed) {
          console.log(`[${correlationId}] Message ${messageId} with key ${idempotencyKey} was already processed, skipping`);
          
          // Still need to delete the message since it's being skipped
          await deleteMessageWithRetries(supabase, queueName, messageId);
          
          // Record as duplicate if metrics are enabled
          if (metrics) {
            await metrics.recordDeduplicated(queueName, "consumer", correlationId);
          }
          
          result.success = true;
          return result;
        }
      } catch (error) {
        console.warn(`[${correlationId}] Error checking if message ${messageId} was already processed:`, error);
        // Continue with processing since we can't be sure if it was processed
      }
    }

    // Process the message with retries and timeout
    console.log(`[${correlationId}] Executing processing function for message ${messageId}`);
    
    // Wrap the processing function with retries
    let processResult;
    let lastError: Error | null = null;
    
    for (retryCount = 0; retryCount <= maxRetries; retryCount++) {
      try {
        if (retryCount > 0) {
          console.log(`[${correlationId}] Retry attempt ${retryCount} of ${maxRetries} for message ${messageId}`);
        }
        
        // Process with timeout if specified
        if (options.timeoutMs) {
          // Process with timeout
          processResult = await Promise.race([
            processingFunction(),
            new Promise((_, reject) => 
              setTimeout(() => reject(new Error(`Processing timeout after ${options.timeoutMs}ms`)), options.timeoutMs)
            )
          ]);
        } else {
          // Process without timeout
          processResult = await processingFunction();
        }
        
        // Success! Break out of retry loop
        lastError = null;
        break;
      } catch (error) {
        lastError = error;
        const errorCategory = classifyError(error);
        result.errorCategory = errorCategory;
        
        console.error(`[${correlationId}] Error processing message ${messageId} (attempt ${retryCount + 1}):`, error);
        
        // Check if we should retry based on error category
        const isRetriable = (
          errorCategory === ErrorCategory.TRANSIENT || 
          errorCategory === ErrorCategory.UNKNOWN
        );
        
        // If this is not a retriable error or we're out of retries, break the loop
        if (!isRetriable || retryCount >= maxRetries) {
          break;
        }
        
        // Calculate backoff delay
        const delayMs = calculateRetryDelay(retryCount, baseDelayMs, maxDelayMs, jitterFactor);
        console.log(`[${correlationId}] Retrying in ${delayMs}ms...`);
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
    
    // Check if we encountered an error
    if (lastError) {
      // If state management is enabled, mark entity as failed
      if (options.stateManagement?.enabled && stateManager) {
        const entityType = options.stateManagement.entityType || queueName;
        const entityId = options.stateManagement.entityId || idempotencyKey;
        
        await stateManager.markAsFailed(
          entityType, 
          entityId, 
          lastError.message,
          {
            correlationId,
            timestamp: new Date().toISOString(),
            retries: retryCount
          }
        );
      }
      
      // Record failure metric if enabled
      if (metrics) {
        const errorType = lastError instanceof Error ? lastError.constructor.name : 'UnknownError';
        await metrics.recordFailure(queueName, "consumer", errorType, lastError.message);
      }
      
      // Send to dead-letter queue if enabled
      if (options.deadLetter?.enabled) {
        const errorCategory = classifyError(lastError);
        const eligibleCategories = options.deadLetter.errorCategories || [
          ErrorCategory.PERMANENT, 
          ErrorCategory.UNKNOWN
        ];
        
        if (eligibleCategories.includes(errorCategory)) {
          const deadLetterSuccess = await sendToDeadLetterQueue(
            supabase, 
            queueName, 
            { idempotencyKey }, // Replace with actual message content if available 
            lastError, 
            correlationId,
            messageId
          );
          
          if (deadLetterSuccess) {
            result.deadLettered = true;
            
            // Mark as DEAD_LETTER in state manager
            if (options.stateManagement?.enabled && stateManager) {
              const entityType = options.stateManagement.entityType || queueName;
              const entityId = options.stateManagement.entityId || idempotencyKey;
              
              await stateManager.markAsDeadLetter(
                entityType,
                entityId,
                lastError.message,
                {
                  correlationId,
                  deadLetteredAt: new Date().toISOString(),
                  retries: retryCount
                }
              );
            }
            
            // Delete original message since it's been moved to DLQ
            await deleteMessageWithRetries(supabase, queueName, messageId);
          }
        }
      }
      
      result.error = lastError.message;
      result.success = false;
      result.retryCount = retryCount;
      
      return result;
    }
    
    console.log(`[${correlationId}] Processing complete for message ${messageId}`);

    // Delete message from queue after successful processing
    const deleteSuccess = await deleteMessageWithRetries(supabase, queueName, messageId);
    if (!deleteSuccess) {
      console.warn(`[${correlationId}] Warning: Could not delete message ${messageId} from ${queueName} after successful processing`);
    }

    // If state management is enabled, mark entity as completed
    if (options.stateManagement?.enabled && stateManager) {
      const entityType = options.stateManagement.entityType || queueName;
      const entityId = options.stateManagement.entityId || idempotencyKey;
      
      await stateManager.markAsCompleted(entityType, entityId, {
        processedAt: new Date().toISOString(),
        result: processResult,
        correlationId
      });
    }

    // If deduplication is enabled, mark as processed to prevent future duplicates
    if (options.deduplication?.enabled && options.deduplication.redis && deduplicationService) {
      try {
        await deduplicationService.markAsProcessed(
          queueName,
          idempotencyKey,
          options.deduplication.ttlSeconds,
          { correlationId, operation: "process" }
        );
        console.log(`[${correlationId}] Marked message with key ${idempotencyKey} as processed for future deduplication`);
      } catch (e) {
        console.warn(`[${correlationId}] Failed to mark message as processed for deduplication: ${e}`);
      }
    }
    
    // Record processing metrics if enabled
    if (metrics) {
      const processingTime = Date.now() - startTime;
      await metrics.recordProcessed(queueName, "consumer", processingTime);
      result.processingTimeMs = processingTime;
    }

    result.success = true;
    result.retryCount = retryCount;
    return result;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(`[${correlationId}] Error processing message ${messageId}:`, error);
    
    // If state management is enabled, mark entity as failed
    if (options.stateManagement?.enabled && stateManager) {
      const entityType = options.stateManagement.entityType || queueName;
      const entityId = options.stateManagement.entityId || idempotencyKey;
      
      await stateManager.markAsFailed(
        entityType, 
        entityId, 
        errorMessage,
        {
          correlationId,
          timestamp: new Date().toISOString()
        }
      );
    }
    
    // Record failure metric if enabled
    if (metrics) {
      const errorType = error instanceof Error ? error.constructor.name : 'UnknownError';
      await metrics.recordFailure(queueName, "consumer", errorType, errorMessage);
    }
    
    result.error = errorMessage;
    result.success = false;
    result.retryCount = retryCount;
    
    return result;
  }
}

// Function to check if processing for an entity should be skipped
export async function checkSkipProcessing(
  supabase: any,
  redis: Redis,
  entityType: string,
  entityId: string
): Promise<boolean> {
  try {
    // Initialize state manager
    const stateManager = getStateManager(supabase, redis);
    
    // Check if entity is already processed
    const isProcessed = await stateManager.isProcessed(entityType, entityId);
    
    return isProcessed;
  } catch (error) {
    console.warn(`Error checking if processing should be skipped: ${error.message}`);
    return false;
  }
}

// Function to log processing metrics
export async function logProcessingMetrics(
  supabase: any,
  queueName: string,
  operation: string,
  results: {
    processed: number,
    success: number,
    error: number,
    details?: any
  }
): Promise<void> {
  try {
    const now = new Date().toISOString();
    
    await supabase.from('queue_metrics').insert({
      queue_name: queueName,
      operation: operation,
      started_at: now,
      finished_at: now,
      processed_count: results.processed,
      success_count: results.success,
      error_count: results.error,
      details: results.details || {}
    });
  } catch (error) {
    console.warn(`Failed to log metrics: ${error.message}`);
  }
}

// Function to requeue a message from the dead-letter queue back to the original queue
export async function requeueFromDeadLetter(
  supabase: any,
  dlqName: string,
  messageId: string
): Promise<boolean> {
  try {
    // Read message from DLQ without deleting it
    const { data: messages, error } = await supabase.functions.invoke("readQueue", {
      body: { 
        queue_name: dlqName,
        batch_size: 1,
        visibility_timeout: 60,
        message_id: messageId
      }
    });
    
    if (error || !messages || messages.length === 0) {
      console.error(`Error reading message ${messageId} from ${dlqName}:`, error || "No message found");
      return false;
    }
    
    const dlqMessage = messages[0];
    const originalMessage = dlqMessage.message.originalMessage;
    const originalQueue = dlqMessage.message.metadata?.originalQueue;
    
    if (!originalMessage || !originalQueue) {
      console.error(`Invalid DLQ message format for ${messageId}`);
      return false;
    }
    
    // Send message back to original queue
    const { error: sendError } = await supabase.functions.invoke("sendToQueue", {
      body: {
        queue_name: originalQueue,
        message: originalMessage,
        metadata: {
          requeuedAt: new Date().toISOString(),
          requeuedFrom: dlqName,
          originalDlqMessageId: messageId,
          retryAttempt: (dlqMessage.message.metadata?.retryAttempt || 0) + 1
        }
      }
    });
    
    if (sendError) {
      console.error(`Error sending message back to ${originalQueue}:`, sendError);
      return false;
    }
    
    // Delete from DLQ
    await deleteMessageWithRetries(supabase, dlqName, messageId);
    
    console.log(`Successfully requeued message ${messageId} from ${dlqName} to ${originalQueue}`);
    return true;
  } catch (error) {
    console.error(`Error requeuing message from DLQ:`, error);
    return false;
  }
}
