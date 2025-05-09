
/**
 * Queue Helper Functions
 * Provides utilities for safe queue message processing with idempotency
 */

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { DeduplicationService, getDeduplicationService } from "./deduplication.ts";
import { CoordinatedStateManager, getStateManager } from "./coordinatedStateManager.ts";
import { ProcessingState } from "./stateManager.ts";
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
  correlationId?: string;
  metrics?: {
    enabled: boolean;
    redis?: Redis;
  };
}

// Return type for process queue
export interface ProcessResult {
  success: boolean;
  deduplication?: boolean;
  stateManaged?: boolean;
  error?: string;
  correlationId?: string;
  processingTimeMs?: number;
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
    `corr_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
  
  console.log(`[${correlationId}] Processing message ${messageId} from queue ${queueName}`);
  
  const startTime = Date.now();
  let stateManager: CoordinatedStateManager | undefined;
  let deduplicationService: DeduplicationService | undefined;
  let metrics: DeduplicationMetrics | undefined;
  
  // Result object to track what happened
  const result: ProcessResult = {
    success: false,
    correlationId
  };

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

    // Process the message with timeout if specified
    console.log(`[${correlationId}] Executing processing function for message ${messageId}`);
    
    let processResult;
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
