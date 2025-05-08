
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { DeduplicationService, getDeduplicationService } from "./deduplication.ts";
import { StateManager, EntityType, ProcessingState } from "./stateManager.ts";

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
  // FIX: Add messageId validation
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

// Function to acquire a distributed processing lock
export async function acquireProcessingLock(
  lockType: string,
  resourceKey: string,
  ttlSeconds: number = 300
): Promise<boolean> {
  const redis = new Redis({
    url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
    token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
  });

  const lockKey = `lock:${lockType}:${resourceKey}`;
  const lockValue = new Date().toISOString();

  try {
    // Acquire lock using SET NX command
    const result = await redis.set(lockKey, lockValue, {
      ex: ttlSeconds,
      nx: true,
    });

    if (result === "OK") {
      console.log(`Acquired lock ${lockKey}`);
      return true;
    } else {
      console.warn(`Failed to acquire lock ${lockKey}`);
      return false;
    }
  } catch (error) {
    console.error(`Error acquiring lock ${lockKey}:`, error);
    return false;
  }
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
      timestamp: now,
      worker_name: workerName,
      issue_type: issueType,
      message: message,
      details: details
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
    entityType?: EntityType | string;
    entityId?: string;
    timeoutMinutes?: number;
  };
  correlationId?: string;
}

// Return type for process queue with more details
export interface ProcessResult {
  success: boolean;
  deduplication?: boolean;
  stateManaged?: boolean;
  error?: string;
  correlationId?: string;
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
  
  let stateManager: StateManager | undefined;
  let deduplicationService: DeduplicationService | undefined;
  
  // Result object to track what happened
  const result: ProcessResult = {
    success: false,
    correlationId
  };

  try {
    // Initialize state management if enabled
    if (options.stateManagement?.enabled) {
      console.log(`[${correlationId}] Using state management`);
      stateManager = new StateManager(supabase);
      
      const entityType = options.stateManagement.entityType || queueName;
      const entityId = options.stateManagement.entityId || idempotencyKey;
      const timeoutMinutes = options.stateManagement.timeoutMinutes || 30;
      
      // Try to acquire a processing lock
      const lockAcquired = await stateManager.acquireProcessingLock(
        entityType, 
        entityId,
        timeoutMinutes
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
          useStrictPayloadMatch: options.deduplication.strictMatching ?? true
        }
      );
      
      if (isDuplicate) {
        console.log(`[${correlationId}] Deduplication: Skipping duplicate message ${messageId} with key ${idempotencyKey} in queue ${queueName}`);
        
        // Delete the message since it's being skipped as a duplicate
        await deleteMessageWithRetries(supabase, queueName, messageId);
        
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
          
          result.success = true;
          return result;
        }
      } catch (error) {
        console.warn(`[${correlationId}] Error checking if message ${messageId} was already processed:`, error);
        // Continue with processing since we can't be sure if it was processed
      }
    }

    // Process the message
    console.log(`[${correlationId}] Executing processing function for message ${messageId}`);
    const processResult = await processingFunction();
    console.log(`[${correlationId}] Processing complete for message ${messageId}, result:`, processResult);

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
          options.deduplication.ttlSeconds
        );
        console.log(`[${correlationId}] Marked message with key ${idempotencyKey} as processed for future deduplication`);
      } catch (e) {
        console.warn(`[${correlationId}] Failed to mark message as processed for deduplication: ${e}`);
      }
    }

    result.success = true;
    return result;
  } catch (error) {
    console.error(`[${correlationId}] Error processing message ${messageId}:`, error);
    
    // If state management is enabled, mark entity as failed
    if (options.stateManagement?.enabled && stateManager) {
      const entityType = options.stateManagement.entityType || queueName;
      const entityId = options.stateManagement.entityId || idempotencyKey;
      
      await stateManager.markAsFailed(
        entityType, 
        entityId, 
        error instanceof Error ? error.message : String(error),
        {
          correlationId,
          timestamp: new Date().toISOString()
        }
      );
    }
    
    result.error = error instanceof Error ? error.message : String(error);
    result.success = false;
    
    // Don't try to delete the message on error, let it return to the queue
    return result;
  }
}
