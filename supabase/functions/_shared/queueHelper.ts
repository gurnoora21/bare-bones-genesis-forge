import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

// Helper function to safely format message ID as string
export function safeMessageIdString(messageId: any): string | null {
  if (messageId === undefined || messageId === null) {
    return null;
  }
  return messageId.toString();
}

// Function to check if a track has already been processed
export async function checkTrackProcessed(
  redis: any, 
  trackId: string,
  queueName: string = "general" // Added queue name parameter with default
): Promise<boolean> {
  if (!redis) {
    console.warn("Redis client is not initialized, skipping Redis check.");
    return false;
  }

  try {
    const key = `processed:${queueName}:${trackId}`;
    return await redis.exists(key) === 1;
  } catch (error) {
    console.error(`Redis check failed for track ${trackId}:`, error);
    return false;
  }
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

// Function to process a queue message with safety checks and idempotency
export async function processQueueMessageSafely(
  supabase: any,
  queueName: string,
  messageId: string | null, // FIX: Allow null messageId
  processingFunction: () => Promise<any>,
  idempotencyKey: string,
  alreadyProcessedCheck?: () => Promise<boolean>,
  options: {
    maxRetries?: number;
    circuitBreaker?: boolean;
    timeoutMs?: number;
  } = {}
): Promise<boolean> {
  // FIX: Handle null or undefined messageId
  if (!messageId) {
    console.error(`Cannot process message with null or undefined ID for queue ${queueName}`);
    return false;
  }

  try {
    // Check if already processed (if check function provided)
    if (alreadyProcessedCheck) {
      try {
        const alreadyProcessed = await alreadyProcessedCheck();
        if (alreadyProcessed) {
          console.log(`Message ${messageId} with key ${idempotencyKey} was already processed, skipping`);
          // Still need to delete the message since it's being skipped
          await deleteMessageWithRetries(supabase, queueName, messageId);
          return true;
        }
      } catch (error) {
        console.warn(`Error checking if message ${messageId} was already processed:`, error);
        // Continue with processing since we can't be sure if it was processed
      }
    }

    // Process the message
    const result = await processingFunction();
    console.log(`Processing complete for message ${messageId}, result:`, result);

    // Delete message from queue after successful processing
    const deleteSuccess = await deleteMessageWithRetries(supabase, queueName, messageId);
    if (!deleteSuccess) {
      console.warn(`Warning: Could not delete message ${messageId} from ${queueName} after successful processing`);
    }

    return true;
  } catch (error) {
    console.error(`Error processing message ${messageId}:`, error);
    
    // Don't try to delete the message on error, let it return to the queue
    return false;
  }
}
