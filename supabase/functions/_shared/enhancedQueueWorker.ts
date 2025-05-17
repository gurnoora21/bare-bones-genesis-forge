import { createClient, SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

/**
 * Common interface for all worker configurations
 */
export interface WorkerConfig {
  queueName: string;
  batchSize?: number;
  maxBatches?: number;
  visibilityTimeoutSeconds?: number;
  timeoutSeconds?: number;
  processorName?: string;
  logDetailedMetrics?: boolean;
  sendToDlqOnMaxRetries?: boolean;
  maxRetries?: number;
  deadLetterQueue?: string;
}

/**
 * Base class for all enhanced workers
 */
export abstract class EnhancedWorkerBase {
  protected supabase: SupabaseClient;
  protected redis: Redis;
  protected queueName: string;
  protected workerId: string;
  protected lockHeartbeatInterval: number | null = null;
  protected processingLock: string | null = null;
  protected messageIds: Map<string, string> = new Map();

  constructor(queueName: string, supabase: SupabaseClient, redis: Redis) {
    this.queueName = queueName;
    this.supabase = supabase;
    this.redis = redis;
    this.workerId = `worker_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
  }

  /**
   * Main method to process a batch of messages
   */
  async processBatch(config: Partial<WorkerConfig> = {}): Promise<{
    processed: number;
    errors: number;
    duplicates: number;
    skipped: number;
    processingTimeMs: number;
    messageIds?: string[];
  }> {
    const startTime = Date.now();
    let processed = 0;
    let errors = 0;
    let duplicates = 0;
    let skipped = 0;
    const messageIds: string[] = [];
    
    const {
      batchSize = 5,
      maxBatches = 1,
      visibilityTimeoutSeconds = 300,
      timeoutSeconds = 50,
      processorName = this.queueName,
      logDetailedMetrics = false,
      sendToDlqOnMaxRetries = true,
      maxRetries = 3,
      deadLetterQueue = `${this.queueName}_dlq`
    } = config;

    try {
      // Acquire processing lock
      const lockKey = `worker:${this.queueName}:${processorName}`;
      const lockAcquired = await this.acquireLock(lockKey, timeoutSeconds + 10);
      
      if (!lockAcquired) {
        console.log(`Another worker is already processing ${this.queueName}, skipping`);
        return { processed, errors, duplicates, skipped: 1, processingTimeMs: Date.now() - startTime };
      }

      // Start heartbeat to keep lock alive
      this.startLockHeartbeat(lockKey);
      
      try {
        let batchCount = 0;
        let continueProcessing = true;
        
        while (continueProcessing && batchCount < maxBatches && (Date.now() - startTime) < timeoutSeconds * 1000) {
          batchCount++;
          
          console.log(`Starting batch ${batchCount} for processor ${processorName}`);
          
          // Read messages from queue
          const messages = await this.readFromQueue(batchSize, visibilityTimeoutSeconds);
          
          if (!messages || messages.length === 0) {
            console.log(`No messages to process in queue ${this.queueName}`);
            break;
          }
          
          console.log(`Retrieved ${messages.length} messages from queue ${this.queueName}`);
          
          // Process each message
          for (const message of messages) {
            try {
              // Ensure message is valid and extract ID - FIX: Improved message ID extraction
              const messageId = this.getMessageId(message);
              if (!messageId) {
                console.warn(`Skipping invalid message without ID:`, JSON.stringify(message).substring(0, 200));
                continue;
              }
              
              // Store message ID for acknowledgment
              this.messageIds.set(messageId, messageId);
              messageIds.push(messageId);
              
              // Check deduplication
              const dedupKey = this.getDedupKey(message);
              let isDuplicate = false;
              
              if (dedupKey) {
                try {
                  isDuplicate = await this.checkDeduplicate(dedupKey);
                } catch (dedupError) {
                  console.warn(`Error checking deduplication: ${dedupError.message}`);
                  // Continue processing even if dedup check fails
                }
              }
              
              if (isDuplicate) {
                console.log(`Skipping duplicate message: ${dedupKey}`);
                await this.deleteMessage(messageId);
                duplicates++;
                continue;
              }
              
              // FIX: Extract the actual message content - improved message extraction
              const messageContent = this.extractMessageContent(message);
              if (!messageContent) {
                console.warn(`Failed to extract message content from:`, JSON.stringify(message).substring(0, 200));
                errors++;
                continue;
              }
              
              // Process the message
              console.log(`Processing message ${messageId}`);
              const result = await this.processMessage(messageContent);
              
              // Mark deduplication if needed
              if (dedupKey) {
                try {
                  await this.markDeduplicated(dedupKey);
                } catch (markError) {
                  console.warn(`Error marking as deduplicated: ${markError.message}`);
                  // Continue even if marking fails
                }
              }
              
              // Delete message from queue
              const deleted = await this.deleteMessage(messageId);
              if (!deleted) {
                console.warn(`Failed to delete message ${messageId} after successful processing`);
              }
              
              processed++;
            } catch (error) {
              console.error(`Error processing message:`, error);
              
              // Get message ID safely (with error handling)
              let messageId: string | undefined;
              try {
                messageId = this.getMessageId(message);
              } catch (idError) {
                console.error(`Failed to get message ID for error handling:`, idError);
              }
              
              // Handle message failure with retry or DLQ
              if (messageId) {
                await this.handleMessageFailure(message, error, maxRetries, sendToDlqOnMaxRetries, deadLetterQueue);
              } else {
                console.error(`Cannot handle failure for message without ID`);
              }
              
              errors++;
            }
          }
        }
      } finally {
        // Always release lock in finally block
        await this.releaseLock();
      }
      
      // Log completion
      console.log(`Processor ${processorName} completed in ${Date.now() - startTime}ms`);
      console.log(`Total messages processed: ${processed}, errors: ${errors}, duplicates: ${duplicates}, skipped: ${skipped}`);
      
      // Save metrics
      try {
        await this.persistMetrics({
          processor: processorName,
          queue: this.queueName,
          processed,
          errors,
          duplicates,
          skipped,
          worker_id: this.workerId,
          processing_time_ms: Date.now() - startTime,
          timestamp: new Date().toISOString(),
          message_ids: logDetailedMetrics ? messageIds : undefined
        });
      } catch (metricsError) {
        console.warn(`Failed to persist metrics: ${metricsError.message}`);
      }

      return {
        processed,
        errors,
        duplicates,
        skipped,
        processingTimeMs: Date.now() - startTime,
        messageIds: logDetailedMetrics ? messageIds : undefined
      };
    } catch (batchError) {
      console.error(`Fatal error in batch processing: ${batchError.message}`);
      
      // Try to release lock in case of fatal error
      try {
        await this.releaseLock();
      } catch (lockError) {
        console.warn(`Error releasing lock after fatal error: ${lockError.message}`);
      }
      
      return { 
        processed, 
        errors: errors + 1, 
        duplicates, 
        skipped,
        processingTimeMs: Date.now() - startTime
      };
    }
  }

  /**
   * Abstract method each worker must implement to process a single message
   */
  abstract processMessage(message: any): Promise<any>;
  
  /**
   * Generate a deduplication key from message
   * Override this in worker implementations if needed
   */
  protected getDedupKey(message: any): string | null {
    if (message.message?._idempotencyKey) {
      return `${this.queueName}:${message.message._idempotencyKey}`;
    }
    return null;
  }

  /**
   * FIX: New helper method to safely extract message ID from different message formats
   */
  protected getMessageId(message: any): string | undefined {
    if (!message) return undefined;
    
    // Direct ID field - most common case
    if (message.id) return message.id.toString();
    
    // msg_id field - alternate format
    if (message.msg_id) return message.msg_id.toString();
    
    // Nested inside message.message object
    if (message.message && typeof message.message === 'object') {
      if (message.message.id) return message.message.id.toString();
      if (message.message.msg_id) return message.message.msg_id.toString();
    }
    
    // Check for messageId field - alternate naming
    if (message.messageId) return message.messageId.toString();
    
    console.warn("Unable to extract message ID from:", JSON.stringify(message).substring(0, 200));
    return undefined;
  }
  
  /**
   * FIX: New helper method to extract the actual message content
   */
  protected extractMessageContent(message: any): any {
    // If there's a message property that is an object or string, use that
    if (message.message) {
      // If it's a string, try to parse it as JSON
      if (typeof message.message === 'string') {
        try {
          return JSON.parse(message.message);
        } catch (e) {
          // If not valid JSON, return as is
          return message.message;
        }
      }
      // If already an object, return it
      return message.message;
    }
    
    // If no message property, use the whole message
    return message;
  }
  
  /**
   * Check if message has been processed before (deduplication)
   */
  protected async checkDeduplicate(dedupKey: string): Promise<boolean> {
    const exists = await this.redis.exists(dedupKey);
    return exists === 1;
  }
  
  /**
   * Mark message as processed for deduplication
   */
  protected async markDeduplicated(dedupKey: string): Promise<void> {
    await this.redis.set(dedupKey, '1', { ex: 86400 }); // 24 hour TTL
  }
  
  /**
   * Read messages from the queue
   */
  protected async readFromQueue(batchSize: number, visibilityTimeoutSeconds: number): Promise<any[]> {
    try {
      const { data, error } = await this.supabase.functions.invoke('readQueue', {
        body: {
          queue_name: this.queueName,
          batch_size: batchSize,
          visibility_timeout: visibilityTimeoutSeconds
        }
      });
      
      if (error) {
        console.error(`Error reading from queue:`, error);
        return [];
      }
      
      return data || [];
    } catch (error) {
      console.error(`Failed to read from queue:`, error);
      return [];
    }
  }
  
  /**
   * Delete a message from the queue
   */
  protected async deleteMessage(messageId: string): Promise<boolean> {
    if (!messageId) {
      console.error(`Cannot delete message: ID is undefined`);
      return false;
    }
    
    try {
      const { data, error } = await this.supabase.functions.invoke('deleteMessage', {
        body: {
          queue_name: this.queueName,
          msg_id: messageId
        }
      });
      
      if (error) {
        console.error(`Error deleting message ${messageId}:`, error);
        return false;
      }
      
      return data?.success || false;
    } catch (error) {
      console.error(`Failed to delete message ${messageId}:`, error);
      return false;
    }
  }
  
  /**
   * Send a message to the dead letter queue
   */
  protected async sendToDLQ(message: any, error: Error, dlqName: string): Promise<boolean> {
    const messageId = this.getMessageId(message);
    
    if (!messageId) {
      console.error(`Cannot send to DLQ: Message ID is undefined`);
      return false;
    }
    
    try {
      const { data, error: dlqError } = await this.supabase.functions.invoke('sendToDLQ', {
        body: {
          queue_name: this.queueName,
          dlq_name: dlqName,
          message_id: messageId,
          message: message.message, // Include original message body
          failure_reason: error.message,
          metadata: {
            worker_id: this.workerId,
            error_stack: error.stack,
            timestamp: new Date().toISOString()
          }
        }
      });
      
      if (dlqError) {
        console.error(`Error sending message ${messageId} to DLQ:`, dlqError);
        return false;
      }
      
      console.log(`Successfully sent message ${messageId} to DLQ ${dlqName}`);
      return data?.success || false;
    } catch (dlqError) {
      console.error(`Failed to send message ${messageId} to DLQ:`, dlqError);
      return false;
    }
  }
  
  /**
   * Handle a failed message with retries or DLQ
   */
  protected async handleMessageFailure(
    message: any, 
    error: Error, 
    maxRetries = 3,
    sendToDlq = true,
    dlqName = `${this.queueName}_dlq`
  ): Promise<void> {
    const messageId = this.getMessageId(message);
    const retryCount = message.read_ct || 0;
    
    if (!messageId) {
      console.error("Cannot handle message failure: Unable to extract message ID");
      return;
    }
    
    // If max retries reached, send to DLQ
    if (retryCount >= maxRetries) {
      console.log(`Message ${messageId} failed after ${retryCount} retries, sending to DLQ: ${dlqName}`);
      
      if (sendToDlq) {
        const sentToDlq = await this.sendToDLQ(message, error, dlqName);
        
        if (sentToDlq) {
          // If sent to DLQ successfully, delete from original queue
          await this.deleteMessage(messageId);
        } else {
          // If DLQ failed, release it so it can be retried after visibility timeout
          console.log(`Failed to send message ${messageId} to DLQ, will be retried later`);
        }
      } else {
        console.log(`DLQ handling disabled, message ${messageId} will remain in queue`);
      }
    } else {
      // For retryable errors, just log and let visibility timeout expire
      console.log(`Message ${messageId} failed (attempt ${retryCount + 1}/${maxRetries}), will retry later: ${error.message}`);
    }
  }
  
  /**
   * Acquire a lock for processing
   */
  protected async acquireLock(lockKey: string, ttlSeconds = 60): Promise<boolean> {
    try {
      // Try to set the lock with NX option
      const lockAcquired = await this.redis.set(
        lockKey,
        this.workerId,
        { nx: true, ex: ttlSeconds }
      );
      
      if (lockAcquired) {
        this.processingLock = lockKey;
        console.log(`Lock acquired for ${lockKey} by ${this.workerId}`);
        return true;
      }
      
      return false;
    } catch (error) {
      console.error(`Error acquiring lock:`, error);
      return false;
    }
  }
  
  /**
   * Start heartbeat to keep lock alive
   */
  protected startLockHeartbeat(lockKey: string): void {
    if (this.lockHeartbeatInterval) {
      clearInterval(this.lockHeartbeatInterval);
    }
    
    // Extend lock every 20 seconds
    this.lockHeartbeatInterval = setInterval(async () => {
      if (this.processingLock) {
        try {
          // Extend TTL by 60 seconds
          await this.redis.expire(this.processingLock, 60);
          console.log(`Extended lock TTL for ${this.processingLock}`);
        } catch (error) {
          console.error(`Error extending lock TTL:`, error);
        }
      }
    }, 20000) as unknown as number;
  }
  
  /**
   * Release lock and stop heartbeat
   */
  protected async releaseLock(): Promise<void> {
    // Stop heartbeat
    if (this.lockHeartbeatInterval) {
      clearInterval(this.lockHeartbeatInterval);
      this.lockHeartbeatInterval = null;
    }
    
    // Release lock
    if (this.processingLock) {
      try {
        // Only delete if we still own it
        await this.redis.eval(`
          if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
          else
            return 0
          end
        `, [this.processingLock], [this.workerId]);
        
        console.log(`Released lock ${this.processingLock}`);
        this.processingLock = null;
      } catch (error) {
        console.error(`Error releasing lock:`, error);
      }
    }
  }
  
  /**
   * Persist metrics for monitoring
   */
  protected async persistMetrics(metrics: any): Promise<void> {
    try {
      // First try to store in Redis for reliability
      await this.redis.set(
        `metrics:${this.queueName}:${this.workerId}:${Date.now()}`,
        JSON.stringify(metrics),
        { ex: 86400 } // 24 hour TTL
      );
      
      // FIX: Use raw_sql_query to ensure correct schema reference
      try {
        await this.supabase.rpc('raw_sql_query', {
          sql_query: `
            INSERT INTO monitoring.pipeline_metrics
            (metric_name, metric_value, tags, timestamp)
            VALUES (
              $1, 
              $2, 
              $3::jsonb, 
              NOW()
            )
          `,
          params: JSON.stringify([
            `worker_batch_${metrics.processor}`,
            metrics.processing_time_ms,
            JSON.stringify({
              queue: metrics.queue,
              processor: metrics.processor,
              processed: metrics.processed,
              errors: metrics.errors,
              worker_id: metrics.worker_id
            })
          ])
        });
      } catch (dbError) {
        // Just log the error but don't throw
        console.warn(`Database metrics insertion failed: ${dbError.message}`);
      }
    } catch (error) {
      // Log but don't throw so processing can continue
      console.warn(`Error persisting metrics:`, error);
    }
  }
  
  /**
   * Shutdown the worker cleanly
   */
  async shutdown(): Promise<void> {
    await this.releaseLock();
  }
}

/**
 * Factory function to create an enhanced worker
 */
export function createEnhancedWorker(queueName: string, supabase: SupabaseClient, redis: Redis) {
  return class extends EnhancedWorkerBase {
    constructor() {
      super(queueName, supabase, redis);
    }
    
    async processMessage(message: any): Promise<any> {
      throw new Error("Worker must implement processMessage method");
    }
  };
}
