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
      logDetailedMetrics = false
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
            // Store message ID for acknowledgment
            if (message.id) {
              const messageId = message.id.toString();
              this.messageIds.set(messageId, messageId);
              messageIds.push(messageId);
            }
            
            // Check deduplication
            const dedupKey = this.getDedupKey(message);
            if (dedupKey) {
              const isDuplicate = await this.checkDeduplicate(dedupKey);
              if (isDuplicate) {
                console.log(`Skipping duplicate message: ${dedupKey}`);
                await this.deleteMessage(message.id);
                duplicates++;
                continue;
              }
            }
            
            // Process the message
            const result = await this.processMessage(message.message);
            
            // Mark deduplication if needed
            if (dedupKey) {
              await this.markDeduplicated(dedupKey);
            }
            
            // Delete message from queue
            await this.deleteMessage(message.id);
            
            processed++;
          } catch (error) {
            console.error(`Error processing message ${message.id}:`, error);
            
            // Handle message failure with retry or DLQ
            await this.handleMessageFailure(message, error);
            errors++;
          }
        }
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
        console.error(`Failed to persist metrics:`, metricsError);
      }

      return {
        processed,
        errors,
        duplicates,
        skipped,
        processingTimeMs: Date.now() - startTime,
        messageIds: logDetailedMetrics ? messageIds : undefined
      };
    } finally {
      // Release lock and stop heartbeat
      await this.releaseLock();
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
  protected async sendToDLQ(message: any, error: Error): Promise<boolean> {
    const dlqName = `${this.queueName}_dlq`;
    const messageId = message.id?.toString();
    
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
      
      return data?.success || false;
    } catch (dlqError) {
      console.error(`Failed to send message ${messageId} to DLQ:`, dlqError);
      return false;
    }
  }
  
  /**
   * Handle a failed message with retries or DLQ
   */
  protected async handleMessageFailure(message: any, error: Error, maxRetries = 3): Promise<void> {
    const messageId = message.id?.toString();
    const retryCount = message.read_ct || 0;
    
    // If max retries reached, send to DLQ
    if (retryCount >= maxRetries) {
      console.log(`Message ${messageId} failed after ${retryCount} retries, sending to DLQ: ${this.queueName}_dlq`);
      const sentToDlq = await this.sendToDLQ(message, error);
      
      if (sentToDlq) {
        // If sent to DLQ successfully, delete from original queue
        await this.deleteMessage(messageId);
      } else {
        // If DLQ failed, release it so it can be retried after visibility timeout
        // Note: We intentionally don't delete here to allow for manual intervention
        console.log(`Failed to send message ${messageId} to DLQ, will be retried later`);
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
      // Store metrics in database or redis for monitoring
      await this.redis.set(
        `metrics:${this.queueName}:${this.workerId}:${Date.now()}`,
        JSON.stringify(metrics),
        { ex: 86400 } // 24 hour TTL
      );
    } catch (error) {
      console.error(`Error persisting metrics:`, error);
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
