/**
 * Idempotent Worker Base Implementation
 * Base class for workers that need to process messages idempotently
 * Now uses PostgreSQL advisory locks for reliable distributed locking
 */
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { EnhancedStateManager, getStateManager } from "./enhancedStateManager.ts";
import { getTransactionManager } from "./transactionManager.ts";
import { getIdempotencyManager } from "./idempotencyManager.ts";

export interface ProcessOptions {
  batchSize?: number;
  timeoutSeconds?: number;
  processorName: string;
  visibilityTimeoutSeconds?: number;
}

export class IdempotentWorker<T = any> {
  protected supabase: any;
  protected queue: string;
  protected redis: Redis | null = null;
  protected stateManager: EnhancedStateManager;
  protected workerId: string;

  constructor(queue: string, supabase?: any, redis?: Redis) {
    this.queue = queue;
    
    if (supabase) {
      this.supabase = supabase;
    } else {
      this.supabase = createClient(
        Deno.env.get("SUPABASE_URL") || "",
        Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || ""
      );
    }
    
    this.redis = redis || null;
    
    // Initialize state manager with Redis used only for caching
    // PostgreSQL advisory locks are the source of truth
    this.stateManager = getStateManager(
      this.supabase, 
      this.redis,
      // Use Redis only for caching, not for locking
      this.redis ? true : false
    );
    
    // Generate a unique worker ID for this instance
    this.workerId = `worker_${this.queue}_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
  }

  /**
   * Safely process a batch of messages from the queue with idempotency
   */
  async processBatch(options: ProcessOptions): Promise<{ 
    processed: number, 
    errors: number, 
    messages: any[] 
  }> {
    const {
      batchSize = 5,
      timeoutSeconds = 60,
      processorName = 'generic',
      visibilityTimeoutSeconds = 30
    } = options;
    
    const batchId = `batch_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
    console.log(`[${batchId}] Starting ${processorName} worker`);
    
    try {
      // 1. Read messages from the queue using transaction to ensure atomicity
      const transactionManager = getTransactionManager(this.supabase);
      
      // Using DB function to dequeue atomically
      const messagesResult = await transactionManager.atomicOperation('pg_dequeue', {
        queue_name: this.queue,
        batch_size: batchSize,
        visibility_timeout: visibilityTimeoutSeconds
      }, {
        operationId: `dequeue_${batchId}`,
        entityType: 'queue_read',
        entityId: this.queue
      });
      
      const parsedMessages = JSON.parse(messagesResult || '[]');
      console.log(`[${batchId}] Retrieved ${parsedMessages.length} messages from queue`);
      
      if (!parsedMessages.length) {
        return { processed: 0, errors: 0, messages: [] };
      }
      
      // 2. Process each message
      let processed = 0;
      let errors = 0;
      const results = [];
      
      for (const message of parsedMessages) {
        try {
          const messageId = message.id;
          console.log(`[${batchId}] Processing message ${messageId}`);
          
          // Parse the message body
          let body: any;
          try {
            body = typeof message.message === 'string' 
              ? JSON.parse(message.message) 
              : message.message;
          } catch (parseError) {
            console.error(`[${batchId}] Failed to parse message body: ${parseError.message}`);
            await this.handleFailedMessage(messageId, "Invalid message format", message);
            errors++;
            continue;
          }
          
          // Extract idempotency key or generate one
          const idempotencyKey = body._idempotencyKey || 
            `${this.queue}:${JSON.stringify(body)}`;
          
          // Process the message with idempotency and transaction safety
          const idempotencyManager = getIdempotencyManager(this.supabase);
          const idempotencyResult = await idempotencyManager.execute(
            {
              operationId: idempotencyKey,
              entityType: this.queue,
              entityId: messageId
            },
            async () => {
              // Acquire lock to ensure exclusive processing
              const lockAcquired = await this.stateManager.acquireProcessingLock(
                this.queue, 
                idempotencyKey,
                {
                  timeoutSeconds: 1, // Non-blocking
                  correlationId: `${batchId}_${messageId}`
                }
              );
              
              if (!lockAcquired) {
                console.warn(`[${batchId}] Already processing ${idempotencyKey}, skipping`);
                throw new Error("Already being processed by another worker");
              }
              
              try {
                // Process the message with timeout protection and transaction
                return await transactionManager.transaction(
                  async () => {
                    // Set up a timeout promise
                    const timeoutPromise = new Promise((_, reject) => {
                      setTimeout(() => reject(new Error(`Processing timed out after ${timeoutSeconds}s`)), 
                        timeoutSeconds * 1000);
                    });
                    
                    // Process with timeout
                    return await Promise.race([
                      this.processMessage(body),
                      timeoutPromise
                    ]);
                  },
                  {
                    timeout: timeoutSeconds * 1000,
                    correlationId: `${batchId}_${messageId}`,
                    retryOnConflict: true
                  }
                );
              } finally {
                // Always release the lock when done, even if there was an error
                await this.stateManager.releaseLock(this.queue, idempotencyKey);
              }
            }
          );
          
          // Handle the result
          if (idempotencyResult.status === 'success') {
            // Success - delete the message from the queue
            await this.supabase.rpc('ensure_message_deleted', {
              queue_name: this.queue,
              message_id: messageId
            });
            
            processed++;
            results.push({ 
              messageId, 
              success: true, 
              result: idempotencyResult.result,
              alreadyProcessed: idempotencyResult.alreadyProcessed
            });
            console.log(`[${batchId}] Successfully processed message ${messageId}`);
          } else {
            // Error during processing
            console.error(`[${batchId}] Error processing message ${messageId}: ${idempotencyResult.error}`);
            errors++;
            results.push({ 
              messageId, 
              success: false, 
              error: idempotencyResult.error
            });
          }
        } catch (error) {
          console.error(`[${batchId}] Unexpected error handling message: ${error.message}`);
          errors++;
        }
      }
      
      return { processed, errors, messages: results };
    } catch (batchError) {
      console.error(`[${batchId}] Batch processing error: ${batchError.message}`);
      return { processed: 0, errors: 1, messages: [] };
    }
  }
  
  /**
   * To be implemented by subclasses
   */
  // @ts-ignore: Abstract method
  async processMessage(message: T): Promise<any> {
    throw new Error("Method not implemented");
  }
  
  /**
   * Handle a failed message
   */
  private async handleFailedMessage(
    messageId: string, 
    errorMessage: string, 
    rawMessage: any
  ): Promise<void> {
    try {
      // Record the problematic message
      await this.supabase.rpc('record_problematic_message', {
        p_queue_name: this.queue,
        p_message_id: messageId,
        p_message_body: rawMessage,
        p_error_type: 'parsing_error',
        p_error_details: errorMessage
      });
      
      // Delete the message from the queue
      await this.supabase.rpc('ensure_message_deleted', {
        queue_name: this.queue,
        message_id: messageId,
        max_attempts: 3
      });
    } catch (error) {
      console.error(`Failed to handle problematic message: ${error.message}`);
    }
  }
}
