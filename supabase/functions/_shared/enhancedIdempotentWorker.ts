/**
 * EnhancedIdempotentWorker - High-performance worker with PostgreSQL-backed queue
 * 
 * Provides reliable queue processing with atomicity, idempotency, and proper error handling
 */
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { getQueueManager, PgQueueManager } from "./pgQueueManager.ts";
import { getIdempotencyManager } from "./idempotencyManager.ts";
import { getTransactionManager } from "./transactionManager.ts";

export interface ProcessOptions {
  batchSize?: number;
  timeoutSeconds?: number;
  processorName: string;
  visibilityTimeoutSeconds?: number;
  maxRetries?: number;
  retryDelaySec?: number;
}

export class EnhancedIdempotentWorker<T = any> {
  protected supabase: any;
  protected queue: string;
  protected queueManager: PgQueueManager;
  protected workerId: string;

  constructor(queue: string, supabase?: any) {
    this.queue = queue;
    
    if (supabase) {
      this.supabase = supabase;
    } else {
      this.supabase = createClient(
        Deno.env.get("SUPABASE_URL") || "",
        Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || ""
      );
    }
    
    // Initialize queue manager for atomic operations
    this.queueManager = getQueueManager(this.supabase);
    
    // Generate a unique worker ID for this instance
    this.workerId = `worker_${this.queue}_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
  }

  /**
   * Safely process a batch of messages from the queue with idempotency
   * using atomic operations and visibility timeouts
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
      visibilityTimeoutSeconds = 120, // longer than timeoutSeconds to avoid premature retries
      maxRetries = 3
    } = options;
    
    const batchId = `batch_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
    console.log(`[${batchId}] Starting ${processorName} worker`);
    
    try {
      // 1. Read messages atomically from the queue with visibility timeout
      const messages = await this.queueManager.readMessages(this.queue, {
        batchSize,
        visibilityTimeoutSeconds
      });
      
      console.log(`[${batchId}] Retrieved ${messages.length} messages from queue ${this.queue}`);
      
      if (!messages.length) {
        return { processed: 0, errors: 0, messages: [] };
      }
      
      // 2. Process each message with idempotency and transaction safety
      let processed = 0;
      let errors = 0;
      const results = [];
      
      for (const queueMsg of messages) {
        try {
          const messageId = queueMsg.id || queueMsg.msg_id;
          console.log(`[${batchId}] Processing message ${messageId}`);
          
          // Parse the message body
          let body: any;
          try {
            if (typeof queueMsg.message === 'string') {
              body = JSON.parse(queueMsg.message);
            } else {
              body = queueMsg.message;
            }
          } catch (parseError) {
            console.error(`[${batchId}] Failed to parse message body: ${parseError.message}`);
            
            // Record problematic message in database for later analysis
            await this.recordProblematicMessage(messageId, "Invalid message format", queueMsg);
            
            // Delete the malformed message to avoid endless loop of errors
            await this.queueManager.deleteMessage(this.queue, messageId);
            
            errors++;
            results.push({ 
              messageId, 
              success: false, 
              error: "Invalid message format" 
            });
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
              // Process the message with timeout protection using transaction manager
              const transactionManager = getTransactionManager(this.supabase);
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
                  retryOnConflict: true,
                  maxRetries
                }
              );
            }
          );
          
          // Handle the result
          if (idempotencyResult.status === 'success') {
            // Success - delete the message from the queue
            const deleteSuccess = await this.queueManager.deleteMessage(this.queue, messageId);
            
            if (!deleteSuccess) {
              console.warn(`[${batchId}] Warning: Message ${messageId} processed successfully but could not be deleted from queue`);
            }
            
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
            
            // If we have a system-level error (not application logic error), we leave the message in the queue
            // The visibility timeout will expire and another worker will retry it
          }
        } catch (error) {
          console.error(`[${batchId}] Unexpected error handling message: ${error.message}`);
          errors++;
          results.push({
            success: false,
            error: error.message
          });
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
   * Record a problematic message for later analysis
   */
  private async recordProblematicMessage(
    messageId: string, 
    errorMessage: string, 
    rawMessage: any
  ): Promise<void> {
    try {
      // Record the problematic message in a dedicated table
      await this.supabase.rpc('record_problematic_message', {
        p_queue_name: this.queue,
        p_message_id: messageId,
        p_message_body: rawMessage,
        p_error_type: 'parsing_error',
        p_error_details: errorMessage
      });
    } catch (error) {
      console.error(`Failed to record problematic message: ${error.message}`);
    }
  }
}
