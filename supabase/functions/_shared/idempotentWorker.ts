
/**
 * Idempotent Worker Base Class
 * 
 * Provides standardized structure for implementing idempotent workers
 * with safe retries and proper transaction management.
 */

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { TransactionManager, getTransactionManager } from "./transactionManager.ts";
import { EnhancedDeduplicationService, getEnhancedDeduplication } from "./enhancedDeduplication.ts";
import { getEnhancedStateManager } from "./enhancedStateManager.ts";
import { ProcessingState, EntityType, StateTransitionResult } from "./stateManager.ts";
import { createEnhancedError, ErrorCategory, ErrorSource } from "./errorHandling.ts";

export interface IdempotentWorkerOptions {
  queueName: string;
  deadLetterQueueName?: string;
  batchSize?: number;
  visibilityTimeoutSeconds?: number;
  maxRetryAttempts?: number;
  useTransactions?: boolean;
  allowPartialFailures?: boolean;
  recordProcessingMetrics?: boolean;
}

export interface WorkerContext {
  correlationId: string;
  batchId: string;
  messageId: string;
  attempt?: number;
}

export interface ProcessingResult {
  success: boolean;
  isDuplicate?: boolean;
  error?: Error;
  metadata?: Record<string, any>;
}

export abstract class IdempotentWorker<TMessage, TResult> {
  protected supabase: any;
  protected redis: Redis;
  protected deduplication: EnhancedDeduplicationService;
  protected stateManager: any;
  protected transactionManager: TransactionManager;
  protected options: IdempotentWorkerOptions;
  
  constructor(options: IdempotentWorkerOptions) {
    this.options = {
      batchSize: 5,
      visibilityTimeoutSeconds: 180,
      maxRetryAttempts: 3,
      useTransactions: true,
      allowPartialFailures: false,
      recordProcessingMetrics: true,
      ...options
    };
    
    // Initialize clients
    this.supabase = createClient(
      Deno.env.get("SUPABASE_URL") || "",
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || ""
    );
    
    this.redis = new Redis({
      url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
      token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
    });
    
    // Initialize helpers
    this.deduplication = getEnhancedDeduplication(this.redis, this.supabase);
    this.stateManager = getEnhancedStateManager(this.supabase, this.redis, true);
    this.transactionManager = getTransactionManager(this.supabase);
  }
  
  /**
   * Main processing entry point - called when the worker is invoked
   */
  async process(request: Request): Promise<Response> {
    // Generate batch ID for correlation
    const batchId = `batch_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
    console.log(`[${batchId}] Starting ${this.options.queueName} worker`);
    
    try {
      // Read messages from queue
      const { data: messages, error } = await this.supabase.rpc('pg_dequeue', { 
        queue_name: this.options.queueName,
        batch_size: this.options.batchSize, 
        visibility_timeout: this.options.visibilityTimeoutSeconds
      });
      
      if (error) {
        console.error(`[${batchId}] Error reading from queue:`, error);
        return new Response(JSON.stringify({ error: error.message }), { status: 500 });
      }
      
      // Parse and validate messages
      const parsedMessages = this.parseMessages(messages, batchId);
      
      console.log(`[${batchId}] Retrieved ${parsedMessages.length} messages from queue`);
      
      if (!parsedMessages || parsedMessages.length === 0) {
        return new Response(
          JSON.stringify({ processed: 0, message: "No messages to process" }),
          { status: 200 }
        );
      }
      
      // Create response for immediate return
      const response = new Response(
        JSON.stringify({ 
          processing: true, 
          message_count: parsedMessages.length,
          batch_id: batchId
        }),
        { status: 200 }
      );
      
      // Process messages in background
      EdgeRuntime.waitUntil(this.processBatch(parsedMessages, batchId));
      
      return response;
    } catch (mainError) {
      console.error(`[${batchId}] Critical error in worker:`, mainError);
      
      // Record error if metrics are enabled
      if (this.options.recordProcessingMetrics) {
        try {
          await this.supabase.from('worker_issues').insert({
            worker_name: this.options.queueName,
            issue_type: 'fatal_error',
            message: mainError.message,
            details: { error: mainError.message, stack: mainError.stack }
          });
        } catch (metricsError) {
          console.error(`[${batchId}] Failed to record error metrics:`, metricsError);
        }
      }
      
      return new Response(JSON.stringify({ error: mainError.message }), { 
        status: 500
      });
    }
  }
  
  /**
   * Process a batch of messages with proper error handling and idempotency
   */
  private async processBatch(messages: Array<{id: string, message: TMessage}>, batchId: string): Promise<void> {
    // Track batch statistics
    let successCount = 0;
    let duplicateCount = 0;
    let errorCount = 0;
    let deadLetteredCount = 0;
    
    // Process each message
    for (const message of messages) {
      const messageId = message.id?.toString();
      const correlationId = `${batchId}:msg_${messageId}`;
      
      console.log(`[${correlationId}] Processing message ${messageId}`);
      
      try {
        // Validate message
        if (!this.validateMessage(message.message)) {
          console.error(`[${correlationId}] Message validation failed`);
          await this.handleInvalidMessage(message, correlationId);
          errorCount++;
          continue;
        }
        
        // Extract entity information
        const entityInfo = this.extractEntityInfo(message.message);
        const entityType = entityInfo?.entityType;
        const entityId = entityInfo?.entityId;
        
        if (entityType && entityId) {
          // Check if already processed first using deduplication service
          const dedupContext = {
            correlationId,
            entityType,
            entityId,
            operation: this.options.queueName
          };
          
          const dedupKey = `${entityType}:${entityId}:${this.options.queueName}`;
          const dedupResult = await this.deduplication.checkDuplicate(
            dedupKey,
            { namespace: this.options.queueName },
            dedupContext
          );
          
          if (dedupResult.isDuplicate) {
            console.log(`[${correlationId}] Duplicate detected from ${dedupResult.source}, skipping`);
            
            // Delete message from queue
            await this.deleteMessage(messageId);
            
            duplicateCount++;
            continue;
          }
          
          // Try to acquire processing lock
          const lockAcquired = await this.stateManager.acquireProcessingLock(
            entityType,
            entityId,
            {
              correlationId,
              heartbeatIntervalSeconds: 15
            }
          );
          
          if (!lockAcquired) {
            console.log(`[${correlationId}] Could not acquire lock for ${entityType}:${entityId}, skipping`);
            errorCount++;
            continue;
          }
          
          // Process the message with transaction if enabled
          try {
            let result: ProcessingResult;
            
            if (this.options.useTransactions) {
              // Process with transaction
              result = await this.transactionManager.transaction(
                async (client: any, txId: string) => {
                  return await this.processMessage(message.message, {
                    correlationId,
                    batchId,
                    messageId,
                    attempt: 1
                  });
                },
                { correlationId }
              );
            } else {
              // Process without transaction
              result = await this.processMessage(message.message, {
                correlationId,
                batchId,
                messageId,
                attempt: 1
              });
            }
            
            if (result.success) {
              // Mark as completed in state manager
              await this.stateManager.markAsCompleted(entityType, entityId, {
                correlationId,
                completedBy: this.options.queueName,
                result: result.metadata
              });
              
              // Mark as processed in deduplication service
              await this.deduplication.markProcessed(
                dedupKey,
                { namespace: this.options.queueName, recordResult: true },
                dedupContext,
                result.metadata
              );
              
              // Delete message from queue
              await this.deleteMessage(messageId);
              
              successCount++;
            } else {
              // Handle process failure
              if (result.error) {
                await this.handleProcessingError(
                  message, 
                  result.error, 
                  correlationId,
                  entityType,
                  entityId
                );
              }
              errorCount++;
            }
          } catch (processError) {
            console.error(`[${correlationId}] Error processing ${entityType}:${entityId}:`, processError);
            
            // Handle processing error
            await this.handleProcessingError(
              message, 
              processError, 
              correlationId,
              entityType,
              entityId
            );
            
            errorCount++;
          }
        } else {
          // No entity info - use simpler processing
          try {
            // Generate idempotency key from message content
            const idempotencyKey = this.createIdempotencyKey(message.message);
            
            // Check if already processed
            const dedupResult = await this.deduplication.checkDuplicate(
              idempotencyKey,
              { namespace: this.options.queueName },
              { correlationId, operation: this.options.queueName }
            );
            
            if (dedupResult.isDuplicate) {
              console.log(`[${correlationId}] Duplicate detected from ${dedupResult.source}, skipping`);
              
              // Delete message from queue
              await this.deleteMessage(messageId);
              
              duplicateCount++;
              continue;
            }
            
            // Process the message
            let result: ProcessingResult;
            
            if (this.options.useTransactions) {
              // Process with transaction
              result = await this.transactionManager.transaction(
                async (client: any, txId: string) => {
                  return await this.processMessage(message.message, {
                    correlationId,
                    batchId,
                    messageId,
                    attempt: 1
                  });
                },
                { correlationId }
              );
            } else {
              // Process without transaction
              result = await this.processMessage(message.message, {
                correlationId,
                batchId,
                messageId,
                attempt: 1
              });
            }
            
            if (result.success) {
              // Mark as processed in deduplication service
              await this.deduplication.markProcessed(
                idempotencyKey,
                { namespace: this.options.queueName, recordResult: true },
                { correlationId, operation: this.options.queueName },
                result.metadata
              );
              
              // Delete message from queue
              await this.deleteMessage(messageId);
              
              successCount++;
            } else {
              // Handle process failure
              if (result.error) {
                await this.handleProcessingError(
                  message, 
                  result.error, 
                  correlationId
                );
              }
              errorCount++;
            }
          } catch (processError) {
            console.error(`[${correlationId}] Error processing message:`, processError);
            
            // Handle processing error
            await this.handleProcessingError(
              message, 
              processError, 
              correlationId
            );
            
            errorCount++;
          }
        }
      } catch (messageError) {
        console.error(`[${correlationId}] Uncaught error processing message:`, messageError);
        
        try {
          // Send to dead letter queue as fallback
          if (this.options.deadLetterQueueName) {
            await this.sendToDeadLetterQueue(
              message,
              messageError,
              correlationId,
              "uncaught_error"
            );
            
            // Delete from original queue
            await this.deleteMessage(messageId);
            
            deadLetteredCount++;
          }
        } catch (dlqError) {
          console.error(`[${correlationId}] Failed to send to dead letter queue:`, dlqError);
        }
        
        errorCount++;
      }
    }
    
    // Log batch results
    console.log(
      `[${batchId}] Batch processing complete: ` +
      `${successCount} successful, ${errorCount} failed, ` +
      `${duplicateCount} duplicates, ${deadLetteredCount} dead-lettered`
    );
    
    // Record metrics if enabled
    if (this.options.recordProcessingMetrics) {
      try {
        await this.supabase.from('queue_metrics').insert({
          queue_name: this.options.queueName,
          operation: "batch_processing",
          started_at: new Date().toISOString(),
          finished_at: new Date().toISOString(),
          processed_count: messages.length,
          success_count: successCount,
          error_count: errorCount,
          details: { 
            duplicates: duplicateCount,
            dead_lettered: deadLetteredCount,
            batch_id: batchId
          }
        });
      } catch (metricsError) {
        console.error(`[${batchId}] Failed to record metrics:`, metricsError);
      }
    }
  }
  
  /**
   * Override this method to implement message validation logic
   */
  protected validateMessage(message: TMessage): boolean {
    return true;
  }
  
  /**
   * Override this method to implement entity extraction
   */
  protected extractEntityInfo(message: TMessage): { entityType: string; entityId: string } | null {
    return null;
  }
  
  /**
   * Override this method to implement message processing
   */
  protected abstract processMessage(
    message: TMessage, 
    context: WorkerContext
  ): Promise<ProcessingResult>;
  
  /**
   * Override this to create custom idempotency keys
   */
  protected createIdempotencyKey(message: TMessage): string {
    return `message:${JSON.stringify(message)}`;
  }
  
  /**
   * Handle invalid message (format, validation)
   */
  private async handleInvalidMessage(
    message: {id: string, message: TMessage},
    correlationId: string
  ): Promise<void> {
    // Send to dead letter queue
    if (this.options.deadLetterQueueName) {
      const validationError = createEnhancedError(
        "Message validation failed",
        ErrorSource.WORKER,
        ErrorCategory.PERMANENT_VALIDATION
      );
      
      await this.sendToDeadLetterQueue(
        message,
        validationError,
        correlationId,
        "validation_error"
      );
      
      // Delete from original queue
      await this.deleteMessage(message.id.toString());
    }
  }
  
  /**
   * Handle processing error
   */
  private async handleProcessingError(
    message: {id: string, message: TMessage},
    error: Error,
    correlationId: string,
    entityType?: string,
    entityId?: string
  ): Promise<void> {
    // Determine if this is a permanent or transient error
    const isRetriable = !this.isPermanentError(error);
    
    if (entityType && entityId) {
      if (isRetriable) {
        // Mark as failed (retriable)
        await this.stateManager.markAsFailed(entityType, entityId, error.message, {
          correlationId,
          error: error.message,
          retriable: true
        });
        
        console.log(`[${correlationId}] Retriable error for ${entityType}:${entityId}, will be reprocessed later`);
      } else {
        // Mark as dead letter
        await this.stateManager.markAsDeadLetter(entityType, entityId, error.message, {
          correlationId,
          error: error.message
        });
        
        // Send to dead letter queue
        if (this.options.deadLetterQueueName) {
          await this.sendToDeadLetterQueue(
            message,
            error,
            correlationId,
            "permanent_error"
          );
          
          // Delete from original queue
          await this.deleteMessage(message.id.toString());
        }
      }
    } else {
      // No entity tracking - always send to DLQ for non-retriable errors
      if (!isRetriable && this.options.deadLetterQueueName) {
        await this.sendToDeadLetterQueue(
          message,
          error,
          correlationId,
          "permanent_error"
        );
        
        // Delete from original queue
        await this.deleteMessage(message.id.toString());
      }
    }
  }
  
  /**
   * Determine if an error is permanent (non-retriable)
   */
  protected isPermanentError(error: Error): boolean {
    // Check for enhanced error with category
    if ('category' in error) {
      const category = (error as any).category;
      return category === ErrorCategory.PERMANENT_VALIDATION ||
             category === ErrorCategory.PERMANENT_NOT_FOUND ||
             category === ErrorCategory.PERMANENT_AUTH ||
             category === ErrorCategory.PERMANENT_BAD_REQUEST;
    }
    
    // Check common error patterns
    const errorMessage = error.message.toLowerCase();
    return errorMessage.includes('not found') ||
           errorMessage.includes('invalid format') ||
           errorMessage.includes('validation failed') ||
           errorMessage.includes('permission denied') ||
           errorMessage.includes('unauthorized') ||
           errorMessage.includes('bad request');
  }
  
  /**
   * Delete a message from the queue
   */
  protected async deleteMessage(messageId: string): Promise<boolean> {
    try {
      const { error } = await this.supabase.rpc(
        'pg_delete_message',
        { queue_name: this.options.queueName, message_id: messageId }
      );
      
      if (error) {
        console.error(`Error deleting message ${messageId}:`, error);
        return false;
      }
      
      return true;
    } catch (deleteError) {
      console.error(`Failed to delete message ${messageId}:`, deleteError);
      return false;
    }
  }
  
  /**
   * Send a message to the dead letter queue
   */
  protected async sendToDeadLetterQueue(
    originalMessage: {id: string, message: TMessage},
    error: Error,
    correlationId: string,
    reason: string
  ): Promise<void> {
    if (!this.options.deadLetterQueueName) {
      return;
    }
    
    try {
      // Create dead letter record
      const deadLetterMessage = {
        original_message: originalMessage.message,
        original_message_id: originalMessage.id,
        original_queue: this.options.queueName,
        error: error.message,
        stack: error.stack,
        reason,
        correlation_id: correlationId,
        sent_to_dlq_at: new Date().toISOString()
      };
      
      // Send to dead letter queue
      await this.supabase.rpc('pg_enqueue', {
        queue_name: this.options.deadLetterQueueName,
        message_body: deadLetterMessage
      });
      
      console.log(`[${correlationId}] Message sent to dead letter queue: ${reason}`);
    } catch (dlqError) {
      console.error(`[${correlationId}] Error sending to dead letter queue:`, dlqError);
    }
  }
  
  /**
   * Parse and validate messages from queue
   */
  private parseMessages(queueData: any, batchId: string): Array<{id: string, message: TMessage}> {
    let parsedMessages: Array<{id: string, message: TMessage}> = [];
    
    try {
      // Handle either string or object formats
      if (typeof queueData === 'string') {
        parsedMessages = JSON.parse(queueData);
      } else if (Array.isArray(queueData)) {
        parsedMessages = queueData;
      } else if (queueData) {
        parsedMessages = [queueData];
      }
      
      // Process each message to ensure proper format
      parsedMessages = parsedMessages.map((message: any) => {
        try {
          // Ensure message has id and body
          const messageId = message.id?.toString() || message.msg_id?.toString();
          let payload = message.message;
          
          // Parse message if it's a string
          if (typeof payload === 'string') {
            try {
              payload = JSON.parse(payload);
            } catch (parseError) {
              console.warn(`[${batchId}] Could not parse message as JSON`, parseError);
            }
          }
          
          return {
            id: messageId,
            message: payload as TMessage
          };
        } catch (messageError) {
          console.error(`[${batchId}] Error processing message format:`, messageError);
          return null;
        }
      }).filter(msg => msg !== null) as Array<{id: string, message: TMessage}>;
      
      return parsedMessages;
    } catch (parseError) {
      console.error(`[${batchId}] Error parsing queue data:`, parseError);
      return [];
    }
  }
}
