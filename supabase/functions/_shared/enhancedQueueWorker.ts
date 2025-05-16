
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

interface QueueMessage {
  id: string;
  message: any;
}

interface BatchResult {
  processed: number;
  errors: number;
  duplicates: number;
  skipped: number;
  processingTimeMs: number;
  sentToDlq: number;
  dlqErrors: number;
}

interface ProcessBatchOptions {
  maxBatches?: number;
  batchSize?: number;
  maxRuntimeMs?: number;
  processorName?: string;
  timeoutSeconds?: number;
  visibilityTimeoutSeconds?: number;
  logDetailedMetrics?: boolean;
  deadLetterQueue?: string;
  maxRetries?: number;
}

export function createEnhancedWorker(queueName: string, supabase: any, redis: any) {
  return class EnhancedQueueWorker {
    protected queueName: string;
    protected supabase: any;
    protected redis: any;
    protected workerId: string;

    constructor() {
      this.queueName = queueName;
      this.supabase = supabase;
      this.redis = redis;
      this.workerId = `worker_${queueName}_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
    }

    async processBatch(options: ProcessBatchOptions): Promise<BatchResult> {
      const {
        maxBatches = 5,
        batchSize = 10,
        maxRuntimeMs = 120000, // 2 minutes by default
        processorName = 'default-processor',
        timeoutSeconds = 30,
        visibilityTimeoutSeconds = 900, // Increased to 15 minutes as per fix #8
        logDetailedMetrics = false,
        deadLetterQueue, // new option
        maxRetries = 3  // new option with default
      } = options;

      const startTime = Date.now();
      let processed = 0;
      let errors = 0;
      let duplicates = 0;
      let skipped = 0;
      let sentToDlq = 0;
      let dlqErrors = 0;

      let batchCount = 0;
      let continueProcessing = true;

      while (batchCount < maxBatches && continueProcessing && (Date.now() - startTime) < maxRuntimeMs) {
        batchCount++;
        console.log(`Starting batch ${batchCount} for processor ${processorName}`);

        const { data: messages, error } = await this.supabase.functions.invoke("readQueue", {
          body: {
            queue_name: this.queueName,
            batch_size: batchSize,
            visibility_timeout: visibilityTimeoutSeconds
          }
        });

        if (error) {
          console.error(`Error reading from queue ${this.queueName}:`, error);
          break; // Stop processing if we can't read from the queue
        }

        if (!messages || messages.length === 0) {
          console.log(`No messages to process in queue ${this.queueName}`);
          break; // No more messages, stop processing
        }

        console.log(`Processing ${messages.length} messages in batch ${batchCount}`);

        const batchStartTime = Date.now();
        for (const message of messages) {
          const messageStartTime = Date.now();
          let retryMetrics = {
            retries: 0,
            success: false,
            duplicate: false,
            skipped: false,
            error: false,
            sentToDlq: 0,
            dlqErrors: 0
          };

          let retryCount = 0;
          let lastError: any = null;

          while (retryCount <= maxRetries && !retryMetrics.success) {
            retryCount++;
            try {
              // Process the message
              const result = await Promise.race([
                this.processMessage(message.message),
                new Promise((_, reject) => setTimeout(() => reject(new Error(`Message processing timeout after ${timeoutSeconds} seconds`)), timeoutSeconds * 1000))
              ]);

              retryMetrics.success = true;
              processed++;
              console.log(`Message ${message.id} processed successfully (attempt ${retryCount})`);

            } catch (error) {
              console.error(`Error processing message ${message.id} (attempt ${retryCount}):`, error);
              lastError = error;
              retryMetrics.error = true;
              errors++;
            } finally {
              // Delete the message from the queue if processing was successful or max retries reached
              if (retryMetrics.success || retryCount >= maxRetries) {
                try {
                  const { error: deleteError } = await this.supabase.functions.invoke("deleteMessage", {
                    body: {
                      queue_name: this.queueName,
                      msg_id: message.id
                    }
                  });

                  if (deleteError) {
                    console.error(`Error deleting message ${message.id} from queue:`, deleteError);
                  } else {
                    console.log(`Message ${message.id} deleted from queue`);
                  }
                } catch (deleteError) {
                  console.error(`Fatal error deleting message ${message.id} from queue:`, deleteError);
                }
              }
            }
          }

          // After reaching maxRetries, we'd add:
          if (deadLetterQueue && retryCount > maxRetries && !retryMetrics.success) {
            console.log(`Message ${message.id} failed after ${retryCount - 1} retries, sending to DLQ: ${deadLetterQueue}`);

            try {
              await this.sendToDLQ(message.id, message.message, deadLetterQueue, lastError?.message || 'Unknown error');
              // Track in metrics
              sentToDlq++;
              retryMetrics.sentToDlq = 1;
            } catch (dlqError) {
              console.error(`Failed to send to DLQ: ${dlqError.message}`);
              // If we can't send to DLQ, we still need to handle the message somehow
              dlqErrors++;
              retryMetrics.dlqErrors = 1;
            }
          }

          const messageEndTime = Date.now();
          if (logDetailedMetrics) {
            console.log(`Detailed metrics for message ${message.id}:`, {
              ...retryMetrics,
              processingTimeMs: messageEndTime - messageStartTime,
              messageId: message.id
            });
          }
        }

        const batchEndTime = Date.now();
        console.log(`Batch ${batchCount} completed in ${batchEndTime - batchStartTime}ms`);
      }

      const endTime = Date.now();
      const processingTimeMs = endTime - startTime;

      console.log(`Processor ${processorName} completed in ${processingTimeMs}ms`);
      console.log(`Total messages processed: ${processed}, errors: ${errors}, duplicates: ${duplicates}, skipped: ${skipped}`);

      return {
        processed,
        errors,
        duplicates,
        skipped,
        processingTimeMs,
        sentToDlq,
        dlqErrors
      };
    }

    // Abstract method to be implemented by subclasses
    async processMessage(message: any): Promise<any> {
      throw new Error("Method not implemented.");
    }

    // Implementation of the sendToDLQ method
    async sendToDLQ(messageId: string, message: any, dlqName: string, failureReason: string): Promise<boolean> {
      try {
        console.log(`Sending message ${messageId} to DLQ ${dlqName}. Reason: ${failureReason}`);
        
        const { data, error } = await this.supabase.functions.invoke("sendToDLQ", {
          body: {
            queue_name: this.queueName,
            dlq_name: dlqName,
            message_id: messageId,
            message: message,
            failure_reason: failureReason,
            metadata: {
              worker_id: this.workerId,
              original_queue: this.queueName,
              sent_to_dlq_at: new Date().toISOString()
            }
          }
        });
        
        if (error) {
          console.error(`Error sending to DLQ: ${error.message}`);
          throw new Error(`DLQ sending failed: ${error.message}`);
        }
        
        return true;
      } catch (error) {
        console.error(`Failed to send to DLQ ${dlqName}: ${error.message}`);
        return false;
      }
    }
  };
}
