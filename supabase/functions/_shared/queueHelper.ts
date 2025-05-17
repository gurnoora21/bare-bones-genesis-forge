
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { DeduplicationService } from "./deduplication.ts";
import { getDeduplicationMetrics } from "./metrics.ts";

// Interface for response from deleteFromQueue function
export interface DeleteMessageResponse {
  success: boolean;
  message?: string;
  idempotent?: boolean;
}

export interface QueueHelper {
  enqueue(
    queueName: string, 
    message: any, 
    dedupKey?: string, 
    options?: { 
      ttl?: number, 
      priority?: number 
    }
  ): Promise<string | null>;
  
  deleteMessage(
    queueName: string, 
    messageId: string
  ): Promise<DeleteMessageResponse>;
  
  sendToDLQ(
    queueName: string,
    messageId: string,
    message: any,
    failureReason: string,
    metadata?: Record<string, any>
  ): Promise<boolean>;
}

function normalizeQueueName(queueName: string): string {
  // Remove any existing prefixes
  const baseName = queueName.replace(/^(pgmq\.|q_)/, '');
  // Return the normalized name without any prefix
  return baseName;
}

class SupabaseQueueHelper implements QueueHelper {
  private supabase: any;
  private redis: Redis;
  private deduplication: DeduplicationService;
  private metrics: any;

  constructor(supabase: any, redis: Redis, deduplicationService: DeduplicationService) {
    this.supabase = supabase;
    this.redis = redis;
    this.deduplication = deduplicationService;
    this.metrics = getDeduplicationMetrics(redis);
  }

  async enqueue(
    queueName: string, 
    message: any, 
    dedupKey?: string, 
    options: { ttl?: number, priority?: number } = {}
  ): Promise<string | null> {
    const executionId = `enqueue_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
    console.log(`[${executionId}] Starting enqueue operation for queue: ${queueName}`);
    
    // Normalize the queue name
    const normalizedQueueName = normalizeQueueName(queueName);
    console.log(`[${executionId}] Normalized queue name: ${normalizedQueueName}`);
    
    // Check deduplication first if a key is provided
    if (dedupKey) {
      console.log(`[${executionId}] Checking deduplication for key: ${dedupKey}`);
      const isDuplicate = await this.deduplication.isDuplicate(
        normalizedQueueName, 
        dedupKey,
        { 
          ttl: options.ttl || 86400, // Default 24h TTL for dedup keys
          logDetails: true 
        }
      );
      
      if (isDuplicate) {
        console.log(`[${executionId}] Skipping duplicate message with key ${dedupKey} for queue ${normalizedQueueName}`);
        
        try {
          // Record the deduplication
          await this.metrics.recordDeduplicated(normalizedQueueName, 'enqueue', dedupKey);
        } catch (metricError) {
          console.warn(`[${executionId}] Failed to record deduplication metric: ${metricError.message}`);
        }
        
        return null;
      }
      console.log(`[${executionId}] No duplicate found, proceeding with enqueue`);
    }

    try {
      console.log(`[${executionId}] Calling sendToQueue function...`);
      
      // Ensure message is properly formatted
      const messageToSend = typeof message === 'string' ? message : JSON.stringify(message);
      
      // First try - call Supabase function to enqueue the message
      try {
        const { data, error } = await this.supabase.functions.invoke('sendToQueue', {
          body: {
            queue_name: normalizedQueueName,
            message: message,
            priority: options.priority
          }
        });

        if (!error) {
          const messageId = data.message_id;
          console.log(`[${executionId}] Message enqueued successfully with ID: ${messageId}`);

          // If deduplication key was provided, mark as processed to prevent duplicates
          if (dedupKey) {
            console.log(`[${executionId}] Marking message as processed for deduplication`);
            await this.deduplication.markAsProcessed(normalizedQueueName, dedupKey, options.ttl || 86400);
          }
          
          // Record the success
          try {
            await this.metrics.recordQueueOperation(
              normalizedQueueName, 
              'enqueue', 
              true, 
              { message_id: messageId }
            );
          } catch (metricError) {
            console.warn(`[${executionId}] Failed to record queue operation metric: ${metricError.message}`);
          }

          return messageId;
        }
        
        // Log the detailed error for debugging
        console.error(`[${executionId}] Error enqueueing message to ${normalizedQueueName}:`, error);
        console.error(`[${executionId}] Error details:`, {
          message: error.message,
          status: error.status,
          statusText: error.statusText,
          response: error.response
        });
        
        // Fallback approach if the first attempt fails with a specific error
        if (error.message?.includes('non-2xx status code')) {
          console.log(`[${executionId}] Trying fallback approach with direct database function...`);
          
          // Try using the direct database function
          const { data: directData, error: directError } = await this.supabase.rpc('start_artist_discovery', {
            artist_name: typeof message === 'object' && message.artistName ? message.artistName : 'unknown'
          });
          
          if (!directError && directData) {
            console.log(`[${executionId}] Message enqueued successfully via direct DB function: ${directData}`);
            
            // If deduplication key was provided, mark as processed
            if (dedupKey) {
              await this.deduplication.markAsProcessed(normalizedQueueName, dedupKey, options.ttl || 86400);
            }
            
            return directData.toString();
          }
          
          console.error(`[${executionId}] Fallback also failed:`, directError);
        }
      } catch (invokeError) {
        console.error(`[${executionId}] Exception invoking sendToQueue:`, invokeError);
      }
      
      // Record the failure
      try {
        await this.metrics.recordQueueOperation(
          normalizedQueueName, 
          'enqueue', 
          false, 
          { error: "Failed to enqueue message after multiple attempts" }
        );
      } catch (metricError) {
        console.warn(`[${executionId}] Failed to record queue operation metric: ${metricError.message}`);
      }
      
      return null;
    } catch (err) {
      console.error(`[${executionId}] Unexpected error enqueueing message to ${normalizedQueueName}:`, err);
      console.error(`[${executionId}] Error details:`, {
        message: err.message,
        stack: err.stack
      });
      
      // Record the failure
      try {
        await this.metrics.recordQueueOperation(
          normalizedQueueName, 
          'enqueue', 
          false, 
          { error: err.message }
        );
      } catch (metricError) {
        console.warn(`[${executionId}] Failed to record queue operation metric: ${metricError.message}`);
      }
      
      return null;
    }
  }

  async deleteMessage(queueName: string, messageId: string): Promise<DeleteMessageResponse> {
    const normalizedQueueName = normalizeQueueName(queueName);
    console.log(`DEBUG: Deleting from normalized queue name: ${normalizedQueueName}`);
    
    try {
      // Call Supabase function to delete the message
      const { data, error } = await this.supabase.functions.invoke('deleteFromQueue', {
        body: {
          queue_name: normalizedQueueName,
          message_id: messageId
        }
      });

      if (error) {
        console.error(`Error deleting message ${messageId} from ${normalizedQueueName}:`, error);
        
        // Record the failure
        try {
          await this.metrics.recordQueueOperation(
            normalizedQueueName, 
            'delete', 
            false, 
            { message_id: messageId, error: error.message }
          );
        } catch (metricError) {
          console.warn(`Failed to record queue operation metric: ${metricError.message}`);
        }
        
        return { 
          success: false, 
          message: `Failed to delete message: ${error.message}`
        };
      }

      // Record the success
      try {
        await this.metrics.recordQueueOperation(
          normalizedQueueName, 
          'delete', 
          true, 
          { message_id: messageId }
        );
      } catch (metricError) {
        console.warn(`Failed to record queue operation metric: ${metricError.message}`);
      }

      return { 
        success: true, 
        message: "Message deleted successfully",
        idempotent: data?.idempotent || false
      };
    } catch (err) {
      console.error(`Unexpected error deleting message ${messageId} from ${normalizedQueueName}:`, err);
      
      // Record the failure but don't fail if metrics recording fails
      try {
        await this.metrics.recordQueueOperation(
          normalizedQueueName, 
          'delete', 
          false, 
          { message_id: messageId, error: err.message }
        );
      } catch (metricError) {
        console.warn(`Failed to record queue operation metric: ${metricError.message}`);
      }
      
      return { 
        success: false, 
        message: `Unexpected error: ${err.message}`
      };
    }
  }

  async sendToDLQ(
    queueName: string,
    messageId: string,
    message: any,
    failureReason: string,
    metadata: Record<string, any> = {}
  ): Promise<boolean> {
    const normalizedQueueName = normalizeQueueName(queueName);
    const dlqName = `${normalizedQueueName}_dlq`;
    console.log(`DEBUG: Sending to normalized DLQ name: ${dlqName}`);
    
    try {
      // Call Supabase function to send to DLQ
      const { data, error } = await this.supabase.functions.invoke('sendToDLQ', {
        body: {
          queue_name: normalizedQueueName,
          dlq_name: dlqName,
          message_id: messageId,
          message: message,
          failure_reason: failureReason,
          metadata
        }
      });

      if (error) {
        console.error(`Error sending message ${messageId} to DLQ ${dlqName}:`, error);
        
        // Record the failure
        try {
          await this.metrics.recordQueueOperation(
            dlqName, 
            'send_to_dlq', 
            false, 
            { message_id: messageId, error: error.message }
          );
        } catch (metricError) {
          console.warn(`Failed to record queue operation metric: ${metricError.message}`);
        }
        
        return false;
      }

      // Record the success
      try {
        await this.metrics.recordQueueOperation(
          dlqName, 
          'send_to_dlq', 
          true, 
          { message_id: messageId }
        );
      } catch (metricError) {
        console.warn(`Failed to record queue operation metric: ${metricError.message}`);
      }

      return true;
    } catch (err) {
      console.error(`Unexpected error sending message ${messageId} to DLQ ${dlqName}:`, err);
      
      // Record the failure but don't fail if metrics recording fails
      try {
        await this.metrics.recordQueueOperation(
          dlqName, 
          'send_to_dlq', 
          false, 
          { message_id: messageId, error: err.message }
        );
      } catch (metricError) {
        console.warn(`Failed to record queue operation metric: ${metricError.message}`);
      }
      
      return false;
    }
  }
}

// Factory function to create QueueHelper
export function getQueueHelper(supabase: any, redis: Redis): QueueHelper {
  const deduplicationService = new DeduplicationService(redis);
  return new SupabaseQueueHelper(supabase, redis, deduplicationService);
}
