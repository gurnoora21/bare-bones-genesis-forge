
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
    // Check deduplication first if a key is provided
    if (dedupKey) {
      const isDuplicate = await this.deduplication.isDuplicate(
        queueName, 
        dedupKey,
        { 
          ttl: options.ttl || 86400, // Default 24h TTL for dedup keys
          logDetails: true 
        }
      );
      
      if (isDuplicate) {
        console.log(`Skipping duplicate message with key ${dedupKey} for queue ${queueName}`);
        
        try {
          // Record the deduplication
          await this.metrics.recordDeduplicated(queueName, 'enqueue', dedupKey);
        } catch (metricError) {
          console.warn(`Failed to record deduplication metric: ${metricError.message}`);
        }
        
        return null;
      }
    }

    try {
      // Call Supabase function to enqueue the message
      const { data, error } = await this.supabase.functions.invoke('sendToQueue', {
        body: {
          queue_name: queueName,
          message: message,
          priority: options.priority
        }
      });

      if (error) {
        console.error(`Error enqueueing message to ${queueName}:`, error);
        
        // Record the failure
        try {
          await this.metrics.recordQueueOperation(
            queueName, 
            'enqueue', 
            false, 
            { error: error.message }
          );
        } catch (metricError) {
          console.warn(`Failed to record queue operation metric: ${metricError.message}`);
        }
        
        return null;
      }

      const messageId = data.message_id;

      // If deduplication key was provided, mark as processed to prevent duplicates
      if (dedupKey) {
        await this.deduplication.markAsProcessed(queueName, dedupKey, options.ttl || 86400);
      }
      
      // Record the success
      try {
        await this.metrics.recordQueueOperation(
          queueName, 
          'enqueue', 
          true, 
          { message_id: messageId }
        );
      } catch (metricError) {
        console.warn(`Failed to record queue operation metric: ${metricError.message}`);
      }

      return messageId;
    } catch (err) {
      console.error(`Unexpected error enqueueing message to ${queueName}:`, err);
      
      // Record the failure
      try {
        await this.metrics.recordQueueOperation(
          queueName, 
          'enqueue', 
          false, 
          { error: err.message }
        );
      } catch (metricError) {
        console.warn(`Failed to record queue operation metric: ${metricError.message}`);
      }
      
      return null;
    }
  }

  async deleteMessage(queueName: string, messageId: string): Promise<DeleteMessageResponse> {
    try {
      // Call Supabase function to delete the message
      const { data, error } = await this.supabase.functions.invoke('deleteFromQueue', {
        body: {
          queue_name: queueName,
          message_id: messageId
        }
      });

      if (error) {
        console.error(`Error deleting message ${messageId} from ${queueName}:`, error);
        
        // Record the failure
        try {
          await this.metrics.recordQueueOperation(
            queueName, 
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
          queueName, 
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
      console.error(`Unexpected error deleting message ${messageId} from ${queueName}:`, err);
      
      // Record the failure but don't fail if metrics recording fails
      try {
        await this.metrics.recordQueueOperation(
          queueName, 
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
    const dlqName = `${queueName}_dlq`;
    
    try {
      // Call Supabase function to send to DLQ
      const { data, error } = await this.supabase.functions.invoke('sendToDLQ', {
        body: {
          queue_name: queueName,
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
            { 
              message_id: messageId, 
              source_queue: queueName, 
              error: error.message 
            }
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
          { 
            message_id: messageId, 
            source_queue: queueName
          }
        );
      } catch (metricError) {
        console.warn(`Failed to record queue operation metric: ${metricError.message}`);
      }

      return data?.success || false;
    } catch (err) {
      console.error(`Unexpected error sending message ${messageId} to DLQ ${dlqName}:`, err);
      
      // Record the failure
      try {
        await this.metrics.recordQueueOperation(
          dlqName, 
          'send_to_dlq', 
          false, 
          { 
            message_id: messageId, 
            source_queue: queueName, 
            error: err.message 
          }
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
