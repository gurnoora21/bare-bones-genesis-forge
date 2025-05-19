import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { DeduplicationService } from "./deduplication.ts";
import { getDeduplicationMetrics } from "./metrics.ts";
import { logDebug, logError } from "./debugHelper.ts";

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
    logDebug("QueueHelper", `Starting enqueue operation for queue: ${queueName}`, { executionId });
    
    // Normalize the queue name
    const normalizedQueueName = normalizeQueueName(queueName);
    logDebug("QueueHelper", `Normalized queue name: ${normalizedQueueName}`, { executionId });
    
    // Check deduplication first if a key is provided
    if (dedupKey) {
      logDebug("QueueHelper", `Checking deduplication for key: ${dedupKey}`, { executionId });
      const isDuplicate = await this.deduplication.isDuplicate(
        normalizedQueueName, 
        dedupKey,
        { 
          ttl: options.ttl || 86400, // Default 24h TTL for dedup keys
          logDetails: true 
        }
      );
      
      if (isDuplicate) {
        logDebug("QueueHelper", `Skipping duplicate message with key ${dedupKey} for queue ${normalizedQueueName}`, { executionId });
        
        try {
          // Record the deduplication
          await this.metrics.recordDeduplicated(normalizedQueueName, 'enqueue', dedupKey);
        } catch (metricError) {
          console.warn(`[${executionId}] Failed to record deduplication metric: ${metricError.message}`);
        }
        
        return null;
      }
      logDebug("QueueHelper", `No duplicate found, proceeding with enqueue`, { executionId });
    }

    try {
      // Format message for enqueuing
      const messageBody = typeof message === 'string' ? JSON.parse(message) : message;
      
      // Use pg_enqueue directly - the standard way to enqueue messages
      const { data, error } = await this.supabase.rpc('pg_enqueue', {
        queue_name: normalizedQueueName,
        message_body: messageBody
      });
      
      if (error) {
        logError("QueueHelper", `Error enqueueing message: ${error.message}`);
        
        // If we get a "function not found" error, try with different parameter names
        if (error.message.includes("Could not find the function")) {
          logDebug("QueueHelper", `Trying alternative parameter names for pg_enqueue`, { executionId });
          
          try {
            // Try with p_ prefix for parameters
            const { data: altData, error: altError } = await this.supabase.rpc('pg_enqueue', {
              p_queue_name: normalizedQueueName,
              p_message: messageBody
            });
            
            if (altError) {
              logError("QueueHelper", `Alternative parameter names also failed: ${altError.message}`);
            } else {
              const messageId = String(altData);
              logDebug("QueueHelper", `Message enqueued successfully with alternative parameters, ID: ${messageId}`, { executionId });
              
              // If deduplication key was provided, mark as processed to prevent duplicates
              if (dedupKey) {
                logDebug("QueueHelper", `Marking message as processed for deduplication`, { executionId });
                await this.deduplication.markAsProcessed(normalizedQueueName, dedupKey, options.ttl || 86400);
              }
              
              return messageId;
            }
          } catch (altErr) {
            logError("QueueHelper", `Error with alternative parameters: ${altErr.message}`);
          }
        }
        
        // Record the failure
        try {
          await this.metrics.recordQueueOperation(
            normalizedQueueName, 
            'enqueue', 
            false, 
            { error: error.message }
          );
        } catch (metricError) {
          console.warn(`[${executionId}] Failed to record queue operation metric: ${metricError.message}`);
        }
        
        return null;
      }
      
      const messageId = String(data);
      logDebug("QueueHelper", `Message enqueued successfully, ID: ${messageId}`, { executionId });
      
      // If deduplication key was provided, mark as processed to prevent duplicates
      if (dedupKey) {
        logDebug("QueueHelper", `Marking message as processed for deduplication`, { executionId });
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
    } catch (err) {
      logError("QueueHelper", `Unexpected error enqueueing message to ${normalizedQueueName}: ${err.message}`);
      
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
      // Use pg_delete_message directly - the standard way to delete messages
      const { data, error } = await this.supabase.rpc('pg_delete_message', {
        queue_name: normalizedQueueName,
        message_id: messageId
      });

      if (error) {
        logError("QueueHelper", `Error deleting message ${messageId} from ${normalizedQueueName}: ${error.message}`);
        
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
      logError("QueueHelper", `Unexpected error deleting message ${messageId} from ${normalizedQueueName}: ${err.message}`);
      
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
      // Prepare the DLQ message
      const dlqMessage = {
        originalMessage: message,
        originalMessageId: messageId,
        failureReason,
        timestamp: new Date().toISOString(),
        ...metadata
      };
      
      // Use pg_enqueue directly to send to DLQ
      const { data, error } = await this.supabase.rpc('pg_enqueue', {
        queue_name: dlqName,
        message_body: dlqMessage
      });

      if (error) {
        logError("QueueHelper", `Error sending message ${messageId} to DLQ ${dlqName}: ${error.message}`);
        
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
      logError("QueueHelper", `Unexpected error sending message ${messageId} to DLQ ${dlqName}: ${err.message}`);
      
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

/**
 * Enqueue a message to a specified queue
 */
export async function enqueue(supabase: any, queueName: string, message: any): Promise<string | null> {
  try {
    // Format message for enqueuing
    const messageBody = typeof message === 'string' ? JSON.parse(message) : message;
    
    // Use pg_enqueue directly
    const { data, error } = await supabase.rpc('pg_enqueue', {
      queue_name: normalizeQueueName(queueName),
      message_body: messageBody
    });
    
    if (error) {
      console.error(`Error enqueueing message to ${queueName}:`, error);
      return null;
    }
    
    return data || null;
  } catch (error) {
    console.error(`Exception enqueueing message to ${queueName}:`, error);
    return null;
  }
}
