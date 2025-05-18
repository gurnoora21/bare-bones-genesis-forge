
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { DeduplicationService } from "./deduplication.ts";
import { getDeduplicationMetrics } from "./metrics.ts";
import { safeStringify, logDebug } from "./debugHelper.ts";

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
      logDebug("QueueHelper", `Calling sendToQueue function...`, { executionId });
      
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
          const messageId = data?.message_id || data;
          logDebug("QueueHelper", `Message enqueued successfully with ID: ${messageId}`, { executionId });

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
        }
        
        // Log the detailed error for debugging
        console.error(`[${executionId}] Error enqueueing message to ${normalizedQueueName}:`, error);
        logDebug("QueueHelper", `Error enqueueing message`, { 
          executionId,
          queueName: normalizedQueueName,
          error: safeStringify(error)
        });
        
        // Fallback approach 1: Try using pg_enqueue database function
        try {
          logDebug("QueueHelper", `Trying fallback 1 - pg_enqueue database function...`, { executionId });
          
          // Convert message to JSONB
          const jsonMessage = typeof message === 'string' ? JSON.parse(message) : message;
          
          const { data: pgData, error: pgError } = await this.supabase.rpc('pg_enqueue', {
            queue_name: normalizedQueueName,
            message_body: jsonMessage
          });
          
          if (!pgError && pgData) {
            const messageId = String(pgData);
            logDebug("QueueHelper", `Message enqueued successfully via pg_enqueue: ${messageId}`, { executionId });
            
            if (dedupKey) {
              await this.deduplication.markAsProcessed(normalizedQueueName, dedupKey, options.ttl || 86400);
            }
            
            return messageId;
          }
          
          logDebug("QueueHelper", `pg_enqueue fallback failed`, { 
            executionId,
            error: safeStringify(pgError)
          });
          
          // Fallback approach 2: Try using raw SQL
          logDebug("QueueHelper", `Trying fallback 2 - direct SQL query...`, { executionId });
          
          // For artist discovery, try the specific function
          if (normalizedQueueName === 'artist_discovery' && typeof message === 'object' && message.artistName) {
            const { data: directData, error: directError } = await this.supabase.rpc('start_artist_discovery', {
              artist_name: message.artistName
            });
            
            if (!directError && directData) {
              const messageId = String(directData);
              logDebug("QueueHelper", `Message enqueued successfully via start_artist_discovery: ${messageId}`, { 
                executionId,
                artistName: message.artistName
              });
              
              if (dedupKey) {
                await this.deduplication.markAsProcessed(normalizedQueueName, dedupKey, options.ttl || 86400);
              }
              
              return messageId;
            }
            
            logDebug("QueueHelper", `start_artist_discovery fallback failed`, { 
              executionId,
              error: safeStringify(directError)
            });
          }
          
          // Fallback approach 3: Try using raw_sql_query with pgmq.send
          logDebug("QueueHelper", `Trying fallback 3 - raw SQL pgmq.send...`, { executionId });
          
          const messageSql = typeof message === 'string' ? message : JSON.stringify(message);
          const { data: rawData, error: rawError } = await this.supabase.rpc('raw_sql_query', {
            sql_query: `SELECT * FROM pgmq.send($1, $2::jsonb) AS msg_id`,
            params: JSON.stringify([normalizedQueueName, messageSql])
          });
          
          if (!rawError && rawData) {
            const messageId = String(rawData);
            logDebug("QueueHelper", `Message enqueued successfully via raw SQL: ${messageId}`, { executionId });
            
            if (dedupKey) {
              await this.deduplication.markAsProcessed(normalizedQueueName, dedupKey, options.ttl || 86400);
            }
            
            return messageId;
          }
          
          logDebug("QueueHelper", `All fallback approaches failed`, { 
            executionId,
            rawError: safeStringify(rawError)
          });
        } catch (fallbackError) {
          logDebug("QueueHelper", `Exception in fallback approaches`, { 
            executionId,
            error: safeStringify(fallbackError)
          });
        }
      } catch (invokeError) {
        console.error(`[${executionId}] Exception invoking sendToQueue:`, invokeError);
        logDebug("QueueHelper", `Exception invoking sendToQueue`, { 
          executionId,
          error: safeStringify(invokeError)
        });
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
      logDebug("QueueHelper", `Unexpected error in enqueue operation`, { 
        executionId,
        queueName: normalizedQueueName,
        error: safeStringify(err)
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
