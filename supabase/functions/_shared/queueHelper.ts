import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { DeduplicationService } from "./deduplication.ts";
import { getDeduplicationMetrics } from "./metrics.ts";
import { logDebug, logError } from "./debugHelper.ts";
import { executeQueueSql } from "./pgmqBridge.ts";

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
          ttlSeconds: options.ttl || 86400, // Default 24h TTL for dedup keys
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
      
      // First try using pg_enqueue
      const { data, error } = await this.supabase.rpc('pg_enqueue', {
        queue_name: normalizedQueueName,
        message_body: messageBody
      });
      
      if (!error) {
        const messageId = String(data);
        logDebug("QueueHelper", `Message enqueued successfully using pg_enqueue, ID: ${messageId}`, { executionId });
        
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
      
      // If pg_enqueue fails, try with alternative parameter names
      if (error.message.includes("Could not find the function")) {
        logDebug("QueueHelper", `Trying alternative parameter names for pg_enqueue`, { executionId });
        
        try {
          // Try with p_ prefix for parameters
          const { data: altData, error: altError } = await this.supabase.rpc('pg_enqueue', {
            p_queue_name: normalizedQueueName,
            p_message: messageBody
          });
          
          if (!altError) {
            const messageId = String(altData);
            logDebug("QueueHelper", `Message enqueued successfully with alternative parameters, ID: ${messageId}`, { executionId });
            
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
          
          logError("QueueHelper", `Alternative parameter names also failed: ${altError.message}`);
        } catch (altErr) {
          logError("QueueHelper", `Error with alternative parameters: ${altErr.message}`);
        }
      }
      
      // If both RPC approaches fail, use direct SQL
      logDebug("QueueHelper", `RPC approaches failed, using direct SQL: ${error.message}`);
      
      // Direct SQL approach to insert into queue
      const sql = `
        INSERT INTO pgmq.q_${normalizedQueueName} (message, visible_at)
        VALUES ($1, NOW())
        RETURNING id
      `;
      
      const messageJson = JSON.stringify(messageBody);
      const result = await executeQueueSql(this.supabase, sql, [messageJson]);
      
      if (!result || !result.length) {
        logError("QueueHelper", `Direct SQL insert failed for queue ${normalizedQueueName}`);
        
        // Record the failure
        try {
          await this.metrics.recordQueueOperation(
            normalizedQueueName, 
            'enqueue', 
            false, 
            { error: "Direct SQL insert failed" }
          );
        } catch (metricError) {
          console.warn(`[${executionId}] Failed to record queue operation metric: ${metricError.message}`);
        }
        
        return null;
      }
      
      const messageId = String(result[0].id);
      logDebug("QueueHelper", `Successfully enqueued message to ${normalizedQueueName} using direct SQL, ID: ${messageId}`);
      
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
      
      // Last resort - try with a more permissive approach
      try {
        const safeSql = `
          INSERT INTO pgmq.q_${normalizedQueueName} (message, visible_at)
          VALUES ('${JSON.stringify(message).replace(/'/g, "''")}', NOW())
          RETURNING id
        `;
        
        const safeResult = await executeQueueSql(this.supabase, safeSql);
        
        if (safeResult && safeResult.length > 0) {
          const messageId = String(safeResult[0].id);
          logDebug("QueueHelper", `Successfully enqueued message using safe SQL approach, ID: ${messageId}`);
          
          // If deduplication key was provided, mark as processed to prevent duplicates
          if (dedupKey) {
            await this.deduplication.markAsProcessed(normalizedQueueName, dedupKey, options.ttl || 86400);
          }
          
          return messageId;
        }
      } catch (safeError) {
        logError("QueueHelper", `Safe insertion also failed: ${safeError.message}`);
      }
      
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
      // Use direct SQL DELETE statement instead of function call
      // Target the pgmq.q_[queue_name] table directly
      const sql = `
        DELETE FROM pgmq.q_${normalizedQueueName} 
        WHERE id = $1 OR msg_id = $1 
        RETURNING true AS deleted
      `;
      
      const result = await executeQueueSql(this.supabase, sql, [messageId]);
      
      if (!result || !result.length) {
        logError("QueueHelper", `No rows deleted for message ${messageId} from queue ${normalizedQueueName}`);
        
        // Try an alternative approach - check if the message exists first
        const checkSql = `
          SELECT EXISTS(
            SELECT 1 FROM pgmq.q_${normalizedQueueName} 
            WHERE id = $1 OR msg_id = $1
          ) AS exists
        `;
        
        const checkResult = await executeQueueSql(this.supabase, checkSql, [messageId]);
        
        if (checkResult && checkResult.length > 0 && checkResult[0].exists) {
          // Message exists but couldn't be deleted - try again with a different approach
          const forceSql = `
            DELETE FROM pgmq.q_${normalizedQueueName} 
            WHERE id::text = $1::text OR msg_id::text = $1::text
            RETURNING true AS deleted
          `;
          
          const forceResult = await executeQueueSql(this.supabase, forceSql, [messageId]);
          
          if (forceResult && forceResult.length > 0) {
            logDebug("QueueHelper", `Successfully deleted message ${messageId} using text comparison`);
            
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
              message: "Message deleted successfully using text comparison"
            };
          }
        } else {
          // Message doesn't exist - might have been already processed
          logDebug("QueueHelper", `Message ${messageId} not found in queue ${normalizedQueueName}, considering as deleted`);
          
          // Record as success since the message is not in the queue
          try {
            await this.metrics.recordQueueOperation(
              normalizedQueueName, 
              'delete', 
              true, 
              { message_id: messageId, idempotent: true }
            );
          } catch (metricError) {
            console.warn(`Failed to record queue operation metric: ${metricError.message}`);
          }
          
          return { 
            success: true, 
            message: "Message already processed or not found",
            idempotent: true
          };
        }
        
        // Record the failure
        try {
          await this.metrics.recordQueueOperation(
            normalizedQueueName, 
            'delete', 
            false, 
            { message_id: messageId, error: "Failed to delete message" }
          );
        } catch (metricError) {
          console.warn(`Failed to record queue operation metric: ${metricError.message}`);
        }
        
        return { 
          success: false, 
          message: "Failed to delete message"
        };
      }
      
      logDebug("QueueHelper", `Successfully deleted message ${messageId} from queue ${normalizedQueueName}`);
      
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
        message: "Message deleted successfully"
      };
    } catch (err) {
      logError("QueueHelper", `Unexpected error deleting message ${messageId} from ${normalizedQueueName}: ${err.message}`);
      
      // Last resort - try with a more permissive approach
      try {
        const safeSql = `
          BEGIN;
          DELETE FROM pgmq.q_${normalizedQueueName} WHERE id::text = '${messageId}'::text;
          DELETE FROM pgmq.q_${normalizedQueueName} WHERE msg_id::text = '${messageId}'::text;
          COMMIT;
        `;
        
        await executeQueueSql(this.supabase, safeSql);
        logDebug("QueueHelper", `Attempted safe deletion for message ${messageId} from queue ${normalizedQueueName}`);
      } catch (safeError) {
        logError("QueueHelper", `Safe deletion also failed: ${safeError.message}`);
      }
      
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
      
      // First try using pg_enqueue
      const { data, error } = await this.supabase.rpc('pg_enqueue', {
        queue_name: dlqName,
        message_body: dlqMessage
      });

      if (!error) {
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
      }
      
      // If pg_enqueue fails, use direct SQL
      logDebug("QueueHelper", `pg_enqueue failed for DLQ, using direct SQL: ${error.message}`);
      
      // Direct SQL approach to insert into DLQ
      const sql = `
        INSERT INTO pgmq.q_${dlqName} (message, visible_at)
        VALUES ($1, NOW())
        RETURNING id
      `;
      
      const messageJson = JSON.stringify(dlqMessage);
      const result = await executeQueueSql(this.supabase, sql, [messageJson]);
      
      if (!result || !result.length) {
        logError("QueueHelper", `Direct SQL insert failed for DLQ ${dlqName}`);
        
        // Record the failure
        try {
          await this.metrics.recordQueueOperation(
            dlqName, 
            'send_to_dlq', 
            false, 
            { message_id: messageId, error: "Direct SQL insert failed" }
          );
        } catch (metricError) {
          console.warn(`Failed to record queue operation metric: ${metricError.message}`);
        }
        
        return false;
      }
      
      logDebug("QueueHelper", `Successfully sent message ${messageId} to DLQ ${dlqName} using direct SQL`);
      
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
      
      // Last resort - try with a more permissive approach
      try {
        const safeSql = `
          INSERT INTO pgmq.q_${dlqName} (message, visible_at)
          VALUES ('${JSON.stringify(message).replace(/'/g, "''")}', NOW())
        `;
        
        await executeQueueSql(this.supabase, safeSql);
        logDebug("QueueHelper", `Attempted safe insertion for message ${messageId} to DLQ ${dlqName}`);
      } catch (safeError) {
        logError("QueueHelper", `Safe insertion also failed: ${safeError.message}`);
      }
      
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
 * Simplified standalone version with direct SQL fallback
 */
export async function enqueue(supabase: any, queueName: string, message: any): Promise<string | null> {
  try {
    // Format message for enqueuing
    const messageBody = typeof message === 'string' ? JSON.parse(message) : message;
    const normalizedQueueName = normalizeQueueName(queueName);
    
    // First try using pg_enqueue
    const { data, error } = await supabase.rpc('pg_enqueue', {
      queue_name: normalizedQueueName,
      message_body: messageBody
    });
    
    if (!error) {
      return data || null;
    }
    
    console.log(`pg_enqueue failed for ${normalizedQueueName}, using direct SQL: ${error.message}`);
    
    // If pg_enqueue fails, use direct SQL
    const sql = `
      INSERT INTO pgmq.q_${normalizedQueueName} (message, visible_at)
      VALUES ($1, NOW())
      RETURNING id
    `;
    
    const messageJson = JSON.stringify(messageBody);
    const result = await executeQueueSql(supabase, sql, [messageJson]);
    
    if (!result || !result.length) {
      console.error(`Direct SQL insert failed for queue ${normalizedQueueName}`);
      return null;
    }
    
    return String(result[0].id);
  } catch (error) {
    console.error(`Exception enqueueing message to ${queueName}:`, error);
    
    // Last resort - try with a more permissive approach
    try {
      const normalizedQueueName = normalizeQueueName(queueName);
      const safeSql = `
        INSERT INTO pgmq.q_${normalizedQueueName} (message, visible_at)
        VALUES ('${JSON.stringify(message).replace(/'/g, "''")}', NOW())
        RETURNING id
      `;
      
      const safeResult = await executeQueueSql(supabase, safeSql);
      
      if (safeResult && safeResult.length > 0) {
        return String(safeResult[0].id);
      }
    } catch (safeError) {
      console.error(`Safe insertion also failed: ${safeError.message}`);
    }
    
    return null;
  }
}
