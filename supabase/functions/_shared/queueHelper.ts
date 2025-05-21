
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.7.1";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { DeduplicationService } from "./deduplication.ts";
import { getDeduplicationMetrics } from "./metrics.ts";
import { logDebug, logError } from "./debugHelper.ts";
import { deleteQueueMessage } from "./pgmqBridge.ts";

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
    originalMessageId: string,
    message: any,
    failureReason: string,
    metadata?: Record<string, any>
  ): Promise<boolean>;
}

// This function is only used for enqueueing messages, not for deletion
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
    // Normalize the queue name for enqueue operations
    const normalizedQueueName = normalizeQueueName(queueName);
    logDebug("QueueHelper", `Enqueueing message to queue: ${normalizedQueueName}`);
    
    // Check deduplication first if a key is provided
    if (dedupKey) {
      const isDuplicate = await this.deduplication.isDuplicate(
        normalizedQueueName, 
        dedupKey,
        { ttlSeconds: options.ttl || 86400 } // Default 24h TTL for dedup keys
      );
      
      if (isDuplicate) {
        logDebug("QueueHelper", `Skipping duplicate message with key ${dedupKey}`);
        return null;
      }
    }

    try {
      // Use pg_enqueue RPC function
      const { data, error } = await this.supabase.rpc('pg_enqueue', {
        queue_name: normalizedQueueName,
        message_body: message
      });
      
      if (error) {
        logError("QueueHelper", `Error enqueueing message to ${normalizedQueueName}: ${error.message}`);
        return null;
      }
      
      // If deduplication key was provided, mark as processed to prevent duplicates
      if (data && dedupKey) {
        await this.deduplication.markAsProcessed(
          normalizedQueueName, 
          dedupKey, 
          options.ttl || 86400
        );
      }
      
      return data;
    } catch (err) {
      logError("QueueHelper", `Error enqueueing message to ${normalizedQueueName}: ${err.message}`);
      return null;
    }
  }

  async deleteMessage(queueName: string, messageId: string): Promise<DeleteMessageResponse> {
    // Pass the queue name as-is, without normalization
    // This ensures consistency with how deleteQueueMessage expects it
    logDebug("QueueHelper", `Deleting message ${messageId} from queue ${queueName}`);
    
    try {
      // Use the deleteQueueMessage function from pgmqBridge.ts with exact queue name
      const success = await deleteQueueMessage(this.supabase, queueName, messageId);
      
      if (success) {
        logDebug("QueueHelper", `Successfully deleted message ${messageId} from queue ${queueName}`);
        return { 
          success: true, 
          message: "Message deleted successfully"
        };
      } else {
        logError("QueueHelper", `Failed to delete message ${messageId} from queue ${queueName}`);
        return { 
          success: false, 
          message: "Failed to delete message"
        };
      }
    } catch (err) {
      logError("QueueHelper", `Error deleting message ${messageId} from ${queueName}: ${err.message}`);
      return { 
        success: false, 
        message: `Error: ${err.message}`
      };
    }
  }

  async sendToDLQ(
    queueName: string,
    originalMessageId: string,
    message: any,
    failureReason: string,
    metadata: Record<string, any> = {}
  ): Promise<boolean> {
    const normalizedQueueName = normalizeQueueName(queueName);
    const dlqName = `${normalizedQueueName}_dlq`;
    logDebug("QueueHelper", `Sending message ${originalMessageId} to DLQ ${dlqName}`);
    
    try {
      // Prepare the DLQ message
      const dlqMessage = {
        originalMessage: message,
        originalMessageId,
        failureReason,
        timestamp: new Date().toISOString(),
        ...metadata
      };
      
      // Use pg_enqueue RPC function
      const { data, error } = await this.supabase.rpc('pg_enqueue', {
        queue_name: dlqName,
        message_body: dlqMessage
      });
      
      if (error) {
        logError("QueueHelper", `Failed to send message to DLQ ${dlqName}: ${error.message}`);
        return false;
      }
      
      logDebug("QueueHelper", `Successfully sent message to DLQ ${dlqName}, new ID: ${data}`);
      return true;
    } catch (err) {
      logError("QueueHelper", `Error sending message to DLQ ${dlqName}: ${err.message}`);
      return false;
    }
  }
}

// Factory function to create QueueHelper
export function getQueueHelper(supabase: any, redis: Redis): QueueHelper {
  const deduplicationService = new DeduplicationService(redis);
  return new SupabaseQueueHelper(supabase, redis, deduplicationService);
}
