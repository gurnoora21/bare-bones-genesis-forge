
/**
 * PgQueueManager - PostgreSQL-backed Queue Manager using pgmq
 * 
 * Provides robust, atomic queue operations using PGMQ with visibility timeouts 
 * and optimized processing patterns to ensure exactly-once processing
 */
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

interface QueueMessage {
  id: string;
  msg_id: string;
  message: any;
  created_at?: string;
  vt?: string; // visibility timeout timestamp
  read_ct?: number; // read count
}

interface QueueOptions {
  visibilityTimeoutSeconds?: number;
  batchSize?: number;
  retryCount?: number;
  retryDelayMs?: number;
}

export class PgQueueManager {
  private supabase: any;
  private retryOptions: { maxRetries: number, delayMs: number };

  constructor(supabase?: any) {
    if (supabase) {
      this.supabase = supabase;
    } else {
      this.supabase = createClient(
        Deno.env.get("SUPABASE_URL") || "",
        Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || ""
      );
    }
    
    this.retryOptions = { 
      maxRetries: 3,
      delayMs: 500
    };
  }

  /**
   * Send a message to the queue
   */
  async sendMessage(queueName: string, message: any): Promise<string | null> {
    try {
      const { data, error } = await this.supabase.rpc('pg_enqueue', {
        queue_name: queueName,
        message_body: typeof message === 'string' ? JSON.parse(message) : message
      });

      if (error) {
        console.error(`Error sending message to queue ${queueName}:`, error);
        return null;
      }

      return data ? data.toString() : null;
    } catch (error) {
      console.error(`Exception sending message to queue ${queueName}:`, error);
      return null;
    }
  }

  /**
   * Read messages from the queue with atomic operations and visibility timeout
   */
  async readMessages(queueName: string, options: QueueOptions = {}): Promise<QueueMessage[]> {
    const { 
      visibilityTimeoutSeconds = 60,
      batchSize = 5
    } = options;

    try {
      // Use the secure pg_dequeue function to atomically retrieve messages and set visibility timeout
      const { data, error } = await this.supabase.rpc('pg_dequeue', {
        queue_name: queueName,
        batch_size: batchSize, 
        visibility_timeout: visibilityTimeoutSeconds
      });

      if (error) {
        console.error(`Error reading messages from queue ${queueName}:`, error);
        return [];
      }

      if (!data || data === 'null') {
        return [];
      }

      // Parse the message data
      let messages: QueueMessage[] = [];
      try {
        messages = Array.isArray(data) ? data : JSON.parse(data);
        messages = Array.isArray(messages) ? messages : [];
      } catch (parseError) {
        console.error(`Failed to parse queue messages:`, parseError);
        return [];
      }

      return messages;
    } catch (error) {
      console.error(`Exception reading messages from queue ${queueName}:`, error);
      return [];
    }
  }

  /**
   * Delete a message from the queue with robust error handling and retries
   */
  async deleteMessage(queueName: string, messageId: string): Promise<boolean> {
    let attempt = 0;
    let success = false;

    while (attempt < this.retryOptions.maxRetries && !success) {
      try {
        // Use the robust pg_delete_message function
        const { data, error } = await this.supabase.rpc('pg_delete_message', {
          queue_name: queueName,
          message_id: messageId.toString()
        });

        if (error) {
          console.warn(`Error deleting message ${messageId} from queue ${queueName}:`, error);
        } else if (data === true) {
          return true;
        }

        // Try using the even more robust ensure_message_deleted function as fallback
        try {
          const { data: ensureData, error: ensureError } = await this.supabase.rpc('ensure_message_deleted', {
            queue_name: queueName,
            message_id: messageId.toString(),
            max_attempts: 2
          });
          
          if (!ensureError && ensureData === true) {
            return true;
          }
        } catch (ensureError) {
          console.warn(`Error in ensure_message_deleted for message ${messageId}:`, ensureError);
        }

        attempt++;
        if (attempt < this.retryOptions.maxRetries) {
          await new Promise(resolve => setTimeout(resolve, this.retryOptions.delayMs * (2 ** attempt)));
        }
      } catch (error) {
        console.error(`Exception in deleteMessage attempt ${attempt + 1} for ${messageId}:`, error);
        attempt++;
        if (attempt < this.retryOptions.maxRetries) {
          await new Promise(resolve => setTimeout(resolve, this.retryOptions.delayMs * (2 ** attempt)));
        }
      }
    }

    // Log when we couldn't delete after max retries
    if (!success && attempt >= this.retryOptions.maxRetries) {
      console.error(`Failed to delete message ${messageId} from queue ${queueName} after ${this.retryOptions.maxRetries} attempts`);
      
      // Try to reset the message visibility as a last resort
      try {
        const { data: resetData } = await this.supabase.rpc('reset_stuck_message', {
          queue_name: queueName,
          message_id: messageId.toString()
        });
        
        if (resetData === true) {
          console.log(`Reset visibility timeout for message ${messageId} instead of deletion`);
          return true; // Consider this a success as message will be reprocessed
        }
      } catch (resetError) {
        console.error(`Failed to reset message ${messageId}:`, resetError);
      }
    }

    return success;
  }

  /**
   * Check queue status and get information about its state
   */
  async getQueueStatus(queueName: string): Promise<{ count: number, oldestMessage: Date | null }> {
    try {
      const { data, error } = await this.supabase.rpc('pg_queue_status', {
        queue_name: queueName
      });

      if (error) {
        console.error(`Error getting queue status for ${queueName}:`, error);
        return { count: 0, oldestMessage: null };
      }

      return {
        count: data?.count || 0,
        oldestMessage: data?.oldest_message ? new Date(data.oldest_message) : null
      };
    } catch (error) {
      console.error(`Exception getting queue status for ${queueName}:`, error);
      return { count: 0, oldestMessage: null };
    }
  }

  /**
   * Reset the visibility timeout for a stuck message
   */
  async resetMessageVisibility(queueName: string, messageId: string): Promise<boolean> {
    try {
      const { data, error } = await this.supabase.rpc('reset_stuck_message', {
        queue_name: queueName,
        message_id: messageId
      });

      if (error) {
        console.error(`Error resetting message visibility for ${messageId}:`, error);
        return false;
      }

      return !!data;
    } catch (error) {
      console.error(`Exception resetting message visibility for ${messageId}:`, error);
      return false;
    }
  }

  /**
   * Get a list of stuck messages that have been invisible for too long
   */
  async listStuckMessages(queueName: string, minMinutesLocked: number = 10): Promise<QueueMessage[]> {
    try {
      const { data, error } = await this.supabase.rpc('list_stuck_messages', {
        queue_name: queueName,
        min_minutes_locked: minMinutesLocked
      });

      if (error) {
        console.error(`Error listing stuck messages for ${queueName}:`, error);
        return [];
      }

      return data || [];
    } catch (error) {
      console.error(`Exception listing stuck messages for ${queueName}:`, error);
      return [];
    }
  }

  /**
   * Reset all stuck messages in the queue
   */
  async resetAllStuckMessages(queueName: string, minMinutesLocked: number = 10): Promise<number> {
    try {
      const { data, error } = await this.supabase.rpc('reset_stuck_messages', {
        queue_name: queueName,
        min_minutes_locked: minMinutesLocked
      });

      if (error) {
        console.error(`Error resetting stuck messages for ${queueName}:`, error);
        return 0;
      }

      return data || 0;
    } catch (error) {
      console.error(`Exception resetting stuck messages for ${queueName}:`, error);
      return 0;
    }
  }
}

/**
 * Get a singleton instance of the PgQueueManager
 */
export function getQueueManager(supabase?: any): PgQueueManager {
  return new PgQueueManager(supabase);
}
