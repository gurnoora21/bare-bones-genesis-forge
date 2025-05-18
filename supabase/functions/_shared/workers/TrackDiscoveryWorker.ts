import { SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { logDebug, formatError } from "../debugHelper.ts";
import { ProcessingResult } from "../types.ts";
import * as queueHelper from '../queueHelper.ts';

interface BatchProcessingOptions {
  batchSize: number;
  processorName: string;
  maxBatches: number;
  timeoutSeconds: number;
  visibilityTimeoutSeconds: number;
  sendToDlqOnMaxRetries: boolean;
  maxRetries: number;
  logDetailedMetrics: boolean;
  deadLetterQueue: string;
}

export class TrackDiscoveryWorker {
  private supabase: SupabaseClient;
  private redis: Redis;
  
  constructor(supabase: SupabaseClient, redis: Redis) {
    this.supabase = supabase;
    this.redis = redis;
  }
  
  /**
   * Process a batch of messages from the queue
   */
  async processBatch(options: BatchProcessingOptions): Promise<ProcessingResult> {
    const { 
      batchSize, 
      processorName, 
      maxBatches, 
      timeoutSeconds, 
      visibilityTimeoutSeconds,
      sendToDlqOnMaxRetries,
      maxRetries,
      logDetailedMetrics,
      deadLetterQueue
    } = options;
    
    let processed = 0;
    let errors = 0;
    let batchCount = 0;
    
    const startTime = Date.now();
    
    while (batchCount < maxBatches && (Date.now() - startTime) < timeoutSeconds * 1000) {
      batchCount++;
      
      try {
        // Read messages from the queue
        const messages = await this.readMessages(batchSize, visibilityTimeoutSeconds);
        if (!messages || messages.length === 0) {
          logDebug(processorName, `No messages in queue, batch ${batchCount}`);
          break; // No more messages in the queue
        }
        
        logDebug(processorName, `Processing batch ${batchCount} with ${messages.length} messages`);
        
        // Process each message in the batch
        for (const message of messages) {
          const messageId = message.id || message.msg_id;
          
          try {
            // Check if message has been processed before using Redis for idempotency
            const isIdempotent = await this.isMessageIdempotent(messageId);
            if (isIdempotent) {
              logDebug(processorName, `Skipping duplicate message ${messageId}`);
              await this.deleteMessage(messageId); // Delete duplicate message
              continue; // Skip to the next message
            }
            
            // Process the message
            await this.processMessage(message);
            
            // Mark message as processed in Redis
            await this.markMessageAsProcessed(messageId);
            
            // Delete the message from the queue
            await this.deleteMessage(messageId);
            
            processed++;
            
            if (logDetailedMetrics) {
              logDebug(processorName, `Successfully processed message ${messageId}`);
            }
          } catch (error) {
            errors++;
            
            const retryCount = message.read_ct || 0;
            
            if (retryCount >= maxRetries) {
              logDebug(processorName, `Max retries exceeded for message ${messageId}`);
              
              // Send to dead letter queue if enabled
              if (sendToDlqOnMaxRetries) {
                await this.sendToDeadLetterQueue(message, deadLetterQueue);
              }
              
              // Delete the message from the queue
              await this.deleteMessage(messageId);
            } else {
              logDebug(processorName, `Error processing message ${messageId}, retry count: ${retryCount}`);
            }
            
            // Log the error
            console.error(`Error processing message ${messageId}:`, formatError(error));
          }
        }
      } catch (batchError) {
        errors++;
        console.error(`Error processing batch ${batchCount}:`, formatError(batchError));
      }
    }
    
    const endTime = Date.now();
    const durationSeconds = (endTime - startTime) / 1000;
    
    logDebug(processorName, `Completed processing in ${durationSeconds} seconds. Total processed: ${processed}, total errors: ${errors}`);
    
    return { processed, errors };
  }
  
  /**
   * Read messages from the queue
   */
  private async readMessages(batchSize: number, visibilityTimeoutSeconds: number): Promise<any[]> {
    try {
      const { data, error } = await this.supabase.rpc('pg_dequeue', {
        queue_name: 'track_discovery',
        batch_size: batchSize,
        visibility_timeout: visibilityTimeoutSeconds
      });
      
      if (error) {
        console.error('Error reading messages from queue:', error);
        return [];
      }
      
      if (!data || data === 'null') {
        return [];
      }
      
      // Parse the message data
      let messages: any[] = [];
      try {
        if (typeof data === 'string') {
          messages = JSON.parse(data);
        } else {
          messages = data;
        }
        messages = Array.isArray(messages) ? messages : [];
      } catch (parseError) {
        console.error(`Failed to parse queue messages:`, parseError);
        return [];
      }
      
      return messages;
    } catch (error) {
      console.error('Error during message retrieval:', error);
      return [];
    }
  }
  
  /**
   * Process a single message
   */
  private async processMessage(message: any): Promise<void> {
    let messageContent = message.message;
    
    // Handle string-encoded JSON messages
    if (typeof messageContent === 'string') {
      try {
        messageContent = JSON.parse(messageContent);
      } catch (e) {
        // If parsing fails, keep the original message
      }
    }
    
    // Extract track details with proper fallbacks
    const trackId = messageContent?.trackId || messageContent?.id;
    const trackName = messageContent?.trackName || messageContent?.name || 'unknown';
    const artistId = messageContent?.artistId;
    const albumId = messageContent?.albumId;
    
    // Log with full details for debugging
    logDebug(
      "TrackDiscovery", 
      `Processing track ${trackName} (${trackId}) by artist ${artistId || 'unknown'}`
    );
    
    // Check if we have the minimum required information
    if (!trackId && !trackName) {
      throw new Error("Invalid track message: missing trackId and trackName");
    }
    
    // Enqueue producer identification
    await this.enqueueProducerIdentification(trackId, trackName, artistId, albumId);
  }
  
  /**
   * Delete a message from the queue
   */
  private async deleteMessage(messageId: string): Promise<boolean> {
    try {
      const { data, error } = await this.supabase.rpc('pg_delete_message', {
        queue_name: 'track_discovery',
        message_id: messageId
      });
      
      if (error) {
        console.error(`Error deleting message ${messageId}:`, error);
        return false;
      }
      
      return data === true;
    } catch (error) {
      console.error(`Exception deleting message ${messageId}:`, error);
      return false;
    }
  }
  
  /**
   * Check if a message has been processed before using Redis
   */
  private async isMessageIdempotent(messageId: string): Promise<boolean> {
    try {
      const exists = await this.redis.exists(`processed:${messageId}`);
      return exists === 1;
    } catch (error) {
      console.error(`Error checking Redis for message ${messageId}:`, error);
      return false; // Assume not idempotent to be safe
    }
  }
  
  /**
   * Mark a message as processed in Redis
   */
  private async markMessageAsProcessed(messageId: string): Promise<void> {
    try {
      await this.redis.set(`processed:${messageId}`, 'true', { ex: 3600 }); // Expire after 1 hour
    } catch (error) {
      console.error(`Error setting Redis key for message ${messageId}:`, error);
    }
  }
  
  /**
   * Send a message to the dead letter queue
   */
  private async sendToDeadLetterQueue(message: any, deadLetterQueue: string): Promise<string | null> {
    try {
      const { id, msg_id, message: originalMessage } = message;
      
      // Construct DLQ message
      const dlqMessage = {
        original_message_id: id || msg_id,
        original_message: originalMessage,
        error_timestamp: Date.now(),
        error_details: 'Max retries exceeded'
      };
      
      // Enqueue to DLQ
      const { data, error } = await this.supabase.rpc('pg_enqueue', {
        queue_name: deadLetterQueue,
        message_body: dlqMessage
      });
      
      if (error) {
        console.error(`Error sending message to DLQ ${deadLetterQueue}:`, error);
        return null;
      }
      
      logDebug("TrackDiscovery", `Sent message to DLQ ${deadLetterQueue}`);
      return data ? data.toString() : null;
    } catch (error) {
      console.error(`Exception sending message to DLQ ${deadLetterQueue}:`, error);
      return null;
    }
  }
  
  /**
   * Enqueue a producer identification message for a track
   * Updated to include optional albumId and ensure all fields are passed
   */
  async enqueueProducerIdentification(
    trackId: string | undefined, 
    trackName: string, 
    artistId: string | undefined,
    albumId?: string | undefined
  ): Promise<string | null> {
    try {
      const message = {
        trackId,
        trackName,
        artistId,
        albumId,
        _timestamp: Date.now(),
        _operation: `producer_identification_${trackId || trackName}`
      };
      
      // Log what we're sending to verify data
      logDebug(
        "TrackDiscovery", 
        `Enqueueing producer identification for track: ${trackName}, artist: ${artistId || 'unknown'}`
      );
      
      // Use the imported queueHelper.enqueue directly rather than this.enqueueMessage
      return await queueHelper.enqueue(this.supabase, 'producer_identification', message);
    } catch (error) {
      console.error('Error enqueueing producer identification:', error);
      return null;
    }
  }
}
