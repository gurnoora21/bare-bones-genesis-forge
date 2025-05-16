
/**
 * QueueHelper - Simplified queue operations with deduplication support
 */
import { SupabaseClient, createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { DeduplicationService, getDeduplicationService } from "./deduplication.ts";
import { PgQueueManager, getQueueManager } from "./pgQueueManager.ts";

export class QueueHelper {
  private supabase: SupabaseClient;
  private deduplicationService: DeduplicationService;
  private queueManager: PgQueueManager;
  private redis?: Redis;
  
  constructor(supabase?: SupabaseClient, redis?: Redis) {
    if (supabase) {
      this.supabase = supabase;
    } else {
      this.supabase = createClient(
        Deno.env.get("SUPABASE_URL") || "",
        Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || ""
      );
    }
    
    this.redis = redis;
    
    if (redis) {
      this.deduplicationService = getDeduplicationService(redis);
    }
    
    this.queueManager = getQueueManager(this.supabase);
  }
  
  /**
   * Enqueue a message with deduplication support
   * @param queueName The queue to send the message to
   * @param message The message payload
   * @param dedupKey Optional explicit deduplication key
   * @returns True if message was enqueued, false if it was a duplicate or failed
   */
  async enqueue(
    queueName: string, 
    message: any, 
    dedupKey?: string
  ): Promise<boolean> {
    if (!queueName) {
      console.error("Queue name is required");
      return false;
    }
    
    // Generate deduplication key if not provided
    const entityId = this.extractEntityId(message);
    const generatedKey = this.generateDedupKey(queueName, message);
    const key = dedupKey || generatedKey;
    
    try {
      // Fast-path deduplication check using atomic Redis SETNX+EX if Redis is available
      if (this.redis) {
        // Use single Redis call for both check and setting with TTL
        // nx: true means only set if key doesn't exist (deduplication)
        // ex: 3600 sets expiry to 1 hour
        const result = await this.redis.set(`dedup:${queueName}:${key}`, entityId || '1', { 
          nx: true, 
          ex: 3600 
        });
        
        // If result is not "OK", key already exists (duplicate message)
        if (result !== "OK") {
          console.log(`Skipping enqueue for duplicate message with key ${key} in queue ${queueName}`);
          return false;
        }
      } else if (this.deduplicationService) {
        // Fall back to old method if no direct Redis access
        const isDuplicate = await this.deduplicationService.isDuplicate(
          queueName, 
          key, 
          { logDetails: true },
          { entityId: entityId }
        );
        
        if (isDuplicate) {
          console.log(`Skipping enqueue for duplicate message with key ${key} in queue ${queueName}`);
          return false;
        }
      }
      
      // Try specialized enqueue function first for higher performance
      let messageId: string | null = null;
      
      switch(queueName) {
        case "artist_discovery":
          if (message.artistName) {
            try {
              const { data, error } = await this.supabase.rpc('start_artist_discovery', {
                artist_name: message.artistName
              });
              
              if (error) {
                console.error(`Error calling start_artist_discovery:`, error);
              } else {
                messageId = data?.toString();
              }
            } catch (error) {
              console.error(`Exception calling start_artist_discovery:`, error);
            }
          }
          break;
          
        case "album_discovery":
          if (message.artistId !== undefined && message.offset !== undefined) {
            try {
              const { data, error } = await this.supabase.rpc('start_album_discovery', {
                artist_id: message.artistId,
                offset_val: message.offset || 0
              });
              
              if (error) {
                console.error(`Error calling start_album_discovery:`, error);
              } else {
                messageId = data?.toString();
              }
            } catch (error) {
              console.error(`Exception calling start_album_discovery:`, error);
            }
          }
          break;
          
        case "track_discovery":
          if (message.albumId !== undefined) {
            try {
              const { data, error } = await this.supabase.rpc('start_track_discovery', {
                album_id: message.albumId,
                offset_val: message.offset || 0
              });
              
              if (error) {
                console.error(`Error calling start_track_discovery:`, error);
              } else {
                messageId = data?.toString();
              }
            } catch (error) {
              console.error(`Exception calling start_track_discovery:`, error);
            }
          }
          break;
      }
      
      // Fall back to generic enqueue if specialized functions didn't work
      if (!messageId) {
        // Prepare message with idempotency key if not present
        const messageToSend = {
          ...message,
          _idempotencyKey: key,
          _timestamp: new Date().toISOString()
        };
        
        messageId = await this.queueManager.sendMessage(queueName, messageToSend);
      }
      
      // For backward compatibility - ensure state is recorded in deduplication service too
      if (messageId && this.deduplicationService && !this.redis) {
        await this.deduplicationService.markAsProcessed(
          queueName, 
          key, 
          3600, // 1 hour TTL for enqueued flags
          { entityId: entityId }
        );
      }
      
      if (!messageId) {
        console.error(`Failed to enqueue message in ${queueName}`);
        return false;
      }
      
      console.log(`Successfully enqueued message in ${queueName} with ID ${messageId}, dedupKey: ${key}`);
      return true;
    } catch (error) {
      console.error(`Error enqueueing message to ${queueName}:`, error);
      return false;
    }
  }
  
  /**
   * Extract entity ID from message based on queue-specific logic
   */
  private extractEntityId(message: any): string | undefined {
    if (!message) return undefined;
    
    if (message.artistId) return message.artistId;
    if (message.albumId) return message.albumId;
    if (message.trackId) return message.trackId;
    if (message.producerId) return message.producerId;
    
    // Handle alternate field names
    if (message.artist_id) return message.artist_id;
    if (message.album_id) return message.album_id;
    if (message.track_id) return message.track_id;
    if (message.producer_id) return message.producer_id;
    
    return undefined;
  }
  
  /**
   * Generate a deduplication key based on message content
   */
  private generateDedupKey(queueName: string, message: any): string {
    if (!message) return `${queueName}:empty:${Date.now()}`;
    
    // Artist discovery
    if (message.artistName) {
      return `${queueName}:artist:name:${message.artistName.toLowerCase()}`;
    }
    if (message.artistId || message.artist_id) {
      const artistId = message.artistId || message.artist_id;
      const offset = message.offset || 0;
      return `${queueName}:artist:${artistId}:offset:${offset}`;
    }
    
    // Album discovery
    if (message.albumId || message.album_id) {
      const albumId = message.albumId || message.album_id;
      const offset = message.offset || 0;
      return `${queueName}:album:${albumId}:offset:${offset}`;
    }
    
    // Track discovery
    if (message.trackId || message.track_id) {
      const trackId = message.trackId || message.track_id;
      return `${queueName}:track:${trackId}`;
    }
    
    // Producer identification
    if (message.producerId || message.producer_id) {
      const producerId = message.producerId || message.producer_id;
      return `${queueName}:producer:${producerId}`;
    }
    
    // Generic fallback - not ideal but prevents empty keys
    return `${queueName}:generic:${JSON.stringify(message).substring(0, 100)}`;
  }
}

/**
 * Get a singleton instance of the QueueHelper
 */
export function getQueueHelper(
  supabase?: SupabaseClient, 
  redis?: Redis
): QueueHelper {
  return new QueueHelper(supabase, redis);
}
