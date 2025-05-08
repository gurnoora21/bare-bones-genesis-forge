
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getEnvironmentTTL } from "./stateManager.ts";

export interface DeduplicationOptions {
  ttlSeconds?: number; // How long to remember processed messages
  useStrictPayloadMatch?: boolean; // Whether to use exact payload matching
  useRedisFallback?: boolean; // Whether to continue on Redis errors
}

export class DeduplicationService {
  private redis: Redis;
  
  constructor(redis: Redis) {
    this.redis = redis;
  }
  
  /**
   * Check if a message is a duplicate based on its payload
   */
  async isDuplicate(
    queueName: string,
    payload: any,
    options: DeduplicationOptions = {}
  ): Promise<boolean> {
    const {
      ttlSeconds = getEnvironmentTTL(), // Use environment-specific TTL by default
      useStrictPayloadMatch = true,
      useRedisFallback = true
    } = options;
    
    try {
      // Normalize payload for consistent hashing
      const payloadStr = typeof payload === 'string' 
        ? payload 
        : JSON.stringify(payload);
      
      // Extract core identifiers from payload for non-strict matching
      let deduplicationKey: string;
      
      if (useStrictPayloadMatch) {
        // Use full payload hash for strict matching
        const payloadHash = await crypto.subtle.digest(
          "SHA-256", 
          new TextEncoder().encode(payloadStr)
        );
        
        const hashHex = Array.from(new Uint8Array(payloadHash))
          .map(b => b.toString(16).padStart(2, '0'))
          .join('');
          
        deduplicationKey = `dedup:${queueName}:${hashHex}`;
      } else {
        // Extract core business identifiers for less strict matching
        try {
          const parsedPayload = typeof payload === 'string' 
            ? JSON.parse(payload) 
            : payload;
            
          // Extract common identifiers used across different queue types
          const identifiers = [];
          
          // Artist discovery
          if (parsedPayload.artistId) identifiers.push(`artist:${parsedPayload.artistId}`);
          if (parsedPayload.artistName) identifiers.push(`artistName:${parsedPayload.artistName}`);
          
          // Album discovery
          if (parsedPayload.albumId) identifiers.push(`album:${parsedPayload.albumId}`);
          if (parsedPayload.offset !== undefined) identifiers.push(`offset:${parsedPayload.offset}`);
          
          // Track discovery
          if (parsedPayload.trackId) identifiers.push(`track:${parsedPayload.trackId}`);
          
          // Producer identification
          if (parsedPayload.producerId) identifiers.push(`producer:${parsedPayload.producerId}`);
          
          // Create a composite key from available identifiers
          const identifierString = identifiers.length > 0 
            ? identifiers.join('-') 
            : payloadStr.substring(0, 100); // Fallback for unknown structures
            
          deduplicationKey = `dedup:${queueName}:${identifierString}`;
        } catch (parseError) {
          // If parsing fails, fall back to strict hash
          const payloadHash = await crypto.subtle.digest(
            "SHA-256", 
            new TextEncoder().encode(payloadStr)
          );
          
          const hashHex = Array.from(new Uint8Array(payloadHash))
            .map(b => b.toString(16).padStart(2, '0'))
            .join('');
            
          deduplicationKey = `dedup:${queueName}:${hashHex}`;
        }
      }
      
      // Check if this key exists in Redis
      const exists = await this.redis.exists(deduplicationKey);
      
      if (exists === 1) {
        console.log(`Duplicate message detected in ${queueName}: ${payloadStr.substring(0, 100)}...`);
        return true; // Duplicate found
      }
      
      // Set key for future deduplication
      await this.redis.set(deduplicationKey, 'true', { ex: ttlSeconds });
      return false; // Not a duplicate
    } catch (error) {
      console.warn(`Redis deduplication check failed: ${error}`);
      return useRedisFallback ? false : true; 
      // Either allow processing on errors (false) or
      // reject processing on errors (true) based on config
    }
  }
  
  /**
   * Mark a message as processed
   */
  async markAsProcessed(
    queueName: string,
    payload: any,
    ttlSeconds = getEnvironmentTTL() // Use environment-specific TTL
  ): Promise<void> {
    try {
      const payloadStr = typeof payload === 'string' 
        ? payload 
        : JSON.stringify(payload);
        
      // Create a hash of the payload
      const payloadHash = await crypto.subtle.digest(
        "SHA-256", 
        new TextEncoder().encode(payloadStr)
      );
      
      const hashHex = Array.from(new Uint8Array(payloadHash))
        .map(b => b.toString(16).padStart(2, '0'))
        .join('');
        
      const deduplicationKey = `dedup:${queueName}:${hashHex}`;
      
      // Set the key with expiration
      await this.redis.set(deduplicationKey, 'true', { ex: ttlSeconds });
    } catch (error) {
      console.warn(`Failed to mark message as processed: ${error.message}`);
    }
  }
}

// Singleton instance
let deduplicationServiceInstance: DeduplicationService | null = null;

/**
 * Get or create the deduplication service instance
 */
export function getDeduplicationService(redis: Redis): DeduplicationService {
  if (!deduplicationServiceInstance && redis) {
    deduplicationServiceInstance = new DeduplicationService(redis);
  }
  return deduplicationServiceInstance || new DeduplicationService(redis);
}
