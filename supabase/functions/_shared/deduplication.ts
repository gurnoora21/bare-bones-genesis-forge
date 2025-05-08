
/**
 * Deduplication Service 
 * 
 * Provides idempotent processing through dual-system approach:
 * 
 * - Redis provides fast lookups and short-term deduplication
 * - Database provides durable record and source of truth
 * 
 * Inspired by Stripe's idempotency implementation:
 * https://stripe.com/blog/idempotency
 */

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getEnvironmentTTL } from "./stateManager.ts";

export interface DeduplicationOptions {
  ttlSeconds?: number;
  useStrictPayloadMatch?: boolean;
  logDetails?: boolean;
}

export class DeduplicationService {
  private redis: Redis;
  
  constructor(redis: Redis) {
    this.redis = redis;
  }
  
  /**
   * Check if an operation with the given key has already been processed
   */
  async isDuplicate(
    namespace: string,
    key: string,
    options: DeduplicationOptions = {}
  ): Promise<boolean> {
    const {
      ttlSeconds = getEnvironmentTTL(),
      logDetails = false
    } = options;
    
    try {
      // Create a namespaced key for Redis
      const dedupKey = this.createDeduplicationKey(namespace, key);
      
      // Check if the key exists
      const exists = await this.redis.exists(dedupKey);
      
      if (logDetails) {
        console.log(`Deduplication check for ${namespace}:${key} - Result: ${exists === 1 ? 'Duplicate' : 'New'}`);
      }
      
      return exists === 1;
    } catch (error) {
      console.warn(`Deduplication check failed for ${namespace}:${key}: ${error.message}`);
      // Default to non-duplicate when Redis fails
      return false;
    }
  }
  
  /**
   * Mark an operation as processed with the given key
   */
  async markAsProcessed(
    namespace: string,
    key: string,
    ttlSeconds?: number
  ): Promise<void> {
    const effectiveTtl = ttlSeconds || getEnvironmentTTL();
    
    try {
      // Create a namespaced key for Redis
      const dedupKey = this.createDeduplicationKey(namespace, key);
      
      // Store the processing timestamp with TTL
      const result = await this.redis.set(
        dedupKey,
        new Date().toISOString(),
        {
          ex: effectiveTtl
        }
      );
      
      if (result !== "OK") {
        console.warn(`Failed to mark ${namespace}:${key} as processed: ${result}`);
      }
    } catch (error) {
      console.warn(`Error marking as processed: ${error.message}`);
    }
  }
  
  /**
   * Create a deduplication key with namespace
   */
  private createDeduplicationKey(namespace: string, key: string): string {
    // Normalize and clean the key to avoid Redis key pattern issues
    const normalizedKey = typeof key === 'string'
      ? key.replace(/[^a-zA-Z0-9_:-]/g, '-')
      : String(key);
    
    return `dedup:${namespace}:${normalizedKey}`;
  }
  
  /**
   * Clear deduplication keys based on pattern
   */
  async clearKeys(
    namespace?: string,
    pattern?: string,
    olderThanSeconds?: number
  ): Promise<number> {
    try {
      // Build the pattern for scanning keys
      const scanPattern = namespace
        ? pattern 
          ? `dedup:${namespace}:${pattern}`
          : `dedup:${namespace}:*`
        : pattern
          ? `dedup:*:${pattern}` 
          : "dedup:*";
      
      let cursor = '0';
      let totalDeleted = 0;
      
      do {
        // Scan for keys matching the pattern
        const [nextCursor, keys] = await this.redis.scan(
          cursor,
          "MATCH",
          scanPattern,
          "COUNT",
          100
        );
        
        cursor = nextCursor;
        
        if (keys && keys.length > 0) {
          let keysToDelete: string[] = [];
          
          // If age filter is specified, check TTL for each key
          if (olderThanSeconds && olderThanSeconds > 0) {
            for (const key of keys) {
              try {
                const ttl = await this.redis.ttl(key);
                
                // Get the default TTL for comparison
                const defaultTtl = getEnvironmentTTL();
                
                // If TTL is negative (no expiry) or less than (default - age),
                // it's older than specified age
                if (ttl < 0 || (defaultTtl - ttl > olderThanSeconds)) {
                  keysToDelete.push(key);
                }
              } catch (ttlError) {
                console.warn(`Error checking TTL for ${key}: ${ttlError.message}`);
              }
            }
          } else {
            // If no age filter, delete all matching keys
            keysToDelete = keys;
          }
          
          // Delete the selected keys
          if (keysToDelete.length > 0) {
            const deleteCount = await this.redis.del(...keysToDelete);
            totalDeleted += deleteCount;
          }
        }
      } while (cursor !== '0');
      
      return totalDeleted;
    } catch (error) {
      console.error(`Error clearing deduplication keys: ${error.message}`);
      return 0;
    }
  }
}

/**
 * Get a singleton instance of DeduplicationService
 */
export function getDeduplicationService(redis: Redis): DeduplicationService {
  return new DeduplicationService(redis);
}
