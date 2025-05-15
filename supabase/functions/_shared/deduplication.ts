
/**
 * Deduplication Service 
 * 
 * Provides idempotent processing through dual-system approach:
 * 
 * - Redis provides fast lookups and short-term deduplication
 * - Database provides durable record and source of truth
 */

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getEnvironmentTTL } from "./stateManager.ts";

export interface DeduplicationOptions {
  ttlSeconds?: number;
  useStrictPayloadMatch?: boolean;
  logDetails?: boolean;
}

export interface DeduplicationContext {
  correlationId?: string;
  operation?: string;
  source?: string;
}

export class DeduplicationService {
  private redis: Redis;
  
  constructor(redis: Redis) {
    this.redis = redis;
  }
  
  /**
   * Check if an operation with the given key has already been processed
   * Returns false on Redis errors to ensure jobs proceed
   */
  async isDuplicate(
    namespace: string,
    key: string,
    options: DeduplicationOptions = {},
    context: DeduplicationContext = {}
  ): Promise<boolean> {
    const {
      ttlSeconds = getEnvironmentTTL(),
      logDetails = false,
    } = options;
    
    const correlationId = context.correlationId || 'untracked';
    
    try {
      // Create a namespaced key for Redis
      const dedupKey = this.createDeduplicationKey(namespace, key);
      
      // Check if the key exists
      const exists = await this.redis.exists(dedupKey);
      
      if (logDetails) {
        console.log(`[${correlationId}] Deduplication check for ${namespace}:${key} - Result: ${exists === 1 ? 'Duplicate' : 'New'}`);
      }
      
      return exists === 1;
    } catch (error) {
      // On Redis error, log but don't block the job
      console.error(`[${correlationId}] Deduplication check failed for ${namespace}:${key}: ${error.message}`);
      
      // Return false (not a duplicate) to allow the job to proceed
      return false;
    }
  }
  
  /**
   * Mark an operation as processed with the given key
   * Always attempts to mark but doesn't block on failures
   */
  async markAsProcessed(
    namespace: string,
    key: string,
    ttlSeconds?: number,
    context: DeduplicationContext = {}
  ): Promise<void> {
    const effectiveTtl = ttlSeconds || getEnvironmentTTL();
    const correlationId = context.correlationId || 'untracked';
    
    try {
      // Create a namespaced key for Redis
      const dedupKey = this.createDeduplicationKey(namespace, key);
      
      // Store the processing timestamp with TTL
      const processedData = {
        timestamp: new Date().toISOString(),
        correlationId: context.correlationId,
        operation: context.operation,
        source: context.source || 'unknown'
      };
      
      const result = await this.redis.set(
        dedupKey,
        JSON.stringify(processedData),
        {
          ex: effectiveTtl
        }
      );
      
      if (result !== "OK") {
        console.warn(`[${correlationId}] Failed to mark ${namespace}:${key} as processed: ${result}`);
      }
    } catch (error) {
      // Log error but don't block execution
      console.error(`[${correlationId}] Error marking as processed: ${error.message}`);
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
        try {
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
            
            // Delete the selected keys in batches
            if (keysToDelete.length > 0) {
              // Process in smaller chunks to prevent huge Redis commands
              for (let i = 0; i < keysToDelete.length; i += 50) {
                const batch = keysToDelete.slice(i, i + 50);
                if (batch.length > 0) {
                  try {
                    const deleteCount = await this.redis.del(...batch);
                    totalDeleted += deleteCount;
                  } catch (deleteError) {
                    console.warn(`Error deleting batch: ${deleteError.message}`);
                  }
                }
              }
            }
          }
        } catch (scanError) {
          console.error(`Error scanning keys: ${scanError.message}`);
          // Move to next cursor to avoid infinite loop
          if (cursor === '0') break;
        }
      } while (cursor !== '0');
      
      return totalDeleted;
    } catch (error) {
      console.error(`Error clearing deduplication keys: ${error.message}`);
      return 0;
    }
  }
  
  /**
   * Force clear all keys for a specific queue
   */
  async forceClearNamespace(namespace: string): Promise<number> {
    try {
      // Use the more forceful KEYS command instead of SCAN for force clearing
      // Note: This is safe since we're narrowing with strict prefix
      const keys = await this.redis.keys(`dedup:${namespace}:*`);
      
      if (keys && keys.length > 0) {
        let totalDeleted = 0;
        
        // Delete in batches to avoid huge commands
        for (let i = 0; i < keys.length; i += 50) {
          const batch = keys.slice(i, i + 50);
          if (batch.length > 0) {
            try {
              const deleteCount = await this.redis.del(...batch);
              totalDeleted += deleteCount;
            } catch (deleteError) {
              console.warn(`Error force deleting batch: ${deleteError.message}`);
            }
          }
        }
        
        return totalDeleted;
      }
      
      return 0;
    } catch (error) {
      console.error(`Error force clearing keys for ${namespace}: ${error.message}`);
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
