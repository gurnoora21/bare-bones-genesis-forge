
/**
 * Deduplication Service 
 * 
 * Provides idempotent processing through Redis-based deduplication
 * with fallback behavior on Redis errors
 */

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

export interface DeduplicationOptions {
  ttlSeconds?: number;
  logDetails?: boolean;
}

export interface DeduplicationContext {
  correlationId?: string;
  operation?: string;
  source?: string;
  entityId?: string;  // Added to support entity-specific keys
}

export class DeduplicationService {
  private redis: Redis;
  
  constructor(redis: Redis) {
    this.redis = redis;
  }
  
  /**
   * Check if an operation with the given key has already been processed
   * Returns false on Redis errors to ensure jobs proceed (fail open)
   */
  async isDuplicate(
    namespace: string,
    key: string,
    options: DeduplicationOptions = {},
    context: DeduplicationContext = {}
  ): Promise<boolean> {
    const {
      ttlSeconds = 86400, // Default 24 hour TTL
      logDetails = false,
    } = options;
    
    const correlationId = context.correlationId || 'untracked';
    const entityId = context.entityId || '';
    
    // Create a more specific key that includes the entity ID if available
    const dedupKey = entityId 
      ? this.createDeduplicationKey(namespace, `${key}:${entityId}`)
      : this.createDeduplicationKey(namespace, key);
    
    try {
      // Check if the key exists
      const exists = await this.redis.exists(dedupKey);
      
      if (logDetails) {
        console.log(`[${correlationId}] Deduplication check for ${namespace}:${key}${entityId ? `:${entityId}` : ''} - Result: ${exists === 1 ? 'Duplicate' : 'New'}`);
      }
      
      return exists === 1;
    } catch (error) {
      // On Redis error, log but don't block the job (fail open)
      console.error(`[${correlationId}] Deduplication check failed for ${namespace}:${key}${entityId ? `:${entityId}` : ''}: ${error.message}`);
      
      // Always return false (not a duplicate) to allow the job to proceed
      // when Redis is unavailable - critical for pipeline continuity
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
    ttlSeconds: number = 86400, // Default 24 hour TTL
    context: DeduplicationContext = {}
  ): Promise<void> {
    const correlationId = context.correlationId || 'untracked';
    const entityId = context.entityId || '';
    
    // Create a more specific key that includes the entity ID if available
    const dedupKey = entityId 
      ? this.createDeduplicationKey(namespace, `${key}:${entityId}`)
      : this.createDeduplicationKey(namespace, key);
    
    try {
      // Store the processing timestamp with TTL
      const processedData = {
        timestamp: new Date().toISOString(),
        correlationId: context.correlationId,
        operation: context.operation,
        source: context.source || 'unknown',
        entityId: context.entityId
      };
      
      // Always set an expiry (TTL) to prevent leaking keys
      const result = await this.redis.set(
        dedupKey,
        JSON.stringify(processedData),
        {
          ex: ttlSeconds
        }
      );
      
      if (result !== "OK") {
        console.warn(`[${correlationId}] Failed to mark ${namespace}:${key}${entityId ? `:${entityId}` : ''} as processed: ${result}`);
      }
    } catch (error) {
      // Log error but don't block execution (fail open)
      console.error(`[${correlationId}] Error marking as processed: ${error.message}`);
      // Continue execution - pipeline should not stop due to Redis unavailability
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
                  
                  // If TTL is negative (no expiry) or less than expected remaining time,
                  // it's older than specified age
                  if (ttl < 0 || ttl < 86400 - olderThanSeconds) {
                    keysToDelete.push(key);
                  }
                } catch (ttlError) {
                  // Log but continue - we'll treat this key as not matching the age filter
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
}

/**
 * Get a singleton instance of DeduplicationService
 */
export function getDeduplicationService(redis: Redis): DeduplicationService {
  return new DeduplicationService(redis);
}
