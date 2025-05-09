
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

export interface DeduplicationContext {
  correlationId?: string;
  operation?: string;
  source?: string;
}

export class DeduplicationService {
  private redis: Redis;
  private circuitBreakerState = {
    failures: 0,
    lastFailure: 0,
    isOpen: false
  };
  
  constructor(redis: Redis) {
    this.redis = redis;
  }
  
  /**
   * Check if an operation with the given key has already been processed
   */
  async isDuplicate(
    namespace: string,
    key: string,
    options: DeduplicationOptions = {},
    context: DeduplicationContext = {}
  ): Promise<boolean> {
    const {
      ttlSeconds = getEnvironmentTTL(),
      logDetails = false
    } = options;
    
    const correlationId = context.correlationId || 'untracked';
    
    // If circuit breaker is open, default to non-duplicate
    if (this.isCircuitOpen()) {
      if (logDetails) {
        console.warn(`[${correlationId}] Deduplication check skipped: circuit breaker open`);
      }
      return false;
    }
    
    try {
      // Create a namespaced key for Redis
      const dedupKey = this.createDeduplicationKey(namespace, key);
      
      // Check if the key exists
      const exists = await this.redis.exists(dedupKey);
      
      if (logDetails) {
        console.log(`[${correlationId}] Deduplication check for ${namespace}:${key} - Result: ${exists === 1 ? 'Duplicate' : 'New'}`);
      }
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      return exists === 1;
    } catch (error) {
      console.warn(`[${correlationId}] Deduplication check failed for ${namespace}:${key}: ${error.message}`);
      this.incrementCircuitFailure();
      
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
    ttlSeconds?: number,
    context: DeduplicationContext = {}
  ): Promise<void> {
    const effectiveTtl = ttlSeconds || getEnvironmentTTL();
    const correlationId = context.correlationId || 'untracked';
    
    // If circuit breaker is open, don't try to interact with Redis
    if (this.isCircuitOpen()) {
      console.warn(`[${correlationId}] Skip markAsProcessed: circuit breaker open`);
      return;
    }
    
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
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      if (result !== "OK") {
        console.warn(`[${correlationId}] Failed to mark ${namespace}:${key} as processed: ${result}`);
      }
    } catch (error) {
      console.warn(`[${correlationId}] Error marking as processed: ${error.message}`);
      this.incrementCircuitFailure();
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
          
          // Delete the selected keys in batches
          if (keysToDelete.length > 0) {
            // Process in smaller chunks to prevent huge Redis commands
            for (let i = 0; i < keysToDelete.length; i += 50) {
              const batch = keysToDelete.slice(i, i + 50);
              if (batch.length > 0) {
                const deleteCount = await this.redis.del(...batch);
                totalDeleted += deleteCount;
              }
            }
          }
        }
      } while (cursor !== '0');
      
      return totalDeleted;
    } catch (error) {
      console.error(`Error clearing deduplication keys: ${error.message}`);
      this.incrementCircuitFailure();
      return 0;
    }
  }
  
  /**
   * Circuit breaker implementation
   */
  private incrementCircuitFailure(): void {
    const now = Date.now();
    
    // Reset counter if last failure was more than 60 seconds ago
    if (now - this.circuitBreakerState.lastFailure > 60000) {
      this.circuitBreakerState.failures = 1;
    } else {
      this.circuitBreakerState.failures++;
    }
    
    this.circuitBreakerState.lastFailure = now;
    
    // Trip circuit breaker after 5 consecutive failures
    if (this.circuitBreakerState.failures >= 5 && !this.circuitBreakerState.isOpen) {
      this.circuitBreakerState.isOpen = true;
      
      // Schedule auto-reset after 30 seconds
      setTimeout(() => {
        console.log('Auto-resetting Redis circuit breaker');
        this.resetCircuitBreaker();
      }, 30000);
      
      console.warn(`Redis circuit breaker tripped. Will auto-reset in 30 seconds`);
    }
  }
  
  private isCircuitOpen(): boolean {
    return this.circuitBreakerState.isOpen;
  }
  
  private resetCircuitBreaker(): void {
    this.circuitBreakerState = {
      failures: 0,
      lastFailure: 0,
      isOpen: false
    };
  }
}

/**
 * Get a singleton instance of DeduplicationService
 */
export function getDeduplicationService(redis: Redis): DeduplicationService {
  return new DeduplicationService(redis);
}
