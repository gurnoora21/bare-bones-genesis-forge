
/**
 * Enhanced Deduplication Service
 * 
 * Provides comprehensive idempotency support through:
 * - Redis-based fast path checks
 * - Database record verification
 * - Multi-tier deduplication strategies
 */

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getEnvironmentTTL } from "./stateManager.ts";

export interface EnhancedDeduplicationOptions {
  ttlSeconds?: number;
  useStrictPayloadMatch?: boolean;
  recordGeneration?: boolean;
  recordResult?: boolean;
  logDetails?: boolean;
  namespace?: string;
}

export interface DeduplicationResult {
  isDuplicate: boolean;
  source?: 'redis' | 'database' | 'memory';
  originalProcessingTime?: number;
  originalResult?: any;
}

export interface DeduplicationContext {
  correlationId?: string;
  operation?: string;
  source?: string;
  entityId?: string;
  entityType?: string;
}

export interface BatchCheckResult {
  duplicates: string[];
  new: string[];
}

export class EnhancedDeduplicationService {
  private redis: Redis;
  private supabase: any;
  private memoryCache: Map<string, {time: number, result?: any}> = new Map();
  private circuitBreakerState = {
    failures: 0,
    lastFailure: 0,
    isOpen: false
  };
  
  constructor(redis: Redis, supabaseClient?: any) {
    this.redis = redis;
    this.supabase = supabaseClient;
  }
  
  /**
   * Check if an operation with the given key has already been processed
   * using a multi-tier approach (memory, Redis, database)
   */
  async checkDuplicate(
    key: string,
    options: EnhancedDeduplicationOptions = {},
    context: DeduplicationContext = {}
  ): Promise<DeduplicationResult> {
    const {
      ttlSeconds = getEnvironmentTTL(),
      logDetails = false,
      namespace = 'default'
    } = options;
    
    const correlationId = context.correlationId || 'untracked';
    const dedupKey = this.createDeduplicationKey(namespace, key);
    
    // First check memory cache (fastest)
    const memoryCacheEntry = this.memoryCache.get(dedupKey);
    if (memoryCacheEntry) {
      if (logDetails) {
        console.log(`[${correlationId}] Memory cache hit for ${dedupKey}`);
      }
      return {
        isDuplicate: true,
        source: 'memory',
        originalProcessingTime: memoryCacheEntry.time,
        originalResult: memoryCacheEntry.result
      };
    }
    
    // If circuit breaker is open, skip Redis check
    if (this.isCircuitOpen()) {
      if (logDetails) {
        console.warn(`[${correlationId}] Redis circuit breaker open, skipping Redis check`);
      }
      // Fall back to database check if available
      if (this.supabase && context.entityType && context.entityId) {
        return this.checkDatabaseDuplicate(context);
      }
      return { isDuplicate: false };
    }
    
    try {
      // Check Redis
      const redisValue = await this.redis.get(dedupKey);
      
      if (redisValue) {
        // Parse stored data to get processing time and result
        let processingTime: number | undefined;
        let storedResult: any = undefined;
        
        if (typeof redisValue === 'string') {
          try {
            const parsed = JSON.parse(redisValue);
            if (parsed && typeof parsed === 'object') {
              processingTime = parsed.timestamp;
              storedResult = parsed.result;
            }
          } catch (parseError) {
            // If not JSON, it's an old format entry
            processingTime = Date.now() - 1000;
          }
        }
        
        if (logDetails) {
          console.log(`[${correlationId}] Redis duplicate found for ${dedupKey}`);
        }
        
        // Store in memory cache for future fast access
        this.memoryCache.set(dedupKey, {
          time: processingTime || Date.now(),
          result: storedResult
        });
        
        // Reset circuit breaker on success
        this.resetCircuitBreaker();
        
        return {
          isDuplicate: true,
          source: 'redis',
          originalProcessingTime: processingTime,
          originalResult: storedResult
        };
      }
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      // If no Redis hit but we have database access, check there
      if (this.supabase && context.entityType && context.entityId) {
        return this.checkDatabaseDuplicate(context);
      }
      
      return { isDuplicate: false };
    } catch (error) {
      console.warn(`[${correlationId}] Redis deduplication check failed: ${error.message}`);
      this.incrementCircuitFailure();
      
      // Fall back to database check if available
      if (this.supabase && context.entityType && context.entityId) {
        return this.checkDatabaseDuplicate(context);
      }
      
      // Default to non-duplicate when Redis fails
      return { isDuplicate: false };
    }
  }
  
  /**
   * Check database for entity state to determine if already processed
   */
  private async checkDatabaseDuplicate(
    context: DeduplicationContext
  ): Promise<DeduplicationResult> {
    const { entityType, entityId, correlationId = 'untracked' } = context;
    
    if (!entityType || !entityId || !this.supabase) {
      return { isDuplicate: false };
    }
    
    try {
      // Check if entity exists and is in COMPLETED state
      const { data } = await this.supabase
        .from('processing_status')
        .select('state, last_processed_at, metadata')
        .eq('entity_type', entityType)
        .eq('entity_id', entityId)
        .maybeSingle();
      
      if (data && data.state === 'COMPLETED') {
        console.log(`[${correlationId}] Database duplicate found for ${entityType}:${entityId}`);
        
        return {
          isDuplicate: true,
          source: 'database',
          originalProcessingTime: data.last_processed_at ? new Date(data.last_processed_at).getTime() : undefined,
          originalResult: data.metadata?.result
        };
      }
      
      return { isDuplicate: false };
    } catch (dbError) {
      console.warn(`[${correlationId}] Database deduplication check failed: ${dbError.message}`);
      return { isDuplicate: false };
    }
  }
  
  /**
   * Batch check multiple keys for duplicates
   * More efficient than individual checks
   */
  async batchCheckDuplicates(
    keys: string[],
    options: EnhancedDeduplicationOptions = {}
  ): Promise<BatchCheckResult> {
    const { namespace = 'default' } = options;
    const result: BatchCheckResult = { duplicates: [], new: [] };
    
    if (keys.length === 0) {
      return result;
    }
    
    // First check memory cache
    const dedupKeys = keys.map(key => this.createDeduplicationKey(namespace, key));
    const memoryMisses: string[] = [];
    
    for (let i = 0; i < keys.length; i++) {
      const dedupKey = dedupKeys[i];
      const key = keys[i];
      
      if (this.memoryCache.has(dedupKey)) {
        result.duplicates.push(key);
      } else {
        memoryMisses.push(key);
      }
    }
    
    // If all were in memory, return early
    if (memoryMisses.length === 0) {
      return result;
    }
    
    // If circuit breaker is open, skip Redis check
    if (this.isCircuitOpen()) {
      // Consider all as new
      result.new = [...memoryMisses];
      return result;
    }
    
    try {
      // Batch check Redis for remaining keys
      const missedDedupKeys = memoryMisses.map(key => this.createDeduplicationKey(namespace, key));
      
      // Use mget to get multiple keys at once
      const redisValues = await this.redis.mget(...missedDedupKeys);
      
      for (let i = 0; i < memoryMisses.length; i++) {
        const key = memoryMisses[i];
        const redisValue = redisValues[i];
        
        if (redisValue) {
          result.duplicates.push(key);
          
          // Add to memory cache
          try {
            let processingTime = Date.now();
            let storedResult: any = undefined;
            
            if (typeof redisValue === 'string') {
              const parsed = JSON.parse(redisValue);
              if (parsed && typeof parsed === 'object') {
                processingTime = parsed.timestamp || processingTime;
                storedResult = parsed.result;
              }
            }
            
            this.memoryCache.set(this.createDeduplicationKey(namespace, key), {
              time: processingTime,
              result: storedResult
            });
          } catch (parseError) {
            // Ignore parsing errors
          }
        } else {
          result.new.push(key);
        }
      }
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      return result;
    } catch (error) {
      console.warn(`Batch Redis deduplication check failed: ${error.message}`);
      this.incrementCircuitFailure();
      
      // Consider all as new if Redis fails
      result.new = [...memoryMisses];
      return result;
    }
  }
  
  /**
   * Mark an operation as processed with the given key
   * Can optionally store the result for future duplicate checks
   */
  async markProcessed(
    key: string,
    options: EnhancedDeduplicationOptions = {},
    context: DeduplicationContext = {},
    result?: any
  ): Promise<boolean> {
    const {
      ttlSeconds = getEnvironmentTTL(),
      recordResult = false,
      namespace = 'default'
    } = options;
    
    const correlationId = context.correlationId || 'untracked';
    const dedupKey = this.createDeduplicationKey(namespace, key);
    
    // Always mark in memory cache
    this.memoryCache.set(dedupKey, {
      time: Date.now(),
      result: recordResult ? result : undefined
    });
    
    // If circuit breaker is open, don't try to interact with Redis
    if (this.isCircuitOpen()) {
      console.warn(`[${correlationId}] Skip markProcessed in Redis: circuit breaker open`);
      return true; // memory cache was still set
    }
    
    try {
      // Store the processing timestamp and optional result
      const processedData = {
        timestamp: Date.now(),
        correlationId,
        operation: context.operation,
        source: context.source || 'unknown',
        result: recordResult ? result : undefined
      };
      
      // Store in Redis with TTL
      const redisResult = await this.redis.set(
        dedupKey,
        JSON.stringify(processedData),
        { ex: ttlSeconds }
      );
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      if (redisResult !== "OK") {
        console.warn(`[${correlationId}] Failed to mark ${dedupKey} as processed in Redis: ${redisResult}`);
        return false;
      }
      
      return true;
    } catch (error) {
      console.warn(`[${correlationId}] Error marking ${dedupKey} as processed in Redis: ${error.message}`);
      this.incrementCircuitFailure();
      return false;
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
  
  /**
   * Clear memory cache entries
   */
  clearMemoryCache(namespace?: string, pattern?: string): number {
    let cleared = 0;
    const searchPattern = namespace 
      ? new RegExp(`^dedup:${namespace}:${pattern || ''}`)
      : pattern 
        ? new RegExp(`^dedup:[^:]+:${pattern}`)
        : null;
    
    for (const key of this.memoryCache.keys()) {
      if (!searchPattern || searchPattern.test(key)) {
        this.memoryCache.delete(key);
        cleared++;
      }
    }
    
    return cleared;
  }
}

// Create a singleton instance
let enhancedDeduplicationInstance: EnhancedDeduplicationService | null = null;

export function getEnhancedDeduplication(redis: Redis, supabaseClient?: any): EnhancedDeduplicationService {
  if (!enhancedDeduplicationInstance) {
    enhancedDeduplicationInstance = new EnhancedDeduplicationService(redis, supabaseClient);
  }
  return enhancedDeduplicationInstance;
}
