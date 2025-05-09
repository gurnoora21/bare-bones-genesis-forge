
/**
 * Metrics and Monitoring Service
 * 
 * Tracks worker health, error rates, and performance metrics
 */

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

export interface MetricsOptions {
  ttlSeconds?: number;
  useCircuitBreaker?: boolean;
}

export class DeduplicationMetrics {
  private redis: Redis;
  private circuitBreakerEnabled: boolean = true;
  private circuitBreakerState = {
    failures: 0,
    lastFailure: 0,
    isOpen: false
  };
  
  constructor(redis: Redis, options: MetricsOptions = {}) {
    this.redis = redis;
    this.circuitBreakerEnabled = options.useCircuitBreaker !== false;
  }
  
  /**
   * Record a successful message deduplication
   */
  async recordDeduplicated(
    queue: string,
    worker: string,
    correlationId?: string
  ): Promise<boolean> {
    if (this.isCircuitOpen()) return false;
    
    try {
      const date = new Date().toISOString().split('T')[0];
      const key = `metrics:dedup:${date}:${queue}:${worker}`;
      
      // Increment deduplication counter
      await this.redis.hincrby(key, 'deduplicated', 1);
      
      // Set expiration if not already set (30 days)
      await this.redis.expire(key, 60 * 60 * 24 * 30);
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      return true;
    } catch (error) {
      this.incrementCircuitFailure();
      console.error(`Error recording deduplication metric: ${error.message}`);
      return false;
    }
  }
  
  /**
   * Record a successfully processed message
   */
  async recordProcessed(
    queue: string,
    worker: string,
    processingTimeMs?: number
  ): Promise<boolean> {
    if (this.isCircuitOpen()) return false;
    
    try {
      const date = new Date().toISOString().split('T')[0];
      const key = `metrics:processed:${date}:${queue}:${worker}`;
      
      // Increment processed counter
      await this.redis.hincrby(key, 'processed', 1);
      
      // Track processing time if provided
      if (processingTimeMs !== undefined) {
        await this.redis.hincrby(key, 'processing_time_total', processingTimeMs);
        await this.redis.hincrby(key, 'processing_count', 1);
      }
      
      // Set expiration (30 days)
      await this.redis.expire(key, 60 * 60 * 24 * 30);
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      return true;
    } catch (error) {
      this.incrementCircuitFailure();
      console.error(`Error recording processing metric: ${error.message}`);
      return false;
    }
  }
  
  /**
   * Record a failed message processing attempt
   */
  async recordFailure(
    queue: string,
    worker: string,
    errorType: string,
    errorMessage?: string
  ): Promise<boolean> {
    if (this.isCircuitOpen()) return false;
    
    try {
      const date = new Date().toISOString().split('T')[0];
      const key = `metrics:failures:${date}:${queue}:${worker}`;
      const errorKey = errorType || 'unknown_error';
      
      // Increment general failure counter
      await this.redis.hincrby(key, 'failures', 1);
      
      // Increment specific error type counter
      await this.redis.hincrby(key, `error:${errorKey}`, 1);
      
      // Store most recent error if provided
      if (errorMessage) {
        await this.redis.hset(
          key, 
          'last_error', 
          JSON.stringify({
            message: errorMessage,
            timestamp: new Date().toISOString()
          })
        );
      }
      
      // Set expiration (30 days)
      await this.redis.expire(key, 60 * 60 * 24 * 30);
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      return true;
    } catch (error) {
      this.incrementCircuitFailure();
      console.error(`Error recording failure metric: ${error.message}`);
      return false;
    }
  }
  
  /**
   * Record worker heartbeat to track health
   */
  async recordHeartbeat(
    worker: string,
    status: 'healthy' | 'warning' | 'critical',
    details?: Record<string, any>
  ): Promise<boolean> {
    if (this.isCircuitOpen()) return false;
    
    try {
      const key = `metrics:heartbeat:${worker}`;
      
      // Record heartbeat with status
      await this.redis.hset(
        key,
        {
          last_seen: new Date().toISOString(),
          status,
          details: details ? JSON.stringify(details) : '{}'
        }
      );
      
      // Set expiration (15 minutes)
      await this.redis.expire(key, 60 * 15);
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      return true;
    } catch (error) {
      this.incrementCircuitFailure();
      console.error(`Error recording heartbeat: ${error.message}`);
      return false;
    }
  }
  
  /**
   * Circuit breaker implementation
   */
  private incrementCircuitFailure(): void {
    if (!this.circuitBreakerEnabled) return;
    
    const now = Date.now();
    
    // Reset counter if last failure was more than 60 seconds ago
    if (now - this.circuitBreakerState.lastFailure > 60000) {
      this.circuitBreakerState.failures = 1;
    } else {
      this.circuitBreakerState.failures++;
    }
    
    this.circuitBreakerState.lastFailure = now;
    
    // Trip circuit breaker after 5 consecutive failures
    if (this.circuitBreakerState.failures >= 5) {
      this.circuitBreakerState.isOpen = true;
      
      // Auto-reset after 30 seconds
      setTimeout(() => {
        console.log('Auto-resetting metrics circuit breaker');
        this.resetCircuitBreaker();
      }, 30000);
      
      console.warn('Metrics circuit breaker tripped. Will auto-reset in 30 seconds');
    }
  }
  
  private isCircuitOpen(): boolean {
    return this.circuitBreakerEnabled && this.circuitBreakerState.isOpen;
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
 * Get a singleton instance of DeduplicationMetrics
 */
export function getDeduplicationMetrics(redis: Redis): DeduplicationMetrics {
  return new DeduplicationMetrics(redis);
}
