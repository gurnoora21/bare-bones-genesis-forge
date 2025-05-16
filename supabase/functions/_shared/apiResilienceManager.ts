
/**
 * API Resilience Manager
 * 
 * Provides circuit breaker, rate limiting, and retry mechanisms for external API calls
 */

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { 
  RedisCircuitBreaker,
  CircuitState,
  CircuitStats,
  createCircuitBreakerFactory
} from "./redisCircuitBreaker.ts";

export enum ServiceHealth {
  HEALTHY = 'HEALTHY',
  DEGRADED = 'DEGRADED',
  UNHEALTHY = 'UNHEALTHY',
  UNKNOWN = 'UNKNOWN'
}

export interface ApiServiceConfig {
  name: string;
  failureThreshold?: number;
  resetTimeoutMs?: number;
  maxRetries?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  jitterFactor?: number;
  rateLimit?: RateLimitConfig;
}

export interface RateLimitConfig {
  requestsPerMinute: number;
  burstLimit?: number;
}

export interface ApiServiceHealth {
  service: string;
  status: ServiceHealth;
  circuitState: CircuitState;
  failureCount: number;
  lastFailure: Date | null;
  lastSuccess: Date | null;
  openCount: number;
}

/**
 * Manages resilience patterns for external API calls
 */
export class ApiResilienceManager {
  private redis: Redis;
  private circuitBreakerFactory: ReturnType<typeof createCircuitBreakerFactory>;
  private circuitBreakers: Record<string, RedisCircuitBreaker> = {};
  
  // Default reset window increased to 90 seconds (1.5x Spotify's 60-second ban window)
  private DEFAULT_RESET_TIMEOUT_MS = 90000; // 1.5x Spotify ban window
  
  constructor(redis: Redis) {
    this.redis = redis;
    this.circuitBreakerFactory = createCircuitBreakerFactory(redis);
  }
  
  /**
   * Execute an API call with circuit breaking, retries, and rate limiting
   */
  async executeApiCall<T>(
    serviceName: string, 
    operation: () => Promise<T>, 
    options: Partial<ApiServiceConfig> = {}
  ): Promise<T> {
    const config = this.getServiceConfig(serviceName, options);
    
    // Get or create circuit breaker
    const circuitBreaker = await this.getCircuitBreaker(serviceName, config);
    
    // Check rate limit before proceeding
    if (config.rateLimit) {
      await this.checkRateLimit(serviceName, config.rateLimit);
    }
    
    // Execute with retry logic
    return this.executeWithRetry(
      () => circuitBreaker.execute(operation),
      config
    );
  }
  
  /**
   * Get configured circuit breaker for a service
   */
  private async getCircuitBreaker(
    serviceName: string, 
    config: ApiServiceConfig
  ): Promise<RedisCircuitBreaker> {
    if (!this.circuitBreakers[serviceName]) {
      this.circuitBreakers[serviceName] = this.circuitBreakerFactory.create({
        name: serviceName,
        failureThreshold: config.failureThreshold || 5,
        resetTimeout: config.resetTimeoutMs || this.DEFAULT_RESET_TIMEOUT_MS, // Using increased default
        maxRetries: config.maxRetries || 3,
        timeout: 10000, // Default timeout in milliseconds
      });
    }
    
    return this.circuitBreakers[serviceName];
  }
  
  /**
   * Rate limit check using Redis
   */
  private async checkRateLimit(
    serviceName: string, 
    rateLimitConfig: RateLimitConfig
  ): Promise<void> {
    const key = `rate:${serviceName}:minute`;
    const currentMinute = Math.floor(Date.now() / 60000);
    const burstLimit = rateLimitConfig.burstLimit || rateLimitConfig.requestsPerMinute;
    
    // Use Redis to track requests per minute
    const minuteKey = `${key}:${currentMinute}`;
    const count = await this.redis.incr(minuteKey);
    
    // Set expiry if this is a new key
    if (count === 1) {
      await this.redis.expire(minuteKey, 120); // 2 minutes expiry for safety
    }
    
    // Check rate limit
    if (count > rateLimitConfig.requestsPerMinute) {
      // Get burst key
      const burstKey = `${key}:burst`;
      const burstCount = await this.redis.incr(burstKey);
      if (burstCount === 1) {
        await this.redis.expire(burstKey, 60); // 1 minute burst window
      }
      
      if (burstCount > burstLimit) {
        throw new Error(`Rate limit exceeded for ${serviceName}: ${count} requests this minute`);
      }
    }
  }
  
  /**
   * Execute an operation with retry logic
   */
  private async executeWithRetry<T>(
    operation: () => Promise<T>,
    config: ApiServiceConfig
  ): Promise<T> {
    const baseDelay = config.baseDelayMs || 100;
    const maxDelay = config.maxDelayMs || 5000;
    const jitterFactor = config.jitterFactor || 0.1;
    const maxRetries = config.maxRetries || 3;
    
    let attempt = 0;
    let lastError: Error | null = null;
    
    while (attempt <= maxRetries) {
      try {
        return await operation();
      } catch (error) {
        lastError = error as Error;
        attempt++;
        
        if (attempt > maxRetries) {
          break;
        }
        
        // Calculate backoff with jitter
        const delay = Math.min(
          baseDelay * Math.pow(2, attempt - 1), // Exponential backoff
          maxDelay
        );
        const jitter = delay * jitterFactor * (Math.random() * 2 - 1); // ±jitterFactor%
        const finalDelay = Math.max(0, delay + jitter);
        
        console.log(`Retry ${attempt}/${maxRetries} for ${config.name} in ${Math.round(finalDelay)}ms`);
        await new Promise(resolve => setTimeout(resolve, finalDelay));
      }
    }
    
    throw lastError || new Error(`Operation failed after ${maxRetries} attempts`);
  }
  
  /**
   * Get service configuration with defaults
   */
  private getServiceConfig(
    serviceName: string,
    options: Partial<ApiServiceConfig>
  ): ApiServiceConfig {
    // Could load from database or config file in the future
    return {
      name: serviceName,
      failureThreshold: 5,
      resetTimeoutMs: this.DEFAULT_RESET_TIMEOUT_MS, // Using increased default
      maxRetries: 3,
      baseDelayMs: 100,
      maxDelayMs: 5000,
      jitterFactor: 0.1,
      ...options
    };
  }
  
  /**
   * Get health status of a service
   */
  async getServiceHealth(serviceName: string): Promise<ApiServiceHealth> {
    const circuitBreaker = await this.getCircuitBreaker(
      serviceName, 
      { name: serviceName }
    );
    
    const stats = await circuitBreaker.getStats();
    
    let status = ServiceHealth.UNKNOWN;
    
    if (stats.state === CircuitState.CLOSED) {
      status = ServiceHealth.HEALTHY;
    } else if (stats.state === CircuitState.HALF_OPEN) {
      status = ServiceHealth.DEGRADED;
    } else if (stats.state === CircuitState.OPEN) {
      status = ServiceHealth.UNHEALTHY;
    }
    
    return {
      service: serviceName,
      status,
      circuitState: stats.state,
      failureCount: stats.failures,
      lastFailure: stats.lastFailure ? new Date(stats.lastFailure) : null,
      lastSuccess: stats.lastSuccess ? new Date(stats.lastSuccess) : null,
      openCount: stats.openCount
    };
  }
  
  /**
   * Get health status for all registered services
   */
  async getAllServicesHealth(): Promise<Record<string, ApiServiceHealth>> {
    const result: Record<string, ApiServiceHealth> = {};
    
    for (const serviceName of Object.keys(this.circuitBreakers)) {
      result[serviceName] = await this.getServiceHealth(serviceName);
    }
    
    return result;
  }
  
  /**
   * Reset circuit breaker for a service
   */
  async resetCircuitBreaker(serviceName: string): Promise<boolean> {
    const circuitBreaker = this.circuitBreakers[serviceName];
    if (!circuitBreaker) {
      return false;
    }
    
    await circuitBreaker.forceReset();
    return true;
  }
  
  /**
   * Reset all circuit breakers
   */
  async resetAllCircuitBreakers(): Promise<string[]> {
    const resetServices: string[] = [];
    
    for (const serviceName of Object.keys(this.circuitBreakers)) {
      await this.circuitBreakers[serviceName].forceReset();
      resetServices.push(serviceName);
    }
    
    return resetServices;
  }
}

// Singleton instance
let apiResilienceManagerInstance: ApiResilienceManager | null = null;

/**
 * Get or create API resilience manager instance
 */
export function getApiResilienceManager(redis: Redis): ApiResilienceManager {
  if (!apiResilienceManagerInstance) {
    apiResilienceManagerInstance = new ApiResilienceManager(redis);
  }
  
  return apiResilienceManagerInstance;
}
