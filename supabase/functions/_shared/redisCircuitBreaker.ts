
/**
 * Redis-backed Circuit Breaker implementation
 * 
 * Maintains circuit breaker state in Redis to ensure consistency across
 * serverless function invocations. Includes automatic timeout and reset
 * capabilities.
 */

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

export enum CircuitState {
  CLOSED = 'CLOSED',     // Normal operation, requests pass through
  OPEN = 'OPEN',         // Circuit is open, fail fast without calling service
  HALF_OPEN = 'HALF_OPEN' // Testing if service recovered
}

export interface CircuitBreakerOptions {
  name: string;                   // Unique name for this circuit
  failureThreshold: number;       // Number of failures before opening circuit
  resetTimeout: number;           // Milliseconds until trying half-open state
  monitorInterval?: number;       // Health check interval (ms)
  maxRetries?: number;            // Maximum retries in half-open state
  timeout?: number;               // Request timeout (ms)
  fallbackValue?: any;            // Value to return when circuit is open
}

export interface CircuitStats {
  state: CircuitState;
  failures: number;
  successes: number;
  lastFailure: number | null;
  lastSuccess: number | null;
  lastStateChange: number;
  openCount: number;
}

const DEFAULT_OPTIONS: Partial<CircuitBreakerOptions> = {
  failureThreshold: 5,
  resetTimeout: 30000,   // 30 seconds
  monitorInterval: 60000, // 1 minute
  maxRetries: 3,
  timeout: 5000          // 5 seconds
};

/**
 * Redis-backed Circuit Breaker that maintains state across function invocations
 */
export class RedisCircuitBreaker {
  private redis: Redis;
  private options: CircuitBreakerOptions;
  private keyPrefix: string = 'circuit:';
  
  constructor(redis: Redis, options: CircuitBreakerOptions) {
    this.redis = redis;
    this.options = { ...DEFAULT_OPTIONS, ...options };
    this.keyPrefix = `circuit:${this.options.name}:`;
  }
  
  /**
   * Generate Redis key for specified property
   */
  private getKey(property: string): string {
    return `${this.keyPrefix}${property}`;
  }
  
  /**
   * Execute an operation with circuit breaker protection
   */
  async execute<T>(operation: () => Promise<T>): Promise<T> {
    // Check current state
    const state = await this.getState();
    
    // If circuit is open, fail fast
    if (state === CircuitState.OPEN) {
      console.log(`Circuit ${this.options.name} is OPEN, failing fast`);
      if (this.options.fallbackValue !== undefined) {
        return this.options.fallbackValue;
      }
      throw new Error(`Circuit ${this.options.name} is open`);
    }
    
    // If half-open, only allow limited test requests
    if (state === CircuitState.HALF_OPEN) {
      const retriesKey = this.getKey('retries');
      const retries = await this.redis.incr(retriesKey);
      
      // Only allow up to maxRetries attempts in half-open state
      if (retries > this.options.maxRetries!) {
        console.log(`Circuit ${this.options.name} exceeded half-open retry limit`);
        if (this.options.fallbackValue !== undefined) {
          return this.options.fallbackValue;
        }
        throw new Error(`Circuit ${this.options.name} retry limit exceeded`);
      }
    }
    
    try {
      // Execute the operation with timeout if specified
      let result: T;
      if (this.options.timeout) {
        result = await this.withTimeout(operation, this.options.timeout);
      } else {
        result = await operation();
      }
      
      // Success - record it
      await this.onSuccess();
      return result;
    } catch (error) {
      // Failure - record it
      await this.onFailure(error);
      throw error;
    }
  }
  
  /**
   * Implements timeout for operations
   */
  private withTimeout<T>(operation: () => Promise<T>, timeoutMs: number): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error(`Operation timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      
      operation()
        .then(result => {
          clearTimeout(timeout);
          resolve(result);
        })
        .catch(error => {
          clearTimeout(timeout);
          reject(error);
        });
    });
  }
  
  /**
   * Handle successful operation
   */
  private async onSuccess(): Promise<void> {
    const state = await this.getState();
    
    if (state === CircuitState.HALF_OPEN) {
      // Service recovered, close the circuit
      console.log(`Circuit ${this.options.name}: recovery confirmed, closing circuit`);
      await this.setState(CircuitState.CLOSED);
    }
    
    // Record success
    const now = Date.now();
    await this.redis.set(this.getKey('lastSuccess'), now);
    await this.redis.incr(this.getKey('successes'));
    
    // Reset failures counter in closed state
    if (state === CircuitState.CLOSED) {
      await this.redis.set(this.getKey('failures'), 0);
    }
    
    // Clean up retries counter if needed
    if (state === CircuitState.HALF_OPEN) {
      await this.redis.del(this.getKey('retries'));
    }
  }
  
  /**
   * Handle failed operation
   */
  private async onFailure(error: any): Promise<void> {
    const state = await this.getState();
    const now = Date.now();
    
    // Record failure
    await this.redis.set(this.getKey('lastFailure'), now);
    const failures = await this.redis.incr(this.getKey('failures'));
    
    if (state === CircuitState.CLOSED && failures >= this.options.failureThreshold) {
      // Too many failures, open the circuit
      console.log(`Circuit ${this.options.name}: threshold reached (${failures} failures), opening circuit`);
      await this.setState(CircuitState.OPEN);
      
      // Set expiry for auto-reset to half-open
      await this.redis.expire(this.getKey('state'), Math.ceil(this.options.resetTimeout / 1000));
    } else if (state === CircuitState.HALF_OPEN) {
      // Failed during test, re-open the circuit
      console.log(`Circuit ${this.options.name}: failed during half-open test, reopening circuit`);
      await this.setState(CircuitState.OPEN);
      
      // Set expiry for auto-reset to half-open
      await this.redis.expire(this.getKey('state'), Math.ceil(this.options.resetTimeout / 1000));
    }
    
    // Log error details
    console.error(`Circuit ${this.options.name} operation failed:`, error.message || error);
  }
  
  /**
   * Get current circuit state
   */
  async getState(): Promise<CircuitState> {
    const state = await this.redis.get(this.getKey('state'));
    if (!state) {
      // Default state is CLOSED
      await this.setState(CircuitState.CLOSED);
      return CircuitState.CLOSED;
    }
    return state as CircuitState;
  }
  
  /**
   * Update circuit state
   */
  private async setState(state: CircuitState): Promise<void> {
    const previousState = await this.getState();
    
    if (state !== previousState) {
      await this.redis.set(this.getKey('state'), state);
      await this.redis.set(this.getKey('lastStateChange'), Date.now());
      
      if (state === CircuitState.OPEN) {
        // Increment open count
        await this.redis.incr(this.getKey('openCount'));
        
        // Set up automatic transition to half-open after resetTimeout
        setTimeout(async () => {
          const currentState = await this.getState();
          if (currentState === CircuitState.OPEN) {
            console.log(`Circuit ${this.options.name}: auto-transitioning to HALF_OPEN state`);
            await this.setState(CircuitState.HALF_OPEN);
            await this.redis.set(this.getKey('retries'), 0);
          }
        }, this.options.resetTimeout);
      }
    } else if (state === CircuitState.HALF_OPEN) {
      // Reset retry counter if re-entering half-open state
      await this.redis.set(this.getKey('retries'), 0);
    }
  }
  
  /**
   * Force reset the circuit to CLOSED state
   */
  async forceReset(): Promise<void> {
    console.log(`Circuit ${this.options.name}: manual force reset to CLOSED state`);
    await this.setState(CircuitState.CLOSED);
    await this.redis.set(this.getKey('failures'), 0);
    await this.redis.set(this.getKey('retries'), 0);
  }
  
  /**
   * Manually open the circuit
   */
  async forceOpen(): Promise<void> {
    console.log(`Circuit ${this.options.name}: manual force OPEN`);
    await this.setState(CircuitState.OPEN);
  }
  
  /**
   * Get circuit statistics
   */
  async getStats(): Promise<CircuitStats> {
    const [
      state,
      failures,
      successes,
      lastFailure,
      lastSuccess,
      lastStateChange,
      openCount
    ] = await this.redis.mget([
      this.getKey('state'),
      this.getKey('failures'),
      this.getKey('successes'),
      this.getKey('lastFailure'),
      this.getKey('lastSuccess'),
      this.getKey('lastStateChange'),
      this.getKey('openCount'),
    ]);
    
    return {
      state: (state || CircuitState.CLOSED) as CircuitState,
      failures: parseInt(failures as string || '0'),
      successes: parseInt(successes as string || '0'),
      lastFailure: lastFailure ? parseInt(lastFailure as string) : null,
      lastSuccess: lastSuccess ? parseInt(lastSuccess as string) : null,
      lastStateChange: parseInt(lastStateChange as string || '0'),
      openCount: parseInt(openCount as string || '0'),
    };
  }
}

/**
 * Create a new Redis-backed circuit breaker
 */
export function createCircuitBreaker(redis: Redis, options: CircuitBreakerOptions): RedisCircuitBreaker {
  return new RedisCircuitBreaker(redis, options);
}

/**
 * Factory to create circuit breakers with the same Redis connection
 */
export function createCircuitBreakerFactory(redis: Redis) {
  return {
    create: (options: CircuitBreakerOptions) => createCircuitBreaker(redis, options)
  };
}
