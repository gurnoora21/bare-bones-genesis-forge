
/**
 * Enhanced Rate Limiter using Upstash's official Ratelimit library
 * Provides efficient serverless-optimized rate limiting with minimal Redis calls
 */

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { Ratelimit } from "https://esm.sh/@upstash/ratelimit@1.0.0";
import { MemoryCache } from "./memoryCache.ts";

export type RateLimitAlgorithm = 'fixed' | 'sliding' | 'token';

export interface RateLimiterOptions {
  // Resource identifier (api name, endpoint, etc)
  identifier: string;
  
  // Number of requests allowed per time window
  limit: number;
  
  // Time window in seconds
  windowSeconds: number;
  
  // Algorithm to use (fixed window, sliding window, token bucket)
  algorithm?: RateLimitAlgorithm;
  
  // If true, store analytics data in Redis
  analytics?: boolean;
  
  // Prefix for Redis keys
  prefix?: string;
  
  // Cost of current operation (default: 1)
  cost?: number;
  
  // Use local memory cache to reduce Redis calls
  useLocalCache?: boolean;
}

export interface RateLimitResponse {
  success: boolean;     // Whether the request is allowed
  limit: number;        // Maximum requests allowed
  remaining: number;    // Remaining requests in current window
  reset: number;        // Timestamp when the limit resets (ms)
  retryAfter?: number;  // Suggested retry time in ms
}

export class EnhancedRateLimiter {
  private redis: Redis;
  private limiters: Map<string, Ratelimit> = new Map();
  private memoryCache: MemoryCache<RateLimitResponse> = new MemoryCache(100, 1000);
  private readonly defaultPrefix = "ratelimit";
  
  constructor(redis: Redis) {
    this.redis = redis;
  }
  
  /**
   * Check if an operation is allowed under rate limits
   */
  async limit(options: RateLimiterOptions): Promise<RateLimitResponse> {
    const {
      identifier,
      limit,
      windowSeconds,
      algorithm = 'sliding',
      analytics = false,
      prefix = this.defaultPrefix,
      cost = 1,
      useLocalCache = true
    } = options;
    
    const limiterKey = `${prefix}:${identifier}:${limit}:${windowSeconds}:${algorithm}`;
    
    // Check memory cache first if enabled
    if (useLocalCache) {
      const cached = this.memoryCache.get(limiterKey);
      if (cached && !this.shouldRecheck(cached)) {
        return cached;
      }
    }
    
    try {
      // Get or create rate limiter
      let limiter = this.limiters.get(limiterKey);
      if (!limiter) {
        limiter = this.createLimiter(algorithm, limit, windowSeconds, prefix, analytics);
        this.limiters.set(limiterKey, limiter);
      }
      
      // Check rate limit
      const result = await limiter.limit(identifier, { cost });
      
      // Cache result locally to reduce Redis calls
      if (useLocalCache && result.success) {
        this.memoryCache.set(
          limiterKey, 
          result, 
          // Cache for shorter duration when close to limit
          result.remaining < limit * 0.2 ? 1 : Math.min(5, windowSeconds / 10)
        );
      }
      
      return result;
    } catch (error) {
      console.error(`Rate limit check failed: ${error.message}`);
      
      // Fallback to allow in case of Redis error
      // This is a safety mechanism to prevent complete system failure
      // but may allow more requests than desired
      return {
        success: true,
        limit,
        remaining: 1,
        reset: Date.now() + 1000,
        retryAfter: 1000
      };
    }
  }
  
  /**
   * Create a rate limiter with the specified algorithm
   */
  private createLimiter(
    algorithm: RateLimitAlgorithm,
    limit: number, 
    windowSeconds: number,
    prefix: string,
    analytics: boolean
  ): Ratelimit {
    const window = `${windowSeconds} s`;
    
    switch (algorithm) {
      case 'fixed':
        return new Ratelimit({
          redis: this.redis,
          limiter: Ratelimit.fixedWindow(limit, window),
          prefix,
          analytics
        });
      
      case 'token':
        return new Ratelimit({
          redis: this.redis,
          limiter: Ratelimit.tokenBucket(limit, window, limit), // refill rate, window, bucket size
          prefix,
          analytics
        });
      
      case 'sliding':
      default:
        return new Ratelimit({
          redis: this.redis,
          limiter: Ratelimit.slidingWindow(limit, window),
          prefix,
          analytics
        });
    }
  }
  
  /**
   * Determine if we should recheck the rate limit
   * based on remaining tokens and time
   */
  private shouldRecheck(cached: RateLimitResponse): boolean {
    // Always recheck if blocked
    if (!cached.success) return true;
    
    // Recheck if close to limit reset
    const timeToReset = cached.reset - Date.now();
    if (timeToReset < 0) return true;
    
    // Recheck if very few tokens remain
    if (cached.remaining <= 1) return true;
    
    return false;
  }
  
  /**
   * Execute an operation with rate limiting
   * Returns the operation result or throws if rate limited
   */
  async execute<T>(
    options: RateLimiterOptions, 
    operation: () => Promise<T>,
    retryOptions?: {
      maxRetries?: number;
      retryDelay?: number;
      retryMultiplier?: number;
    }
  ): Promise<T> {
    const { 
      maxRetries = 3, 
      retryDelay = 1000, 
      retryMultiplier = 2 
    } = retryOptions || {};
    
    let retries = 0;
    
    while (true) {
      const result = await this.limit(options);
      
      if (result.success) {
        return await operation();
      }
      
      // Rate limited - check if we should retry
      if (retries >= maxRetries) {
        throw new Error(`Rate limit exceeded for ${options.identifier}. Try again in ${Math.ceil(result.retryAfter! / 1000)} seconds.`);
      }
      
      // Calculate backoff delay with jitter
      const jitter = Math.random() * 0.2 + 0.9; // 0.9-1.1
      const delay = retryDelay * Math.pow(retryMultiplier, retries) * jitter;
      
      console.log(`Rate limited, retry ${retries + 1}/${maxRetries} after ${Math.ceil(delay)}ms`);
      await new Promise(resolve => setTimeout(resolve, delay));
      
      retries++;
    }
  }
  
  /**
   * Sleep for a calculated period when rate limited
   */
  async handleRateLimit(response: RateLimitResponse, baseDelay: number = 1000): Promise<void> {
    if (!response.success) {
      // Use suggested retry time or calculate a reasonable delay
      const delay = response.retryAfter || 
                    (response.reset - Date.now()) || 
                    baseDelay;
                    
      console.log(`Rate limited, waiting ${Math.ceil(delay / 1000)} seconds before retry`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  /**
   * Get preset options for common APIs
   */
  static presets = {
    spotify: {
      search: {
        identifier: "spotify:search",
        limit: 30,
        windowSeconds: 30,
        algorithm: "sliding" as RateLimitAlgorithm
      },
      default: {
        identifier: "spotify:default", 
        limit: 100,
        windowSeconds: 30,
        algorithm: "sliding" as RateLimitAlgorithm
      }
    },
    genius: {
      default: {
        identifier: "genius:default",
        limit: 25,
        windowSeconds: 5,
        algorithm: "token" as RateLimitAlgorithm
      }
    }
  };
}

// Singleton instance
let rateLimiterInstance: EnhancedRateLimiter | null = null;

/**
 * Get or create the enhanced rate limiter instance
 */
export function getEnhancedRateLimiter(redis: Redis): EnhancedRateLimiter {
  if (!rateLimiterInstance) {
    rateLimiterInstance = new EnhancedRateLimiter(redis);
  }
  
  return rateLimiterInstance;
}
