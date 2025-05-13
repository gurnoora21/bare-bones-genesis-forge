
/**
 * Optimized rate limiter using the official Upstash Ratelimit library
 * with local memory caching to minimize Redis calls
 */

import { getRedis } from "./upstashRedis.ts";
import { EnhancedRateLimiter, getEnhancedRateLimiter } from "./enhancedRateLimiter.ts";
import { MemoryCache } from "./memoryCache.ts";

// Import RateLimitAlgorithm type from enhancedRateLimiter.ts and define our own options
import { RateLimitAlgorithm } from "./enhancedRateLimiter.ts";

// Define our own RateLimiterOptions since it's not exported from enhancedRateLimiter.ts
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
}

export class RateLimiter {
  private redis = getRedis();
  private enhancedLimiter: EnhancedRateLimiter;
  private responseCache: MemoryCache<any> = new MemoryCache(1000, 60000);
  
  constructor() {
    this.enhancedLimiter = getEnhancedRateLimiter(this.redis.redis);
  }
  
  /**
   * Execute a function with rate limiting
   * If rate limit is exceeded, will wait and retry with exponential backoff
   */
  async execute<T>(
    options: RateLimiterOptions,
    fn: () => Promise<T>
  ): Promise<T> {
    try {
      const startTime = Date.now();
      
      // Use the enhanced rate limiter
      const result = await this.enhancedLimiter.execute(
        options,
        async () => {
          try {
            // Execute operation
            const result = await fn();
            
            // Track successful API call
            await this.redis.trackApiCall(
              options.identifier.split(":")[0] || "api", 
              options.identifier.split(":")[1] || "default",
              true
            );
            
            return result;
          } catch (error) {
            // Track failed API call
            await this.redis.trackApiCall(
              options.identifier.split(":")[0] || "api", 
              options.identifier.split(":")[1] || "default", 
              false
            );
            throw error;
          }
        },
        {
          maxRetries: 5,
          retryDelay: 1000,
          retryMultiplier: 2
        }
      );
      
      return result;
    } catch (error) {
      // Rethrow with more details
      throw new Error(`Rate limit error for ${options.identifier}: ${error.message}`);
    }
  }
  
  /**
   * Execute a function with both rate limiting and result caching
   */
  async executeWithCache<T>(
    options: RateLimiterOptions & { 
      cacheKey: string,
      cacheTtl: number  // in seconds
    },
    fn: () => Promise<T>
  ): Promise<T> {
    const { cacheKey, cacheTtl, ...limiterOptions } = options;
    
    // Try memory cache first (faster than Redis)
    const memoryCachedResult = this.responseCache.get(cacheKey);
    if (memoryCachedResult !== null && memoryCachedResult !== undefined) {
      console.log(`Memory cache hit for ${cacheKey}`);
      return memoryCachedResult;
    }
    
    // Try Redis cache
    const cachedResult = await this.redis.cacheGet(cacheKey);
    if (cachedResult !== null) {
      console.log(`Redis cache hit for ${cacheKey}`);
      // Update memory cache
      this.responseCache.set(cacheKey, cachedResult, cacheTtl);
      return cachedResult;
    }
    
    // Cache miss, execute with rate limiting
    console.log(`Cache miss for ${cacheKey}`);
    const result = await this.execute(limiterOptions, fn);
    
    // Cache the result in both memory and Redis
    this.responseCache.set(cacheKey, result, cacheTtl);
    await this.redis.cacheSet(cacheKey, result, cacheTtl);
    
    return result;
  }
}

// Export a singleton instance
let limiterInstance: RateLimiter | null = null;

export function getRateLimiter(): RateLimiter {
  if (!limiterInstance) {
    limiterInstance = new RateLimiter();
  }
  return limiterInstance;
}

// Preset rate limiter configurations using the enhanced library
export const RATE_LIMITERS = {
  // Spotify API has a rolling 30-second window
  SPOTIFY: {
    DEFAULT: {
      identifier: "spotify:default",
      limit: 100,  // Relatively safe default limit
      windowSeconds: 30, // 30 seconds rolling window
      algorithm: "sliding",
      analytics: false
    },
    SEARCH: {
      identifier: "spotify:search",
      limit: 15,  // Lower limit for search
      windowSeconds: 30,
      algorithm: "sliding",
      analytics: false
    },
    BATCH: {
      identifier: "spotify:batch",
      limit: 5,   // Even lower for batch operations
      windowSeconds: 30,
      algorithm: "token",
      analytics: false
    }
  },
  
  // Genius API recommended max ~5 req/sec
  GENIUS: {
    DEFAULT: {
      identifier: "genius:default",
      limit: 25,   // 5 req/sec = 25 per 5 seconds
      windowSeconds: 5,  // 5 second window
      algorithm: "token", // Token bucket for smoother rate
      analytics: false
    }
  }
};
