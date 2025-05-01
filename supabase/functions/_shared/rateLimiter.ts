
/**
 * Distributed rate limiter using token bucket algorithm
 * and Upstash Redis for cross-process coordination
 */

import { getRedis } from "./upstashRedis.ts";

export interface RateLimiterOptions {
  // API identifier (spotify, genius, etc)
  api: string;
  
  // Endpoint identifier (e.g., 'search', 'artists', etc.)
  endpoint?: string;
  
  // Number of requests allowed per interval
  tokensPerInterval: number;
  
  // Interval in seconds
  interval: number;
  
  // Number of tokens to consume (default: 1)
  cost?: number;
  
  // Maximum retry attempts
  maxRetries?: number;
  
  // Base delay for exponential backoff (ms)
  baseDelay?: number;
}

export class RateLimiter {
  private redis = getRedis();
  
  /**
   * Execute a function with rate limiting
   * If rate limit is exceeded, will wait and retry with exponential backoff
   */
  async execute<T>(
    options: RateLimiterOptions,
    fn: () => Promise<T>
  ): Promise<T> {
    const {
      api,
      endpoint = "default",
      tokensPerInterval,
      interval,
      cost = 1,
      maxRetries = 5,
      baseDelay = 1000
    } = options;
    
    const key = `ratelimit:${api}:${endpoint}`;
    let attempts = 0;
    
    // Generate a cache key if provided in the options
    const cacheKey = `${api}:${endpoint}:${JSON.stringify(options)}`;
    
    while (attempts < maxRetries) {
      // Check if we're under the rate limit
      const allowed = await this.redis.evalTokenBucket(key, tokensPerInterval, interval, cost);
      
      if (allowed) {
        try {
          // Track this API call
          const startTime = Date.now();
          const result = await fn();
          
          // Record successful API call
          await this.redis.trackApiCall(api, endpoint, true);
          
          return result;
        } catch (error) {
          // Record failed API call
          await this.redis.trackApiCall(api, endpoint, false);
          
          // Check if error is rate-limit related
          if (error instanceof Error && 
              (error.message.includes("429") || error.message.includes("rate limit"))) {
            attempts++;
            
            // Calculate backoff with jitter
            const jitter = Math.random() * 0.3 + 0.85; // 0.85-1.15
            const delay = Math.floor(baseDelay * Math.pow(2, attempts) * jitter);
            
            console.log(`Rate limited by ${api} API (${endpoint}), retry ${attempts}/${maxRetries} after ${delay}ms`);
            await new Promise(resolve => setTimeout(resolve, delay));
            continue;
          }
          
          // For non-rate-limit errors, rethrow
          throw error;
        }
      } else {
        // We're rate limited locally by our token bucket
        attempts++;
        
        // Use a shorter backoff for local rate limiting
        const delay = Math.floor(baseDelay * attempts);
        console.log(`Local rate limit for ${api} (${endpoint}), retry ${attempts}/${maxRetries} after ${delay}ms`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
    
    throw new Error(`Rate limit exceeded for ${api} after ${maxRetries} attempts`);
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
    
    // Try to get from cache first
    const cachedResult = await this.redis.cacheGet(cacheKey);
    if (cachedResult !== null) {
      console.log(`Cache hit for ${cacheKey}`);
      return cachedResult;
    }
    
    // Cache miss, execute with rate limiting
    console.log(`Cache miss for ${cacheKey}`);
    const result = await this.execute(limiterOptions, fn);
    
    // Cache the result
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

// Preset rate limiter configurations
export const RATE_LIMITERS = {
  // Spotify API has a rolling 30-second window
  SPOTIFY: {
    DEFAULT: {
      api: "spotify",
      tokensPerInterval: 100,  // Relatively safe default limit
      interval: 30,            // 30 seconds rolling window
      maxRetries: 5
    },
    SEARCH: {
      api: "spotify",
      endpoint: "search",
      tokensPerInterval: 15,  // Lower limit for search
      interval: 30,
      maxRetries: 3
    },
    BATCH: {
      api: "spotify",
      endpoint: "batch",
      tokensPerInterval: 5,   // Even lower for batch operations
      interval: 30,
      maxRetries: 5
    }
  },
  
  // Genius API recommended max ~5 req/sec
  GENIUS: {
    DEFAULT: {
      api: "genius",
      tokensPerInterval: 25,   // 5 req/sec = 25 per 5 seconds
      interval: 5,             // 5 second window
      maxRetries: 3
    }
  }
};
