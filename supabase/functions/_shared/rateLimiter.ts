
/**
 * Lightweight rate limiter for Edge Functions
 * Uses memory-based token bucket with configurable limits
 */
export interface RateLimiterOptions {
  api: string;  // API name for identification
  endpoint?: string;  // Specific endpoint being accessed
  tokensPerInterval?: number;  // Default token bucket size
  interval?: number;  // Interval in seconds
  maxRetries?: number;  // Maximum number of retry attempts
  backoffFactor?: number;  // Factor for exponential backoff
  initialBackoffMs?: number;  // Initial backoff time in ms
}

export class RateLimiter {
  private tokenBuckets: Map<string, { tokens: number, lastRefill: number }> = new Map();
  private defaultOptions: RateLimiterOptions;

  constructor(defaultOptions: Partial<RateLimiterOptions> = {}) {
    this.defaultOptions = {
      api: 'default',
      endpoint: 'all',
      tokensPerInterval: 10,
      interval: 1, // 1 second
      maxRetries: 3,
      backoffFactor: 2,
      initialBackoffMs: 300,
      ...defaultOptions
    };
  }

  // Get a unique key for the rate limiter bucket
  private getBucketKey(api: string, endpoint: string): string {
    return `${api}:${endpoint}`;
  }

  // Refill tokens based on elapsed time
  private refillTokens(bucketKey: string, tokensPerInterval: number, interval: number): number {
    const now = Date.now() / 1000; // Convert to seconds
    const bucket = this.tokenBuckets.get(bucketKey);

    if (!bucket) {
      // Initialize bucket if it doesn't exist
      this.tokenBuckets.set(bucketKey, {
        tokens: tokensPerInterval,
        lastRefill: now
      });
      return tokensPerInterval;
    }

    // Calculate elapsed time and tokens to add
    const elapsedSeconds = now - bucket.lastRefill;
    const tokensToAdd = Math.floor(elapsedSeconds / interval) * tokensPerInterval;
    
    if (tokensToAdd > 0) {
      // Update bucket
      bucket.tokens = Math.min(tokensPerInterval, bucket.tokens + tokensToAdd);
      bucket.lastRefill = now;
    }
    
    return bucket.tokens;
  }

  // Consume a token from the bucket
  private consumeToken(bucketKey: string): boolean {
    const bucket = this.tokenBuckets.get(bucketKey);
    if (!bucket || bucket.tokens <= 0) {
      return false;
    }
    
    bucket.tokens--;
    return true;
  }

  // Execute a function with rate limiting
  async execute<T>(
    options: Partial<RateLimiterOptions>,
    fn: () => Promise<T>
  ): Promise<T> {
    // Merge options with defaults
    const opts = { ...this.defaultOptions, ...options };
    const { api, endpoint, tokensPerInterval, interval, maxRetries, backoffFactor, initialBackoffMs } = opts;
    
    if (!api) {
      throw new Error("API name must be specified for rate limiting");
    }
    
    // Generate bucket key
    const bucketKey = this.getBucketKey(api, endpoint || 'default');
    let retries = 0;
    let backoffMs = initialBackoffMs || 300;
    
    while (retries <= maxRetries) {
      // Refill tokens based on elapsed time
      const availableTokens = this.refillTokens(bucketKey, tokensPerInterval, interval);
      
      // Check if we can consume a token
      if (availableTokens > 0 && this.consumeToken(bucketKey)) {
        try {
          return await fn();
        } catch (error) {
          if (error.message && error.message.includes('rate limit') && retries < maxRetries) {
            console.log(`Rate limited by API (attempt ${retries + 1}/${maxRetries}), backing off for ${backoffMs}ms`);
            await new Promise(resolve => setTimeout(resolve, backoffMs));
            backoffMs *= backoffFactor;
            retries++;
            continue;
          }
          throw error;
        }
      } else {
        if (retries >= maxRetries) {
          throw new Error(`Rate limit exceeded for ${api}:${endpoint}`);
        }
        
        console.log(`Local rate limit hit (attempt ${retries + 1}/${maxRetries}), backing off for ${backoffMs}ms`);
        await new Promise(resolve => setTimeout(resolve, backoffMs));
        backoffMs *= backoffFactor;
        retries++;
      }
    }
    
    throw new Error(`Rate limit error for ${api}: Maximum retries exceeded`);
  }
}

// Singleton instance for shared rate limiting across functions
let globalRateLimiter: RateLimiter | null = null;

export function getRateLimiter(options: Partial<RateLimiterOptions> = {}): RateLimiter {
  if (!globalRateLimiter) {
    globalRateLimiter = new RateLimiter(options);
  }
  return globalRateLimiter;
}

// Add the missing RATE_LIMITERS export that's being imported elsewhere
export const RATE_LIMITERS = {
  SPOTIFY: {
    DEFAULT: {
      api: "spotify",
      endpoint: "default",
      tokensPerInterval: 100,
      interval: 30, // 100 requests per 30 seconds
      maxRetries: 3,
      backoffFactor: 2,
      initialBackoffMs: 1000
    },
    SEARCH: {
      api: "spotify",
      endpoint: "search",
      tokensPerInterval: 30,
      interval: 30, // 30 requests per 30 seconds
      maxRetries: 3,
      backoffFactor: 2,
      initialBackoffMs: 1000
    }
  },
  GENIUS: {
    DEFAULT: {
      api: "genius",
      endpoint: "default",
      tokensPerInterval: 25,
      interval: 5, // 25 requests per 5 seconds
      maxRetries: 3,
      backoffFactor: 2,
      initialBackoffMs: 500
    }
  }
};
