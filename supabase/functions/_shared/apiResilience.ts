/**
 * API resilience system with enhanced rate limiting and circuit breakers
 * Provides protection against API quota exhaustion and service outages
 */

// Standard types for resilience configuration
export interface ResilienceOptions {
  timeout?: number;
  retries?: number;
  exponentialBackoff?: boolean;
  jitter?: boolean;
  circuitBreakerOptions?: CircuitBreakerOptions;
  rateLimitOptions?: RateLimitOptions;
}

// Circuit breaker specific options
export interface CircuitBreakerOptions {
  failureThreshold?: number;
  resetTimeout?: number;
  halfOpenSuccessThreshold?: number;
}

// Rate limiting specific options
export interface RateLimitOptions {
  tokensPerSecond?: number;
  bucketSize?: number;
  adaptiveFactor?: number;
  adaptiveWindow?: number;
}

// Current state of a circuit breaker
export enum CircuitState {
  CLOSED = 'CLOSED',
  OPEN = 'OPEN',
  HALF_OPEN = 'HALF_OPEN'
}

// Types of API resilience
export enum ResilienceType {
  CIRCUIT_BREAKER = 'CIRCUIT_BREAKER',
  RATE_LIMITER = 'RATE_LIMITER',
  RETRY = 'RETRY',
  TIMEOUT = 'TIMEOUT'
}

// Service health status enum
export enum ServiceStatus {
  HEALTHY = 'HEALTHY',
  DEGRADED = 'DEGRADED',
  UNAVAILABLE = 'UNAVAILABLE'
}

// In-memory token bucket for rate limiting
export class LocalTokenBucket {
  private tokens: number;
  private lastRefill: number;
  private readonly tokensPerSecond: number;
  private readonly maxTokens: number;
  
  constructor(tokensPerSecond: number, bucketSize: number) {
    this.tokens = bucketSize;
    this.lastRefill = Date.now();
    this.tokensPerSecond = tokensPerSecond;
    this.maxTokens = bucketSize;
  }
  
  refill(): void {
    const now = Date.now();
    const timePassed = (now - this.lastRefill) / 1000; // in seconds
    const newTokens = timePassed * this.tokensPerSecond;
    
    if (newTokens > 0) {
      this.tokens = Math.min(this.maxTokens, this.tokens + newTokens);
      this.lastRefill = now;
    }
  }
  
  consume(tokens: number): boolean {
    this.refill();
    
    if (this.tokens >= tokens) {
      this.tokens -= tokens;
      return true;
    }
    
    return false;
  }
  
  getTokens(): number {
    this.refill();
    return this.tokens;
  }
}

// Adaptive token bucket using Redis for distributed rate limiting
export class AdaptiveTokenBucket {
  private readonly redis: any;
  private readonly key: string;
  private readonly tokensPerSecond: number;
  private readonly maxTokens: number;
  private readonly adaptiveFactor: number;
  private readonly adaptiveWindow: number;
  private lastAdaptation: number;
  private failureCount: number;
  private successCount: number;
  
  constructor(
    redis: any,
    key: string,
    tokensPerSecond: number,
    bucketSize: number,
    adaptiveFactor: number = 0.75,
    adaptiveWindow: number = 60 // in seconds
  ) {
    this.redis = redis;
    this.key = `ratelimit:${key}`;
    this.tokensPerSecond = tokensPerSecond;
    this.maxTokens = bucketSize;
    this.adaptiveFactor = adaptiveFactor;
    this.adaptiveWindow = adaptiveWindow;
    this.lastAdaptation = Date.now();
    this.failureCount = 0;
    this.successCount = 0;
  }
  
  async initialize(): Promise<void> {
    const currentTokens = await this.redis.get(this.key);
    
    if (currentTokens === null) {
      await this.redis.set(this.key, this.maxTokens.toString());
      await this.redis.expire(this.key, this.adaptiveWindow * 2);
    }
  }
  
  async refill(): Promise<void> {
    try {
      const currentTokens = await this.redis.get(this.key);
      
      if (currentTokens === null) {
        await this.initialize();
        return;
      }
      
      const now = Date.now();
      const lastRefillKey = `${this.key}:lastRefill`;
      const lastRefillStr = await this.redis.get(lastRefillKey);
      const lastRefill = lastRefillStr ? parseInt(lastRefillStr, 10) : now - 1000;
      
      const timePassed = (now - lastRefill) / 1000; // in seconds
      const newTokens = timePassed * this.tokensPerSecond;
      
      if (newTokens > 0) {
        const updatedTokens = Math.min(
          this.maxTokens,
          parseFloat(currentTokens) + newTokens
        );
        
        await this.redis.set(this.key, updatedTokens.toString());
        await this.redis.set(lastRefillKey, now.toString());
        
        // Extend expiration time
        await this.redis.expire(this.key, this.adaptiveWindow * 2);
        await this.redis.expire(lastRefillKey, this.adaptiveWindow * 2);
      }
    } catch (error) {
      console.error(`Error refilling token bucket: ${error.message}`);
    }
  }
  
  async getTokens(): Promise<number> {
    await this.refill();
    const currentTokens = await this.redis.get(this.key);
    return currentTokens ? parseFloat(currentTokens) : this.maxTokens;
  }
  
  async consume(tokens: number): Promise<boolean> {
    if (!this.redis) {
      return false;
    }
    
    try {
      await this.refill();
      const currentTokens = await this.redis.get(this.key);
      
      if (!currentTokens || parseFloat(currentTokens) < tokens) {
        return false;
      }
      
      // FIX: Replace eval with direct DECRBY
      try {
        // Instead of using eval which isn't available, use direct DECRBY 
        await this.redis.decrby(this.key, tokens);
        return true;
      } catch (error) {
        console.error(`Error consuming tokens: ${error.message}`);
        return false;
      }
    } catch (error) {
      console.error(`Error consuming from token bucket: ${error.message}`);
      return false;
    }
  }
  
  recordSuccess(): void {
    this.successCount++;
    this.adapt();
  }
  
  recordFailure(): void {
    this.failureCount++;
    this.adapt();
  }
  
  async adapt(): Promise<void> {
    const now = Date.now();
    
    if (now - this.lastAdaptation > this.adaptiveWindow * 1000) {
      const failureRate = this.failureCount / (this.successCount + this.failureCount + 1e-9);
      
      if (failureRate > 0.5) {
        this.tokensPerSecond *= (1 - this.adaptiveFactor);
        console.warn(`Adapting rate limit: reducing to ${this.tokensPerSecond} tokens/second`);
      } else {
        this.tokensPerSecond /= (1 - this.adaptiveFactor);
        this.tokensPerSecond = Math.min(this.tokensPerSecond, this.maxTokens);
        console.log(`Adapting rate limit: increasing to ${this.tokensPerSecond} tokens/second`);
      }
      
      this.failureCount = 0;
      this.successCount = 0;
      this.lastAdaptation = now;
    }
  }
  
  getCurrentLimit(): number {
    return this.tokensPerSecond;
  }
}

// In-memory circuit breaker
export class CircuitBreaker {
  private state: CircuitState = CircuitState.CLOSED;
  private failureCount: number = 0;
  private lastFailureTime: number = 0;
  private readonly failureThreshold: number;
  private readonly resetTimeout: number;
  private readonly halfOpenSuccessThreshold: number;
  
  constructor(options: CircuitBreakerOptions = {}) {
    this.failureThreshold = options.failureThreshold || 5;
    this.resetTimeout = options.resetTimeout || 30000; // 30 seconds
    this.halfOpenSuccessThreshold = options.halfOpenSuccessThreshold || 3;
  }
  
  getState(): CircuitState {
    return this.state;
  }
  
  allowRequest(): boolean {
    if (this.state === CircuitState.OPEN) {
      if (Date.now() - this.lastFailureTime < this.resetTimeout) {
        return false;
      }
      this.state = CircuitState.HALF_OPEN;
      return true;
    }
    return true;
  }
  
  recordSuccess(): void {
    if (this.state === CircuitState.HALF_OPEN) {
      this.halfOpenSuccessThreshold--;
      if (this.halfOpenSuccessThreshold <= 0) {
        this.reset();
      }
    }
  }
  
  recordFailure(): void {
    this.failureCount++;
    this.lastFailureTime = Date.now();
    
    if (this.failureCount >= this.failureThreshold) {
      this.open();
    }
  }
  
  open(): void {
    this.state = CircuitState.OPEN;
    this.failureCount = 0;
    console.warn('Circuit breaker opened');
  }
  
  reset(): void {
    this.state = CircuitState.CLOSED;
    this.failureCount = 0;
    console.log('Circuit breaker reset');
  }
}

// Central API resilience manager
export class ApiResilienceManager {
  private readonly service: string;
  private readonly namespace?: string;
  private readonly redis: any;
  private readonly circuitBreaker?: CircuitBreaker;
  private readonly rateLimiter?: AdaptiveTokenBucket | LocalTokenBucket;
  private readonly options: ResilienceOptions;
  
  constructor(
    service: string,
    namespace?: string,
    redis?: any,
    options: ResilienceOptions = {}
  ) {
    this.service = service;
    this.namespace = namespace;
    this.redis = redis;
    this.options = options;
    
    if (options.circuitBreakerOptions) {
      this.circuitBreaker = new CircuitBreaker(options.circuitBreakerOptions);
    }
    
    if (options.rateLimitOptions) {
      const {
        tokensPerSecond = 5,
        bucketSize = 5,
        adaptiveFactor,
        adaptiveWindow
      } = options.rateLimitOptions;
      
      if (redis) {
        this.rateLimiter = new AdaptiveTokenBucket(
          redis,
          `${service}:${namespace || 'default'}`,
          tokensPerSecond,
          bucketSize,
          adaptiveFactor,
          adaptiveWindow
        );
        (this.rateLimiter as AdaptiveTokenBucket).initialize().catch(console.error);
      } else {
        this.rateLimiter = new LocalTokenBucket(tokensPerSecond, bucketSize);
      }
    }
  }
  
  async execute<T>(fn: () => Promise<T>): Promise<T> {
    if (this.circuitBreaker && !this.circuitBreaker.allowRequest()) {
      throw new Error('Circuit breaker is open');
    }
    
    if (this.rateLimiter) {
      const tokensToConsume = 1;
      let allowed: boolean;
      
      if (this.rateLimiter instanceof AdaptiveTokenBucket) {
        allowed = await this.rateLimiter.consume(tokensToConsume);
      } else {
        allowed = this.rateLimiter.consume(tokensToConsume);
      }
      
      if (!allowed) {
        throw new Error('Rate limit exceeded');
      }
    }
    
    try {
      const result = await this.withTimeout(fn, this.options.timeout);
      this.circuitBreaker?.recordSuccess();
      (this.rateLimiter as AdaptiveTokenBucket)?.recordSuccess();
      return result;
    } catch (error) {
      this.circuitBreaker?.recordFailure();
      (this.rateLimiter as AdaptiveTokenBucket)?.recordFailure();
      throw error;
    }
  }
  
  private async withTimeout<T>(fn: () => Promise<T>, timeout?: number): Promise<T> {
    if (!timeout) {
      return fn();
    }
    
    return Promise.race([
      fn(),
      new Promise<T>((_, reject) =>
        setTimeout(() => reject(new Error('Timeout exceeded')), timeout)
      ),
    ]);
  }
  
  // Add a method to get API health status
  async getServiceHealth(): Promise<{ status: ServiceStatus, details?: any }> {
    let status = ServiceStatus.HEALTHY;
    const details = {
      circuitBreakerState: this.circuitBreaker ? this.circuitBreaker.getState() : 'NOT_CONFIGURED',
      rateLimit: this.rateLimiter instanceof AdaptiveTokenBucket ? 
        await this.rateLimiter.getCurrentLimit() : 
        (this.rateLimiter ? 'USING_LOCAL_BUCKET' : 'NOT_CONFIGURED')
    };
    
    // Check if circuit breaker is open
    if (this.circuitBreaker && this.circuitBreaker.getState() === CircuitState.OPEN) {
      status = ServiceStatus.UNAVAILABLE;
    }
    
    // Check if we're rate limited
    if (this.rateLimiter instanceof AdaptiveTokenBucket) {
      try {
        const tokens = await this.rateLimiter.getTokens();
        if (tokens < 1) {
          status = status === ServiceStatus.UNAVAILABLE ? 
            ServiceStatus.UNAVAILABLE : ServiceStatus.DEGRADED;
        }
      } catch (error) {
        console.warn("Error checking rate limiter status:", error);
      }
    }
    
    return { status, details };
  }
  
  // Add method to execute API call with specialized response handling
  async executeApiCall<T>(
    endpoint: string,
    apiCallFn: () => Promise<Response>,
    options: {
      correlationId?: string;
      processResponse?: (response: Response) => Promise<T>;
      fallbackFn?: () => Promise<T>;
    } = {}
  ): Promise<T> {
    const { 
      correlationId = `api_${Date.now()}`,
      processResponse = async (r) => await r.json() as T,
      fallbackFn
    } = options;
    
    try {
      return await this.execute(async () => {
        console.log(`[${correlationId}] Making API call to ${endpoint}`);
        const response = await apiCallFn();
        return await processResponse(response);
      });
    } catch (error) {
      console.error(`[${correlationId}] API call failed: ${error.message}`);
      
      // Try using fallback if available
      if (fallbackFn) {
        try {
          console.log(`[${correlationId}] Attempting to use fallback for ${endpoint}`);
          return await fallbackFn();
        } catch (fallbackError) {
          console.error(`[${correlationId}] Fallback also failed: ${fallbackError.message}`);
        }
      }
      
      throw error;
    }
  }
}

// Resilience factory method
export function getApiResilienceManager(
  service: string,
  namespace?: string,
  redis?: any,
  options?: ResilienceOptions
): ApiResilienceManager {
  return new ApiResilienceManager(service, namespace, redis, options);
}

// Add specialized Genius API resilience manager factory
export function getGeniusApiResilienceManager(): ApiResilienceManager {
  // Import Redis from our shared module to use for rate limiting
  let redis;
  try {
    const { getRedis } = require("./upstashRedis.ts");
    redis = getRedis();
  } catch (error) {
    console.warn("Failed to initialize Redis for Genius API resilience:", error.message);
  }
  
  // Configure resilience options specifically for Genius API
  const geniusResilienceOptions: ResilienceOptions = {
    timeout: 10000, // 10-second timeout for Genius API calls
    retries: 2,     // Retry failed requests twice
    exponentialBackoff: true,
    jitter: true,
    circuitBreakerOptions: {
      failureThreshold: 5,      // Open after 5 consecutive failures
      resetTimeout: 60000,      // Try again after 1 minute
      halfOpenSuccessThreshold: 2  // Close after 2 successful requests
    },
    rateLimitOptions: {
      tokensPerSecond: 0.2,  // About 5 requests per second for Genius API
      bucketSize: 10,       // Allow burst of up to 10 requests
      adaptiveFactor: 0.8,  // Aggressively adjust rate limit on failures
      adaptiveWindow: 120   // Check for adjustments every 2 minutes
    }
  };
  
  // Create and return the manager
  return getApiResilienceManager(
    "genius-api",
    "default",
    redis,
    geniusResilienceOptions
  );
}
