/**
 * API resilience system with enhanced rate limiting and circuit breakers
 * Provides protection against API quota exhaustion and service outages
 */

import { ErrorCategory, ErrorSource, createEnhancedError, retryWithBackoff } from "./errorHandling.ts";
import { getRedis } from "./upstashRedis.ts";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

// Service health status
export enum ServiceStatus {
  HEALTHY = 'HEALTHY',           // All operations normal
  DEGRADED = 'DEGRADED',         // Some operations failing
  CIRCUIT_OPEN = 'CIRCUIT_OPEN', // Circuit breaker tripped
  RATE_LIMITED = 'RATE_LIMITED', // Rate limiting in effect
  UNKNOWN = 'UNKNOWN'            // Status not yet determined
}

// Circuit breaker states
export enum CircuitState {
  CLOSED = 'CLOSED',       // Normal operation, requests go through
  OPEN = 'OPEN',           // Circuit tripped, requests fail fast
  HALF_OPEN = 'HALF_OPEN'  // Testing if service is back, limited requests
}

// Configuration for circuit breakers
export interface CircuitBreakerConfig {
  failureThreshold: number;         // Number of failures before opening
  failureWindow: number;            // Time window for counting failures (ms)
  resetTimeout: number;             // Time before testing service again (ms)
  halfOpenSuccessThreshold: number; // Successes needed to close circuit again
}

// Adaptive rate limiter configuration
export interface AdaptiveRateLimitConfig {
  initialLimit: number;     // Initial tokens per interval
  interval: number;         // Interval in ms
  adaptiveReduction: number; // Factor to reduce by on 429 (e.g., 0.8 = 80%)
  recoveryRate: number;     // Rate to recover capacity (e.g., 0.1 = 10% per interval)
  minLimit: number;         // Minimum token limit
}

// External API configuration
export interface ApiConfig {
  name: string;
  endpoints: Record<string, {
    rateLimit: AdaptiveRateLimitConfig;
    circuitBreaker: CircuitBreakerConfig;
  }>;
  defaultEndpoint: string;
  serviceHealthKey: string;
}

// Default configurations
export const DEFAULT_CIRCUIT_BREAKER: CircuitBreakerConfig = {
  failureThreshold: 5,
  failureWindow: 60 * 1000, // 1 minute
  resetTimeout: 30 * 1000,  // 30 seconds
  halfOpenSuccessThreshold: 2
};

export const DEFAULT_RATE_LIMIT: AdaptiveRateLimitConfig = {
  initialLimit: 100,
  interval: 60 * 1000, // 1 minute
  adaptiveReduction: 0.8,
  recoveryRate: 0.1,
  minLimit: 10
};

// Spotify API configuration
export const SPOTIFY_API_CONFIG: ApiConfig = {
  name: 'spotify',
  serviceHealthKey: 'service:health:spotify',
  defaultEndpoint: 'default',
  endpoints: {
    default: {
      rateLimit: {
        initialLimit: 100,
        interval: 30 * 1000, // 30 seconds
        adaptiveReduction: 0.8,
        recoveryRate: 0.05,
        minLimit: 10
      },
      circuitBreaker: DEFAULT_CIRCUIT_BREAKER
    },
    search: {
      rateLimit: {
        initialLimit: 15,
        interval: 30 * 1000, // 30 seconds
        adaptiveReduction: 0.7,
        recoveryRate: 0.1,
        minLimit: 5
      },
      circuitBreaker: {
        ...DEFAULT_CIRCUIT_BREAKER,
        failureThreshold: 3
      }
    },
    artists: {
      rateLimit: {
        initialLimit: 50,
        interval: 30 * 1000, // 30 seconds 
        adaptiveReduction: 0.8,
        recoveryRate: 0.1,
        minLimit: 10
      },
      circuitBreaker: DEFAULT_CIRCUIT_BREAKER
    },
    albums: {
      rateLimit: {
        initialLimit: 50,
        interval: 30 * 1000, // 30 seconds
        adaptiveReduction: 0.8, 
        recoveryRate: 0.1,
        minLimit: 10
      },
      circuitBreaker: DEFAULT_CIRCUIT_BREAKER
    },
    tracks: {
      rateLimit: {
        initialLimit: 50,
        interval: 30 * 1000, // 30 seconds
        adaptiveReduction: 0.8,
        recoveryRate: 0.1,
        minLimit: 10
      },
      circuitBreaker: DEFAULT_CIRCUIT_BREAKER
    },
    batch: {
      rateLimit: {
        initialLimit: 5,
        interval: 30 * 1000, // 30 seconds
        adaptiveReduction: 0.5,
        recoveryRate: 0.05,
        minLimit: 2
      },
      circuitBreaker: {
        ...DEFAULT_CIRCUIT_BREAKER,
        failureThreshold: 3,
        resetTimeout: 60 * 1000 // 1 minute
      }
    }
  }
};

// Genius API configuration
export const GENIUS_API_CONFIG: ApiConfig = {
  name: 'genius',
  serviceHealthKey: 'service:health:genius',
  defaultEndpoint: 'default',
  endpoints: {
    default: {
      rateLimit: {
        initialLimit: 25,
        interval: 5 * 1000, // 5 seconds
        adaptiveReduction: 0.7,
        recoveryRate: 0.1,
        minLimit: 5
      },
      circuitBreaker: DEFAULT_CIRCUIT_BREAKER
    },
    search: {
      rateLimit: {
        initialLimit: 15,
        interval: 5 * 1000, // 5 seconds
        adaptiveReduction: 0.7,
        recoveryRate: 0.1,
        minLimit: 3
      },
      circuitBreaker: DEFAULT_CIRCUIT_BREAKER
    },
    songs: {
      rateLimit: {
        initialLimit: 20,
        interval: 5 * 1000, // 5 seconds
        adaptiveReduction: 0.7,
        recoveryRate: 0.1,
        minLimit: 5
      },
      circuitBreaker: DEFAULT_CIRCUIT_BREAKER
    }
  }
};

/**
 * Enhanced token bucket with adaptive rate limiting
 */
export class AdaptiveTokenBucket {
  private redis: Redis;
  private config: AdaptiveRateLimitConfig;
  private key: string;
  private currentLimit: number;
  private lastSync: number = 0;
  private syncInterval: number = 30000; // 30 seconds

  constructor(
    redis: Redis,
    key: string,
    config: AdaptiveRateLimitConfig
  ) {
    this.redis = redis;
    this.key = `ratelimit:${key}`;
    this.config = config;
    this.currentLimit = config.initialLimit;
  }

  /**
   * Try to consume tokens from the bucket
   */
  async consume(tokens: number = 1): Promise<boolean> {
    // Sync with Redis periodically
    await this.maybeSync();
    
    // Check if we have enough tokens
    const currentTokens = await this.getTokens();
    if (currentTokens < tokens) {
      return false;
    }
    
    // Consume tokens - FIX: Replace eval with direct DECRBY
    try {
      // Instead of using eval which isn't available, use direct DECRBY 
      await this.redis.decrby(this.key, tokens);
      return true;
    } catch (error) {
      console.error(`Error consuming tokens: ${error.message}`);
      return false;
    }
  }

  /**
   * Adjust rate limit based on API responses
   */
  async adaptToResponse(response: Response): Promise<void> {
    // Check for rate limiting headers
    if (response.status === 429) {
      // Get retry-after header if available
      const retryAfter = response.headers.get('retry-after');
      const retrySeconds = retryAfter ? parseInt(retryAfter, 10) : 30;
      
      // Reduce limit on rate limiting
      await this.reduceLimit();
      
      // Set tokens to 0 for retry period
      await this.redis.set(this.key, '0', {
        px: retrySeconds * 1000
      });
      
      // Store the reset time
      await this.redis.set(`${this.key}:reset`, Date.now() + (retrySeconds * 1000), {
        px: retrySeconds * 1000 + 5000 // Small buffer
      });
    } else if (response.ok) {
      // Gradually recover capacity on successful responses
      await this.recoverLimit();
    }
  }

  /**
   * Reduce the rate limit after a 429 response
   */
  async reduceLimit(): Promise<void> {
    const newLimit = Math.max(
      this.config.minLimit,
      Math.floor(this.currentLimit * this.config.adaptiveReduction)
    );
    
    // Update current limit
    this.currentLimit = newLimit;
    
    // Store in Redis
    await this.redis.set(`${this.key}:limit`, newLimit.toString(), {
      ex: 86400 // 24 hour TTL
    });
    
    console.warn(`Rate limit for ${this.key} reduced to ${newLimit}`);
  }

  /**
   * Gradually recover rate limit capacity
   */
  async recoverLimit(): Promise<void> {
    const initialLimit = this.config.initialLimit;
    
    // If we're already at initial limit, no need to recover
    if (this.currentLimit >= initialLimit) {
      return;
    }
    
    // Calculate new limit with recovery
    const recoveryAmount = Math.ceil(initialLimit * this.config.recoveryRate);
    const newLimit = Math.min(
      initialLimit,
      this.currentLimit + recoveryAmount
    );
    
    // Update current limit
    this.currentLimit = newLimit;
    
    // Store in Redis
    await this.redis.set(`${this.key}:limit`, newLimit.toString(), {
      ex: 86400 // 24 hour TTL
    });
  }

  /**
   * Get current tokens in the bucket
   */
  private async getTokens(): Promise<number> {
    // Get current token count
    const tokens = await this.redis.get(this.key);
    
    // If no tokens found, initialize with current limit
    if (tokens === null) {
      await this.refillBucket();
      return this.currentLimit;
    }
    
    return parseInt(tokens as string, 10);
  }

  /**
   * Refill the token bucket
   */
  private async refillBucket(): Promise<void> {
    await this.redis.set(this.key, this.currentLimit.toString(), {
      px: this.config.interval
    });
  }

  /**
   * Sync with Redis periodically
   */
  private async maybeSync(): Promise<void> {
    const now = Date.now();
    
    // Check if we need to sync
    if (now - this.lastSync < this.syncInterval) {
      return;
    }
    
    // Update last sync time
    this.lastSync = now;
    
    try {
      // Get current limit from Redis
      const storedLimit = await this.redis.get(`${this.key}:limit`);
      if (storedLimit !== null) {
        this.currentLimit = parseInt(storedLimit as string, 10);
      }
      
      // Check if we should refill the bucket
      const tokens = await this.redis.get(this.key);
      if (tokens === null) {
        await this.refillBucket();
      }
    } catch (error) {
      console.warn(`Failed to sync token bucket for ${this.key}: ${error.message}`);
    }
  }
}

/**
 * Circuit breaker implementation
 */
export class CircuitBreaker {
  private redis: Redis;
  private name: string;
  private config: CircuitBreakerConfig;
  private state: CircuitState = CircuitState.CLOSED;
  private failureCount: number = 0;
  private lastFailureTime: number = 0;
  private transitionTime: number = 0;
  private successCount: number = 0;
  private lastSync: number = 0;
  private syncInterval: number = 5000; // 5 seconds

  constructor(
    redis: Redis,
    name: string,
    config: CircuitBreakerConfig
  ) {
    this.redis = redis;
    this.name = `circuit:${name}`;
    this.config = config;
  }

  /**
   * Execute a function with circuit breaker protection
   */
  async execute<T>(fn: () => Promise<T>): Promise<T> {
    // Sync with Redis
    await this.syncState();
    
    // Check circuit state
    if (this.state === CircuitState.OPEN) {
      // If circuit is open, check if reset timeout has elapsed
      if (Date.now() - this.transitionTime > this.config.resetTimeout) {
        // Transition to half-open
        await this.transitionTo(CircuitState.HALF_OPEN);
      } else {
        // Circuit still open, fail fast
        throw createEnhancedError(
          `Circuit breaker open for ${this.name}`,
          ErrorSource.SYSTEM,
          ErrorCategory.TRANSIENT_SERVICE,
          { 
            circuitBreaker: this.name,
            circuitState: this.state,
            transitionTime: new Date(this.transitionTime).toISOString(),
            resetTimeout: this.config.resetTimeout
          }
        );
      }
    }
    
    try {
      // Execute function
      const result = await fn();
      
      // Record success
      await this.recordSuccess();
      
      return result;
    } catch (error) {
      // Record failure
      await this.recordFailure(error);
      
      // Rethrow the original error
      throw error;
    }
  }

  /**
   * Record a successful operation
   */
  private async recordSuccess(): Promise<void> {
    if (this.state === CircuitState.HALF_OPEN) {
      this.successCount++;
      
      // If we've reached the success threshold, close the circuit
      if (this.successCount >= this.config.halfOpenSuccessThreshold) {
        await this.transitionTo(CircuitState.CLOSED);
      }
      
      // Update success count in Redis
      await this.redis.set(`${this.name}:success_count`, this.successCount, {
        ex: 60 // 1 minute TTL
      });
    } else if (this.state === CircuitState.CLOSED) {
      // Reset failure count after successful requests in closed state
      if (this.failureCount > 0) {
        this.failureCount = 0;
        await this.redis.del(`${this.name}:failure_count`);
      }
    }
  }

  /**
   * Record a failed operation
   */
  private async recordFailure(error: Error): Promise<void> {
    const now = Date.now();
    
    // Only count failures within the window
    if (now - this.lastFailureTime > this.config.failureWindow) {
      // Reset failure count if outside window
      this.failureCount = 0;
    }
    
    // Increment failure count
    this.failureCount++;
    this.lastFailureTime = now;
    
    // Update Redis
    await this.redis.set(`${this.name}:failure_count`, this.failureCount, {
      ex: Math.ceil(this.config.failureWindow / 1000)
    });
    await this.redis.set(`${this.name}:last_failure_time`, now.toString(), {
      ex: Math.ceil(this.config.failureWindow / 1000)
    });
    
    // Save the last error
    await this.redis.set(`${this.name}:last_error`, JSON.stringify({
      message: error.message,
      name: error.name,
      time: now
    }), {
      ex: 3600 // 1 hour TTL
    });
    
    // Check if circuit should trip
    if (this.state === CircuitState.CLOSED && this.failureCount >= this.config.failureThreshold) {
      await this.transitionTo(CircuitState.OPEN);
    } else if (this.state === CircuitState.HALF_OPEN) {
      // Any failure in half-open should reopen the circuit
      await this.transitionTo(CircuitState.OPEN);
    }
  }

  /**
   * Transition circuit to a new state
   */
  private async transitionTo(newState: CircuitState): Promise<void> {
    // Record transition
    const oldState = this.state;
    this.state = newState;
    this.transitionTime = Date.now();
    
    // Reset counters as needed
    if (newState === CircuitState.CLOSED) {
      this.failureCount = 0;
      this.successCount = 0;
    } else if (newState === CircuitState.HALF_OPEN) {
      this.successCount = 0;
    }
    
    // Update state in Redis
    await this.redis.set(`${this.name}:state`, newState, {
      ex: 3600 // 1 hour TTL
    });
    await this.redis.set(`${this.name}:transition_time`, this.transitionTime.toString(), {
      ex: 3600 // 1 hour TTL
    });
    
    // Store transition history
    await this.redis.lpush(`${this.name}:transitions`, JSON.stringify({
      from: oldState,
      to: newState,
      time: this.transitionTime
    }));
    await this.redis.ltrim(`${this.name}:transitions`, 0, 9); // Keep last 10
    
    // Log transition
    console.log(`Circuit breaker ${this.name} transitioned from ${oldState} to ${newState}`);
  }

  /**
   * Sync state with Redis
   */
  private async syncState(): Promise<void> {
    const now = Date.now();
    
    // Only sync periodically
    if (now - this.lastSync < this.syncInterval) {
      return;
    }
    
    this.lastSync = now;
    
    try {
      // Get state from Redis
      const state = await this.redis.get(`${this.name}:state`);
      if (state !== null) {
        this.state = state as CircuitState;
      }
      
      // Get transition time
      const transitionTime = await this.redis.get(`${this.name}:transition_time`);
      if (transitionTime !== null) {
        this.transitionTime = parseInt(transitionTime as string, 10);
      }
      
      // Get failure count
      const failureCount = await this.redis.get(`${this.name}:failure_count`);
      if (failureCount !== null) {
        this.failureCount = parseInt(failureCount as string, 10);
      }
      
      // Get last failure time
      const lastFailureTime = await this.redis.get(`${this.name}:last_failure_time`);
      if (lastFailureTime !== null) {
        this.lastFailureTime = parseInt(lastFailureTime as string, 10);
      }
      
      // Get success count if in HALF_OPEN state
      if (this.state === CircuitState.HALF_OPEN) {
        const successCount = await this.redis.get(`${this.name}:success_count`);
        if (successCount !== null) {
          this.successCount = parseInt(successCount as string, 10);
        }
      }
    } catch (error) {
      console.warn(`Failed to sync circuit breaker ${this.name} state: ${error.message}`);
    }
  }

  /**
   * Get the current state of the circuit breaker
   */
  async getState(): Promise<{
    state: CircuitState;
    failureCount: number;
    transitionTime: number;
    successCount: number;
    lastFailureTime: number;
  }> {
    await this.syncState();
    
    return {
      state: this.state,
      failureCount: this.failureCount,
      transitionTime: this.transitionTime,
      successCount: this.successCount,
      lastFailureTime: this.lastFailureTime
    };
  }
}

/**
 * Full API resilience manager combining rate limiting and circuit breaking
 */
export class ApiResilienceManager {
  private redis: Redis;
  private apiConfig: ApiConfig;
  private rateLimiters: Record<string, AdaptiveTokenBucket> = {};
  private circuitBreakers: Record<string, CircuitBreaker> = {};
  private serviceStatus: ServiceStatus = ServiceStatus.UNKNOWN;
  private lastStatusRefresh = 0;
  private statusRefreshInterval = 30000; // 30 seconds

  constructor(
    apiConfig: ApiConfig
  ) {
    this.redis = getRedis();
    this.apiConfig = apiConfig;
  }

  /**
   * Get rate limiter for specific endpoint
   */
  private getRateLimiter(endpoint: string): AdaptiveTokenBucket {
    // Create limiter if it doesn't exist
    if (!this.rateLimiters[endpoint]) {
      const config = this.apiConfig.endpoints[endpoint] || 
                    this.apiConfig.endpoints[this.apiConfig.defaultEndpoint];
      
      this.rateLimiters[endpoint] = new AdaptiveTokenBucket(
        this.redis,
        `${this.apiConfig.name}:${endpoint}`,
        config.rateLimit
      );
    }
    
    return this.rateLimiters[endpoint];
  }

  /**
   * Get circuit breaker for specific endpoint
   */
  private getCircuitBreaker(endpoint: string): CircuitBreaker {
    // Create circuit breaker if it doesn't exist
    if (!this.circuitBreakers[endpoint]) {
      const config = this.apiConfig.endpoints[endpoint] || 
                    this.apiConfig.endpoints[this.apiConfig.defaultEndpoint];
      
      this.circuitBreakers[endpoint] = new CircuitBreaker(
        this.redis,
        `${this.apiConfig.name}:${endpoint}`,
        config.circuitBreaker
      );
    }
    
    return this.circuitBreakers[endpoint];
  }

  /**
   * Execute an API call with full resilience
   */
  async executeApiCall<T>(
    endpoint: string,
    fn: () => Promise<Response>,
    options: {
      processResponse?: (response: Response) => Promise<T>;
      fallbackFn?: () => Promise<T>;
      entityType?: string;
      entityId?: string;
      correlationId?: string;
      cost?: number; // Token cost for this operation
    } = {}
  ): Promise<T> {
    const {
      processResponse = async (r) => await r.json() as T,
      fallbackFn,
      entityType,
      entityId, 
      correlationId = `api_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      cost = 1
    } = options;
    
    // If endpoint doesn't exist in config, use default
    const effectiveEndpoint = this.apiConfig.endpoints[endpoint] ? 
                             endpoint : this.apiConfig.defaultEndpoint;
    
    // Get service health status
    await this.refreshServiceStatus();
    
    // Handle degraded service with fallback
    if ((this.serviceStatus === ServiceStatus.CIRCUIT_OPEN || 
         this.serviceStatus === ServiceStatus.RATE_LIMITED) && fallbackFn) {
      console.warn(`Service ${this.apiConfig.name} is ${this.serviceStatus}, using fallback`);
      return await fallbackFn();
    }
    
    // Get rate limiter and circuit breaker
    const rateLimiter = this.getRateLimiter(effectiveEndpoint);
    const circuitBreaker = this.getCircuitBreaker(effectiveEndpoint);
    
    // Check rate limit
    const allowedByRateLimit = await rateLimiter.consume(cost);
    if (!allowedByRateLimit) {
      // Update service status
      await this.updateServiceStatus(ServiceStatus.RATE_LIMITED);
      
      if (fallbackFn) {
        console.warn(`Rate limited for ${this.apiConfig.name}:${effectiveEndpoint}, using fallback`);
        return await fallbackFn();
      }
      
      throw createEnhancedError(
        `Rate limit exceeded for ${this.apiConfig.name}:${effectiveEndpoint}`,
        ErrorSource.SYSTEM,
        ErrorCategory.TRANSIENT_RATE_LIMIT,
        {
          api: this.apiConfig.name,
          endpoint: effectiveEndpoint
        }
      );
    }
    
    // Execute with circuit breaker
    return await circuitBreaker.execute(async () => {
      // Execute the API call
      const response = await fn();
      
      // Adapt rate limiter to response
      await rateLimiter.adaptToResponse(response);
      
      // Handle different response statuses
      if (response.status === 429) {
        // Update service status
        await this.updateServiceStatus(ServiceStatus.RATE_LIMITED);
        
        // Get retry after header
        const retryAfter = response.headers.get('retry-after');
        const retrySeconds = retryAfter ? parseInt(retryAfter, 10) : 30;
        
        throw createEnhancedError(
          `Rate limited by ${this.apiConfig.name} API, retry after ${retrySeconds} seconds`,
          ErrorSource.SYSTEM,
          ErrorCategory.TRANSIENT_RATE_LIMIT,
          {
            api: this.apiConfig.name,
            endpoint: effectiveEndpoint,
            retryAfter: retrySeconds
          }
        );
      }
      
      if (!response.ok) {
        if (response.status >= 500) {
          // Service error
          throw createEnhancedError(
            `Service error from ${this.apiConfig.name} API: ${response.status}`,
            ErrorSource.SYSTEM,
            ErrorCategory.TRANSIENT_SERVICE,
            {
              api: this.apiConfig.name,
              endpoint: effectiveEndpoint,
              status: response.status
            }
          );
        } else {
          // Client error
          throw createEnhancedError(
            `Error response from ${this.apiConfig.name} API: ${response.status}`,
            ErrorSource.SYSTEM,
            response.status === 404 ? ErrorCategory.PERMANENT_NOT_FOUND :
            response.status === 403 ? ErrorCategory.PERMANENT_FORBIDDEN :
            response.status === 401 ? ErrorCategory.PERMANENT_AUTH :
            ErrorCategory.PERMANENT_BAD_REQUEST,
            {
              api: this.apiConfig.name,
              endpoint: effectiveEndpoint,
              status: response.status
            }
          );
        }
      }
      
      // Update service status to healthy
      await this.updateServiceStatus(ServiceStatus.HEALTHY);
      
      // Process the response
      return await processResponse(response);
    });
  }

  /**
   * Update service status
   */
  private async updateServiceStatus(status: ServiceStatus): Promise<void> {
    // Only update if status changed
    if (this.serviceStatus === status) {
      return;
    }
    
    this.serviceStatus = status;
    
    // Update Redis
    await this.redis.set(this.apiConfig.serviceHealthKey, JSON.stringify({
      status,
      updatedAt: new Date().toISOString()
    }), {
      ex: 300 // 5 minute TTL
    });
    
    // Log status change
    console.log(`Service ${this.apiConfig.name} status changed to ${status}`);
  }

  /**
   * Refresh service status from Redis
   */
  private async refreshServiceStatus(): Promise<void> {
    const now = Date.now();
    
    // Only refresh periodically
    if (now - this.lastStatusRefresh < this.statusRefreshInterval) {
      return;
    }
    
    this.lastStatusRefresh = now;
    
    try {
      // Get status from Redis
      const status = await this.redis.get(this.apiConfig.serviceHealthKey);
      if (status !== null) {
        const parsed = JSON.parse(status as string);
        this.serviceStatus = parsed.status;
      }
    } catch (error) {
      console.warn(`Failed to refresh service status for ${this.apiConfig.name}: ${error.message}`);
    }
  }

  /**
   * Get the current service health status
   */
  async getServiceHealth(): Promise<{
    status: ServiceStatus;
    endpoints: Record<string, {
      circuitState: CircuitState;
      failureCount: number;
      rateRemaining?: number;
    }>;
  }> {
    await this.refreshServiceStatus();
    
    const endpoints: Record<string, any> = {};
    
    // Collect health info for all endpoints
    for (const endpoint of Object.keys(this.apiConfig.endpoints)) {
      const circuitBreaker = this.getCircuitBreaker(endpoint);
      const state = await circuitBreaker.getState();
      
      endpoints[endpoint] = {
        circuitState: state.state,
        failureCount: state.failureCount
      };
    }
    
    return {
      status: this.serviceStatus,
      endpoints
    };
  }
}

// Create and export singleton instances
let spotifyApiResilienceManager: ApiResilienceManager | null = null;
let geniusApiResilienceManager: ApiResilienceManager | null = null;

export function getSpotifyApiResilienceManager(): ApiResilienceManager {
  if (!spotifyApiResilienceManager) {
    spotifyApiResilienceManager = new ApiResilienceManager(SPOTIFY_API_CONFIG);
  }
  return spotifyApiResilienceManager;
}

export function getGeniusApiResilienceManager(): ApiResilienceManager {
  if (!geniusApiResilienceManager) {
    geniusApiResilienceManager = new ApiResilienceManager(GENIUS_API_CONFIG);
  }
  return geniusApiResilienceManager;
}
