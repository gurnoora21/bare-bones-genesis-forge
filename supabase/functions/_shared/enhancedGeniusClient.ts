
/**
 * Enhanced Genius API Client with resilience patterns
 * Uses the new rate limiter implementation
 */

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { ApiResilienceManager, ServiceHealth } from "./apiResilienceManager.ts";
import { getEnhancedRateLimiter, EnhancedRateLimiter } from "./enhancedRateLimiter.ts";
import { RATE_LIMITERS } from "./rateLimiter.ts";

export interface GeniusSearchResult {
  hits: Array<{
    result: {
      id: number;
      title: string;
      primary_artist: {
        id: number;
        name: string;
      };
      url: string;
    }
  }>;
}

export interface GeniusSongData {
  song: {
    id: number;
    title: string;
    url: string;
    album?: {
      id: number;
      name: string;
    };
    writer_artists?: Array<{
      id: number;
      name: string;
      url: string;
    }>;
    producer_artists?: Array<{
      id: number;
      name: string;
      url: string;
    }>;
    custom_performances?: Array<{
      label: string;
      artists: Array<{
        id: number;
        name: string;
      }>;
    }>;
    description_annotation?: {
      body?: {
        plain?: string;
      };
    };
  };
}

export class EnhancedGeniusClient {
  private accessToken: string;
  private apiBase: string = "https://api.genius.com";
  private redis: Redis;
  private resilienceManager: ApiResilienceManager;
  private rateLimiter: EnhancedRateLimiter;
  private serviceName = "genius-api";
  
  constructor(accessToken: string, redis: Redis, resilienceManager: ApiResilienceManager) {
    this.accessToken = accessToken;
    this.redis = redis;
    this.resilienceManager = resilienceManager;
    this.rateLimiter = getEnhancedRateLimiter(redis);
  }
  
  /**
   * Search Genius for a song
   */
  async searchSong(query: string): Promise<GeniusSearchResult> {
    const cacheKey = `genius:search:${encodeURIComponent(query.toLowerCase())}`;
    
    // Try to get from cache first
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      console.log(`Cache hit for Genius search: ${query}`);
      return JSON.parse(cached as string);
    }
    
    // Not in cache, make API call with resilience patterns
    // Use both the resilience manager and the rate limiter
    const result = await this.resilienceManager.executeApiCall<GeniusSearchResult>(
      this.serviceName,
      async () => {
        // Apply rate limiting
        const limiterResponse = await this.rateLimiter.limit({
          ...RATE_LIMITERS.GENIUS.DEFAULT,
          identifier: `genius:search`
        });
        
        // If rate limited, wait as recommended
        if (!limiterResponse.success) {
          console.log(`Rate limited by Genius API, waiting ${limiterResponse.retryAfter || 1000}ms`);
          await new Promise(resolve => setTimeout(resolve, limiterResponse.retryAfter || 1000));
          throw new Error("Rate limit exceeded, retrying after delay");
        }
        
        // Make the API call
        const response = await fetch(
          `${this.apiBase}/search?q=${encodeURIComponent(query)}`,
          {
            headers: {
              'Authorization': `Bearer ${this.accessToken}`,
              'Accept': 'application/json'
            }
          }
        );
        
        // Handle rate limiting explicitly
        if (response.status === 429) {
          const retryAfter = response.headers.get('retry-after');
          throw new Error(`Genius API rate limit exceeded. Retry after ${retryAfter || 'unknown'} seconds`);
        }
        
        if (!response.ok) {
          throw new Error(`Genius API search error: ${response.status} ${response.statusText}`);
        }
        
        const data = await response.json();
        return data.response as GeniusSearchResult;
      },
      {
        rateLimit: { requestsPerMinute: 5 }, // Genius rate limit is ~5 req/sec
        maxRetries: 3
      }
    );
    
    // Cache the result for 1 day
    await this.redis.set(cacheKey, JSON.stringify(result), { ex: 86400 });
    
    return result;
  }
  
  /**
   * Get song details by ID
   */
  async getSongById(songId: number): Promise<GeniusSongData> {
    const cacheKey = `genius:song:${songId}`;
    
    // Try to get from cache first
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      console.log(`Cache hit for Genius song id: ${songId}`);
      return JSON.parse(cached as string);
    }
    
    // Use the rate limiter to ensure we don't exceed limits
    return await this.rateLimiter.execute({
      ...RATE_LIMITERS.GENIUS.DEFAULT,
      identifier: `genius:song`,
    }, async () => {
      // Make API call with resilience patterns
      const result = await this.resilienceManager.executeApiCall<GeniusSongData>(
        this.serviceName,
        async () => {
          const response = await fetch(
            `${this.apiBase}/songs/${songId}`,
            {
              headers: {
                'Authorization': `Bearer ${this.accessToken}`,
                'Accept': 'application/json'
              }
            }
          );
          
          // Handle rate limiting explicitly
          if (response.status === 429) {
            const retryAfter = response.headers.get('retry-after');
            throw new Error(`Genius API rate limit exceeded. Retry after ${retryAfter || 'unknown'} seconds`);
          }
          
          if (!response.ok) {
            throw new Error(`Genius API song error: ${response.status} ${response.statusText}`);
          }
          
          const data = await response.json();
          return data.response as GeniusSongData;
        },
        {
          maxRetries: 3
        }
      );
      
      // Cache the result for 30 days (song details rarely change)
      await this.redis.set(cacheKey, JSON.stringify(result), { ex: 2592000 });
      
      return result;
    });
  }
  
  /**
   * Get service health status
   */
  async getServiceHealth(): Promise<ServiceHealth> {
    const health = await this.resilienceManager.getServiceHealth(this.serviceName);
    return health.status;
  }
  
  /**
   * Reset the Genius API circuit breaker
   */
  async resetCircuitBreaker(): Promise<boolean> {
    return await this.resilienceManager.resetCircuitBreaker(this.serviceName);
  }
}

/**
 * Create enhanced Genius client with resilience features
 */
export function createEnhancedGeniusClient(
  accessToken: string,
  redis: Redis,
  resilienceManager: ApiResilienceManager
): EnhancedGeniusClient {
  return new EnhancedGeniusClient(accessToken, redis, resilienceManager);
}
