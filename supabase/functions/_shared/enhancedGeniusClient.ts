
/**
 * Enhanced Genius Client with improved error handling, retry logic,
 * and better rate limiting
 */

import { 
  ErrorCategory, 
  ErrorSource, 
  createEnhancedError,
  retryWithBackoff
} from "./errorHandling.ts";

import {
  getGeniusApiResilienceManager,
  ServiceStatus
} from "./apiResilience.ts";

import {
  validateGeniusSong,
  createValidationError
} from "./dataValidation.ts";

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getRedis } from "./upstashRedis.ts";

/**
 * Enhanced Genius client with better error handling and resilience
 */
export class EnhancedGeniusClient {
  private accessToken: string;
  private redis: Redis;
  private apiResilienceManager = getGeniusApiResilienceManager();
  
  constructor() {
    this.accessToken = Deno.env.get("GENIUS_ACCESS_TOKEN") || "";
    this.redis = getRedis();
    
    if (!this.accessToken) {
      throw createEnhancedError(
        "Genius access token must be provided as environment variable",
        ErrorSource.SYSTEM,
        ErrorCategory.PERMANENT_AUTH
      );
    }
  }
  
  /**
   * Make an authenticated API call with resilience and caching
   */
  private async apiCall<T>(
    endpoint: string,
    apiEndpointName: string = "default",
    options: {
      correlationId?: string;
    } = {}
  ): Promise<T> {
    const {
      correlationId = `genius_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`
    } = options;
    
    const url = endpoint.startsWith("https://")
      ? endpoint
      : `https://api.genius.com${endpoint}`;
    
    // Make the API call with rate limiting and circuit breaking
    return await this.apiResilienceManager.executeApiCall<T>(
      apiEndpointName,
      async () => {
        return fetch(url, {
          headers: {
            "Authorization": `Bearer ${this.accessToken}`,
            "X-Correlation-ID": correlationId
          }
        });
      },
      {
        correlationId,
        // Process the response
        processResponse: async (response) => {
          if (response.status === 429) {
            // Handle rate limiting
            const retryAfter = response.headers.get("Retry-After") || "60";
            throw createEnhancedError(
              `Genius API rate limit exceeded. Retry after ${retryAfter} seconds.`,
              ErrorSource.GENIUS_API,
              ErrorCategory.TRANSIENT_RATE_LIMIT,
              { retryAfter }
            );
          }
          
          if (response.status === 401) {
            throw createEnhancedError(
              "Genius API authorization failed",
              ErrorSource.GENIUS_API,
              ErrorCategory.PERMANENT_AUTH
            );
          }
          
          if (!response.ok) {
            const text = await response.text();
            throw createEnhancedError(
              `Genius API error: ${response.status} ${text}`,
              ErrorSource.GENIUS_API,
              response.status >= 500 ? ErrorCategory.TRANSIENT_SERVICE : 
              response.status === 404 ? ErrorCategory.PERMANENT_NOT_FOUND :
              ErrorCategory.PERMANENT_BAD_REQUEST
            );
          }
          
          return await response.json() as T;
        },
        // Use a fallback cache if available
        fallbackFn: async () => {
          try {
            // Check if we have a cached response
            const cacheKey = `genius:cache:${endpoint.replace(/\//g, ":")}`;
            const cachedData = await this.redis.get(cacheKey);
            
            if (cachedData) {
              console.log(`[${correlationId}] Using cached response for ${endpoint}`);
              return JSON.parse(cachedData as string) as T;
            }
          } catch (cacheError) {
            console.warn(`[${correlationId}] Cache fallback failed:`, cacheError.message);
          }
          
          throw new Error("No fallback data available");
        }
      }
    );
  }
  
  /**
   * Search for a song on Genius with retry and validation
   */
  async search(trackName: string, artistName: string): Promise<any> {
    const query = encodeURIComponent(`${trackName} ${artistName}`);
    const cacheKey = `genius:cache:search:${trackName.toLowerCase()}-${artistName.toLowerCase()}`;
    
    // Check cache first
    try {
      const cachedResult = await this.redis.get(cacheKey);
      if (cachedResult) {
        return JSON.parse(cachedResult as string);
      }
    } catch (cacheError) {
      console.warn(`Cache check failed:`, cacheError.message);
    }
    
    // Make API call with resilience
    const result = await this.apiCall<any>(`/search?q=${query}`, "search");
    
    let song = null;
    if (result.response && result.response.hits && result.response.hits.length > 0) {
      // Find the most relevant hit (usually the first one)
      const hit = result.response.hits.find((h: any) => h.type === "song") || result.response.hits[0];
      song = hit.result;
      
      // Cache successful result
      try {
        await this.redis.set(cacheKey, JSON.stringify(song), {
          ex: 86400 // 24 hours
        });
      } catch (cacheError) {
        console.warn(`Failed to cache search result:`, cacheError.message);
      }
    }
    
    return song;
  }
  
  /**
   * Get detailed song information including producer credits
   */
  async getSong(songId: number): Promise<any> {
    const cacheKey = `genius:cache:song:${songId}`;
    
    // Check cache first
    try {
      const cachedResult = await this.redis.get(cacheKey);
      if (cachedResult) {
        return JSON.parse(cachedResult as string);
      }
    } catch (cacheError) {
      console.warn(`Cache check failed:`, cacheError.message);
    }
    
    // Make API call with resilience
    const result = await this.apiCall<any>(`/songs/${songId}`, "songs");
    
    let song = null;
    if (result.response && result.response.song) {
      song = result.response.song;
      
      // Validate song data
      const validation = validateGeniusSong(song);
      if (!validation.valid) {
        console.warn(`Invalid song data for ID ${songId}:`, validation.errors);
      }
      
      // Cache successful result
      try {
        await this.redis.set(cacheKey, JSON.stringify(song), {
          ex: 86400 // 24 hours
        });
      } catch (cacheError) {
        console.warn(`Failed to cache song data:`, cacheError.message);
      }
    }
    
    return song;
  }
  
  /**
   * Extract producer information from song details with validation
   */
  extractProducers(songDetails: any): { name: string, confidence: number, source: string }[] {
    if (!songDetails) {
      return [];
    }
    
    const producers: { name: string, confidence: number, source: string }[] = [];
    
    // Check for direct producer_artists field
    if (songDetails.producer_artists && Array.isArray(songDetails.producer_artists)) {
      songDetails.producer_artists.forEach((producer: any) => {
        if (producer && producer.name) {
          producers.push({
            name: producer.name,
            confidence: 0.95, // High confidence since these are explicitly labeled
            source: 'genius_producer_artists'
          });
        }
      });
    }
    
    // Check custom performance roles
    if (songDetails.custom_performances && Array.isArray(songDetails.custom_performances)) {
      songDetails.custom_performances.forEach((performance: any) => {
        if (performance.label && 
            (performance.label.toLowerCase().includes('produc') || 
             performance.label.toLowerCase().includes('beat'))) {
          
          performance.artists.forEach((artist: any) => {
            producers.push({
              name: artist.name,
              confidence: 0.9,
              source: 'genius_custom_performances'
            });
          });
        }
      });
    }
    
    // Check description for producer mentions
    if (songDetails.description && songDetails.description.html) {
      const description = songDetails.description.html;
      
      // Simple producer extraction via patterns like "Produced by X" or "Producer: X"
      const producerPatterns = [
        /[Pp]roduced by ([^<.,]+)/g,
        /[Pp]roducer(?:s)?:? ([^<.,]+)/g,
        /[Bb]eat(?:s)? by ([^<.,]+)/g
      ];
      
      producerPatterns.forEach(pattern => {
        let match;
        while ((match = pattern.exec(description)) !== null) {
          const producer = match[1].trim();
          // Avoid capturing too much text
          if (producer.length < 50 && producer.split(" ").length < 6) {
            producers.push({
              name: producer,
              confidence: 0.7, // Lower confidence for text extraction
              source: 'genius_description'
            });
          }
        }
      });
    }
    
    return producers;
  }
  
  /**
   * Get the service health status
   */
  async getServiceHealth(): Promise<ServiceStatus> {
    const health = await this.apiResilienceManager.getServiceHealth();
    return health.status;
  }
}

// Create singleton instance
let geniusClientInstance: EnhancedGeniusClient | null = null;

export function getGeniusClient(): EnhancedGeniusClient {
  if (!geniusClientInstance) {
    geniusClientInstance = new EnhancedGeniusClient();
  }
  return geniusClientInstance;
}
