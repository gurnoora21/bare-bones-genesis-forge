
/**
 * Enhanced Spotify Client with improved error handling, retry logic,
 * and better rate limiting
 */

import { 
  ErrorCategory, 
  ErrorSource, 
  createEnhancedError,
  retryWithBackoff
} from "./errorHandling.ts";

import {
  getSpotifyApiResilienceManager,
  ServiceStatus
} from "./apiResilience.ts";

import {
  validateSpotifyArtist,
  validateSpotifyAlbum,
  validateSpotifyTrack,
  createValidationError
} from "./dataValidation.ts";

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getRedis } from "./upstashRedis.ts";

/**
 * Enhanced Spotify client with better error handling and resilience
 */
export class EnhancedSpotifyClient {
  private clientId: string;
  private clientSecret: string;
  private accessToken: string | null = null;
  private tokenExpiresAt: number = 0;
  private redis: Redis;
  private apiResilienceManager = getSpotifyApiResilienceManager();
  
  constructor() {
    this.clientId = Deno.env.get("SPOTIFY_CLIENT_ID") || "";
    this.clientSecret = Deno.env.get("SPOTIFY_CLIENT_SECRET") || "";
    this.redis = getRedis();
    
    if (!this.clientId || !this.clientSecret) {
      throw createEnhancedError(
        "Spotify credentials are missing. Please set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET environment variables.", 
        ErrorSource.SYSTEM,
        ErrorCategory.PERMANENT_AUTH
      );
    }
  }

  /**
   * Get or refresh access token with retry logic and Redis caching
   */
  async getAccessToken(): Promise<string> {
    const now = Date.now();
    
    // Check if we have a valid token
    if (this.accessToken && now < this.tokenExpiresAt - 60000) { // 1 minute buffer
      return this.accessToken;
    }
    
    // Try to get token from Redis first
    const tokenKey = "spotify:token";
    try {
      const tokenData = await this.redis.get(tokenKey);
      if (tokenData) {
        const { token, expiresAt } = JSON.parse(tokenData as string);
        // Check if token from Redis is still valid
        if (now < expiresAt - 60000) { // 1 minute buffer
          this.accessToken = token;
          this.tokenExpiresAt = expiresAt;
          return this.accessToken;
        }
      }
    } catch (redisError) {
      console.warn("Failed to get token from Redis:", redisError.message);
    }
    
    // Get a new token with retry logic
    return await retryWithBackoff(
      async () => {
        const response = await fetch("https://accounts.spotify.com/api/token", {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": `Basic ${btoa(`${this.clientId}:${this.clientSecret}`)}`
          },
          body: "grant_type=client_credentials"
        });
        
        if (!response.ok) {
          const text = await response.text();
          throw createEnhancedError(
            `Failed to get Spotify token: ${response.status} ${text}`,
            ErrorSource.SPOTIFY_API,
            response.status === 401 ? ErrorCategory.PERMANENT_AUTH : 
            response.status === 429 ? ErrorCategory.TRANSIENT_RATE_LIMIT : 
            ErrorCategory.TRANSIENT_SERVICE
          );
        }
        
        const data = await response.json();
        const expiresIn = data.expires_in || 3600;
        this.accessToken = data.access_token;
        this.tokenExpiresAt = now + (expiresIn * 1000);
        
        // Store in Redis
        try {
          await this.redis.set(tokenKey, JSON.stringify({
            token: this.accessToken,
            expiresAt: this.tokenExpiresAt
          }), {
            ex: expiresIn - 60 // Set TTL slightly less than token expiry
          });
        } catch (redisError) {
          console.warn("Failed to cache token in Redis:", redisError.message);
        }
        
        return this.accessToken;
      },
      {
        name: "getSpotifyToken",
        source: ErrorSource.SPOTIFY_API,
        maxRetries: 3,
        baseDelayMs: 1000,
        maxDelayMs: 10000,
        jitterFactor: 0.3,
      }
    );
  }

  /**
   * Make an authenticated API call with resilience and caching
   */
  private async apiCall<T>(
    endpoint: string,
    apiEndpointName: string = "default",
    options: {
      method?: string;
      body?: any;
      correlationId?: string;
    } = {}
  ): Promise<T> {
    const {
      method = "GET",
      body = null,
      correlationId = `spotify_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`
    } = options;
    
    const url = endpoint.startsWith("https://")
      ? endpoint
      : `https://api.spotify.com/v1${endpoint}`;
    
    // Get access token
    const token = await this.getAccessToken();
    
    // Make the API call with rate limiting and circuit breaking
    return await this.apiResilienceManager.executeApiCall<T>(
      apiEndpointName,
      async () => {
        const headers: Record<string, string> = {
          "Authorization": `Bearer ${token}`,
          "Content-Type": "application/json",
          "X-Correlation-ID": correlationId
        };
        
        const requestOptions: RequestInit = {
          method,
          headers,
          body: body ? JSON.stringify(body) : null
        };
        
        return fetch(url, requestOptions);
      },
      {
        correlationId,
        // Process the response
        processResponse: async (response) => {
          if (response.status === 429) {
            // Handle rate limiting
            const retryAfter = response.headers.get("Retry-After");
            throw createEnhancedError(
              `Spotify API rate limit exceeded. Retry after ${retryAfter || 'unknown'} seconds.`,
              ErrorSource.SPOTIFY_API,
              ErrorCategory.TRANSIENT_RATE_LIMIT,
              { retryAfter }
            );
          }
          
          if (response.status === 401) {
            // Clear access token on auth error
            this.accessToken = null;
            throw createEnhancedError(
              "Spotify API authorization failed",
              ErrorSource.SPOTIFY_API,
              ErrorCategory.PERMANENT_AUTH
            );
          }
          
          if (!response.ok) {
            const text = await response.text();
            throw createEnhancedError(
              `Spotify API error: ${response.status} ${text}`,
              ErrorSource.SPOTIFY_API,
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
            const cacheKey = `spotify:cache:${endpoint.replace(/\//g, ":")}`;
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
   * Search for artists by name with validation
   */
  async searchArtists(name: string, limit: number = 1): Promise<any> {
    const queryParams = new URLSearchParams({
      q: name,
      type: "artist",
      limit: limit.toString()
    }).toString();
    
    const data = await this.apiCall<any>(`/search?${queryParams}`, "search");
    
    // Cache result for 24 hours
    try {
      const cacheKey = `spotify:cache:search:artist:${name.toLowerCase()}`;
      await this.redis.set(cacheKey, JSON.stringify(data), {
        ex: 86400 // 24 hours
      });
    } catch (cacheError) {
      console.warn(`Failed to cache artist search:`, cacheError.message);
    }
    
    return data;
  }
  
  /**
   * Get artist by ID with validation
   */
  async getArtist(artistId: string): Promise<any> {
    const data = await this.apiCall<any>(`/artists/${artistId}`, "artists");
    
    // Validate the artist data
    const validation = validateSpotifyArtist(data);
    if (!validation.valid) {
      throw createValidationError("SpotifyArtist", validation);
    }
    
    // Cache result for 24 hours
    try {
      const cacheKey = `spotify:cache:artist:${artistId}`;
      await this.redis.set(cacheKey, JSON.stringify(data), {
        ex: 86400 // 24 hours
      });
    } catch (cacheError) {
      console.warn(`Failed to cache artist:`, cacheError.message);
    }
    
    return data;
  }
  
  /**
   * Get multiple artists by ID
   */
  async getArtistAlbums(artistId: string, limit: number = 50, offset: number = 0): Promise<any> {
    const queryParams = new URLSearchParams({
      limit: limit.toString(),
      offset: offset.toString()
    }).toString();
    
    return await this.apiCall<any>(`/artists/${artistId}/albums?${queryParams}`, "albums");
  }
  
  /**
   * Get multiple albums by ID (up to 20 at a time)
   */
  async getAlbums(albumIds: string[]): Promise<any> {
    if (albumIds.length === 0) {
      return { albums: [] };
    }
    
    // Spotify allows up to 20 IDs at once
    const maxIdsPerRequest = 20;
    const batches = [];
    
    // Split into batches of 20
    for (let i = 0; i < albumIds.length; i += maxIdsPerRequest) {
      const batch = albumIds.slice(i, i + maxIdsPerRequest);
      batches.push(batch);
    }
    
    // Process each batch
    const results = [];
    
    for (const batch of batches) {
      const queryParams = new URLSearchParams({
        ids: batch.join(',')
      }).toString();
      
      const data = await this.apiCall<any>(`/albums?${queryParams}`, "batch");
      
      // Validate each album
      for (const album of data.albums || []) {
        if (album) {
          const validation = validateSpotifyAlbum(album);
          if (!validation.valid) {
            console.warn(`Invalid album data for ID ${album.id}:`, validation.errors);
          }
        }
      }
      
      results.push(...(data.albums || []));
    }
    
    return { albums: results };
  }
  
  /**
   * Get album tracks
   */
  async getAlbumTracks(albumId: string, limit: number = 50, offset: number = 0): Promise<any> {
    const queryParams = new URLSearchParams({
      limit: limit.toString(),
      offset: offset.toString()
    }).toString();
    
    return await this.apiCall<any>(`/albums/${albumId}/tracks?${queryParams}`, "tracks");
  }
  
  /**
   * Get multiple tracks by ID (up to 50 at a time)
   */
  async getTracks(trackIds: string[]): Promise<any> {
    if (trackIds.length === 0) {
      return { tracks: [] };
    }
    
    // Spotify allows up to 50 IDs at once
    const maxIdsPerRequest = 50;
    const batches = [];
    
    // Split into batches of 50
    for (let i = 0; i < trackIds.length; i += maxIdsPerRequest) {
      const batch = trackIds.slice(i, i + maxIdsPerRequest);
      batches.push(batch);
    }
    
    // Process each batch
    const results = [];
    
    for (const batch of batches) {
      const queryParams = new URLSearchParams({
        ids: batch.join(',')
      }).toString();
      
      const data = await this.apiCall<any>(`/tracks?${queryParams}`, "batch");
      
      // Validate each track
      for (const track of data.tracks || []) {
        if (track) {
          const validation = validateSpotifyTrack(track);
          if (!validation.valid) {
            console.warn(`Invalid track data for ID ${track.id}:`, validation.errors);
          }
        }
      }
      
      results.push(...(data.tracks || []));
    }
    
    return { tracks: results };
  }
  
  /**
   * Get audio features for tracks
   */
  async getAudioFeatures(trackIds: string[]): Promise<any> {
    if (trackIds.length === 0) {
      return { audio_features: [] };
    }
    
    // Spotify allows up to 100 IDs at once
    const maxIdsPerRequest = 100;
    const batches = [];
    
    // Split into batches of 100
    for (let i = 0; i < trackIds.length; i += maxIdsPerRequest) {
      const batch = trackIds.slice(i, i + maxIdsPerRequest);
      batches.push(batch);
    }
    
    // Process each batch
    const results = [];
    
    for (const batch of batches) {
      const queryParams = new URLSearchParams({
        ids: batch.join(',')
      }).toString();
      
      const data = await this.apiCall<any>(`/audio-features?${queryParams}`, "batch");
      results.push(...(data.audio_features || []));
    }
    
    return { audio_features: results };
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
let spotifyClientInstance: EnhancedSpotifyClient | null = null;

export function getSpotifyClient(): EnhancedSpotifyClient {
  if (!spotifyClientInstance) {
    spotifyClientInstance = new EnhancedSpotifyClient();
  }
  return spotifyClientInstance;
}
