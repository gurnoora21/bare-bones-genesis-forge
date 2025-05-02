
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { getRateLimiter, RATE_LIMITERS } from "./rateLimiter.ts";

/**
 * SpotifyClient handles authentication and API calls to the Spotify Web API,
 * implementing proper rate limiting and retry logic using our distributed
 * rate limiter.
 */
export class SpotifyClient {
  private clientId: string;
  private clientSecret: string;
  private accessToken: string | null;
  private tokenExpiry: number;
  private rateLimiter = getRateLimiter();
  
  constructor() {
    this.clientId = Deno.env.get("SPOTIFY_CLIENT_ID") || "";
    this.clientSecret = Deno.env.get("SPOTIFY_CLIENT_SECRET") || "";
    this.accessToken = null;
    this.tokenExpiry = 0;
    
    if (!this.clientId || !this.clientSecret) {
      throw new Error("Spotify client ID and client secret must be provided as environment variables");
    }
  }

  /**
   * Get an access token for the Spotify API
   */
  async getAccessToken(): Promise<string> {
    const now = Date.now();
    
    // If we have a valid token that's not expired, return it
    if (this.accessToken && now < this.tokenExpiry - 60000) {
      return this.accessToken;
    }
    
    // Otherwise, get a new token using the rate limiter
    return this.rateLimiter.execute({
      ...RATE_LIMITERS.SPOTIFY.DEFAULT,
      endpoint: "token"
    }, async () => {
      try {
        const response = await fetch("https://accounts.spotify.com/api/token", {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": `Basic ${btoa(`${this.clientId}:${this.clientSecret}`)}`
          },
          body: "grant_type=client_credentials"
        });
        
        if (!response.ok) {
          throw new Error(`Failed to get access token: ${response.statusText}`);
        }
        
        const data = await response.json();
        this.accessToken = data.access_token;
        this.tokenExpiry = now + (data.expires_in * 1000);
        return this.accessToken;
      } catch (error) {
        console.error("Error getting Spotify access token:", error);
        throw error;
      }
    });
  }

  /**
   * Make an authenticated request to the Spotify API with rate limiting and caching
   */
  async apiRequest(endpoint: string, method = "GET", body?: any, cacheTime?: number): Promise<any> {
    const token = await this.getAccessToken();
    const url = endpoint.startsWith("https://") 
      ? endpoint 
      : `https://api.spotify.com/v1${endpoint}`;
    
    const options: RequestInit = {
      method,
      headers: {
        "Authorization": `Bearer ${token}`,
        "Content-Type": "application/json"
      }
    };
    
    if (body && (method === "POST" || method === "PUT")) {
      options.body = JSON.stringify(body);
    }
    
    // Determine appropriate rate limiter config based on endpoint
    const limiterConfig = endpoint.includes("/search") 
      ? RATE_LIMITERS.SPOTIFY.SEARCH 
      : (endpoint.includes("ids=") ? RATE_LIMITERS.SPOTIFY.BATCH : RATE_LIMITERS.SPOTIFY.DEFAULT);
    
    const endpointName = endpoint.split('?')[0].split('/').filter(Boolean).pop() || 'default';
    
    // Use the caching rate limiter if a cache time is provided
    if (cacheTime && method === "GET") {
      const cacheKey = `spotify:${endpoint}`;
      
      return this.rateLimiter.executeWithCache({
        ...limiterConfig,
        endpoint: endpointName,
        cacheKey,
        cacheTtl: cacheTime
      }, async () => {
        return this.performApiRequest(url, options);
      });
    }
    
    // Otherwise use regular rate limiter
    return this.rateLimiter.execute({
      ...limiterConfig,
      endpoint: endpointName
    }, async () => {
      return this.performApiRequest(url, options);
    });
  }
  
  /**
   * Helper method to perform the actual API request
   */
  private async performApiRequest(url: string, options: RequestInit): Promise<any> {
    const response = await fetch(url, options);
    
    if (response.status === 204) {
      return null; // No content
    }
    
    if (response.status === 429) {
      // Extract retry-after header
      const retryAfter = parseInt(response.headers.get("Retry-After") || "1", 10);
      throw new Error(`Rate limited by Spotify API, retry after ${retryAfter} seconds`);
    }
    
    if (!response.ok) {
      throw new Error(`Spotify API error: ${response.status} ${await response.text()}`);
    }
    
    return await response.json();
  }

  // Artist-related methods with caching
  
  async getArtistById(id: string): Promise<any> {
    return this.apiRequest(`/artists/${id}`, "GET", undefined, 60 * 60 * 24); // Cache for 24 hours
  }
  
  async getArtistByName(name: string): Promise<any> {
    return this.apiRequest(`/search?q=${encodeURIComponent(name)}&type=artist&limit=1`, "GET", undefined, 60 * 60 * 24); // Cache for 24 hours
  }
  
  async getArtistAlbums(artistId: string, offset = 0, limit = 50): Promise<any> {
    return this.apiRequest(`/artists/${artistId}/albums?offset=${offset}&limit=${limit}&include_groups=album,single`, "GET", undefined, 60 * 60 * 12); // Cache for 12 hours
  }

  // NEW: Direct API methods that bypass caching and rate limiting for fallback scenarios
  
  /**
   * Get artist by ID directly without caching
   */
  async getArtistByIdDirect(id: string): Promise<any> {
    const token = await this.getAccessToken();
    const url = `https://api.spotify.com/v1/artists/${id}`;
    
    const response = await fetch(url, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`,
        "Content-Type": "application/json"
      }
    });
    
    if (!response.ok) {
      throw new Error(`Spotify API error: ${response.status} ${await response.text()}`);
    }
    
    return await response.json();
  }
  
  /**
   * Search for artist by name directly without caching
   */
  async searchArtistDirect(name: string): Promise<any> {
    const token = await this.getAccessToken();
    const url = `https://api.spotify.com/v1/search?q=${encodeURIComponent(name)}&type=artist&limit=1`;
    
    const response = await fetch(url, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`,
        "Content-Type": "application/json"
      }
    });
    
    if (!response.ok) {
      throw new Error(`Spotify API error: ${response.status} ${await response.text()}`);
    }
    
    return await response.json();
  }

  // Album-related methods with caching
  
  async getAlbumDetails(albumIds: string[]): Promise<any[]> {
    // Spotify allows batching up to 20 album IDs in one request
    const results = [];
    
    for (let i = 0; i < albumIds.length; i += 20) {
      const batchIds = albumIds.slice(i, i + 20);
      const response = await this.apiRequest(`/albums?ids=${batchIds.join(',')}`, "GET", undefined, 60 * 60 * 24); // Cache for 24 hours
      
      if (response && response.albums) {
        results.push(...response.albums);
      }
    }
    
    return results;
  }
  
  async getAlbumTracks(albumId: string, offset = 0, limit = 50): Promise<any> {
    return this.apiRequest(`/albums/${albumId}/tracks?offset=${offset}&limit=${limit}`, "GET", undefined, 60 * 60 * 24); // Cache for 24 hours
  }

  // Track-related methods with caching
  
  async getTrackDetails(trackIds: string[]): Promise<any[]> {
    // Spotify allows batching up to 50 track IDs in one request
    const results = [];
    
    for (let i = 0; i < trackIds.length; i += 50) {
      const batchIds = trackIds.slice(i, i + 50);
      const response = await this.apiRequest(`/tracks?ids=${batchIds.join(',')}`, "GET", undefined, 60 * 60 * 24); // Cache for 24 hours
      
      if (response && response.tracks) {
        results.push(...response.tracks);
      }
    }
    
    return results;
  }
}
