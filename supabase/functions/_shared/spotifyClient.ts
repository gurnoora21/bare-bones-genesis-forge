import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

/**
 * SpotifyClient handles authentication and API calls to the Spotify Web API,
 * implementing proper rate limiting and retry logic.
 */
export class SpotifyClient {
  private clientId: string;
  private clientSecret: string;
  private accessToken: string | null;
  private tokenExpiry: number;
  
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
    
    // Otherwise, get a new token
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
  }

  /**
   * Make an authenticated request to the Spotify API with rate limiting and retries
   */
  async apiRequest(endpoint: string, method = "GET", body?: any): Promise<any> {
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
    
    // Implement exponential backoff retries
    const maxRetries = 5;
    let retryCount = 0;
    let lastError: Error | null = null;
    
    while (retryCount < maxRetries) {
      try {
        const response = await fetch(url, options);
        
        if (response.status === 204) {
          return null; // No content
        }
        
        if (response.status === 429) {
          // Rate limited - get retry after time
          const retryAfter = parseInt(response.headers.get("Retry-After") || "1", 10);
          console.log(`Rate limited, waiting for ${retryAfter} seconds`);
          await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
          retryCount++;
          continue;
        }
        
        if (!response.ok) {
          throw new Error(`Spotify API error: ${response.status} ${await response.text()}`);
        }
        
        return await response.json();
      } catch (error) {
        console.error(`API request failed (attempt ${retryCount + 1}/${maxRetries}):`, error);
        lastError = error instanceof Error ? error : new Error(String(error));
        
        // Exponential backoff with jitter
        const baseDelay = Math.pow(2, retryCount) * 1000;
        const jitter = Math.random() * 1000;
        const delay = baseDelay + jitter;
        
        await new Promise(resolve => setTimeout(resolve, delay));
        retryCount++;
      }
    }
    
    throw lastError || new Error("Max retries exceeded");
  }

  // Artist-related methods
  
  async getArtistById(id: string): Promise<any> {
    return this.apiRequest(`/artists/${id}`);
  }
  
  async getArtistByName(name: string): Promise<any> {
    const result = await this.apiRequest(`/search?q=${encodeURIComponent(name)}&type=artist&limit=1`);
    
    if (result.artists && result.artists.items && result.artists.items.length > 0) {
      return result.artists.items[0];
    }
    
    return null;
  }
  
  async getArtistAlbums(artistId: string, offset = 0, limit = 50): Promise<any> {
    return this.apiRequest(`/artists/${artistId}/albums?offset=${offset}&limit=${limit}&include_groups=album,single`);
  }

  // Album-related methods
  
  async getAlbumDetails(albumIds: string[]): Promise<any[]> {
    // Spotify allows batching up to 20 album IDs in one request
    const results = [];
    
    for (let i = 0; i < albumIds.length; i += 20) {
      const batchIds = albumIds.slice(i, i + 20);
      const response = await this.apiRequest(`/albums?ids=${batchIds.join(',')}`);
      
      if (response && response.albums) {
        results.push(...response.albums);
      }
    }
    
    return results;
  }
  
  async getAlbumTracks(albumId: string, offset = 0, limit = 50): Promise<any> {
    return this.apiRequest(`/albums/${albumId}/tracks?offset=${offset}&limit=${limit}`);
  }

  // Track-related methods
  
  async getTrackDetails(trackIds: string[]): Promise<any[]> {
    // Spotify allows batching up to 50 track IDs in one request
    const results = [];
    
    for (let i = 0; i < trackIds.length; i += 50) {
      const batchIds = trackIds.slice(i, i + 50);
      const response = await this.apiRequest(`/tracks?ids=${batchIds.join(',')}`);
      
      if (response && response.tracks) {
        results.push(...response.tracks);
      }
    }
    
    return results;
  }
}
