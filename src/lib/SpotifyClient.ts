
import { EnvConfig } from './EnvConfig';
import {
  Album, AlbumPage, Artist, SpotifyError, Track, TrackPage
} from './types/spotify';

export interface SpotifyToken {
  access_token: string;
  token_type: string;
  expires_at: number;
}

export interface RequestOptions {
  retries?: number;
  signal?: AbortSignal;
}

export interface CacheEntry<T> {
  data: T;
  etag?: string;
  timestamp: number;
  expiresAt: number;
}

export class SpotifyClient {
  private static instance: SpotifyClient;
  private token: SpotifyToken | null = null;
  private tokenPromise: Promise<string> | null = null;
  
  // Cache settings
  private cache: Map<string, CacheEntry<any>> = new Map();
  private cacheTTL = 3600 * 1000; // 1 hour default
  
  // Rate limiting
  private requestCounter = 0;
  private requestWindowReset = Date.now() + 1000;
  private maxRequestsPerSecond = 10;
  
  // Retrying
  private defaultMaxRetries = 3;
  private baseDelayMs = 1000;

  private constructor() {
    // Private constructor for singleton pattern
    
    // Reset the request counter every second
    setInterval(() => {
      this.requestWindowReset = Date.now() + 1000;
      this.requestCounter = 0;
    }, 1000);
  }

  public static getInstance(): SpotifyClient {
    if (!SpotifyClient.instance) {
      SpotifyClient.instance = new SpotifyClient();
    }
    return SpotifyClient.instance;
  }

  // Token management
  private async getToken(): Promise<string> {
    // Return valid token if we have one
    if (this.token && this.token.expires_at > Date.now()) {
      return this.token.access_token;
    }

    // If a token request is already in progress, return that promise
    if (this.tokenPromise) {
      return this.tokenPromise;
    }

    // Get new token
    this.tokenPromise = this.fetchNewToken();
    try {
      return await this.tokenPromise;
    } finally {
      this.tokenPromise = null;
    }
  }

  private async fetchNewToken(): Promise<string> {
    const clientId = EnvConfig.SPOTIFY_CLIENT_ID;
    const clientSecret = EnvConfig.SPOTIFY_CLIENT_SECRET;

    if (!clientId || !clientSecret) {
      throw new Error('Missing Spotify credentials');
    }

    try {
      const response = await fetch('https://accounts.spotify.com/api/token', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Authorization': `Basic ${btoa(`${clientId}:${clientSecret}`)}`
        },
        body: 'grant_type=client_credentials'
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to get Spotify token: ${response.status} - ${errorText}`);
      }

      const data = await response.json();
      
      // Store token with expiration (subtract 60 seconds as buffer)
      this.token = {
        access_token: data.access_token,
        token_type: data.token_type,
        expires_at: Date.now() + (data.expires_in * 1000) - 60000
      };

      return this.token.access_token;
    } catch (error) {
      console.error('Error fetching Spotify token:', error);
      throw error;
    }
  }

  // Caching
  private getCached<T>(key: string): { hit: boolean, data?: T, etag?: string } {
    const entry = this.cache.get(key);
    if (entry && entry.expiresAt > Date.now()) {
      return { hit: true, data: entry.data, etag: entry.etag };
    }
    return { hit: false };
  }

  private setCached<T>(key: string, data: T, etag?: string, ttl = this.cacheTTL): void {
    this.cache.set(key, {
      data,
      etag,
      timestamp: Date.now(),
      expiresAt: Date.now() + ttl
    });
  }

  // Rate limiting
  private async checkRateLimit(): Promise<void> {
    if (Date.now() > this.requestWindowReset) {
      // Window has reset
      this.requestCounter = 0;
      this.requestWindowReset = Date.now() + 1000;
    }

    if (this.requestCounter >= this.maxRequestsPerSecond) {
      // We've hit the rate limit, wait until the next window
      const waitTime = this.requestWindowReset - Date.now();
      if (waitTime > 0) {
        await new Promise(resolve => setTimeout(resolve, waitTime));
      }
      this.requestCounter = 0;
      this.requestWindowReset = Date.now() + 1000;
    }
    
    this.requestCounter++;
  }

  // API request with retries, rate limiting, and caching
  private async request<T>(
    url: string, 
    options: RequestOptions = {}
  ): Promise<T> {
    const { retries = this.defaultMaxRetries, signal } = options;
    const cacheKey = url;
    const cached = this.getCached<T>(cacheKey);

    // Prepare headers with auth token
    const token = await this.getToken();
    const headers: HeadersInit = {
      'Authorization': `Bearer ${token}`
    };

    // Add ETag for conditional request if we have it
    if (cached.etag) {
      headers['If-None-Match'] = cached.etag;
    }

    // Apply rate limiting
    await this.checkRateLimit();

    let attempt = 0;
    let lastError: Error | null = null;

    while (attempt <= retries) {
      try {
        // Check if the operation was aborted
        if (signal?.aborted) {
          throw new Error('Request aborted');
        }

        const response = await fetch(url, { headers, signal });
        
        // Handle 304 Not Modified - return cached data
        if (response.status === 304 && cached.hit) {
          return cached.data;
        }

        // Handle rate limiting
        if (response.status === 429) {
          const retryAfter = response.headers.get('Retry-After');
          const waitTime = retryAfter ? parseInt(retryAfter, 10) * 1000 : this.baseDelayMs * Math.pow(2, attempt);
          console.warn(`Rate limited by Spotify API, waiting ${waitTime}ms`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
          attempt++;
          continue;
        }

        // Handle other error responses
        if (!response.ok) {
          const errorText = await response.text();
          const error: SpotifyError = {
            status: response.status,
            message: errorText
          };
          
          // Don't retry 4xx errors except rate limiting (which we already handled)
          if (response.status >= 400 && response.status < 500) {
            throw new Error(`Spotify API error: ${response.status} - ${errorText}`);
          }
          
          // For server errors, retry
          throw new Error(`Spotify API server error: ${response.status} - ${errorText}`);
        }
        
        // Get the ETag for caching
        const etag = response.headers.get('ETag') || undefined;
        
        // Parse and cache the response data
        const data = await response.json() as T;
        this.setCached(cacheKey, data, etag);
        
        return data;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        
        // Don't retry client errors (except rate limiting which we handle above)
        if (lastError.message.includes('4')) {
          throw lastError;
        }
        
        // For other errors, retry with exponential backoff
        const delay = this.baseDelayMs * Math.pow(2, attempt) * (0.5 + Math.random() * 0.5);
        console.warn(`Spotify API request failed, retrying in ${delay}ms`, lastError);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
      
      attempt++;
    }
    
    throw lastError || new Error('Maximum retries exceeded');
  }

  // Artist API methods
  public async getArtistById(id: string, options?: RequestOptions): Promise<Artist> {
    return this.request<Artist>(
      `https://api.spotify.com/v1/artists/${id}`,
      options
    );
  }

  public async getArtistByName(name: string, options?: RequestOptions): Promise<Artist | null> {
    const response = await this.request<{
      artists: {
        items: Artist[];
      }
    }>(
      `https://api.spotify.com/v1/search?q=${encodeURIComponent(name)}&type=artist&limit=1`,
      options
    );
    
    return response.artists.items.length > 0 ? response.artists.items[0] : null;
  }

  // Album API methods
  public async getArtistAlbums(
    artistId: string, 
    offset = 0, 
    limit = 50,
    options?: RequestOptions
  ): Promise<AlbumPage> {
    return this.request<AlbumPage>(
      `https://api.spotify.com/v1/artists/${artistId}/albums?limit=${limit}&offset=${offset}`,
      options
    );
  }

  public async getAlbumDetails(ids: string[], options?: RequestOptions): Promise<Album[]> {
    // Spotify allows up to 20 albums per request
    if (ids.length === 0) {
      return [];
    }
    
    if (ids.length === 1) {
      const album = await this.request<Album>(
        `https://api.spotify.com/v1/albums/${ids[0]}`,
        options
      );
      return [album];
    }
    
    // Handle batch requests (max 20)
    const batches: string[][] = [];
    for (let i = 0; i < ids.length; i += 20) {
      batches.push(ids.slice(i, i + 20));
    }
    
    const results: Album[] = [];
    
    for (const batch of batches) {
      const response = await this.request<{ albums: Album[] }>(
        `https://api.spotify.com/v1/albums?ids=${batch.join(',')}`,
        options
      );
      
      results.push(...response.albums);
    }
    
    return results;
  }

  // Track API methods
  public async getAlbumTracks(
    albumId: string, 
    offset = 0, 
    limit = 50,
    options?: RequestOptions
  ): Promise<TrackPage> {
    return this.request<TrackPage>(
      `https://api.spotify.com/v1/albums/${albumId}/tracks?limit=${limit}&offset=${offset}`,
      options
    );
  }

  public async getTrackDetails(ids: string[], options?: RequestOptions): Promise<Track[]> {
    // Spotify allows up to 50 tracks per request
    if (ids.length === 0) {
      return [];
    }
    
    if (ids.length === 1) {
      const track = await this.request<Track>(
        `https://api.spotify.com/v1/tracks/${ids[0]}`,
        options
      );
      return [track];
    }
    
    // Handle batch requests (max 50)
    const batches: string[][] = [];
    for (let i = 0; i < ids.length; i += 50) {
      batches.push(ids.slice(i, i + 50));
    }
    
    const results: Track[] = [];
    
    for (const batch of batches) {
      const response = await this.request<{ tracks: Track[] }>(
        `https://api.spotify.com/v1/tracks?ids=${batch.join(',')}`,
        options
      );
      
      results.push(...response.tracks);
    }
    
    return results;
  }

  // Audio Features API
  public async getAudioFeatures(ids: string[], options?: RequestOptions): Promise<any[]> {
    // Spotify allows up to 100 audio features per request
    if (ids.length === 0) {
      return [];
    }
    
    if (ids.length === 1) {
      const features = await this.request<any>(
        `https://api.spotify.com/v1/audio-features/${ids[0]}`,
        options
      );
      return [features];
    }
    
    // Handle batch requests (max 100)
    const batches: string[][] = [];
    for (let i = 0; i < ids.length; i += 100) {
      batches.push(ids.slice(i, i + 100));
    }
    
    const results: any[] = [];
    
    for (const batch of batches) {
      const response = await this.request<{ audio_features: any[] }>(
        `https://api.spotify.com/v1/audio-features?ids=${batch.join(',')}`,
        options
      );
      
      results.push(...response.audio_features);
    }
    
    return results;
  }
}
