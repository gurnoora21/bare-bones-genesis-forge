
// Spotify API client for Edge Functions

export class SpotifyClient {
  private clientId: string;
  private clientSecret: string;
  private token: string | null = null;
  private tokenExpires: number = 0;
  private readonly baseUrl = "https://api.spotify.com/v1";
  
  constructor() {
    this.clientId = Deno.env.get("SPOTIFY_CLIENT_ID") || "";
    this.clientSecret = Deno.env.get("SPOTIFY_CLIENT_SECRET") || "";
    
    if (!this.clientId || !this.clientSecret) {
      throw new Error("Spotify API credentials not configured");
    }
  }
  
  // Core auth and request methods
  private async getToken(): Promise<string> {
    if (this.token && Date.now() < this.tokenExpires) {
      return this.token;
    }
    
    const response = await fetch("https://accounts.spotify.com/api/token", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": `Basic ${btoa(`${this.clientId}:${this.clientSecret}`)}`
      },
      body: "grant_type=client_credentials"
    });
    
    if (!response.ok) {
      const error = await response.text();
      throw new Error(`Spotify token request failed: ${error}`);
    }
    
    const data = await response.json();
    this.token = data.access_token;
    this.tokenExpires = Date.now() + (data.expires_in * 1000);
    return this.token;
  }
  
  private async request(endpoint: string, method: string = "GET", data?: any): Promise<any> {
    const token = await this.getToken();
    
    const headers: HeadersInit = {
      "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json"
    };
    
    const options: RequestInit = { method, headers };
    if (data) {
      options.body = JSON.stringify(data);
    }
    
    const response = await fetch(`${this.baseUrl}${endpoint}`, options);
    
    if (response.status === 429) {
      const retryAfter = parseInt(response.headers.get("Retry-After") || "1", 10);
      console.log(`Rate limited by Spotify. Retrying after ${retryAfter} seconds`);
      await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
      return this.request(endpoint, method, data);
    }
    
    if (!response.ok) {
      const error = await response.text();
      throw new Error(`Spotify API request failed (${response.status}): ${error}`);
    }
    
    return await response.json();
  }
  
  // Artist methods
  async getArtistById(id: string): Promise<any> {
    return this.request(`/artists/${id}`);
  }
  
  async getArtistByName(name: string): Promise<any> {
    const response = await this.request(`/search?q=${encodeURIComponent(name)}&type=artist&limit=1`);
    return response.artists.items.length > 0 ? response.artists.items[0] : null;
  }
  
  // Album methods
  async getArtistAlbums(artistId: string, offset: number = 0, limit: number = 50): Promise<any> {
    return this.request(`/artists/${artistId}/albums?limit=${limit}&offset=${offset}&include_groups=album,single`);
  }
  
  async getAlbumDetails(albumIds: string[]): Promise<any[]> {
    if (albumIds.length === 0) {
      return [];
    }
    
    const batchSize = 20;
    const results: any[] = [];
    
    for (let i = 0; i < albumIds.length; i += batchSize) {
      const batch = albumIds.slice(i, i + batchSize);
      const response = await this.request(`/albums?ids=${batch.join(",")}`);
      results.push(...response.albums);
    }
    
    return results;
  }
  
  // Track methods
  async getAlbumTracks(albumId: string, offset: number = 0, limit: number = 50): Promise<any> {
    return this.request(`/albums/${albumId}/tracks?limit=${limit}&offset=${offset}`);
  }
  
  async getTrackDetails(trackIds: string[]): Promise<any[]> {
    if (trackIds.length === 0) {
      return [];
    }
    
    const batchSize = 50;
    const results: any[] = [];
    
    for (let i = 0; i < trackIds.length; i += batchSize) {
      const batch = trackIds.slice(i, i + batchSize);
      const response = await this.request(`/tracks?ids=${batch.join(",")}`);
      results.push(...response.tracks.filter((t: any) => t !== null));
    }
    
    return results;
  }
  
  // Audio features
  async getAudioFeatures(trackIds: string[]): Promise<any[]> {
    if (trackIds.length === 0) {
      return [];
    }
    
    const batchSize = 100;
    const results: any[] = [];
    
    for (let i = 0; i < trackIds.length; i += batchSize) {
      const batch = trackIds.slice(i, i + batchSize);
      const response = await this.request(`/audio-features?ids=${batch.join(",")}`);
      results.push(...response.audio_features.filter((f: any) => f !== null));
    }
    
    return results;
  }
}
