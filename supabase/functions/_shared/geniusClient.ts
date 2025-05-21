
import { getRateLimiter, RATE_LIMITERS } from "./rateLimiter.ts";

/**
 * GeniusClient handles authentication and API calls to the Genius API,
 * implementing proper rate limiting and retry logic using our distributed
 * rate limiter.
 */
export class GeniusClient {
  private accessToken: string;
  private rateLimiter = getRateLimiter();
  
  constructor() {
    this.accessToken = Deno.env.get("GENIUS_ACCESS_TOKEN") || "";
    
    if (!this.accessToken) {
      throw new Error("Genius access token must be provided as environment variable");
    }
  }

  /**
   * Make a rate-limited request to the Genius API
   */
  async apiRequest(endpoint: string): Promise<any> {
    const url = endpoint.startsWith("https://")
      ? endpoint
      : `https://api.genius.com${endpoint}`;
    
    // Extract endpoint name for rate limiting
    const endpointName = endpoint.split('?')[0].split('/').filter(Boolean).pop() || 'default';
    
    // Determine if this is a search endpoint
    const limiterConfig = endpoint.includes("/search")
      ? { ...RATE_LIMITERS.GENIUS.DEFAULT, endpoint: "search" }
      : { ...RATE_LIMITERS.GENIUS.DEFAULT, endpoint: endpointName };
    
    // Determine cache TTL based on endpoint type
    // Search results change less frequently than individual resources
    const cacheTtl = endpoint.includes("/search") ? 60 * 60 * 24 : 60 * 60 * 12;
    
    // Use the rate limiter execute method instead of executeWithCache
    return this.rateLimiter.execute({
      ...limiterConfig,
      endpoint: limiterConfig.endpoint
    }, async () => {
      const response = await fetch(url, {
        headers: {
          "Authorization": `Bearer ${this.accessToken}`
        }
      });
      
      if (response.status === 429) {
        throw new Error("Genius API rate limited");
      }
      
      if (!response.ok) {
        throw new Error(`Genius API error: ${response.status} ${await response.text()}`);
      }
      
      return await response.json();
    });
  }

  /**
   * Search for a song on Genius
   */
  async search(trackName: string, artistName: string): Promise<any> {
    const query = encodeURIComponent(`${trackName} ${artistName}`);
    const result = await this.apiRequest(`/search?q=${query}`);
    
    // Return the full response object instead of just the first hit
    // This allows the caller to access the hits array and process results properly
    return result.response;
  }

  /**
   * Get detailed song information including producer credits
   */
  async getSong(songId: number): Promise<any> {
    const result = await this.apiRequest(`/songs/${songId}`);
    
    if (result.response && result.response.song) {
      return result.response.song;
    }
    
    return null;
  }

  /**
   * Extract producer information from song details
   */
  extractProducers(songDetails: any): { name: string, confidence: number, source: string }[] {
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
}

// Create a singleton instance
let geniusClientInstance: GeniusClient | null = null;

/**
 * Get or create a GeniusClient instance
 */
export function getGeniusClient(): GeniusClient {
  if (!geniusClientInstance) {
    geniusClientInstance = new GeniusClient();
  }
  return geniusClientInstance;
}
