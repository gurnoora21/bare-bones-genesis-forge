
/**
 * GeniusClient handles authentication and API calls to the Genius API,
 * implementing proper rate limiting and retry logic.
 */
export class GeniusClient {
  private accessToken: string;
  private lastRequestTime: number;
  private requestDelay: number; // Minimum milliseconds between requests
  
  constructor() {
    this.accessToken = Deno.env.get("GENIUS_ACCESS_TOKEN") || "";
    this.lastRequestTime = 0;
    this.requestDelay = 200; // 5 requests per second max
    
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
    
    // Implement rate limiting
    const now = Date.now();
    const timeToWait = Math.max(0, this.requestDelay - (now - this.lastRequestTime));
    
    if (timeToWait > 0) {
      await new Promise(resolve => setTimeout(resolve, timeToWait));
    }
    
    // Implement exponential backoff retries
    const maxRetries = 3;
    let retryCount = 0;
    let lastError: Error | null = null;
    
    while (retryCount < maxRetries) {
      try {
        this.lastRequestTime = Date.now();
        
        const response = await fetch(url, {
          headers: {
            "Authorization": `Bearer ${this.accessToken}`
          }
        });
        
        if (response.status === 429) {
          // Rate limited - back off more aggressively
          console.log("Genius API rate limited, backing off");
          await new Promise(resolve => setTimeout(resolve, 2000 * Math.pow(2, retryCount)));
          retryCount++;
          continue;
        }
        
        if (!response.ok) {
          throw new Error(`Genius API error: ${response.status} ${await response.text()}`);
        }
        
        return await response.json();
      } catch (error) {
        console.error(`Genius API request failed (attempt ${retryCount + 1}/${maxRetries}):`, error);
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

  /**
   * Search for a song on Genius
   */
  async search(trackName: string, artistName: string): Promise<any> {
    const query = encodeURIComponent(`${trackName} ${artistName}`);
    const result = await this.apiRequest(`/search?q=${query}`);
    
    if (result.response && result.response.hits && result.response.hits.length > 0) {
      // Find the most relevant hit (usually the first one)
      const hit = result.response.hits.find((h: any) => h.type === "song") || result.response.hits[0];
      return hit.result;
    }
    
    return null;
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
