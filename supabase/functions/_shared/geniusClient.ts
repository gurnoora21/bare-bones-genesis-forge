
// Genius API client for Edge Functions

export class GeniusClient {
  private accessToken: string;
  private readonly baseUrl = "https://api.genius.com";
  private rateLimitDelay = 200; // ms between requests
  private lastRequestTime = 0;
  
  constructor() {
    this.accessToken = Deno.env.get("GENIUS_ACCESS_TOKEN") || "";
    
    if (!this.accessToken) {
      console.warn("Genius API token not configured, some producer data may be missing");
    }
  }
  
  private async throttledRequest(url: string): Promise<any> {
    // Respect rate limits with simple throttling
    const now = Date.now();
    const timeSinceLastRequest = now - this.lastRequestTime;
    
    if (timeSinceLastRequest < this.rateLimitDelay) {
      await new Promise(resolve => 
        setTimeout(resolve, this.rateLimitDelay - timeSinceLastRequest)
      );
    }
    
    this.lastRequestTime = Date.now();
    
    const response = await fetch(url, {
      headers: {
        "Authorization": `Bearer ${this.accessToken}`,
        "Content-Type": "application/json"
      }
    });
    
    if (response.status === 429) {
      this.rateLimitDelay += 100; // Increase delay on rate limit
      await new Promise(resolve => setTimeout(resolve, 2000));
      return this.throttledRequest(url);
    }
    
    if (!response.ok) {
      const error = await response.text();
      throw new Error(`Genius API request failed (${response.status}): ${error}`);
    }
    
    return await response.json();
  }
  
  async search(trackName: string, artistName: string): Promise<any> {
    if (!this.accessToken) return null;
    
    // Format query string
    const query = encodeURIComponent(`${trackName} ${artistName}`);
    const url = `${this.baseUrl}/search?q=${query}`;
    
    try {
      const data = await this.throttledRequest(url);
      
      if (data.response.hits && data.response.hits.length > 0) {
        // Find best match
        return data.response.hits[0].result;
      }
      
      return null;
    } catch (error) {
      console.error(`Error searching Genius: ${error}`);
      return null;
    }
  }
  
  async getSong(songId: number): Promise<any> {
    if (!this.accessToken) return null;
    
    const url = `${this.baseUrl}/songs/${songId}`;
    
    try {
      const data = await this.throttledRequest(url);
      return data.response.song;
    } catch (error) {
      console.error(`Error getting song details from Genius: ${error}`);
      return null;
    }
  }
  
  extractProducers(songData: any): Array<{name: string, confidence: number, source: string}> {
    const producers: Array<{name: string, confidence: number, source: string}> = [];
    
    try {
      // Extract producer credits from Genius data
      if (songData.producer_artists) {
        songData.producer_artists.forEach((producer: any) => {
          producers.push({
            name: producer.name,
            confidence: 0.95,
            source: 'genius_producer_credit'
          });
        });
      }
      
      // Try to find producer info in song description
      if (songData.description && songData.description.plain) {
        const producerMatches = songData.description.plain
          .match(/produced by[:\s]+([^.\n,]+)/gi);
        
        if (producerMatches) {
          producerMatches.forEach((match: string) => {
            const producerName = match
              .replace(/produced by[:\s]+/i, '')
              .trim();
            
            if (producerName) {
              producers.push({
                name: producerName,
                confidence: 0.8,
                source: 'genius_description'
              });
            }
          });
        }
      }
    } catch (error) {
      console.warn('Error extracting producers from Genius data:', error);
    }
    
    return producers;
  }
}
