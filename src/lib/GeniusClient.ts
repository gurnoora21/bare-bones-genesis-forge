
import { EnvConfig } from './EnvConfig';
import { BaseWorker } from './BaseWorker';

export interface GeniusSearchResult {
  id: number;
  title: string;
  artist_names: string;
  url: string;
  header_image_url?: string;
  primary_artist?: {
    id: number;
    name: string;
  };
}

export interface GeniusSearchResponse {
  meta: {
    status: number;
  };
  response: {
    hits: Array<{
      type: string;
      result: GeniusSearchResult;
    }>;
  };
}

export interface GeniusProducer {
  id?: number;
  name: string;
  confidence: number;
  source: string;
}

export interface GeniusSongDetails {
  id: number;
  title: string;
  url: string;
  embed_content?: string;
  producer_artists?: Array<{
    id: number;
    name: string;
  }>;
  custom_performances?: Array<{
    label: string;
    artists: Array<{
      id: number;
      name: string;
    }>;
  }>;
  description?: {
    dom?: {
      children: any[];
    };
    plain: string;
  };
  album?: {
    id: number;
    name: string;
  };
  media?: any[];
  writer_artists?: Array<{
    id: number;
    name: string;
  }>;
}

export interface GeniusSongResponse {
  meta: {
    status: number;
  };
  response: {
    song: GeniusSongDetails;
  };
}

export class GeniusClient {
  private static instance: GeniusClient;
  private apiKey: string;
  private baseUrl = 'https://api.genius.com';
  private cache: Map<string, { data: any; timestamp: number }> = new Map();
  private cacheTTLMs = 3600000; // 1 hour
  private requestCount = 0;
  private resetTime = Date.now();
  private readonly MAX_REQUESTS_PER_SECOND = 5;
  private readonly WINDOW_MS = 1000; // 1 second
  private readonly MAX_RETRIES = 3;
  private readonly RETRY_DELAY_MS = 1000; // Base retry delay

  private constructor() {
    this.apiKey = EnvConfig.GENIUS_ACCESS_TOKEN;
    if (!this.apiKey) {
      console.warn('Genius API token not configured. Set GENIUS_ACCESS_TOKEN in your environment.');
    }
  }

  public static getInstance(): GeniusClient {
    if (!GeniusClient.instance) {
      GeniusClient.instance = new GeniusClient();
    }
    return GeniusClient.instance;
  }

  /**
   * Search for a song on Genius
   */
  public async search(trackName: string, artistName: string): Promise<GeniusSearchResult | null> {
    const cacheKey = `search:${trackName}:${artistName}`;
    const cached = this.getCached<GeniusSearchResult>(cacheKey);
    
    if (cached) {
      return cached;
    }

    try {
      const searchQuery = `${trackName} ${artistName}`;
      const url = `${this.baseUrl}/search?q=${encodeURIComponent(searchQuery)}`;
      
      const response = await this.fetchWithRetry<GeniusSearchResponse>(url);
      
      if (!response.response.hits.length) {
        this.setCached(cacheKey, null);
        return null;
      }
      
      const result = response.response.hits[0].result;
      this.setCached(cacheKey, result);
      
      return result;
    } catch (error) {
      console.error(`Error searching Genius for ${trackName} by ${artistName}:`, error);
      return null;
    }
  }

  /**
   * Get detailed information about a song
   */
  public async getSong(id: number): Promise<GeniusSongDetails | null> {
    const cacheKey = `song:${id}`;
    const cached = this.getCached<GeniusSongDetails>(cacheKey);
    
    if (cached) {
      return cached;
    }

    try {
      const url = `${this.baseUrl}/songs/${id}`;
      const response = await this.fetchWithRetry<GeniusSongResponse>(url);
      
      const result = response.response.song;
      this.setCached(cacheKey, result);
      
      return result;
    } catch (error) {
      console.error(`Error getting Genius song details for ID ${id}:`, error);
      return null;
    }
  }

  /**
   * Extract producers from a Genius song
   */
  public extractProducers(song: GeniusSongDetails): GeniusProducer[] {
    const producers: GeniusProducer[] = [];
    
    // Extract from producer_artists field (most reliable)
    if (song.producer_artists && Array.isArray(song.producer_artists)) {
      song.producer_artists.forEach(producer => {
        producers.push({
          id: producer.id,
          name: producer.name,
          confidence: 0.95,
          source: 'genius_producer_artists'
        });
      });
    }
    
    // Extract from custom_performances that mention production
    if (song.custom_performances && Array.isArray(song.custom_performances)) {
      const productionCredits = song.custom_performances.filter(perf => 
        perf.label.toLowerCase().includes('produc') || 
        perf.label.toLowerCase().includes('beat') ||
        perf.label.toLowerCase().includes('instrumental')
      );
      
      productionCredits.forEach(credit => {
        if (credit.artists && Array.isArray(credit.artists)) {
          credit.artists.forEach(artist => {
            producers.push({
              id: artist.id,
              name: artist.name,
              confidence: 0.9,
              source: 'genius_custom_performances'
            });
          });
        }
      });
    }
    
    // Look for producers mentioned in description (less reliable)
    if (song.description?.plain) {
      const description = song.description.plain;
      
      // Look for common production credit patterns
      const producerPatterns = [
        /produced by\s+([^,.;]+)/gi,
        /production(?:\s+by)?\s+([^,.;]+)/gi,
        /beat(?:s)?\s+by\s+([^,.;]+)/gi
      ];
      
      for (const pattern of producerPatterns) {
        let match;
        while ((match = pattern.exec(description)) !== null) {
          if (match[1]) {
            const producerName = match[1].trim();
            // Avoid very short or very long names
            if (producerName.length > 2 && producerName.length < 50) {
              producers.push({
                name: producerName,
                confidence: 0.7, // Lower confidence for text extraction
                source: 'genius_description'
              });
            }
          }
        }
      }
    }
    
    // Deduplicate producers
    const uniqueProducers = this.deduplicateProducers(producers);
    
    return uniqueProducers;
  }

  /**
   * Remove duplicate producers, keeping the one with highest confidence
   */
  private deduplicateProducers(producers: GeniusProducer[]): GeniusProducer[] {
    const uniqueProducers = new Map<string, GeniusProducer>();
    
    for (const producer of producers) {
      const normalizedName = this.normalizeProducerName(producer.name);
      
      if (!normalizedName) continue;
      
      const existing = uniqueProducers.get(normalizedName);
      
      if (!existing || producer.confidence > existing.confidence) {
        uniqueProducers.set(normalizedName, producer);
      }
    }
    
    return Array.from(uniqueProducers.values());
  }

  /**
   * Normalize producer names for deduplication
   */
  private normalizeProducerName(name: string): string {
    if (!name) return '';
    
    // Remove extraneous information
    let normalized = name
      .replace(/\([^)]*\)/g, '') // Remove text in parentheses
      .replace(/\[[^\]]*\]/g, '') // Remove text in brackets
      
    // Remove special characters and trim
    normalized = normalized
      .replace(/[^\w\s]/g, ' ') // Replace special chars with space
      .replace(/\s+/g, ' ')     // Replace multiple spaces with a single space
      .trim()
      .toLowerCase();
      
    return normalized;
  }

  /**
   * Fetch with rate limiting and retries
   */
  private async fetchWithRetry<T>(url: string, retries = this.MAX_RETRIES): Promise<T> {
    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        await this.waitForRateLimit();
        
        const response = await fetch(url, {
          headers: {
            'Authorization': `Bearer ${this.apiKey}`
          }
        });
        
        if (response.status === 429) {
          const retryAfter = response.headers.get('Retry-After');
          const waitTime = retryAfter ? parseInt(retryAfter, 10) * 1000 : this.RETRY_DELAY_MS * Math.pow(2, attempt);
          console.warn(`Rate limited by Genius API. Waiting ${waitTime}ms before retry.`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
          continue;
        }
        
        if (!response.ok) {
          throw new Error(`HTTP error ${response.status}: ${response.statusText}`);
        }
        
        return await response.json() as T;
      } catch (error) {
        if (attempt === retries) {
          throw error;
        }
        
        console.warn(`Attempt ${attempt + 1} failed, retrying...`, error);
        const delay = this.RETRY_DELAY_MS * Math.pow(2, attempt) * (0.5 + Math.random() * 0.5); // Exponential backoff with jitter
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
    
    throw new Error('Maximum retries exceeded');
  }

  /**
   * Wait if rate limit is reached
   */
  private async waitForRateLimit(): Promise<void> {
    const now = Date.now();
    
    if (now > this.resetTime) {
      // Reset the counter for a new time window
      this.requestCount = 0;
      this.resetTime = now + this.WINDOW_MS;
    }
    
    if (this.requestCount >= this.MAX_REQUESTS_PER_SECOND) {
      // Wait until the rate limit resets
      const waitTime = this.resetTime - now;
      await new Promise(resolve => setTimeout(resolve, waitTime > 0 ? waitTime : 1));
      this.requestCount = 0;
      this.resetTime = Date.now() + this.WINDOW_MS;
    }
    
    this.requestCount++;
  }

  /**
   * Get cached response if available
   */
  private getCached<T>(key: string): T | null {
    const cached = this.cache.get(key);
    if (cached && Date.now() - cached.timestamp < this.cacheTTLMs) {
      return cached.data as T;
    }
    return null;
  }

  /**
   * Cache response data
   */
  private setCached<T>(key: string, data: T): void {
    this.cache.set(key, { data, timestamp: Date.now() });
  }
}
