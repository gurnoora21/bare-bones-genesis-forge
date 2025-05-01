
import { createClient } from '@supabase/supabase-js';
import { EnvConfig } from '../lib/EnvConfig';

interface ProducerIdentificationMessage {
  trackId: string;
  trackName: string;
  albumId: string;
  artistId: string;
}

interface ProducerCandidate {
  name: string;
  confidence: number;
  source: string;
}

export class ProducerIdentificationWorker {
  private supabase;
  private queueName = 'producer_identification';
  private batchSize = 5;
  private visibilityTimeout = 240;
  private maxRetries = 3;
  
  // Rate limiting properties
  private requestCounts: Record<string, { count: number; resetTime: number }> = {};
  private maxRequestsPerWindow: Record<string, number> = {
    genius: 5  // 5 requests per second for Genius
  };
  private windowMs: Record<string, number> = {
    genius: 1000  // 1 second window for Genius
  };
  
  constructor() {
    this.supabase = createClient(
      EnvConfig.SUPABASE_URL,
      EnvConfig.SUPABASE_SERVICE_ROLE_KEY
    );
  }

  async processBatch(): Promise<any> {
    try {
      // Get messages from queue
      const { data: messages, error } = await this.supabase.rpc('pg_dequeue', {
        queue_name: this.queueName,
        batch_size: this.batchSize,
        visibility_timeout: this.visibilityTimeout
      });
      
      if (error) throw error;
      
      if (!messages || !Array.isArray(messages) || messages.length === 0) {
        return { processedCount: 0, successCount: 0, errorCount: 0 };
      }
      
      const processPromises = messages.map(async (message: any) => {
        try {
          // Parse the message body - ensure it's a string before parsing
          const messageBody = typeof message.message_body === 'string' 
            ? message.message_body 
            : JSON.stringify(message.message_body);
            
          const payload = JSON.parse(messageBody) as ProducerIdentificationMessage;
          
          // Process the message
          await this.processMessage(payload);
          
          // Delete the message from the queue after successful processing
          await this.supabase.rpc('pg_delete_message', {
            queue_name: this.queueName,
            message_id: message.id
          });
          
          return true;
        } catch (err) {
          console.error(`Error processing message:`, err);
          
          // Return the message to the queue for retry
          await this.supabase.rpc('pg_release_message', {
            queue_name: this.queueName,
            message_id: message.id
          });
          
          return false;
        }
      });
      
      const results = await Promise.allSettled(processPromises);
      const successCount = results.filter(r => r.status === 'fulfilled' && r.value === true).length;
      
      return {
        processedCount: messages.length,
        successCount,
        errorCount: messages.length - successCount
      };
    } catch (err) {
      console.error('Error processing batch:', err);
      return { processedCount: 0, successCount: 0, errorCount: 1, error: err };
    }
  }

  // Main message processing function
  async processMessage(message: ProducerIdentificationMessage): Promise<void> {
    const { trackId, trackName, albumId, artistId } = message;

    // Get track details
    const { data: trackData, error: trackError } = await this.supabase
      .from('tracks')
      .select('id, name, metadata, spotify_id')
      .eq('id', trackId)
      .single();

    if (trackError || !trackData) {
      throw new Error(`Track not found with ID: ${trackId}`);
    }

    // Get artist details (for search)
    const { data: artistData, error: artistError } = await this.supabase
      .from('artists')
      .select('id, name')
      .eq('id', artistId)
      .single();

    if (artistError || !artistData) {
      throw new Error(`Artist not found with ID: ${artistId}`);
    }

    // Ensure artist name is a string
    if (typeof artistData.name !== 'string') {
      throw new Error(`Invalid artist name for artist ${artistId}`);
    }

    // Get producers from Spotify metadata if available
    const spotifyProducers = this.extractProducersFromSpotify(trackData.metadata);
    
    // Get producers from Genius API
    const geniusProducers = await this.getProducersFromGenius(trackData.name, artistData.name);
    
    // Combine producer candidates and remove duplicates
    const allProducerCandidates = this.combineAndDedupProducers([
      ...spotifyProducers,
      ...geniusProducers
    ]);

    // Process each producer
    for (const producerCandidate of allProducerCandidates) {
      // Normalize producer name
      const normalizedName = this.normalizeProducerName(producerCandidate.name);
      
      if (!normalizedName) continue; // Skip empty names
      
      // Upsert producer
      const { data: producer, error } = await this.supabase
        .from('producers')
        .upsert({
          name: producerCandidate.name,
          normalized_name: normalizedName
        })
        .select()
        .single();

      if (error) {
        console.error(`Error upserting producer ${producerCandidate.name}:`, error);
        continue;
      }

      // Associate producer with track
      const { error: associationError } = await this.supabase
        .from('track_producers')
        .upsert({
          track_id: trackId,
          producer_id: producer.id,
          confidence: producerCandidate.confidence,
          source: producerCandidate.source
        });

      if (associationError) {
        console.error(`Error associating producer ${producer.id} with track ${trackId}:`, associationError);
      }

      // If this is a new producer or hasn't been enriched yet, enqueue social enrichment
      if (!producer.enriched_at && !producer.enrichment_failed) {
        await this.enqueueMessage('social_enrichment', {
          producerId: producer.id,
          producerName: producer.name
        });
      }
    }
  }

  private async enqueueMessage(queueName: string, message: any): Promise<void> {
    const { error } = await this.supabase.rpc('pg_enqueue', {
      queue_name: queueName,
      message_body: JSON.stringify(message)
    });
    
    if (error) {
      console.error(`Error enqueuing message to ${queueName}:`, error);
      throw error;
    }
  }

  private extractProducersFromSpotify(metadata: any): ProducerCandidate[] {
    const producers: ProducerCandidate[] = [];
    
    try {
      // Extract producers from Spotify metadata
      if (metadata?.producers) {
        metadata.producers.forEach((producer: string) => {
          producers.push({
            name: producer,
            confidence: 0.9,
            source: 'spotify_metadata'
          });
        });
      }
      
      // Look for producer information in track credits if available
      if (metadata?.credits) {
        const producerCredits = metadata.credits.filter((credit: any) => 
          credit.role?.toLowerCase().includes('produc') || 
          credit.role?.toLowerCase().includes('beat') ||
          credit.role?.toLowerCase().includes('instrumental')
        );
        
        producerCredits.forEach((credit: any) => {
          producers.push({
            name: credit.name,
            confidence: 0.85,
            source: 'spotify_credits'
          });
        });
      }
    } catch (e) {
      console.warn('Error extracting Spotify producers:', e);
    }
    
    return producers;
  }

  private async getProducersFromGenius(trackName: string, artistName: string): Promise<ProducerCandidate[]> {
    const producers: ProducerCandidate[] = [];
    
    try {
      // First, search for the song on Genius
      await this.waitForRateLimit('genius');
      
      const geniusAccessToken = EnvConfig.GENIUS_ACCESS_TOKEN;
      if (!geniusAccessToken) {
        throw new Error('Genius API token not configured');
      }
      
      const searchQuery = `${trackName} ${artistName}`;
      const response = await fetch(
        `https://api.genius.com/search?q=${encodeURIComponent(searchQuery)}`,
        {
          headers: {
            'Authorization': `Bearer ${geniusAccessToken}`
          }
        }
      );
      
      if (!response.ok) {
        throw new Error(`Genius search failed: ${response.statusText}`);
      }
      
      const searchData = await response.json();
      
      // Get the first result
      const firstResult = searchData.response.hits[0]?.result;
      if (!firstResult) {
        return producers;
      }
      
      // Get song details
      await this.waitForRateLimit('genius');
      const songResponse = await fetch(
        `https://api.genius.com/songs/${firstResult.id}`,
        {
          headers: {
            'Authorization': `Bearer ${geniusAccessToken}`
          }
        }
      );
      
      if (!songResponse.ok) {
        throw new Error(`Genius song details failed: ${songResponse.statusText}`);
      }
      
      const songData = await songResponse.json();
      const song = songData.response.song;
      
      // Extract producers from song details
      if (song.producer_artists) {
        song.producer_artists.forEach((producer: any) => {
          producers.push({
            name: producer.name,
            confidence: 0.95,
            source: 'genius_producer_artists'
          });
        });
      }
      
      // Look for producer mentions in description
      if (song.description?.dom?.children) {
        // Note: This is a simplified version. In a real implementation,
        // you might need more sophisticated text processing.
        const description = JSON.stringify(song.description.dom.children);
        
        // Look for producers mentioned in description
        const producerMatches = description.match(/produced by ([^,.]+)/gi);
        if (producerMatches) {
          producerMatches.forEach((match: string) => {
            const producerName = match.replace(/produced by /i, '').trim();
            producers.push({
              name: producerName,
              confidence: 0.75,
              source: 'genius_description'
            });
          });
        }
      }
    } catch (e) {
      console.warn('Error getting Genius producers:', e);
    }
    
    return producers;
  }

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

  private combineAndDedupProducers(producers: ProducerCandidate[]): ProducerCandidate[] {
    const uniqueProducers = new Map<string, ProducerCandidate>();
    
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

  private async waitForRateLimit(apiName: string): Promise<void> {
    const now = Date.now();
    
    if (!this.requestCounts[apiName]) {
      this.requestCounts[apiName] = { count: 0, resetTime: now + this.windowMs[apiName] };
    }
    
    if (now > this.requestCounts[apiName].resetTime) {
      // Reset the counter for a new time window
      this.requestCounts[apiName] = { count: 0, resetTime: now + this.windowMs[apiName] };
    }
    
    if (this.requestCounts[apiName].count >= this.maxRequestsPerWindow[apiName]) {
      // Wait until the rate limit resets
      const waitTime = this.requestCounts[apiName].resetTime - now;
      await new Promise(resolve => setTimeout(resolve, waitTime > 0 ? waitTime : 1));
      this.requestCounts[apiName] = { count: 0, resetTime: Date.now() + this.windowMs[apiName] };
    }
    
    this.requestCounts[apiName].count++;
  }
}

// Node.js version of Edge function handler
export async function handleProducerIdentification(): Promise<any> {
  try {
    const worker = new ProducerIdentificationWorker();
    const metrics = await worker.processBatch();
    
    return metrics;
  } catch (error) {
    console.error('Error in producer identification worker:', error);
    return { error: error instanceof Error ? error.message : 'Unknown error' };
  }
}
