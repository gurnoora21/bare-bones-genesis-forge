
import { createClient } from '@supabase/supabase-js';
import { BaseWorker } from '../lib/BaseWorker';
import { EnvConfig } from '../lib/EnvConfig';
import { GeniusClient } from '../lib/GeniusClient';

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

export class ProducerIdentificationWorker extends BaseWorker<ProducerIdentificationMessage> {
  private geniusClient: GeniusClient;
  
  constructor() {
    super({
      queueName: 'producer_identification',
      batchSize: 5,
      visibilityTimeout: 240,
      maxRetries: 3
    });
    
    this.geniusClient = GeniusClient.getInstance();
  }

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

    // Extract producers from Spotify metadata if available
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

      // Make sure producer exists and has an id before proceeding
      if (!producer || typeof producer.id === 'undefined') {
        console.error(`Missing producer data for ${producerCandidate.name}`);
        continue;
      }

      // Ensure producer id is a string
      const producerId = String(producer.id);
      
      // Associate producer with track
      const { error: associationError } = await this.supabase
        .from('track_producers')
        .upsert({
          track_id: trackId,
          producer_id: producerId,
          confidence: producerCandidate.confidence,
          source: producerCandidate.source
        });

      if (associationError) {
        console.error(`Error associating producer ${producerId} with track ${trackId}:`, associationError);
      }

      // If this is a new producer or hasn't been enriched yet, enqueue social enrichment
      if (!producer.enriched_at && !producer.enrichment_failed) {
        await this.enqueue('social_enrichment', {
          producerId: producerId,
          producerName: producer.name
        });
      }
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
    try {
      // First, search for the song on Genius
      const searchResult = await this.geniusClient.search(trackName, artistName);
      
      if (!searchResult) {
        return [];
      }
      
      // Get song details
      const songDetails = await this.geniusClient.getSong(searchResult.id);
      
      if (!songDetails) {
        return [];
      }
      
      // Extract producers from song details
      const geniusProducers = this.geniusClient.extractProducers(songDetails);
      
      // Convert to our internal format
      return geniusProducers.map(producer => ({
        name: producer.name,
        confidence: producer.confidence,
        source: producer.source
      }));
    } catch (e) {
      console.warn('Error getting Genius producers:', e);
      return [];
    }
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
