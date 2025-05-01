
import { createClient } from '@supabase/supabase-js';
import { BaseWorker } from '../lib/BaseWorker';
import { SpotifyClient } from '../lib/SpotifyClient';
import { EnvConfig } from '../lib/EnvConfig';

interface ArtistDiscoveryMessage {
  artistId?: string;
  artistName?: string;
}

export class ArtistDiscoveryWorker extends BaseWorker<ArtistDiscoveryMessage> {
  private spotifyClient: SpotifyClient;

  constructor() {
    super({
      queueName: 'artist_discovery',
      batchSize: 10,
      visibilityTimeout: 120,
      maxRetries: 3
    });
    this.spotifyClient = SpotifyClient.getInstance();
  }

  async processMessage(message: ArtistDiscoveryMessage): Promise<void> {
    if (!message.artistId && !message.artistName) {
      throw new Error('Either artistId or artistName must be provided');
    }

    let artistData: any;

    // If we only have the artist name, search for it first
    if (!message.artistId && message.artistName) {
      artistData = await this.spotifyClient.getArtistByName(message.artistName);
      if (!artistData) {
        throw new Error(`Artist not found: ${message.artistName}`);
      }
    } else {
      // Get artist details by ID
      artistData = await this.spotifyClient.getArtistById(message.artistId!);
    }

    // Store artist in database
    const { data: artist, error } = await this.supabase
      .from('artists')
      .upsert({
        spotify_id: artistData.id,
        name: artistData.name,
        followers: artistData.followers.total,
        popularity: artistData.popularity,
        image_url: artistData.images[0]?.url,
        metadata: artistData
      })
      .select()
      .single();

    if (error) {
      throw error;
    }

    // Enqueue album discovery
    await this.enqueue('album_discovery', {
      artistId: artist.id,
      offset: 0
    });

    console.log(`Processed artist: ${artist.name} (ID: ${artist.id})`);
  }
}

// Node.js version of Edge function handler
export async function handleArtistDiscovery(): Promise<any> {
  try {
    const worker = new ArtistDiscoveryWorker();
    const metrics = await worker.processBatch();
    
    return metrics;
  } catch (error) {
    console.error('Error in artist discovery worker:', error);
    return { error: error instanceof Error ? error.message : 'Unknown error' };
  }
}
