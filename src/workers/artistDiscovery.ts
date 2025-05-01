import { createClient } from '@supabase/supabase-js';
import { BaseWorker } from '../lib/BaseWorker.ts';
import { SpotifyAuth } from '../lib/SpotifyAuth.ts';
import { EnvConfig } from '../lib/EnvConfig';

interface ArtistDiscoveryMessage {
  artistId?: string;
  artistName?: string;
}

export class ArtistDiscoveryWorker extends BaseWorker<ArtistDiscoveryMessage> {
  private spotifyAuth: SpotifyAuth;

  constructor() {
    super({
      queueName: 'artist_discovery',
      batchSize: 10,
      visibilityTimeout: 120,
      maxRetries: 3
    });
    this.spotifyAuth = SpotifyAuth.getInstance();
  }

  async processMessage(message: ArtistDiscoveryMessage): Promise<void> {
    if (!message.artistId && !message.artistName) {
      throw new Error('Either artistId or artistName must be provided');
    }

    let artistId = message.artistId;
    let artistData: any;

    // If we only have the artist name, search for it first
    if (!artistId && message.artistName) {
      artistId = await this.findArtistByName(message.artistName);
      if (!artistId) {
        throw new Error(`Artist not found: ${message.artistName}`);
      }
    }

    // Get artist details
    artistData = await this.getArtistDetails(artistId!);

    // Store artist in database
    const { data: artist, error } = await this.supabase
      .from('artists')
      .upsert({
        spotify_id: artistId,
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

  private async findArtistByName(artistName: string): Promise<string | null> {
    await this.waitForRateLimit('spotify');

    return this.withCircuitBreaker('spotify', async () => {
      const token = await this.spotifyAuth.getToken();
      const encodedName = encodeURIComponent(artistName);
      
      const response = await this.cachedFetch<any>(
        `https://api.spotify.com/v1/search?q=${encodedName}&type=artist&limit=1`,
        {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );

      const artists = response.artists?.items;
      if (!artists || artists.length === 0) {
        return null;
      }

      return artists[0].id;
    });
  }

  private async getArtistDetails(artistId: string): Promise<any> {
    await this.waitForRateLimit('spotify');

    return this.withCircuitBreaker('spotify', async () => {
      const token = await this.spotifyAuth.getToken();
      
      return this.cachedFetch<any>(
        `https://api.spotify.com/v1/artists/${artistId}`,
        {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );
    });
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
