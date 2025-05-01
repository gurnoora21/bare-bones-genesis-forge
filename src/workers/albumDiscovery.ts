
import { createClient } from '@supabase/supabase-js';
import { BaseWorker } from '../lib/BaseWorker.ts';
import { SpotifyAuth } from '../lib/SpotifyAuth.ts';
import { EnvConfig } from '../lib/EnvConfig';

interface AlbumDiscoveryMessage {
  artistId: string;
  offset: number;
}

export class AlbumDiscoveryWorker extends BaseWorker<AlbumDiscoveryMessage> {
  private spotifyAuth: SpotifyAuth;
  private readonly ALBUMS_PER_PAGE = 50;  // Spotify maximum

  constructor() {
    super({
      queueName: 'album_discovery',
      batchSize: 5,
      visibilityTimeout: 180,
      maxRetries: 3
    });
    this.spotifyAuth = SpotifyAuth.getInstance();
  }

  async processMessage(message: AlbumDiscoveryMessage): Promise<void> {
    const { artistId, offset } = message;

    // Get artist details from database (to get spotify_id)
    const { data: artist, error: artistError } = await this.supabase
      .from('artists')
      .select('id, spotify_id')
      .eq('id', artistId)
      .single();

    if (artistError || !artist) {
      throw new Error(`Artist not found with ID: ${artistId}`);
    }

    // Ensure spotify_id is a string
    if (typeof artist.spotify_id !== 'string') {
      throw new Error(`Invalid Spotify ID for artist ${artistId}`);
    }

    // Fetch albums from Spotify
    const albumsData = await this.getArtistAlbums(artist.spotify_id, offset);
    console.log(`Retrieved ${albumsData.items.length} albums for artist ${artistId}, offset ${offset}`);

    // Process albums
    for (const albumData of albumsData.items) {
      // Skip compilations, appearances, etc. if needed
      if (albumData.album_type !== 'album' && albumData.album_type !== 'single') {
        continue;
      }
      
      // Get full album details (batch this in a real implementation)
      const fullAlbumData = await this.getAlbumDetails(albumData.id);
      
      // Store album in database
      const { data: album, error } = await this.supabase
        .from('albums')
        .upsert({
          artist_id: artistId,
          name: fullAlbumData.name,
          spotify_id: fullAlbumData.id,
          release_date: fullAlbumData.release_date,
          cover_url: fullAlbumData.images[0]?.url,
          metadata: fullAlbumData
        })
        .select()
        .single();

      if (error) {
        console.error(`Error storing album ${fullAlbumData.name}:`, error);
        continue; // Continue with other albums
      }

      // Enqueue track discovery
      await this.enqueue('track_discovery', {
        albumId: album.id,
        albumName: album.name,
        artistId: artistId,
        offset: 0
      });
    }

    // If there are more albums, enqueue the next batch
    if (albumsData.next) {
      await this.enqueue('album_discovery', {
        artistId: artistId,
        offset: offset + this.ALBUMS_PER_PAGE
      });
    }
  }

  private async getArtistAlbums(spotifyArtistId: string, offset: number): Promise<any> {
    await this.waitForRateLimit('spotify');

    return this.withCircuitBreaker('spotify', async () => {
      const token = await this.spotifyAuth.getToken();
      
      return this.cachedFetch<any>(
        `https://api.spotify.com/v1/artists/${spotifyArtistId}/albums?limit=${this.ALBUMS_PER_PAGE}&offset=${offset}`,
        {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );
    });
  }

  private async getAlbumDetails(albumId: string): Promise<any> {
    await this.waitForRateLimit('spotify');

    return this.withCircuitBreaker('spotify', async () => {
      const token = await this.spotifyAuth.getToken();
      
      return this.cachedFetch<any>(
        `https://api.spotify.com/v1/albums/${albumId}`,
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
export async function handleAlbumDiscovery(): Promise<any> {
  try {
    const worker = new AlbumDiscoveryWorker();
    const metrics = await worker.processBatch();
    
    return metrics;
  } catch (error) {
    console.error('Error in album discovery worker:', error);
    return { error: error instanceof Error ? error.message : 'Unknown error' };
  }
}
