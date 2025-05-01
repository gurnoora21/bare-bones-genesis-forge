
import { createClient } from '@supabase/supabase-js';
import { BaseWorker } from '../lib/BaseWorker';
import { SpotifyClient } from '../lib/SpotifyClient';
import { EnvConfig } from '../lib/EnvConfig';

interface AlbumDiscoveryMessage {
  artistId: string;
  offset: number;
}

export class AlbumDiscoveryWorker extends BaseWorker<AlbumDiscoveryMessage> {
  private spotifyClient: SpotifyClient;
  private readonly ALBUMS_PER_PAGE = 50;  // Spotify maximum

  constructor() {
    super({
      queueName: 'album_discovery',
      batchSize: 5,
      visibilityTimeout: 180,
      maxRetries: 3
    });
    this.spotifyClient = SpotifyClient.getInstance();
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
    const albumsData = await this.spotifyClient.getArtistAlbums(artist.spotify_id, offset);
    console.log(`Retrieved ${albumsData.items.length} albums for artist ${artistId}, offset ${offset}`);

    if (albumsData.items.length === 0) {
      return; // No albums to process
    }

    // Extract album IDs for batch retrieval
    const albumIds = albumsData.items
      .filter(album => album.album_type === 'album' || album.album_type === 'single')
      .map(album => album.id);

    if (albumIds.length === 0) {
      return; // No album or single type albums found
    }

    // Get detailed album information in batches
    const fullAlbums = await this.spotifyClient.getAlbumDetails(albumIds);

    // Process each album
    for (const fullAlbumData of fullAlbums) {
      // Store album in database
      const { data: album, error } = await this.supabase
        .from('albums')
        .upsert({
          artist_id: artistId,
          spotify_id: fullAlbumData.id,
          name: fullAlbumData.name,
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
        artistId,
        offset: 0
      });
    }

    // If there are more albums, enqueue the next batch
    if (albumsData.next) {
      await this.enqueue('album_discovery', {
        artistId,
        offset: offset + this.ALBUMS_PER_PAGE
      });
    }
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
