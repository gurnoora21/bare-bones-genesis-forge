
import { createClient } from '@supabase/supabase-js';
import { BaseWorker } from '../lib/BaseWorker';
import { SpotifyClient } from '../lib/SpotifyClient';
import { EnvConfig } from '../lib/EnvConfig';

interface TrackDiscoveryMessage {
  albumId: string;
  albumName: string;
  artistId: string;
  offset?: number;
}

export class TrackDiscoveryWorker extends BaseWorker<TrackDiscoveryMessage> {
  private spotifyClient: SpotifyClient;
  private readonly TRACKS_PER_PAGE = 50;  // Spotify maximum

  constructor() {
    super({
      queueName: 'track_discovery',
      batchSize: 5,
      visibilityTimeout: 180,
      maxRetries: 3
    });
    this.spotifyClient = SpotifyClient.getInstance();
  }

  async processMessage(message: TrackDiscoveryMessage): Promise<void> {
    const { albumId, albumName, artistId, offset = 0 } = message;

    // Get album details from database (to get spotify_id)
    const { data: album, error: albumError } = await this.supabase
      .from('albums')
      .select('spotify_id')
      .eq('id', albumId)
      .single();

    if (albumError || !album) {
      throw new Error(`Album not found with ID: ${albumId}`);
    }

    // Ensure spotify_id is a string
    if (typeof album.spotify_id !== 'string') {
      throw new Error(`Invalid Spotify ID for album ${albumId}`);
    }

    // Fetch tracks from Spotify
    const tracksData = await this.spotifyClient.getAlbumTracks(album.spotify_id, offset);
    console.log(`Retrieved ${tracksData.items.length} tracks for album ${albumName}, offset ${offset}`);

    if (tracksData.items.length === 0) {
      return; // No tracks to process
    }

    // Extract track IDs for batch retrieval
    const trackIds = tracksData.items.map(track => track.id);

    // Get detailed track information in batch
    const fullTracks = await this.spotifyClient.getTrackDetails(trackIds);

    // Process each track
    for (const trackData of fullTracks) {
      // Store track in database
      const { data: track, error } = await this.supabase
        .from('tracks')
        .upsert({
          album_id: albumId,
          artist_id: artistId,
          spotify_id: trackData.id,
          name: trackData.name,
          duration_ms: trackData.duration_ms,
          track_number: trackData.track_number,
          popularity: trackData.popularity,
          preview_url: trackData.preview_url,
          metadata: trackData
        })
        .select()
        .single();

      if (error) {
        console.error(`Error storing track ${trackData.name}:`, error);
        continue; // Continue with other tracks
      }

      // Enqueue producer identification
      await this.enqueue('producer_identification', {
        trackId: track.id,
        trackName: track.name,
        albumId,
        artistId
      });
    }

    // If there are more tracks, enqueue the next batch
    if (tracksData.next) {
      await this.enqueue('track_discovery', {
        albumId,
        albumName,
        artistId,
        offset: offset + this.TRACKS_PER_PAGE
      });
    }
  }
}

// Node.js version of Edge function handler
export async function handleTrackDiscovery(): Promise<any> {
  try {
    const worker = new TrackDiscoveryWorker();
    const metrics = await worker.processBatch();
    
    return metrics;
  } catch (error) {
    console.error('Error in track discovery worker:', error);
    return { error: error instanceof Error ? error.message : 'Unknown error' };
  }
}
