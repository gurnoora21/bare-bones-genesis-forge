
import { createClient } from '@supabase/supabase-js';
import { BaseWorker } from '../lib/BaseWorker.ts';
import { SpotifyAuth } from '../lib/SpotifyAuth.ts';

interface TrackDiscoveryMessage {
  albumId: string;
  albumName: string;
  artistId: string;
  offset?: number;
}

export class TrackDiscoveryWorker extends BaseWorker<TrackDiscoveryMessage> {
  private spotifyAuth: SpotifyAuth;
  private readonly TRACKS_PER_PAGE = 50;  // Spotify maximum

  constructor() {
    super({
      queueName: 'track_discovery',
      batchSize: 5,
      visibilityTimeout: 180,
      maxRetries: 3
    });
    this.spotifyAuth = SpotifyAuth.getInstance();
  }

  async processMessage(message: TrackDiscoveryMessage): Promise<void> {
    const { albumId, albumName, artistId, offset = 0 } = message;

    // Get album details from database (to get spotify_id)
    const { data: album, error: albumError } = await this.supabase
      .from('albums')
      .select('id, spotify_id')
      .eq('id', albumId)
      .single();

    if (albumError || !album) {
      throw new Error(`Album not found with ID: ${albumId}`);
    }

    // Get tracks from Spotify
    const tracksData = await this.getAlbumTracks(album.spotify_id, offset);
    console.log(`Retrieved ${tracksData.items.length} tracks for album ${albumId}, offset ${offset}`);

    // Get detailed track information
    const trackIds = tracksData.items.map(t => t.id);
    let detailedTracks = [];
    
    if (trackIds.length > 0) {
      // Get detailed track data in batches (Spotify limit: 50)
      for (let i = 0; i < trackIds.length; i += 50) {
        const batchIds = trackIds.slice(i, i + 50).join(',');
        const batchData = await this.getTracksDetails(batchIds);
        detailedTracks = [...detailedTracks, ...batchData.tracks];
      }
    }

    // Process each track
    for (const trackData of detailedTracks) {
      // Store track in database
      const { data: track, error } = await this.supabase
        .from('tracks')
        .upsert({
          album_id: albumId,
          name: trackData.name,
          spotify_id: trackData.id,
          duration_ms: trackData.duration_ms,
          popularity: trackData.popularity,
          spotify_preview_url: trackData.preview_url,
          metadata: trackData
        })
        .select()
        .single();

      if (error) {
        console.error(`Error storing track ${trackData.name}:`, error);
        continue; // Continue with other tracks
      }

      // Create or update normalized track entry
      const normalizedTrackName = this.normalizeTrackName(trackData.name);
      
      // Upsert into normalized_tracks table
      await this.supabase.from('normalized_tracks').upsert({
        artist_id: artistId,
        normalized_name: normalizedTrackName,
        representative_track_id: track.id
      }, { onConflict: 'artist_id,normalized_name' });

      // Enqueue producer identification
      await this.enqueue('producer_identification', {
        trackId: track.id,
        trackName: track.name,
        albumId: albumId,
        artistId: artistId
      });
    }

    // Check if there are more tracks
    if (tracksData.next) {
      await this.enqueue('track_discovery', {
        albumId: albumId,
        albumName: albumName,
        artistId: artistId,
        offset: offset + this.TRACKS_PER_PAGE
      });
    }
  }

  private normalizeTrackName(trackName: string): string {
    // Remove featured artists (everything after " feat.", " ft.", " (feat", etc.)
    let normalized = trackName.replace(/\s+(?:feat|ft|featuring)\.?\s+.*$/i, '')
      .replace(/\s*\((?:feat|ft|featuring)\.?\s+.*\).*$/i, '');
    
    // Remove text in parentheses (like "radio edit", "remix", etc.)
    normalized = normalized.replace(/\s*\([^)]*\)\s*/g, ' ');
    
    // Remove special characters and extra spaces
    normalized = normalized.replace(/[^\w\s]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim()
      .toLowerCase();
    
    return normalized;
  }

  private async getAlbumTracks(spotifyAlbumId: string, offset: number): Promise<any> {
    await this.waitForRateLimit('spotify');

    return this.withCircuitBreaker('spotify', async () => {
      const token = await this.spotifyAuth.getToken();
      
      return this.cachedFetch<any>(
        `https://api.spotify.com/v1/albums/${spotifyAlbumId}/tracks?limit=${this.TRACKS_PER_PAGE}&offset=${offset}`,
        {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );
    });
  }

  private async getTracksDetails(trackIds: string): Promise<any> {
    await this.waitForRateLimit('spotify');

    return this.withCircuitBreaker('spotify', async () => {
      const token = await this.spotifyAuth.getToken();
      
      return this.cachedFetch<any>(
        `https://api.spotify.com/v1/tracks?ids=${trackIds}`,
        {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );
    });
  }
}

// Edge function handler
Deno.serve(async (req) => {
  try {
    const worker = new TrackDiscoveryWorker();
    const metrics = await worker.processBatch();
    
    return new Response(JSON.stringify(metrics), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    });
  } catch (error) {
    console.error('Error in track discovery worker:', error);
    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
});
