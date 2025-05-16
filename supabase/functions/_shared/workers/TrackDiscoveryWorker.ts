
/**
 * TrackDiscoveryWorker class
 * Extends EnhancedWorkerBase to provide standardized track discovery functionality
 */

import { EnhancedWorkerBase } from "../enhancedWorkerBase.ts";
import { StructuredLogger } from "../structuredLogger.ts";
import { DeduplicationService } from "../deduplication.ts";
import { SpotifyClient } from "../spotifyClient.ts";

interface TrackDiscoveryMessage {
  albumId: string;
  albumName: string;
  artistId: string;
  offset?: number;
}

/**
 * Worker for discovering tracks from albums
 */
export class TrackDiscoveryWorker extends EnhancedWorkerBase<TrackDiscoveryMessage> {
  private spotifyClient: SpotifyClient;
  private deduplicationService: DeduplicationService;
  private redis: any;
  
  constructor(supabase: any, redis: any) {
    super('track_discovery', supabase, 'TrackDiscovery');
    
    // Initialize services
    this.spotifyClient = new SpotifyClient();
    this.deduplicationService = new DeduplicationService(redis);
    this.redis = redis;
  }
  
  /**
   * Implementation of abstract handleMessage method from base class
   */
  async handleMessage(message: TrackDiscoveryMessage, logger: StructuredLogger): Promise<any> {
    const { albumId, albumName, artistId, offset = 0 } = message;
    
    logger.info(`Processing tracks for album ${albumName} (ID: ${albumId}) at offset ${offset}`);
    
    // Generate a deduplication key
    const dedupKey = `album:${albumId}:offset:${offset}`;
    
    // Check if this album+offset was already processed
    const alreadyProcessed = await this.deduplicationService.isDuplicate(
      'track_discovery', 
      dedupKey,
      { logDetails: true },
      { entityId: albumId }
    );
    
    if (alreadyProcessed) {
      logger.info(`Tracks for album ${albumId} at offset ${offset} already processed, skipping`);
      return { 
        processed: 0, 
        skipped: true, 
        reason: "already_processed" 
      };
    }
    
    // Get album from database
    const { data: album, error: albumError } = await this.supabase
      .from('albums')
      .select('id, spotify_id')
      .eq('id', albumId)
      .single();

    if (albumError || !album) {
      const errMsg = `Album not found with ID: ${albumId}`;
      logger.error(errMsg);
      throw new Error(errMsg);
    }
    
    logger.info(`Found album in database: ${album.id} with Spotify ID: ${album.spotify_id}`);

    // Get artist from database
    const { data: artist, error: artistError } = await this.supabase
      .from('artists')
      .select('id, name')
      .eq('id', artistId)
      .single();

    if (artistError || !artist) {
      const errMsg = `Artist not found with ID: ${artistId}`;
      logger.error(errMsg);
      throw new Error(errMsg);
    }
    
    logger.info(`Found artist in database: ${artist.name} (ID: ${artist.id})`);

    // Ensure spotify_id is a string
    if (typeof album.spotify_id !== 'string') {
      const errMsg = `Invalid Spotify ID for album ${albumId}: ${album.spotify_id}`;
      logger.error(errMsg);
      throw new Error(errMsg);
    }
    
    // Fetch tracks from Spotify
    logger.info(`Fetching tracks from Spotify for album ${albumName} (ID: ${album.spotify_id})`);
    const tracksData = await this.spotifyClient.getAlbumTracks(album.spotify_id, offset);
    logger.info(`Found ${tracksData.items.length} tracks in album ${albumName} (total: ${tracksData.total})`);

    if (!tracksData.items || tracksData.items.length === 0) {
      logger.info(`No tracks found for album ${albumName}`);
      
      // Mark this batch as processed
      await this.deduplicationService.markAsProcessed(
        'track_discovery', 
        dedupKey,
        86400, // 24 hour TTL
        { entityId: albumId }
      );
      
      return { processed: 0 };
    }

    // Filter tracks that have the artist as primary
    const tracksToProcess = this.filterTracksWithPrimaryArtist(tracksData.items, artist.id);
    
    logger.info(`${tracksToProcess.length} tracks have the artist as primary artist`);

    // Early return if no tracks to process
    if (tracksToProcess.length === 0) {
      logger.info(`No primary artist tracks found for artist ${artist.name} in album ${albumName}`);
      
      // Mark this batch as processed even with zero tracks
      await this.deduplicationService.markAsProcessed(
        'track_discovery', 
        dedupKey,
        86400, // 24 hour TTL
        { entityId: albumId }
      );
      
      return { processed: 0, skipped: true, reason: "no_primary_tracks" };
    }

    // Process tracks in batches of 50 (Spotify API limit)
    let processedCount = 0;
    let errorCount = 0;
    const processedTrackIds = [];
    
    for (let i = 0; i < tracksToProcess.length; i += 50) {
      const batch = tracksToProcess.slice(i, i + 50);
      const trackIds = batch.map(t => t.id);
      
      logger.info(`Processing batch of ${batch.length} tracks, IDs: ${trackIds.slice(0, 3)}...`);
      
      try {
        // Get detailed track info
        const trackDetails = await this.spotifyClient.getTrackDetails(trackIds);
        logger.info(`Received ${trackDetails.length} track details from Spotify`);
        
        if (!trackDetails || trackDetails.length === 0) {
          logger.error(`No track details returned from Spotify for IDs: ${trackIds}`);
          errorCount += batch.length;
          continue;
        }
        
        // Process track batch atomically
        const tracksForBatch = trackDetails.map(track => ({
          name: track.name,
          spotify_id: track.id,
          duration_ms: track.duration_ms,
          popularity: track.popularity,
          spotify_preview_url: track.preview_url,
          metadata: {
            disc_number: track.disc_number,
            track_number: track.track_number,
            artists: track.artists,
            updated_at: new Date().toISOString()
          }
        }));
        
        // Execute atomic batch processing using DB function
        const { data: batchResult, error: batchError } = await this.supabase.rpc(
          'process_track_batch',
          {
            p_track_data: tracksForBatch,
            p_album_id: albumId,
            p_artist_id: artistId
          }
        );
        
        if (batchError) {
          throw new Error(`Error processing track batch: ${batchError.message}`);
        }
        
        // Process results
        if (batchResult.error) {
          logger.error(`Error from track batch processing: ${batchResult.error}`);
          errorCount += batch.length;
        } else {
          logger.info(`Successfully processed ${batchResult.processed} tracks in batch`);
          processedCount += batchResult.processed;
          
          // Extract track IDs for producer identification
          if (batchResult.results) {
            const resultArray = Array.isArray(batchResult.results) 
              ? batchResult.results 
              : [batchResult.results];
              
            for (const track of resultArray) {
              if (track.track_id) {
                processedTrackIds.push(track.track_id);
                
                // Enqueue producer identification
                await this.enqueueProducerIdentification(track, albumId, artistId);
              }
            }
          }
        }
      } catch (batchError) {
        logger.error(`Error processing batch of tracks: ${batchError.message}`, batchError);
        errorCount += batch.length;
      }
    }
    
    // If there are more tracks, enqueue the next page
    await this.enqueueNextPageIfNeeded(tracksData, albumId, albumName, artistId, offset);
    
    // Mark this batch as processed
    await this.deduplicationService.markAsProcessed(
      'track_discovery', 
      dedupKey,
      86400, // 24 hour TTL
      { 
        entityId: albumId,
        processedCount,
        errorCount,
        tracksTotal: tracksData.total
      }
    );
    
    return { 
      processed: processedCount, 
      errors: errorCount,
      tracksTotal: tracksData.total,
      hasMoreTracks: offset + tracksData.items.length < tracksData.total
    };
  }
  
  /**
   * Filter tracks to only include those where the specified artist is primary
   */
  private filterTracksWithPrimaryArtist(tracks: any[], artistId: string): any[] {
    return tracks.filter(track => {
      // Consider the artist primary if they're the first artist listed
      if (track.artists && track.artists.length > 0) {
        // Some APIs return artist.id, others might return artist.spotify_id
        const firstArtistId = track.artists[0].id || track.artists[0].spotify_id;
        return firstArtistId === artistId;
      }
      return false;
    });
  }
  
  /**
   * Enqueue producer identification for a track
   */
  private async enqueueProducerIdentification(track: any, albumId: string, artistId: string): Promise<void> {
    const producerMsg = {
      trackId: track.track_id,
      trackName: track.name,
      albumId: albumId,
      artistId: artistId
    };
    
    // Check if producer identification was already enqueued
    const producerKey = `enqueued:producer:${track.track_id}`;
    let alreadyEnqueued = false;
    
    try {
      alreadyEnqueued = await this.redis.exists(producerKey) === 1;
    } catch (redisError) {
      console.warn(`Redis check failed for producer identification:`, redisError);
    }
    
    if (!alreadyEnqueued) {
      // Enqueue producer identification task
      await this.enqueueMessage('producer_identification', producerMsg, `track:${track.track_id}`);
      
      // Mark as enqueued in Redis
      try {
        await this.redis.set(producerKey, 'true', { ex: 86400 }); // 24 hour TTL
      } catch (redisError) {
        console.warn(`Failed to mark producer identification as enqueued:`, redisError);
      }
    }
  }
  
  /**
   * Enqueue the next page of tracks if needed
   */
  private async enqueueNextPageIfNeeded(
    tracksData: any, 
    albumId: string, 
    albumName: string, 
    artistId: string,
    offset: number
  ): Promise<void> {
    if (tracksData.items.length > 0 && offset + tracksData.items.length < tracksData.total) {
      const newOffset = offset + tracksData.items.length;
      this.logger.info(`Enqueueing next page of tracks for album ${albumName} with offset ${newOffset}`);
      
      // Use an idempotency key for the next page enqueue
      const nextPageKey = `enqueued:nextpage:${albumId}:${newOffset}`;
      let nextPageEnqueued = false;
      
      try {
        nextPageEnqueued = await this.redis.exists(nextPageKey) === 1;
      } catch (redisError) {
        // If Redis check fails, continue with enqueuing
        console.warn(`Redis check failed for next page:`, redisError);
      }
      
      if (!nextPageEnqueued) {
        // Enqueue next batch with new offset
        await this.enqueueMessage('track_discovery', { 
          albumId, 
          albumName,
          artistId, 
          offset: newOffset 
        }, `album:${albumId}:offset:${newOffset}`);
        
        // Mark as enqueued in Redis
        try {
          await this.redis.set(nextPageKey, 'true', { ex: 86400 }); // 24 hour TTL
        } catch (redisError) {
          console.warn(`Failed to mark next page as enqueued:`, redisError);
        }
      }
    }
  }
}
