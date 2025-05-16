import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { createEnhancedWorker } from "../_shared/enhancedQueueWorker.ts";
import { getQueueHelper } from "../_shared/queueHelper.ts";
import { getDeduplicationService } from "../_shared/deduplication.ts";
import { SpotifyClient } from "../_shared/spotifyClient.ts";

// Initialize clients
const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const supabase = createClient(supabaseUrl, supabaseKey);

const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

// Initialize services
const queueHelper = getQueueHelper(supabase, redis);
const deduplicationService = getDeduplicationService(redis);
const spotifyClient = new SpotifyClient(
  Deno.env.get("SPOTIFY_CLIENT_ID") || "",
  Deno.env.get("SPOTIFY_CLIENT_SECRET") || ""
);

// Define DLQ configuration
const DLQ_CONFIG = {
  queueName: "album_discovery_dlq",
  maxRetries: 3,
  retryDelayMs: 5000, // 5 seconds between retries
}

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Define the album discovery worker implementation
class AlbumDiscoveryWorker extends createEnhancedWorker<any> {
  constructor() {
    super('album_discovery', supabase, redis);
  }
  
  async processMessage(message: any): Promise<any> {
    console.log("Processing album discovery message:", message);
    
    // Extract album job info
    const artistId = message.artistId;
    const offset = message.offset || 0;
    
    if (!artistId) {
      throw new Error("Message missing artistId");
    }
    
    // Generate deduplication key
    const dedupKey = `artist:${artistId}:albums:offset:${offset}`;
    
    // Check if already processed (extra safety)
    const isDuplicate = await deduplicationService.isDuplicate(
      'album_discovery', 
      dedupKey,
      { logDetails: true },
      { entityId: artistId }
    );
    
    if (isDuplicate) {
      console.log(`Album page for artist ${artistId} at offset ${offset} already processed, skipping`);
      return { status: 'skipped', reason: 'already_processed' };
    }
    
    // Get artist details from database
    const { data: artist, error: artistError } = await supabase
      .from('artists')
      .select('id, name, spotify_id')
      .eq('id', artistId)
      .single();
    
    if (artistError || !artist) {
      throw new Error(`Error fetching artist ${artistId}: ${artistError?.message || 'Artist not found'}`);
    }
    
    if (!artist.spotify_id) {
      throw new Error(`Artist ${artistId} has no Spotify ID`);
    }
    
    console.log(`Fetching albums for artist ${artist.name} (${artist.spotify_id}) at offset ${offset}`);
    
    // Get albums from Spotify
    const albumsData = await spotifyClient.getArtistAlbums(artist.spotify_id, {
      limit: 50,
      offset: offset,
      include_groups: "album,single,compilation"
    });
    
    if (!albumsData || !albumsData.items) {
      console.log(`No albums found for artist ${artist.name} at offset ${offset}`);
      
      // Mark as processed even with zero results (valid end of pagination)
      await deduplicationService.markAsProcessed(
        'album_discovery', 
        dedupKey,
        86400, // 24 hour TTL
        { entityId: artistId }
      );
      
      return { status: 'completed', albumsProcessed: 0, message: 'No albums returned from Spotify' };
    }
    
    console.log(`Found ${albumsData.items.length} albums for artist ${artist.name}`);
    
    // Process metrics
    const results = {
      albumsProcessed: 0,
      tracksQueued: 0,
      nextPagesQueued: 0,
      errors: 0
    };
    
    // Process each album
    for (const album of albumsData.items) {
      try {
        if (!album.id || !album.name) {
          console.warn(`Skipping album with missing data:`, album);
          continue;
        }
        
        // Prepare album data
        const albumData = {
          artist_id: artist.id,
          name: album.name,
          spotify_id: album.id,
          release_date: album.release_date || null,
          cover_url: album.images && album.images.length > 0 ? album.images[0].url : null,
          metadata: {
            album_type: album.album_type,
            total_tracks: album.total_tracks,
            spotify_url: album.external_urls?.spotify,
            label: album.label,
            copyrights: album.copyrights
          }
        };
        
        // Insert or update album
        const { data: savedAlbum, error: albumError } = await supabase
          .from('albums')
          .upsert(albumData, {
            onConflict: 'spotify_id',
            returning: 'minimal'
          })
          .select('id')
          .single();
        
        if (albumError) {
          // Log to worker_issues for consistent error tracking
          await this.logWorkerIssue(
            'database_error',
            `Error upserting album ${album.name}: ${albumError.message}`,
            { 
              album_name: album.name,
              spotify_id: album.id,
              artist_id: artist.id 
            }
          );
          results.errors++;
          continue;
        }
        
        const dbAlbumId = savedAlbum?.id;
        
        if (!dbAlbumId) {
          await this.logWorkerIssue(
            'data_error',
            `Failed to get album ID after upsert for ${album.name}`,
            {
              album_name: album.name,
              spotify_id: album.id
            }
          );
          results.errors++;
          continue;
        }
        
        console.log(`Successfully saved/updated album ${album.name} (${dbAlbumId})`);
        results.albumsProcessed++;
        
        // Queue track discovery for this album
        const trackEnqueued = await queueHelper.enqueue('track_discovery', {
          albumId: dbAlbumId,
          albumName: album.name,
          artistId: artist.id,
          offset: 0
        });
        
        if (trackEnqueued) {
          console.log(`Successfully queued track discovery for album ${dbAlbumId}`);
          results.tracksQueued++;
        } else {
          await this.logWorkerIssue(
            'queue_error',
            `Failed to queue track discovery for album ${dbAlbumId}`,
            {
              album_id: dbAlbumId,
              album_name: album.name
            }
          );
          results.errors++;
        }
      } catch (albumError) {
        console.error(`Error processing album ${album.name}:`, albumError);
        await this.logWorkerIssue(
          'processing_error',
          `Error processing album: ${albumError.message}`,
          {
            album_name: album.name || 'unknown'
          }
        );
        results.errors++;
      }
    }
    
    // Check if there are more albums to fetch (pagination)
    if (albumsData.next) {
      // Queue the next page
      const nextOffset = offset + albumsData.items.length;
      const nextPageKey = `artist:${artistId}:albums:offset:${nextOffset}`;
      
      const nextPageEnqueued = await queueHelper.enqueue('album_discovery', {
        artistId: artistId,
        offset: nextOffset
      }, nextPageKey);
      
      if (nextPageEnqueued) {
        console.log(`Queued next album page for artist ${artist.name} at offset ${nextOffset}`);
        results.nextPagesQueued++;
      } else {
        await this.logWorkerIssue(
          'queue_error',
          `Failed to queue next album page for artist ${artist.name}`,
          {
            artist_id: artistId,
            next_offset: nextOffset
          }
        );
        results.errors++;
      }
    }
    
    // Mark this page as processed
    await deduplicationService.markAsProcessed(
      'album_discovery', 
      dedupKey,
      86400, // 24 hour TTL
      { entityId: artistId }
    );
    
    return { 
      status: 'completed',
      ...results
    };
  }
  
  // Helper to standardize worker issue logging
  private async logWorkerIssue(
    issueType: string, 
    message: string, 
    details?: Record<string, any>
  ): Promise<void> {
    try {
      await this.supabase.from('worker_issues').insert({
        worker_name: 'albumDiscovery',
        issue_type: issueType,
        message: message,
        details: details || {},
        created_at: new Date().toISOString()
      });
    } catch (error) {
      console.error(`Failed to log worker issue: ${error.message}`);
    }
  }
  
  // Helper method to send to DLQ after max retries
  private async sendToDLQ(
    messageId: string, 
    message: any, 
    failureReason: string
  ): Promise<boolean> {
    try {
      console.log(`Moving message ${messageId} to DLQ due to: ${failureReason}`);
      
      // Try to use the DB function to move to DLQ
      const { data, error } = await this.supabase.rpc('move_to_dead_letter_queue', {
        source_queue: 'album_discovery',
        dlq_name: DLQ_CONFIG.queueName,
        message_id: messageId,
        failure_reason: failureReason,
        metadata: {
          worker_name: 'albumDiscovery',
          failed_at: new Date().toISOString(),
          retries: DLQ_CONFIG.maxRetries
        }
      });
      
      if (error) {
        console.error(`Error moving message to DLQ: ${error.message}`);
        
        // Fallback: direct send to DLQ if function fails
        await this.supabase.functions.invoke("sendToQueue", {
          body: {
            queue_name: DLQ_CONFIG.queueName,
            message: {
              ...message,
              _dlq_metadata: {
                source_queue: 'album_discovery',
                original_message_id: messageId,
                moved_at: new Date().toISOString(),
                failure_reason: failureReason,
                move_method: 'direct_fallback'
              }
            }
          }
        });
        
        return true;
      }
      
      return data === true;
    } catch (error) {
      console.error(`Failed to send message to DLQ: ${error.message}`);
      return false;
    }
  }
}

// Process albums with self-draining capabilities
async function processAlbumDiscovery() {
  console.log("Starting album discovery batch processing");
  
  try {
    const worker = new AlbumDiscoveryWorker();
    
    // Process multiple batches within the time limit with DLQ support
    const result = await worker.drainQueue({
      maxBatches: 8,          // Process up to 8 batches in one invocation
      maxRuntimeMs: 240000,   // 4 minute runtime limit (below Edge Function timeout)
      batchSize: 5,           // 5 messages per batch
      processorName: 'album-discovery',
      timeoutSeconds: 60,     // Timeout per message
      visibilityTimeoutSeconds: 900, // Increased to 15 minutes per fix #8
      logDetailedMetrics: true,
      deadLetterQueue: DLQ_CONFIG.queueName,  // Enable DLQ for poison messages
      maxRetries: DLQ_CONFIG.maxRetries       // Number of retries before sending to DLQ
    });
    
    return {
      processed: result.processed,
      errors: result.errors,
      duplicates: result.duplicates,
      skipped: result.skipped,
      processingTimeMs: result.processingTimeMs,
      success: result.errors === 0
    };
  } catch (batchError) {
    console.error("Fatal error in album discovery batch:", batchError);
    return { 
      error: batchError.message,
      success: false
    };
  }
}

// Handle HTTP requests
serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }
  
  try {
    // Process the batch in the background using EdgeRuntime.waitUntil
    const resultPromise = processAlbumDiscovery();
    
    if (typeof EdgeRuntime !== 'undefined' && EdgeRuntime.waitUntil) {
      EdgeRuntime.waitUntil(resultPromise);
      
      // Return immediately with acknowledgment
      return new Response(
        JSON.stringify({ message: "Album discovery batch processing started" }),
        { 
          headers: { 
            ...corsHeaders, 
            'Content-Type': 'application/json' 
          } 
        }
      );
    } else {
      // If EdgeRuntime.waitUntil is not available, wait for completion
      const result = await resultPromise;
      
      return new Response(
        JSON.stringify(result),
        { 
          headers: { 
            ...corsHeaders, 
            'Content-Type': 'application/json' 
          } 
        }
      );
    }
  } catch (error) {
    console.error("Error in album discovery handler:", error);
    
    return new Response(
      JSON.stringify({ error: error.message, success: false }),
      { 
        status: 500,
        headers: { 
          ...corsHeaders, 
          'Content-Type': 'application/json' 
        } 
      }
    );
  }
});
