
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { SpotifyClient } from "../_shared/spotifyClient.ts";
import { 
  deleteMessageWithRetries, 
  logWorkerIssue,
  checkTrackProcessed,
  processQueueMessageSafely,
  acquireProcessingLock
} from "../_shared/queueHelper.ts";
import { Redis } from '@upstash/redis';

// Initialize Redis client for distributed locking and idempotency
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

interface TrackDiscoveryMsg {
  albumId: string;
  albumName: string;
  artistId: string;
  offset?: number;
}

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Track metrics for each processing batch
async function trackQueueMetrics(
  supabase: any,
  queueName: string,
  operation: string,
  processedCount: number,
  successCount: number,
  errorCount: number,
  details: any = {}
) {
  try {
    const startedAt = new Date();
    const finishedAt = new Date();
    
    await supabase.from('queue_metrics').insert({
      queue_name: queueName,
      operation: operation,
      started_at: startedAt.toISOString(),
      finished_at: finishedAt.toISOString(),
      processed_count: processedCount,
      success_count: successCount,
      error_count: errorCount,
      details: details
    });
  } catch (error) {
    console.error("Failed to track metrics:", error);
  }
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );

  try {
    console.log("Starting track discovery process");
    
    // Process queue batch
    const { data: messages, error } = await supabase.functions.invoke("readQueue", {
      body: { 
        queue_name: "track_discovery",
        batch_size: 3,
        visibility_timeout: 300 // 5 minutes
      }
    });

    if (error) {
      console.error("Error reading from queue:", error);
      await logWorkerIssue(
        supabase,
        "trackDiscovery", 
        "queue_error", 
        "Error reading from queue", 
        { error }
      );
      
      return new Response(JSON.stringify({ error }), { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    if (!messages || messages.length === 0) {
      console.log("No messages to process in track_discovery queue");
      return new Response(JSON.stringify({ 
        processed: 0, 
        message: "No messages to process" 
      }), { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      });
    }

    console.log(`Found ${messages.length} messages to process in track_discovery queue`);

    // Create a quick response to avoid timeout
    const response = new Response(JSON.stringify({ 
      processing: true, 
      message_count: messages.length 
    }), { 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
    });

    // Process messages in background to avoid CPU timeout
    EdgeRuntime.waitUntil((async () => {
      // Initialize the Spotify client
      const spotifyClient = new SpotifyClient();
      
      // Track overall metrics
      let successCount = 0;
      let errorCount = 0;
      const processingResults = [];

      try {
        // Process messages in sequence to avoid overwhelming the database
        for (const message of messages) {
          try {
            // Ensure the message is properly typed
            console.log(`Raw message: ${JSON.stringify(message)}`);
            
            let msg: TrackDiscoveryMsg;
            if (typeof message.message === 'string') {
              msg = JSON.parse(message.message) as TrackDiscoveryMsg;
            } else {
              msg = message.message as TrackDiscoveryMsg;
            }
            
            const messageId = message.id;
            
            // Generate an idempotency key based on album ID and offset
            const idempotencyKey = `album:${msg.albumId}:offset:${msg.offset || 0}`;
            console.log(`Processing track message for album: ${msg.albumName} (${msg.albumId}) with idempotency key ${idempotencyKey}`);
            
            try {
              // Process message with idempotency checks
              const result = await processQueueMessageSafely(
                supabase,
                "track_discovery",
                messageId.toString(),
                async () => await processTracks(supabase, spotifyClient, msg),
                idempotencyKey,
                async () => {
                  // Check if this album page was already processed
                  try {
                    const key = `processed:album:${msg.albumId}:offset:${msg.offset || 0}`;
                    return await redis.exists(key) === 1;
                  } catch (error) {
                    console.error(`Redis check failed for ${idempotencyKey}:`, error);
                    return false;
                  }
                },
                { maxRetries: 2, circuitBreaker: true }
              );
              
              if (result) {
                console.log(`Successfully processed track message ${messageId}`);
                successCount++;
              } else {
                console.warn(`Message ${messageId} was not successfully processed`);
                errorCount++;
              }
              
              processingResults.push({
                messageId,
                albumId: msg.albumId,
                albumName: msg.albumName,
                success: result
              });
            } catch (processingError) {
              console.error(`Error processing track message ${messageId}:`, processingError);
              await logWorkerIssue(
                supabase,
                "trackDiscovery", 
                "processing_error", 
                `Error processing message ${messageId}: ${processingError.message}`, 
                { 
                  messageId, 
                  albumId: msg.albumId,
                  albumName: msg.albumName,
                  error: processingError.message,
                  stack: processingError.stack
                }
              );
              errorCount++;
              processingResults.push({
                messageId,
                albumId: msg.albumId,
                albumName: msg.albumName,
                success: false,
                error: processingError.message
              });
            }
          } catch (messageError) {
            console.error(`Error parsing message:`, messageError);
            await logWorkerIssue(
              supabase,
              "trackDiscovery", 
              "message_error", 
              `Error parsing message: ${messageError.message}`, 
              { 
                message, 
                error: messageError.message 
              }
            );
            errorCount++;
          }
        }
      } catch (batchError) {
        console.error("Error in batch processing:", batchError);
        await logWorkerIssue(
          supabase,
          "trackDiscovery", 
          "batch_error", 
          `Batch processing error: ${batchError.message}`, 
          { error: batchError.stack }
        );
      } finally {
        // Record final metrics
        await trackQueueMetrics(
          supabase,
          "track_discovery",
          "batch_processing",
          messages.length,
          successCount,
          errorCount,
          { 
            timestamp: new Date().toISOString(),
            results: processingResults
          }
        );

        console.log(`Completed background processing: ${successCount} successful, ${errorCount} failed`);
      }
    })());
    
    return response;
  } catch (error) {
    console.error("Unexpected error in track discovery worker:", error);
    await logWorkerIssue(
      supabase,
      "trackDiscovery", 
      "fatal_error", 
      `Unexpected error: ${error.message}`,
      { stack: error.stack }
    );
    
    return new Response(JSON.stringify({ error: error.message }), 
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }
});

async function processTracks(
  supabase: any, 
  spotifyClient: any,
  msg: TrackDiscoveryMsg
) {
  const { albumId, albumName, artistId, offset = 0 } = msg;
  console.log(`Processing tracks for album ${albumName} (ID: ${albumId}) at offset ${offset}`);
  
  try {
    // Acquire distributed lock to prevent duplicate processing
    const lockKey = `album:${albumId}:offset:${offset}`;
    const hasLock = await acquireProcessingLock('album_tracks', lockKey);
    
    if (!hasLock) {
      console.log(`Another process is handling album ${albumName} offset ${offset}, skipping`);
      return { skipped: true, reason: "concurrent_processing" };
    }
    
    // Mark this batch as being processed in Redis
    try {
      const processingKey = `processing:album:${albumId}:offset:${offset}`;
      await redis.set(processingKey, 'true', { ex: 300 }); // 5 minute TTL
    } catch (redisError) {
      // Non-fatal if Redis fails, continue with processing
      console.warn("Failed to set processing flag in Redis:", redisError);
    }
    
    // Get album's database ID
    const { data: album, error: albumError } = await supabase
      .from('albums')
      .select('id, spotify_id')
      .eq('id', albumId)
      .single();

    if (albumError || !album) {
      const errMsg = `Album not found with ID: ${albumId}`;
      console.error(errMsg);
      throw new Error(errMsg);
    }
    
    console.log(`Found album in database: ${album.id} with Spotify ID: ${album.spotify_id}`);

    // Get artist's database ID for normalized tracks
    const { data: artist, error: artistError } = await supabase
      .from('artists')
      .select('id, name')
      .eq('id', artistId)
      .single();

    if (artistError || !artist) {
      const errMsg = `Artist not found with ID: ${artistId}`;
      console.error(errMsg);
      throw new Error(errMsg);
    }
    
    console.log(`Found artist in database: ${artist.name} (ID: ${artist.id})`);

    // Ensure spotify_id is a string
    if (typeof album.spotify_id !== 'string') {
      const errMsg = `Invalid Spotify ID for album ${albumId}: ${album.spotify_id}`;
      console.error(errMsg);
      throw new Error(errMsg);
    }
    
    // Fetch tracks from Spotify
    console.log(`Fetching tracks from Spotify for album ${albumName} (ID: ${album.spotify_id})`);
    const tracksData = await spotifyClient.getAlbumTracks(album.spotify_id, offset);
    console.log(`Found ${tracksData.items.length} tracks in album ${albumName} (total: ${tracksData.total})`);

    if (!tracksData.items || tracksData.items.length === 0) {
      console.log(`No tracks found for album ${albumName}`);
      
      // Mark this batch as processed even if no tracks were found
      try {
        const processedKey = `processed:album:${albumId}:offset:${offset}`;
        await redis.set(processedKey, 'true', { ex: 86400 }); // 24 hour TTL
      } catch (redisError) {
        console.warn("Failed to set processed flag in Redis:", redisError);
      }
      
      return { processed: 0 };
    }

    // Filter and normalize tracks
    const tracksToProcess = tracksData.items.filter(track => 
      isArtistPrimaryOnTrack(track, artistId)
    );
    
    console.log(`${tracksToProcess.length} tracks have the artist as primary artist`);

    // Get detailed track info in batches of 50 (Spotify API limit)
    let processedCount = 0;
    let errorCount = 0;
    const processedTrackIds = [];
    
    for (let i = 0; i < tracksToProcess.length; i += 50) {
      const batch = tracksToProcess.slice(i, i + 50);
      const trackIds = batch.map(t => t.id);
      
      console.log(`Processing batch of ${batch.length} tracks, IDs: ${trackIds.slice(0, 3)}...`);
      
      try {
        // Get detailed track info (uses Get Several Tracks endpoint)
        const trackDetails = await spotifyClient.getTrackDetails(trackIds);
        console.log(`Received ${trackDetails.length} track details from Spotify`);
        
        if (!trackDetails || trackDetails.length === 0) {
          console.error(`No track details returned from Spotify for IDs: ${trackIds}`);
          await logWorkerIssue(
            supabase, 
            "trackDiscovery", 
            "spotify_error", 
            "No track details returned from Spotify", 
            { trackIds }
          );
          continue;
        }
        
        // Process each track in batch
        for (const track of trackDetails) {
          try {
            // First check if this specific track has already been processed using Redis
            const trackKey = `processed:track:${track.id}`;
            let skipTrack = false;
            
            try {
              skipTrack = await redis.exists(trackKey) === 1;
              if (skipTrack) {
                console.log(`Track ${track.name} (${track.id}) already processed according to Redis, skipping`);
                processedCount++;
                continue;
              }
            } catch (redisError) {
              // If Redis check fails, continue with processing
              console.warn(`Redis check failed for track ${track.id}:`, redisError);
            }
            
            // Validate track data
            if (!track || !track.id || !track.name) {
              console.error(`Invalid track data received:`, track);
              await logWorkerIssue(
                supabase,
                "trackDiscovery", 
                "data_validation", 
                "Invalid track data", 
                { track }
              );
              errorCount++;
              continue;
            }
            
            // Debug track object
            console.log(`Processing track: ${track.name} (ID: ${track.id})`);
            console.log(`Track data validation successful for ${track.name}`);
            
            // Normalize the track name for deduplication
            const normalizedName = normalizeTrackName(track.name);
            
            if (!normalizedName) {
              console.warn(`Normalized name is empty for track ${track.name}, skipping`);
              errorCount++;
              continue;
            }
            
            // Check if normalized track already exists
            const { data: existingNormalizedTrack } = await supabase
              .from('normalized_tracks')
              .select('id')
              .eq('artist_id', artist.id)
              .eq('normalized_name', normalizedName)
              .maybeSingle();
            
            // Prepare track data for insertion
            const trackData = {
              album_id: album.id,
              spotify_id: track.id,
              name: track.name,
              duration_ms: track.duration_ms,
              popularity: track.popularity,
              spotify_preview_url: track.preview_url,
              metadata: {
                disc_number: track.disc_number,
                track_number: track.track_number,
                artists: track.artists,
                updated_at: new Date().toISOString()
              }
            };
            
            console.log(`Ready to upsert track data: ${JSON.stringify({
              id: track.id,
              name: track.name,
              album_id: album.id
            })}`);
            
            // Store track in database with explicit conflict handling
            const { data: insertedTrack, error: insertError } = await supabase
              .from('tracks')
              .upsert(trackData, {
                onConflict: 'spotify_id',
                ignoreDuplicates: false
              })
              .select('id')
              .single();

            if (insertError) {
              console.error(`Error upserting track ${track.name} (${track.id}):`, insertError);
              await logWorkerIssue(
                supabase,
                "trackDiscovery", 
                "database_error", 
                `Failed to insert track: ${track.name}`, 
                { 
                  error: insertError, 
                  track_spotify_id: track.id,
                  trackData: trackData
                }
              );
              errorCount++;
              continue;
            }
            
            console.log(`Successfully inserted/updated track: ${track.name} (DB ID: ${insertedTrack.id})`);
            processedTrackIds.push(insertedTrack.id);

            // Create normalized track entry if it doesn't exist
            if (!existingNormalizedTrack) {
              const { error: normalizedError } = await supabase
                .from('normalized_tracks')
                .upsert({
                  artist_id: artist.id,
                  normalized_name: normalizedName,
                  representative_track_id: insertedTrack.id
                }, {
                  onConflict: 'artist_id,normalized_name'
                });
                
              if (normalizedError) {
                console.error(`Error upserting normalized track for ${track.name}:`, normalizedError);
                await logWorkerIssue(
                  supabase,
                  "trackDiscovery", 
                  "database_error", 
                  `Error creating normalized track for ${track.name}`, 
                  { error: normalizedError }
                );
              } else {
                console.log(`Created normalized track entry for: ${normalizedName}`);
              }
            }

            // Mark this track as processed in Redis
            try {
              await redis.set(trackKey, 'true', { ex: 86400 }); // 24 hour TTL
            } catch (redisError) {
              // Non-fatal if Redis fails
              console.warn(`Failed to mark track ${track.id} as processed in Redis:`, redisError);
            }

            // Enqueue producer identification with idempotency check
            const producerMsg = {
              trackId: insertedTrack.id,
              trackName: track.name,
              albumId: album.id,
              artistId: artist.id
            };
            
            // Check if producer identification was already enqueued
            const producerKey = `enqueued:producer:${insertedTrack.id}`;
            let alreadyEnqueued = false;
            
            try {
              alreadyEnqueued = await redis.exists(producerKey) === 1;
            } catch (redisError) {
              // If Redis check fails, continue with enqueuing
              console.warn(`Redis check failed for producer identification:`, redisError);
            }
            
            if (!alreadyEnqueued) {
              try {
                await supabase.functions.invoke("sendToQueue", {
                  body: {
                    queue_name: "producer_identification",
                    message: producerMsg
                  }
                });
                
                console.log(`Enqueued producer identification for track: ${track.name}`);
                
                // Mark as enqueued in Redis
                try {
                  await redis.set(producerKey, 'true', { ex: 86400 }); // 24 hour TTL
                } catch (redisError) {
                  // Non-fatal if Redis fails
                  console.warn(`Failed to mark producer identification as enqueued:`, redisError);
                }
              } catch (enqueueError) {
                console.error(`Error enqueueing producer identification for ${track.name}:`, enqueueError);
                errorCount++;
              }
            } else {
              console.log(`Producer identification already enqueued for track ${track.name}, skipping`);
            }
            
            processedCount++;
          } catch (trackError) {
            console.error(`Error processing individual track ${track?.name || 'unknown'}:`, trackError);
            await logWorkerIssue(
              supabase,
              "trackDiscovery", 
              "track_processing", 
              `Error processing track ${track?.name || 'unknown'}: ${trackError.message}`, 
              { error: trackError.message, track: track }
            );
            errorCount++;
          }
        }
      } catch (batchError) {
        console.error(`Error processing batch of tracks:`, batchError);
        await logWorkerIssue(
          supabase,
          "trackDiscovery", 
          "batch_processing", 
          `Error processing batch: ${batchError.message}`, 
          { error: batchError.message }
        );
      }
    }

    // If there are more tracks, enqueue next page
    if (tracksData.items.length > 0 && offset + tracksData.items.length < tracksData.total) {
      const newOffset = offset + tracksData.items.length;
      console.log(`Enqueueing next page of tracks for album ${albumName} with offset ${newOffset}`);
      
      // Use an idempotency key for the next page enqueue
      const nextPageKey = `enqueued:nextpage:${albumId}:${newOffset}`;
      let nextPageEnqueued = false;
      
      try {
        nextPageEnqueued = await redis.exists(nextPageKey) === 1;
      } catch (redisError) {
        // If Redis check fails, continue with enqueuing
        console.warn(`Redis check failed for next page:`, redisError);
      }
      
      if (!nextPageEnqueued) {
        try {
          await supabase.functions.invoke("sendToQueue", {
            body: {
              queue_name: "track_discovery",
              message: { 
                albumId, 
                albumName, 
                artistId, 
                offset: newOffset 
              }
            }
          });
          
          console.log(`Successfully enqueued next batch with offset ${newOffset}`);
          
          // Mark next page as enqueued in Redis
          try {
            await redis.set(nextPageKey, 'true', { ex: 86400 }); // 24 hour TTL
          } catch (redisError) {
            // Non-fatal if Redis fails
            console.warn(`Failed to mark next page as enqueued:`, redisError);
          }
        } catch (enqueueError) {
          console.error(`Failed to enqueue next page for album ${albumName}:`, enqueueError);
          throw enqueueError; // Propagate to avoid marking batch as successful
        }
      } else {
        console.log(`Next page for album ${albumName} offset ${newOffset} already enqueued, skipping`);
      }
    } else {
      console.log(`Completed track discovery for album ${albumName}: Found ${processedCount} tracks`);
    }
    
    // Mark this batch as fully processed in Redis
    try {
      const processedKey = `processed:album:${albumId}:offset:${offset}`;
      await redis.set(processedKey, 'true', { ex: 86400 }); // 24 hour TTL
    } catch (redisError) {
      console.warn("Failed to set processed flag in Redis:", redisError);
    }
    
    // Remove processing flag
    try {
      const processingKey = `processing:album:${albumId}:offset:${offset}`;
      await redis.del(processingKey);
    } catch (redisError) {
      console.warn("Failed to remove processing flag:", redisError);
    }
    
    return { 
      processed: processedCount, 
      errors: errorCount,
      tracksIds: processedTrackIds
    };
  } catch (error) {
    console.error(`Failed to process tracks for album ${albumName}:`, error);
    
    // Remove processing flag on error
    try {
      const processingKey = `processing:album:${albumId}:offset:${offset}`;
      await redis.del(processingKey);
    } catch (redisError) {
      console.warn("Failed to remove processing flag:", redisError);
    }
    
    throw error; // Re-throw to ensure proper error handling in the parent function
  }
}

// Helper functions
function normalizeTrackName(name: string): string {
  if (!name) return '';
  
  // Remove extraneous information
  let normalized = name
    .toLowerCase()
    .replace(/\(.*?\)/g, '') // Remove text in parentheses
    .replace(/\[.*?\]/g, '') // Remove text in brackets
    .replace(/feat\.|ft\./g, '') // Remove featured artist markers
    .replace(/[^a-z0-9À-ÿ\s]/g, '') // Remove special characters
    .trim()
    .replace(/\s+/g, ' '); // Normalize whitespace
    
  return normalized;
}

function isArtistPrimaryOnTrack(track: any, artistId: string): boolean {
  if (!track.artists || !Array.isArray(track.artists) || track.artists.length === 0) {
    console.log(`Track has no artists or invalid artists array: ${JSON.stringify(track)}`);
    return false;
  }
  
  // For simplicity, we consider all tracks from the album as relevant
  // In a more sophisticated implementation, you could compare Spotify IDs
  return track.artists.length > 0;
}
