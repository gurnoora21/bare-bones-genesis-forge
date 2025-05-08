
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { SpotifyClient } from "../_shared/spotifyClient.ts";
import { deleteMessageWithRetries, logWorkerIssue, processQueueMessageSafely } from "../_shared/queueHelper.ts";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { DeduplicationService } from "../_shared/deduplication.ts";
import { getDeduplicationMetrics } from "../_shared/metrics.ts";

interface AlbumDiscoveryMsg {
  artistId: string;
  offset: number;
}

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );

  // Initialize Redis client
  const redis = new Redis({
    url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
    token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
  });
  
  // Initialize metrics
  const metrics = getDeduplicationMetrics(redis);

  try {
    // Process queue batch
    const { data: queueData, error: queueError } = await supabase.rpc('pg_dequeue', { 
      queue_name: "album_discovery",
      batch_size: 3, // Process fewer albums at once
      visibility_timeout: 300 // 5 minutes
    });

    if (queueError) {
      console.error("Error reading from queue:", queueError);
      await logWorkerIssue(
        supabase,
        "albumDiscovery", 
        "queue_error", 
        "Error reading from queue", 
        { error: queueError }
      );
      return new Response(
        JSON.stringify({ error: queueError }), 
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Parse the messages
    let messages = [];
    try {
      console.log("Raw queue data received:", typeof queueData, queueData ? JSON.stringify(queueData).substring(0, 300) + "..." : "null");
      
      // Handle either string or object formats
      if (typeof queueData === 'string') {
        messages = JSON.parse(queueData);
      } else if (queueData) {
        messages = queueData;
      }
    } catch (e) {
      console.error("Error parsing queue data:", e);
      await logWorkerIssue(
        supabase,
        "albumDiscovery", 
        "queue_parsing", 
        `Error parsing queue data: ${e.message}`, 
        { queueData, error: e.message }
      );
      messages = [];
    }
    
    console.log(`Retrieved ${messages.length} messages from queue`);

    if (!messages || messages.length === 0) {
      return new Response(
        JSON.stringify({ processed: 0, message: "No messages to process" }), 
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Create a quick response to avoid timeout
    const response = new Response(
      JSON.stringify({ processing: true, message_count: messages.length }), 
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );

    // Initialize the Spotify client
    const spotifyClient = new SpotifyClient();

    // Process messages with background tasks
    EdgeRuntime.waitUntil((async () => {
      let successCount = 0;
      let errorCount = 0;
      let dedupedCount = 0;
      
      for (const message of messages) {
        try {
          // Debug log the raw message structure
          console.log(`Raw album message:`, JSON.stringify(message));
          
          // Ensure the message is properly typed
          let msg: AlbumDiscoveryMsg;
          if (typeof message.message === 'string') {
            try {
              msg = JSON.parse(message.message) as AlbumDiscoveryMsg;
            } catch (parseError) {
              console.error(`Failed to parse message as JSON:`, parseError);
              await logWorkerIssue(
                supabase,
                "albumDiscovery", 
                "parse_error", 
                `Failed to parse message as JSON: ${parseError.message}`, 
                { message }
              );
              errorCount++;
              continue;
            }
          } else {
            msg = message.message as AlbumDiscoveryMsg;
          }
          
          // Enhanced message ID extraction with fallbacks
          let messageId: string;
            
          if (message.id !== undefined && message.id !== null) {
            messageId = String(message.id);
          } else if (message.msg_id !== undefined && message.msg_id !== null) {
            messageId = String(message.msg_id);
          } else {
            // If no ID is available, generate one deterministically from the message content
            console.warn(`No message ID found, generating a deterministic one from artist ID and offset`);
            messageId = `generated-${msg.artistId}-${msg.offset}-${Date.now()}`;
              
            await logWorkerIssue(
              supabase,
              "albumDiscovery", 
              "missing_message_id", 
              "Message had no ID, using generated ID", 
              { 
                generatedId: messageId,
                originalMessage: message 
              }
            );
          }
          
          console.log(`Processing album message for artist ID: ${msg.artistId}, offset: ${msg.offset} with message ID: ${messageId}`);
          
          // Create idempotency key based on artist ID and offset
          const idempotencyKey = `artist:${msg.artistId}:albums:offset:${msg.offset}`;
          
          try {
            // Process message with deduplication
            const result = await processQueueMessageSafely(
              supabase,
              "album_discovery",
              messageId,
              async () => await processAlbums(supabase, spotifyClient, msg, redis),
              idempotencyKey,
              async () => {
                // Check if this album page was already processed
                try {
                  const key = `processed:artist:${msg.artistId}:albums:offset:${msg.offset}`;
                  return await redis.exists(key) === 1;
                } catch (error) {
                  console.error(`Redis check failed for ${idempotencyKey}:`, error);
                  return false;
                }
              },
              {
                maxRetries: 2,
                deduplication: {
                  enabled: true,
                  redis,
                  ttlSeconds: 3600, // 1 hour deduplication window
                  strictMatching: false
                }
              }
            );
            
            if (result) {
              if (typeof result === 'object' && result.deduplication) {
                dedupedCount++;
                await metrics.recordDeduplicated("album_discovery", "consumer");
                console.log(`Message ${messageId} was handled by deduplication`);
              } else {
                successCount++;
                console.log(`Successfully processed album discovery for artist ${msg.artistId}, offset ${msg.offset}`);
                
                // Mark this batch as processed in Redis
                try {
                  const processedKey = `processed:artist:${msg.artistId}:albums:offset:${msg.offset}`;
                  await redis.set(processedKey, 'true', { ex: 86400 }); // 24 hour TTL
                  
                  // Also mark terminal batches specially to avoid re-fetching later
                  if (typeof result === 'object' && result.terminal === true) {
                    const terminalKey = `terminal:artist:${msg.artistId}:albums`;
                    await redis.set(terminalKey, 'true', { ex: 86400 * 7 }); // 7 day TTL
                    console.log(`Marked artist ${msg.artistId} as having all albums fetched`);
                  }
                } catch (redisError) {
                  console.warn(`Failed to mark batch as processed in Redis:`, redisError);
                }
              }
              
              // Double check message deletion in case processQueueMessageSafely failed to delete
              // This is an emergency fallback, the regular process should handle this
              if (!result.deduplication) {
                try {
                  // Check if the message still exists and delete it if it does
                  console.log(`Double-checking deletion of message ${messageId}`);
                  const deleteSuccess = await deleteMessageWithRetries(
                    supabase, 
                    "album_discovery", 
                    messageId, 
                    2
                  );
                  
                  if (deleteSuccess) {
                    console.log(`Successfully deleted message ${messageId} via fallback method`);
                  }
                } catch (deleteError) {
                  console.warn(`Error in fallback message deletion:`, deleteError);
                }
              }
            } else {
              console.error(`Failed to process message ${messageId}`);
              errorCount++;
            }
          } catch (processError) {
            console.error(`Error processing album message ${messageId}:`, processError);
            await logWorkerIssue(
              supabase,
              "albumDiscovery", 
              "processing_error", 
              `Error processing message ${messageId}: ${processError.message}`,
              { messageId, msg, error: processError.message }
            );
            errorCount++;
          }
        } catch (messageError) {
          console.error(`Error parsing message:`, messageError);
          errorCount++;
        }
      }
      
      console.log(`Completed background processing: ${successCount} successful, ${dedupedCount} deduplicated, ${errorCount} failed`);
      
      // Record metrics
      try {
        await supabase.from('queue_metrics').insert({
          queue_name: "album_discovery",
          operation: "batch_processing",
          started_at: new Date().toISOString(),
          finished_at: new Date().toISOString(),
          processed_count: messages.length,
          success_count: successCount,
          error_count: errorCount,
          details: { 
            deduplicated_count: dedupedCount
          }
        });
      } catch (metricsError) {
        console.error("Failed to record metrics:", metricsError);
      }
    })());
    
    return response;
  } catch (error) {
    console.error("Error in album discovery worker:", error);
    await logWorkerIssue(
      supabase,
      "albumDiscovery", 
      "fatal_error", 
      `Fatal error: ${error.message}`,
      { error: error.message, stack: error.stack }
    );
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});

async function processAlbums(
  supabase: any, 
  spotifyClient: any,
  msg: AlbumDiscoveryMsg,
  redis: Redis
) {
  const { artistId, offset } = msg;
  
  // Get artist details from database
  const { data: artist, error: artistError } = await supabase
    .from('artists')
    .select('id, spotify_id')
    .eq('id', artistId)
    .single();

  if (artistError || !artist) {
    throw new Error(`Artist not found with ID: ${artistId}`);
  }

  // Check if we've already determined this artist's album discovery is complete
  try {
    const terminalKey = `terminal:artist:${artistId}:albums`;
    const isTerminal = await redis.exists(terminalKey) === 1;
    
    if (isTerminal) {
      console.log(`Artist ${artistId} already has all albums fetched, skipping`);
      return { terminal: true, skipped: true };
    }
  } catch (redisError) {
    // Non-fatal if Redis check fails
    console.warn(`Failed to check terminal status:`, redisError);
  }

  // Fetch albums from Spotify
  const albumsData = await spotifyClient.getArtistAlbums(artist.spotify_id, offset);
  console.log(`Retrieved ${albumsData.items.length} albums for artist ${artistId}, offset ${offset} (total: ${albumsData.total})`);

  if (albumsData.items.length === 0) {
    // If we get empty results, mark this as a terminal batch
    console.log(`No albums found for artist ${artistId} at offset ${offset}, marking as terminal`);
    return { terminal: true, albums: 0 };
  }

  // Extract album IDs for batch retrieval (focusing on albums and singles)
  const albumIds = albumsData.items
    .filter(album => album.album_type === 'album' || album.album_type === 'single')
    .filter(album => isArtistPrimary(album, artist.spotify_id))
    .map(album => album.id);

  console.log(`Filtered ${albumIds.length} relevant albums where artist is primary out of ${albumsData.items.length} total`);
  
  if (albumIds.length === 0) {
    // If no relevant albums in this batch, check if we've reached the end
    const isLastBatch = !albumsData.next || albumsData.total <= offset + albumsData.items.length;
    
    if (isLastBatch) {
      console.log(`No more relevant albums for artist ${artistId}, marking as terminal`);
      return { terminal: true, albums: 0 };
    } else {
      // Enqueue next batch since this one had no relevant albums
      await supabase.functions.invoke("sendToQueue", {
        body: {
          queue_name: "album_discovery",
          message: { 
            artistId, 
            offset: offset + albumsData.items.length 
          }
        }
      });
      
      console.log(`No relevant albums in this batch, enqueued next batch at offset ${offset + albumsData.items.length}`);
      return { nextBatchEnqueued: true, albums: 0 };
    }
  }

  // Get detailed album information in batches
  const fullAlbums = await spotifyClient.getAlbumDetails(albumIds);
  let processedCount = 0;
  
  // Process each album
  for (const fullAlbumData of fullAlbums) {
    try {
      // Store album in database with better error handling
      const { data: album, error } = await supabase
        .from('albums')
        .upsert({
          artist_id: artistId,
          spotify_id: fullAlbumData.id,
          name: fullAlbumData.name,
          release_date: formatReleaseDate(fullAlbumData.release_date),
          cover_url: fullAlbumData.images[0]?.url,
          metadata: fullAlbumData
        }, {
          onConflict: 'spotify_id',
          ignoreDuplicates: false
        })
        .select()
        .single();

      if (error) {
        console.error(`Error storing album ${fullAlbumData.name}:`, error);
        await logWorkerIssue(
          supabase,
          "albumDiscovery", 
          "database_error", 
          `Error storing album ${fullAlbumData.name}`, 
          { 
            error, 
            albumData: {
              id: fullAlbumData.id,
              name: fullAlbumData.name
            }
          }
        );
        continue; // Continue with other albums
      }

      // Create a cache key to check if we've already enqueued track discovery
      const trackDiscoveryKey = `enqueued:track_discovery:album:${album.id}`;
      let alreadyEnqueued = false;
      
      try {
        alreadyEnqueued = await redis.exists(trackDiscoveryKey) === 1;
      } catch (redisError) {
        // Non-fatal if Redis check fails
        console.warn(`Failed to check track discovery enqueued status:`, redisError);
      }
      
      if (!alreadyEnqueued) {
        // Enqueue track discovery
        await supabase.functions.invoke("sendToQueue", {
          body: {
            queue_name: "track_discovery",
            message: { 
              albumId: album.id,
              albumName: album.name,
              artistId
            }
          }
        });
        
        // Mark as enqueued in Redis
        try {
          await redis.set(trackDiscoveryKey, 'true', { ex: 86400 }); // 24 hour TTL
        } catch (redisError) {
          // Non-fatal if Redis set fails
          console.warn(`Failed to mark track discovery as enqueued:`, redisError);
        }
      } else {
        console.log(`Track discovery already enqueued for album ${album.name}, skipping`);
      }
      
      processedCount++;
      console.log(`Processed album ${album.name}, ${alreadyEnqueued ? "already enqueued" : "enqueued"} track discovery`);
    } catch (albumError) {
      console.error(`Error processing album ${fullAlbumData.name}:`, albumError);
      await logWorkerIssue(
        supabase,
        "albumDiscovery", 
        "album_processing", 
        `Error processing album ${fullAlbumData.name}`, 
        { 
          error: albumError.message, 
          albumId: fullAlbumData.id
        }
      );
    }
  }

  // Determine if we've reached the end of albums
  const isLastBatch = !albumsData.next || albumsData.total <= offset + albumsData.items.length;
  
  // If there are more albums, enqueue the next batch
  if (!isLastBatch) {
    // Create a unique key to check if we've already enqueued the next page
    const nextPageKey = `enqueued:album_discovery:artist:${artistId}:offset:${offset + albumsData.items.length}`;
    let nextPageEnqueued = false;
    
    try {
      nextPageEnqueued = await redis.exists(nextPageKey) === 1;
    } catch (redisError) {
      // Non-fatal if Redis check fails
      console.warn(`Failed to check next page enqueued status:`, redisError);
    }
    
    if (!nextPageEnqueued) {
      try {
        await supabase.functions.invoke("sendToQueue", {
          body: {
            queue_name: "album_discovery",
            message: { 
              artistId, 
              offset: offset + albumsData.items.length 
            }
          }
        });
        
        console.log(`Enqueued next page of albums for artist ${artistId}: ${offset + albumsData.items.length}/${albumsData.total}`);
        
        // Mark next page as enqueued
        try {
          await redis.set(nextPageKey, 'true', { ex: 3600 }); // 1 hour TTL
        } catch (redisError) {
          // Non-fatal if Redis set fails
          console.warn(`Failed to mark next page as enqueued:`, redisError);
        }
      } catch (enqueueError) {
        console.error(`Error enqueueing next page:`, enqueueError);
        throw enqueueError; // Propagate error to ensure proper handling
      }
    } else {
      console.log(`Next page already enqueued for artist ${artistId} at offset ${offset + albumsData.items.length}`);
    }
    
    return { 
      processedAlbums: processedCount, 
      nextBatchEnqueued: true,
      terminal: false
    };
  } else {
    console.log(`Completed album discovery for artist ${artistId}: Found ${offset + albumsData.items.length} total albums with ${processedCount} in this batch`);
    
    // Mark this as the terminal batch for this artist
    return { 
      processedAlbums: processedCount, 
      nextBatchEnqueued: false,
      terminal: true
    };
  }
}

// Helper functions
function isArtistPrimary(album: any, artistId: string): boolean {
  return album.artists && 
         album.artists.length > 0 && 
         album.artists[0].id === artistId;
}

function formatReleaseDate(releaseDate: string): string | null {
  if (!releaseDate) return null;
  
  if (/^\d{4}$/.test(releaseDate)) {
    return `${releaseDate}-01-01`;
  } else if (/^\d{4}-\d{2}$/.test(releaseDate)) {
    return `${releaseDate}-01`;
  }
  
  return releaseDate;
}
