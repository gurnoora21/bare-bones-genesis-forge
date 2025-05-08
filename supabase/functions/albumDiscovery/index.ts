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
          // Ensure the message is properly typed
          let msg: AlbumDiscoveryMsg;
          if (typeof message.message === 'string') {
            msg = JSON.parse(message.message) as AlbumDiscoveryMsg;
          } else {
            msg = message.message as AlbumDiscoveryMsg;
          }
          
          const messageId = message.id || message.msg_id;
          console.log(`Processing album message for artist ID: ${msg.artistId}, offset: ${msg.offset}`);
          
          // Create idempotency key based on artist ID and offset
          const idempotencyKey = `artist:${msg.artistId}:albums:offset:${msg.offset}`;
          
          try {
            // Process message with deduplication
            const result = await processQueueMessageSafely(
              supabase,
              "album_discovery",
              messageId.toString(),
              async () => await processAlbums(supabase, spotifyClient, msg),
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
                } catch (redisError) {
                  console.warn(`Failed to mark batch as processed in Redis:`, redisError);
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
  msg: AlbumDiscoveryMsg
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

  // Fetch albums from Spotify
  const albumsData = await spotifyClient.getArtistAlbums(artist.spotify_id, offset);
  console.log(`Retrieved ${albumsData.items.length} albums for artist ${artistId}, offset ${offset}`);

  if (albumsData.items.length === 0) {
    return; // No albums to process
  }

  // Extract album IDs for batch retrieval (focusing on albums and singles)
  const albumIds = albumsData.items
    .filter(album => album.album_type === 'album' || album.album_type === 'single')
    .filter(album => isArtistPrimary(album, artist.spotify_id))
    .map(album => album.id);

  if (albumIds.length === 0) {
    return; // No relevant albums found
  }

  // Get detailed album information in batches
  const fullAlbums = await spotifyClient.getAlbumDetails(albumIds);

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
      
      console.log(`Processed album ${album.name}, enqueued track discovery`);
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

  // If there are more albums, enqueue the next batch with proper termination condition
  if (albumsData.next && albumsData.total > offset + albumsData.items.length) {
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
  } else {
    console.log(`Completed album discovery for artist ${artistId}: Found ${offset + albumsData.items.length} albums`);
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
