
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { SpotifyClient } from "../_shared/spotifyClient.ts";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { processQueueMessageSafely, logWorkerIssue, deleteMessageWithRetries } from "../_shared/queueHelper.ts";
import { DeduplicationService } from "../_shared/deduplication.ts";
import { getDeduplicationMetrics } from "../_shared/metrics.ts";
import { StructuredLogger } from "../_shared/structuredLogger.ts";

interface AlbumDiscoveryMsg {
  artistId: string;
  offset: number;
}

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Initialize logger
const logger = new StructuredLogger({
  service: "albumDiscovery",
  processId: crypto.randomUUID()
});

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

  logger.info("Starting album discovery process");

  try {
    // Process queue batch
    logger.info("Attempting to dequeue from album_discovery queue");
    const { data: queueData, error: queueError } = await supabase.rpc('pg_dequeue', { 
      queue_name: "album_discovery",
      batch_size: 5,
      visibility_timeout: 300
    });

    if (queueError) {
      logger.error("Error reading from queue", queueError);
      await logWorkerIssue(
        supabase,
        "albumDiscovery", 
        "queue_error", 
        `Error reading from queue: ${queueError.message}`, 
        { error: queueError }
      );
      
      return new Response(JSON.stringify({ error: queueError.message }), { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      });
    }

    // Parse the JSONB result from pg_dequeue
    let messages = [];
    
    logger.info("Raw album queue data received", { 
      dataType: typeof queueData,
      dataLength: queueData ? JSON.stringify(queueData).length : 0,
      dataSample: queueData ? JSON.stringify(queueData).substring(0, 200) : "null"
    });
    
    try {
      if (typeof queueData === 'string') {
        messages = JSON.parse(queueData);
        logger.info("Parsed queue data from string", { messageCount: messages.length });
      } else if (queueData) {
        messages = queueData;
        logger.info("Used queue data directly", { messageCount: messages.length });
      } else {
        logger.info("No messages in queue");
        messages = [];
      }
    } catch (e) {
      logger.error("Error parsing queue data", e);
      await logWorkerIssue(
        supabase,
        "albumDiscovery", 
        "queue_parsing", 
        `Error parsing queue data: ${e.message}`, 
        { queueData, error: e.message }
      );
    }

    logger.info(`Retrieved ${messages.length} messages from queue`);

    if (!messages || messages.length === 0) {
      return new Response(
        JSON.stringify({ message: "No messages to process in album_discovery queue" }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Initialize Spotify client
    let spotifyClient;
    try {
      spotifyClient = new SpotifyClient();
      logger.info("Spotify client initialized successfully");
    } catch (error) {
      logger.error("Failed to initialize Spotify client", error);
      await logWorkerIssue(
        supabase,
        "albumDiscovery", 
        "spotify_init_error", 
        `Failed to initialize Spotify client: ${error.message}`, 
        { error: error.message }
      );
      
      return new Response(JSON.stringify({ error: "Failed to initialize Spotify client" }), { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      });
    }

    // Create a quick response to avoid timeout
    const response = new Response(
      JSON.stringify({ 
        processing: true, 
        message_count: messages.length,
        message: "Processing album discovery tasks in the background" 
      }), 
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );

    // Process messages in background to avoid CPU timeout
    EdgeRuntime.waitUntil((async () => {
      let successCount = 0;
      let errorCount = 0;
      let dedupedCount = 0;
      
      try {
        for (const message of messages) {
          // Log the raw album message for debugging
          logger.info("Raw album message", { 
            message: JSON.stringify(message),
            messageId: message.id || message.msg_id
          });
          
          try {
            let albumMsg: AlbumDiscoveryMsg;
            
            // Carefully parse the message with detailed logging
            if (typeof message.message === 'string') {
              logger.info("Parsing message from string", { raw: message.message });
              try {
                albumMsg = JSON.parse(message.message) as AlbumDiscoveryMsg;
              } catch (parseError) {
                logger.error("JSON parse error", parseError, { message: message.message });
                throw new Error(`Invalid JSON in message: ${parseError.message}`);
              }
            } else if (message.message && typeof message.message === 'object') {
              logger.info("Using message object directly");
              albumMsg = message.message as AlbumDiscoveryMsg;
            } else {
              logger.error("Invalid message format", null, { message });
              throw new Error(`Invalid message format: ${JSON.stringify(message)}`);
            }
            
            const messageId = message.id || message.msg_id;
            logger.info("Processing album message", { 
              artistId: albumMsg.artistId, 
              offset: albumMsg.offset,
              messageId 
            });
            
            if (!albumMsg.artistId) {
              logger.error("Missing artistId in message", null, { message: albumMsg });
              throw new Error("Missing artistId in album discovery message");
            }
            
            // Generate idempotency key
            const idempotencyKey = `artist:${albumMsg.artistId}:albums:offset:${albumMsg.offset}`;
            
            try {
              const result = await processQueueMessageSafely(
                supabase,
                "album_discovery",
                messageId.toString(),
                async () => {
                  // Check if artist exists
                  const { data: artist, error: artistError } = await supabase
                    .from('artists')
                    .select('id, name, spotify_id')
                    .eq('id', albumMsg.artistId)
                    .single();
                    
                  if (artistError || !artist) {
                    logger.error("Artist not found", artistError, { artistId: albumMsg.artistId });
                    throw new Error(`Artist not found: ${albumMsg.artistId}`);
                  }
                  
                  logger.info(`Processing albums for artist`, { 
                    name: artist.name, 
                    id: artist.id,
                    spotifyId: artist.spotify_id
                  });
                  
                  // Process the albums
                  const result = await processAlbums(
                    supabase, 
                    spotifyClient, 
                    artist.id,
                    artist.spotify_id,
                    albumMsg.offset
                  );
                  
                  return result;
                },
                idempotencyKey,
                async () => {
                  // Check if we already processed this artist+offset combination
                  try {
                    const key = `processed:${idempotencyKey}`;
                    return await redis.exists(key) === 1;
                  } catch (redisError) {
                    logger.warn("Redis check failed", redisError);
                    return false;
                  }
                },
                {
                  maxRetries: 2,
                  deduplication: {
                    enabled: true,
                    redis,
                    ttlSeconds: 86400,
                    strictMatching: false
                  }
                }
              );
              
              if (result) {
                if (typeof result === 'object' && result.deduplication) {
                  dedupedCount++;
                  await metrics.recordDeduplicated("album_discovery", "consumer");
                  logger.info(`Message ${messageId} was handled by deduplication`);
                } else {
                  successCount++;
                  logger.info(`Successfully processed albums`, { 
                    artistId: albumMsg.artistId,
                    albumCount: result.albumCount,
                    hasMore: result.hasMore
                  });
                  
                  // Mark as processed in Redis
                  try {
                    const key = `processed:${idempotencyKey}`;
                    await redis.set(key, 'true', { ex: 86400 });
                  } catch (redisError) {
                    logger.warn(`Failed to mark as processed in Redis`, redisError);
                  }
                }
              } else {
                logger.error(`Failed to process message ${messageId}`);
                errorCount++;
              }
            } catch (processError) {
              logger.error("Error processing album message", processError);
              await logWorkerIssue(
                supabase,
                "albumDiscovery", 
                "processing_error", 
                `Error processing album: ${processError.message}`, 
                { message: albumMsg, error: processError }
              );
              errorCount++;
            }
          } catch (parseError) {
            logger.error("Error parsing album message", parseError);
            await logWorkerIssue(
              supabase,
              "albumDiscovery", 
              "message_parsing", 
              `Error parsing album message: ${parseError.message}`, 
              { message, error: parseError }
            );
            errorCount++;
          }
        }
      } catch (batchError) {
        logger.error("Error in batch processing", batchError);
        await logWorkerIssue(
          supabase,
          "albumDiscovery", 
          "batch_error", 
          `Error in batch processing: ${batchError.message}`, 
          { error: batchError }
        );
      } finally {
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
          
          logger.info("Background processing complete", {
            successful: successCount,
            deduplicated: dedupedCount,
            failed: errorCount
          });
        } catch (metricsError) {
          logger.error("Failed to record metrics", metricsError);
        }
      }
    })());
    
    return response;
  } catch (error) {
    logger.error("Major error in album discovery function", error);
    await logWorkerIssue(
      supabase,
      "albumDiscovery", 
      "fatal_error", 
      `Major error: ${error.message}`, 
      { error: error }
    );
    
    return new Response(JSON.stringify({ error: error.message }), { 
      status: 500, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
    });
  }
});

// Process albums for an artist with the given offset
async function processAlbums(
  supabase: any, 
  spotifyClient: any, 
  artistId: string, 
  spotifyArtistId: string,
  offset: number = 0
): Promise<{ albumCount: number, hasMore: boolean }> {
  const logger = new StructuredLogger({
    service: "albumDiscovery",
    processId: crypto.randomUUID(),
    entityType: "artist",
    entityId: artistId
  });
  
  logger.info("Starting album processing", { 
    artistId, 
    spotifyArtistId, 
    offset 
  });

  try {
    // Fetch the artist's albums from Spotify
    logger.info("Fetching albums from Spotify API");
    
    // Improved error logging for API requests
    let apiResponse, albums;
    try {
      logger.info("Calling Spotify API", { spotifyArtistId, offset });
      
      // Manually create a request since the SpotifyClient might have issues
      const token = await getSpotifyToken();
      const response = await fetch(
        `https://api.spotify.com/v1/artists/${spotifyArtistId}/albums?limit=50&offset=${offset}&include_groups=album,single`,
        {
          headers: {
            "Authorization": `Bearer ${token}`,
            "Content-Type": "application/json"
          }
        }
      );
      
      if (!response.ok) {
        const errorText = await response.text();
        logger.error("Spotify API error", null, { 
          status: response.status, 
          statusText: response.statusText, 
          errorText 
        });
        throw new Error(`Spotify API error: ${response.status} ${response.statusText}`);
      }
      
      apiResponse = await response.json();
      albums = apiResponse.items || [];
      
      logger.info("Retrieved albums from Spotify", { 
        count: albums.length,
        total: apiResponse.total || 0,
        hasMore: (offset + albums.length) < (apiResponse.total || 0)
      });
    } catch (apiError) {
      logger.error("Error calling Spotify API", apiError);
      throw apiError;
    }

    // Store albums in the database
    const albumIds = [];
    logger.info(`Processing ${albums.length} albums`);
    
    for (const album of albums) {
      try {
        const { data, error } = await supabase
          .from('albums')
          .upsert({
            spotify_id: album.id,
            artist_id: artistId,
            name: album.name,
            release_date: album.release_date,
            album_type: album.album_type,
            total_tracks: album.total_tracks,
            image_url: album.images?.[0]?.url,
            metadata: album
          }, {
            onConflict: 'spotify_id',
            ignoreDuplicates: false
          })
          .select('id')
          .single();
        
        if (error) {
          logger.error("Error storing album", error, { albumName: album.name });
          continue;
        }
        
        logger.info(`Stored album`, { 
          id: data.id, 
          name: album.name, 
          spotifyId: album.id
        });
        
        albumIds.push(data.id);
        
        // Enqueue track discovery for this album
        const { data: msgId, error: queueError } = await supabase.rpc('pg_enqueue', {
          queue_name: 'track_discovery',
          message_body: {
            albumId: data.id,
            offset: 0,
            _idempotencyKey: `album:${data.id}:tracks:offset:0`
          }
        });
        
        if (queueError) {
          logger.error("Error enqueueing track discovery", queueError, { albumId: data.id });
        } else {
          logger.info("Enqueued track discovery", { albumId: data.id, messageId: msgId });
        }
      } catch (albumError) {
        logger.error("Error processing album", albumError, { 
          albumName: album.name, 
          albumId: album.id 
        });
      }
    }
    
    // Check if there are more albums to process
    const hasMore = (offset + albums.length) < (apiResponse.total || 0);
    
    // If there are more albums, enqueue the next batch
    if (hasMore) {
      const nextOffset = offset + albums.length;
      logger.info("Enqueueing next batch of albums", { nextOffset });
      
      const { data: msgId, error: queueError } = await supabase.rpc('pg_enqueue', {
        queue_name: 'album_discovery',
        message_body: { 
          artistId, 
          offset: nextOffset,
          _idempotencyKey: `artist:${artistId}:albums:offset:${nextOffset}`
        }
      });
      
      if (queueError) {
        logger.error("Error enqueueing next batch", queueError);
      } else {
        logger.info("Enqueued next album batch", { nextOffset, messageId: msgId });
      }
    }
    
    return { albumCount: albums.length, hasMore };
  } catch (error) {
    logger.error("Error in album processing", error);
    throw error;
  }
}

// Helper function to get a Spotify token
async function getSpotifyToken(): Promise<string> {
  const clientId = Deno.env.get("SPOTIFY_CLIENT_ID") || "";
  const clientSecret = Deno.env.get("SPOTIFY_CLIENT_SECRET") || "";
  
  if (!clientId || !clientSecret) {
    throw new Error("Spotify client ID and client secret must be provided as environment variables");
  }
  
  const response = await fetch("https://accounts.spotify.com/api/token", {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "Authorization": `Basic ${btoa(`${clientId}:${clientSecret}`)}`
    },
    body: "grant_type=client_credentials"
  });
  
  if (!response.ok) {
    throw new Error(`Failed to get Spotify access token: ${response.statusText}`);
  }
  
  const data = await response.json();
  return data.access_token;
}
