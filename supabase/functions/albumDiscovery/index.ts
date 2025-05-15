
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { 
  processQueueMessageSafely, 
  deleteMessageWithRetries,
  enqueueMessage,
  logWorkerIssue
} from "../_shared/queueHelper.ts";

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
  
  // Create a process ID to track this invocation in logs
  const processId = crypto.randomUUID();
  
  // Enhanced logging with consistent format
  const log = (level: string, message: string, data: any = {}) => {
    console.log(JSON.stringify({
      timestamp: new Date().toISOString(),
      level,
      message,
      service: "albumDiscovery",
      processId,
      ...data
    }));
  };

  log("info", "Starting album discovery process");
  
  try {
    // Process queue batch
    log("info", "Attempting to dequeue from album_discovery queue");
    const { data: queueData, error: queueError } = await supabase.rpc('pg_dequeue', { 
      queue_name: "album_discovery",
      batch_size: 5,
      visibility_timeout: 300 // 5 minutes
    });

    if (queueError) {
      log("error", "Error reading from queue", { error: queueError });
      return new Response(JSON.stringify({ error: queueError }), { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      });
    }

    // Parse the JSONB result from pg_dequeue
    let messages = [];
    try {
      log("info", "Raw album queue data received", { 
        dataType: typeof queueData, 
        dataLength: queueData ? queueData.length : 0,
        dataSample: queueData ? JSON.stringify(queueData).substring(0, 100) + "..." : "null"
      });
      
      if (!queueData || (Array.isArray(queueData) && queueData.length === 0)) {
        log("info", "No messages in queue");
        log("info", "Retrieved 0 messages from queue");
        return new Response(
          JSON.stringify({ processed: 0, message: "No messages to process" }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      // Handle either string or object formats
      if (typeof queueData === 'string') {
        messages = JSON.parse(queueData);
      } else {
        messages = queueData;
      }
    } catch (e) {
      log("error", "Error parsing queue data", { error: e.message, rawData: queueData });
      return new Response(
        JSON.stringify({ error: "Failed to parse queue data" }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    log("info", `Retrieved ${messages.length} messages from queue`);

    if (messages.length === 0) {
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

    // Process messages in background to avoid CPU timeout
    EdgeRuntime.waitUntil((async () => {
      try {
        let successCount = 0;
        let errorCount = 0;
        let tracksQueued = 0;

        for (const message of messages) {
          try {
            let msg: AlbumDiscoveryMsg;
            const messageId = message.id || message.msg_id;
            
            // Parse the message - more robust handling of different formats
            if (typeof message.message === 'string') {
              try {
                msg = JSON.parse(message.message) as AlbumDiscoveryMsg;
              } catch (e) {
                log("error", `Invalid JSON in message ${messageId}`, { message: message.message });
                await deleteMessageWithRetries(supabase, "album_discovery", messageId.toString());
                errorCount++;
                continue;
              }
            } else if (message.message && typeof message.message === 'object') {
              msg = message.message as AlbumDiscoveryMsg;
            } else {
              log("error", `Invalid message format for ${messageId}`, { message });
              await deleteMessageWithRetries(supabase, "album_discovery", messageId.toString());
              errorCount++;
              continue;
            }
            
            log("info", `Processing album discovery message ${messageId}`, { msg });
            
            if (!msg.artistId) {
              log("error", `Invalid message: missing artistId`, { messageId, msg });
              await deleteMessageWithRetries(supabase, "album_discovery", messageId.toString());
              errorCount++;
              continue;
            }
            
            // Create idempotency key based on artist ID and offset
            const idempotencyKey = `artist:${msg.artistId}:albums:offset:${msg.offset || 0}`;
            
            try {
              // We'll enable deduplication at the message processing level
              // This protects against duplicate album discovery messages
              const result = await processQueueMessageSafely(
                supabase,
                "album_discovery",
                messageId.toString(),
                async () => {
                  const processed = await processAlbumDiscovery(
                    supabase, 
                    msg, 
                    log, 
                    redis
                  );
                  
                  // Track how many track discovery jobs we enqueued
                  tracksQueued += processed.tracksQueued || 0;
                  
                  return processed;
                },
                idempotencyKey,
                async () => {
                  // Check if this album page was already processed
                  try {
                    const key = `processed:album:artist:${msg.artistId}:offset:${msg.offset || 0}`;
                    const exists = await redis.exists(key);
                    return exists === 1;
                  } catch (redisError) {
                    // If Redis check fails, log but allow processing
                    log("warn", `Redis check failed for ${idempotencyKey}`, { error: redisError.message });
                    return false;
                  }
                },
                {
                  maxRetries: 3,
                  deduplication: {
                    enabled: true,
                    redis,
                    ttlSeconds: 86400
                  }
                }
              );
              
              successCount++;
              log("info", `Successfully processed album discovery for artist ID: ${msg.artistId}`, {
                savedAlbums: result.processed,
                artist: result.artist
              });
              
            } catch (processError) {
              log("error", `Error processing album discovery message ${messageId}`, { 
                error: processError.message,
                msg
              });
              
              // Log to worker_issues table
              await logWorkerIssue(
                supabase,
                "albumDiscovery",
                "processing_error",
                `Error processing album discovery for artist ${msg.artistId}`,
                {
                  artistId: msg.artistId,
                  offset: msg.offset || 0,
                  error: processError.message,
                  stack: processError.stack
                }
              );
              
              errorCount++;
            }
          } catch (parseError) {
            log("error", `Error parsing message`, { error: parseError.message, message });
            errorCount++;
            
            // Try to delete the message to avoid reprocessing
            try {
              if (message.id || message.msg_id) {
                await deleteMessageWithRetries(supabase, "album_discovery", (message.id || message.msg_id).toString());
              }
            } catch (deleteError) {
              log("error", "Failed to delete invalid message", { error: deleteError.message });
            }
          }
        }

        log("info", `Background processing complete`, {
          successes: successCount,
          errors: errorCount,
          tracksQueued,
          messageCount: messages.length
        });
      } catch (backgroundError) {
        log("error", "Error in background processing", { error: backgroundError.message, stack: backgroundError.stack });
      }
    })());
    
    return response;
  } catch (mainError) {
    log("error", "Major error in albumDiscovery function", { error: mainError.message, stack: mainError.stack });
    return new Response(JSON.stringify({ error: mainError.message }), { 
      status: 500, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
    });
  }
});

async function processAlbumDiscovery(
  supabase: any, 
  msg: AlbumDiscoveryMsg,
  log: (level: string, message: string, data?: any) => void,
  redis: Redis
): Promise<any> {
  const { artistId, offset = 0 } = msg;

  // First check if the artist exists in our database
  const { data: artist, error: artistError } = await supabase
    .from('artists')
    .select('id, name, spotify_id')
    .eq('id', artistId)
    .single();

  if (artistError || !artist) {
    log("error", `Artist not found in database with ID ${artistId}`, { error: artistError?.message });
    throw new Error(`Artist not found with ID: ${artistId}`);
  }

  log("info", `Fetching albums for artist: ${artist.name} (ID: ${artistId}, offset: ${offset})`);

  try {
    // Get the Spotify access token
    const spotifyToken = await getSpotifyToken();

    // Get albums from Spotify API directly
    const url = `https://api.spotify.com/v1/artists/${artist.spotify_id}/albums?limit=50&offset=${offset}`;
    const response = await fetch(url, {
      headers: {
        "Authorization": `Bearer ${spotifyToken}`,
        "Content-Type": "application/json"
      }
    });
    
    if (!response.ok) {
      throw new Error(`Spotify API error: ${response.status} ${await response.text()}`);
    }
    
    const albumsData = await response.json();
    
    if (!albumsData || !albumsData.items || !Array.isArray(albumsData.items)) {
      log("warn", `No albums returned from Spotify for artist ${artist.name}`, { artistId });
      
      // Even with no results, mark this offset as processed to avoid retries
      try {
        const processedKey = `processed:album:artist:${artistId}:offset:${offset}`;
        await redis.set(processedKey, 'true', { ex: 86400 }); // 24 hour TTL
      } catch (redisError) {
        // Non-fatal
        log("warn", `Failed to mark empty page as processed: ${redisError.message}`);
      }
      
      return { processed: 0, message: "No albums found", artist: artist.name, tracksQueued: 0 };
    }

    log("info", `Retrieved ${albumsData.items.length} albums from Spotify for artist ${artist.name}`);

    // Process each album
    let savedAlbums = 0;
    let tracksQueued = 0;
    
    for (const albumData of albumsData.items) {
      try {
        // Normalize and store album data
        const albumToInsert = {
          artist_id: artistId,
          name: albumData.name,
          spotify_id: albumData.id,
          release_date: albumData.release_date,
          cover_url: albumData.images?.[0]?.url,
          metadata: albumData
        };

        // Upsert album to database
        const { data: album, error: albumError } = await supabase
          .from('albums')
          .upsert(albumToInsert, {
            onConflict: 'spotify_id',
            ignoreDuplicates: false
          })
          .select('id')
          .single();

        if (albumError) {
          log("error", `Error storing album ${albumData.name}`, { error: albumError.message, albumData });
          
          // Log to worker_issues table
          await logWorkerIssue(
            supabase,
            "albumDiscovery",
            "database_error",
            `Error storing album ${albumData.name}`,
            {
              artistId,
              artistName: artist.name,
              albumName: albumData.name,
              albumId: albumData.id,
              error: albumError.message
            }
          );
          
          continue;
        }

        log("info", `Stored album in database: ${albumData.name} with ID ${album.id}`);

        // Queue track discovery for this album using direct SQL function
        try {
          const { data: trackMsg, error: trackError } = await supabase.rpc(
            'start_track_discovery',
            { album_id: album.id, offset_val: 0 }
          );
          
          if (trackError) {
            log("error", `Failed to enqueue track discovery for album ${album.id}`, { error: trackError.message });
            
            // Log to worker_issues table
            await logWorkerIssue(
              supabase,
              "albumDiscovery",
              "track_queue_error",
              `Failed to enqueue track discovery for album ${albumData.name}`,
              {
                artistId,
                artistName: artist.name,
                albumId: album.id,
                albumName: albumData.name,
                error: trackError.message
              }
            );
          } else {
            tracksQueued++;
            log("info", `Enqueued track discovery for album ${album.id}, message ID: ${trackMsg}`);
            
            // Mark track as queued in Redis to avoid duplicate processing
            try {
              const trackQueuedKey = `enqueued:track_discovery:album:${album.id}:offset:0`;
              await redis.set(trackQueuedKey, 'true', { ex: 86400 }); // 24 hour TTL
            } catch (redisError) {
              // Non-fatal
              log("warn", `Failed to mark track job as queued in Redis: ${redisError.message}`);
            }
          }
        } catch (trackQueueError) {
          log("error", `Exception enqueueing track discovery for album ${album.id}`, { error: trackQueueError.message });
          
          // Log to worker_issues table
          await logWorkerIssue(
            supabase,
            "albumDiscovery",
            "track_queue_exception",
            `Exception enqueueing track discovery for album ${albumData.name}`,
            {
              artistId,
              artistName: artist.name,
              albumId: album.id,
              albumName: albumData.name,
              error: trackQueueError.message
            }
          );
        }

        savedAlbums++;
      } catch (albumError) {
        log("error", `Error processing album ${albumData?.name || 'unknown'}`, { 
          error: albumError.message, 
          albumData
        });
        
        // Log to worker_issues table
        await logWorkerIssue(
          supabase,
          "albumDiscovery",
          "album_processing_error",
          `Error processing album ${albumData?.name || 'unknown'}`,
          {
            artistId,
            artistName: artist.name,
            albumData,
            error: albumError.message
          }
        );
      }
    }

    // If there are more albums to fetch, queue next batch
    if (albumsData.next) {
      const nextOffset = offset + albumsData.items.length;
      log("info", `More albums available, enqueueing next page at offset ${nextOffset}`);
      
      try {
        // Check if next page is already queued
        const nextPageKey = `enqueued:album_discovery:artist:${artistId}:offset:${nextOffset}`;
        let isNextPageQueued = false;
        
        try {
          isNextPageQueued = await redis.exists(nextPageKey) === 1;
        } catch (redisError) {
          // If Redis check fails, assume not queued
          log("warn", `Redis check failed for next page: ${redisError.message}`);
        }
        
        if (!isNextPageQueued) {
          // Queue the next batch using SQL function
          const { data: nextBatchMsg, error: nextBatchError } = await supabase.rpc(
            'start_album_discovery',
            { artist_id: artistId, offset_val: nextOffset }
          );
          
          if (nextBatchError) {
            log("error", `Failed to enqueue next album batch at offset ${nextOffset}`, { error: nextBatchError.message });
            
            // Log to worker_issues
            await logWorkerIssue(
              supabase,
              "albumDiscovery",
              "next_page_error",
              `Failed to enqueue next album batch for artist ${artist.name}`,
              {
                artistId,
                artistName: artist.name,
                offset: nextOffset,
                error: nextBatchError.message
              }
            );
          } else {
            log("info", `Successfully enqueued next album batch at offset ${nextOffset}, message ID: ${nextBatchMsg}`);
            
            // Mark as enqueued in Redis
            try {
              await redis.set(nextPageKey, 'true', { ex: 86400 }); // 24 hour TTL
            } catch (redisError) {
              // Non-fatal
              log("warn", `Failed to mark next page as enqueued: ${redisError.message}`);
            }
          }
        } else {
          log("info", `Next album batch at offset ${nextOffset} already enqueued, skipping`);
        }
      } catch (nextPageError) {
        log("error", `Exception enqueueing next album batch at offset ${nextOffset}`, { error: nextPageError.message });
        
        // Log to worker_issues
        await logWorkerIssue(
          supabase,
          "albumDiscovery",
          "next_page_exception",
          `Exception enqueueing next album batch for artist ${artist.name}`,
          {
            artistId,
            artistName: artist.name,
            offset: nextOffset,
            error: nextPageError.message
          }
        );
      }
    }

    // Mark this batch as processed in Redis
    try {
      const processedKey = `processed:album:artist:${artistId}:offset:${offset}`;
      await redis.set(processedKey, 'true', { ex: 86400 }); // 24 hour TTL
    } catch (redisError) {
      // Non-fatal
      log("warn", `Failed to set processed flag in Redis: ${redisError.message}`);
    }
    
    return {
      processed: savedAlbums,
      artist: artist.name,
      message: `Processed ${savedAlbums} albums for artist ${artist.name}`,
      tracksQueued
    };

  } catch (error) {
    log("error", `Error in album discovery process`, { error: error.message, artistId });
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
