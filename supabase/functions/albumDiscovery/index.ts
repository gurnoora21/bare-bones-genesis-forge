
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { SpotifyClient } from "../_shared/spotifyClient.ts";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { processQueueMessageSafely, deleteMessageWithRetries } from "../_shared/queueHelper.ts";
import { DeduplicationService } from "../_shared/deduplication.ts";

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
        let spotifyClient: SpotifyClient;
        
        // Initialize Spotify client with automatic retries
        try {
          spotifyClient = new SpotifyClient();
          log("info", "Spotify client initialized successfully");
        } catch (spotifyError) {
          log("error", "Failed to initialize Spotify client", { error: spotifyError.message });
          spotifyClient = { // Minimal placeholder to avoid errors
            getArtistAlbums: async () => { throw new Error("Spotify client not available"); }
          } as SpotifyClient;
        }
        
        let successCount = 0;
        let errorCount = 0;
        let dedupedCount = 0;

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
              // Process message with MINIMAL deduplication - we want to make sure albums are processed
              await processQueueMessageSafely(
                supabase,
                "album_discovery",
                messageId.toString(),
                async () => await processAlbumDiscovery(supabase, spotifyClient, msg, log),
                idempotencyKey, 
                async () => {
                  // Check if artist has albums already, if yes, we might be duplicating
                  // But we need to be careful as duplicate album discovery is better than none
                  try {
                    const { count, error } = await supabase
                      .from('albums')
                      .select('*', { count: 'exact', head: true })
                      .eq('artist_id', msg.artistId);
                      
                    // Only consider duplicates if album count > 0 AND offset = 0
                    if (!error && count && count > 0 && msg.offset === 0) {
                      log("info", `Artist ${msg.artistId} already has ${count} albums - potential duplicate, but proceeding anyway`);
                    }
                    
                    // Always return false to force processing - we'd rather have duplicate processing 
                    // than missing album discovery
                    return false;
                  } catch (dbError) {
                    log("warn", `Error checking existing albums for artist ${msg.artistId}`, { error: dbError.message });
                    return false;
                  }
                },
                {
                  maxRetries: 3,
                  deduplication: {
                    enabled: false, // CRITICAL: disable deduplication to fix pipeline
                    redis,
                    ttlSeconds: 86400,
                    bypassForQueues: ['album_discovery'] // Also add safety bypass in case deduplication is re-enabled
                  }
                }
              );
              
              successCount++;
              log("info", `Successfully processed album discovery for artist ID: ${msg.artistId}`);
              
            } catch (processError) {
              log("error", `Error processing album discovery message ${messageId}`, { 
                error: processError.message,
                msg
              });
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

        log("info", `Background processing complete: ${successCount} successful, ${dedupedCount} deduplicated, ${errorCount} failed`);
        
        // Record metrics
        try {
          await supabase.from('queue_metrics').insert({
            queue_name: "album_discovery",
            operation: "batch_processing",
            started_at: new Date().toISOString(),
            finished_at: new Date().toISOString(),
            processed_count: messages.length,
            success_count: successCount,
            error_count: errorCount
          });
        } catch (metricsError) {
          log("warn", "Failed to record metrics", { error: metricsError.message });
        }
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
  spotifyClient: SpotifyClient,
  msg: AlbumDiscoveryMsg,
  log: (level: string, message: string, data?: any) => void
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
    // Get albums from Spotify
    const albumsData = await spotifyClient.getArtistAlbums(artist.spotify_id, offset);
    
    if (!albumsData || !albumsData.items || !Array.isArray(albumsData.items)) {
      log("warn", `No albums returned from Spotify for artist ${artist.name}`, { artistId });
      return { processed: 0, message: "No albums found" };
    }

    log("info", `Retrieved ${albumsData.items.length} albums from Spotify for artist ${artist.name}`);

    // Process each album
    let savedAlbums = 0;
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
          continue;
        }

        log("info", `Stored album in database: ${albumData.name} with ID ${album.id}`);

        // Queue track discovery for this album
        const { data: trackJobId, error: trackJobError } = await supabase.rpc('pg_enqueue', {
          queue_name: 'track_discovery',
          message_body: { 
            albumId: album.id, 
            offset: 0,
            _idempotencyKey: `album:${album.id}:tracks:offset:0` 
          }
        });

        if (trackJobError) {
          log("error", `Error enqueueing track discovery for album ${album.id}`, { error: trackJobError.message });
        } else {
          log("info", `Enqueued track discovery for album ${album.id}, message ID: ${trackJobId}`);
        }

        savedAlbums++;
      } catch (albumError) {
        log("error", `Error processing album ${albumData?.name || 'unknown'}`, { 
          error: albumError.message, 
          albumData
        });
      }
    }

    // If there are more albums to fetch, queue next batch
    if (albumsData.next) {
      const nextOffset = offset + albumsData.items.length;
      
      // Queue next batch of albums
      const { data: nextBatchId, error: nextBatchError } = await supabase.rpc('pg_enqueue', {
        queue_name: 'album_discovery',
        message_body: {
          artistId,
          offset: nextOffset,
          _idempotencyKey: `artist:${artistId}:albums:offset:${nextOffset}`
        }
      });

      if (nextBatchError) {
        log("error", `Error enqueueing next album batch at offset ${nextOffset}`, { 
          error: nextBatchError.message 
        });
      } else {
        log("info", `Enqueued next album batch at offset ${nextOffset}, message ID: ${nextBatchId}`);
      }
    }

    return {
      processed: savedAlbums,
      artist: artist.name,
      message: `Processed ${savedAlbums} albums for artist ${artist.name}`
    };

  } catch (error) {
    log("error", `Error in album discovery process`, { error: error.message, artistId });
    throw error;
  }
}
