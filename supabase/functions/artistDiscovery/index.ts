
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { SpotifyClient } from "../_shared/spotifyClient.ts";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { 
  processQueueMessageSafely, 
  deleteMessageWithRetries, 
  enqueueMessage 
} from "../_shared/queueHelper.ts";
import { DeduplicationService } from "../_shared/deduplication.ts";

interface ArtistDiscoveryMsg {
  artistId?: string;
  artistName?: string;
}

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Simple logging function
function log(level: string, message: string, details: any = {}) {
  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    level,
    message,
    service: "artistDiscovery",
    ...details
  }));
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
  
  // Initialize Redis client
  const redis = new Redis({
    url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
    token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
  });
  
  // Initialize deduplication service
  const deduplicationService = new DeduplicationService(redis);

  log("info", "Starting artist discovery worker process");

  try {
    // Process queue batch
    log("info", "Attempting to dequeue from artist_discovery queue");
    const { data: queueData, error: queueError } = await supabase.rpc('pg_dequeue', { 
      queue_name: "artist_discovery",
      batch_size: 5,
      visibility_timeout: 180 // 3 minutes
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
      log("info", "Raw queue data received", { 
        dataType: typeof queueData,
        data: queueData ? JSON.stringify(queueData).substring(0, 300) + "..." : "null"
      });
      
      // Handle either string or object formats
      if (typeof queueData === 'string') {
        messages = JSON.parse(queueData);
      } else if (queueData) {
        messages = queueData;
      }
    } catch (e) {
      log("error", "Error parsing queue data", { error: e.message, queueData });
    }

    log("info", `Retrieved ${messages.length} messages from queue`);

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

    // Process messages in background to avoid CPU timeout
    EdgeRuntime.waitUntil((async () => {
      try {
        // Initialize Spotify client
        let spotifyClient: SpotifyClient | null = null;
        try {
          spotifyClient = new SpotifyClient();
          log("info", "Spotify client initialized successfully");
        } catch (spotifyError) {
          log("error", "Failed to initialize Spotify client", { error: spotifyError.message });
        }
        
        let successCount = 0;
        let errorCount = 0;
        let albumsQueued = 0;

        for (const message of messages) {
          try {
            let msg: ArtistDiscoveryMsg;
            
            // Handle potential message format issues
            if (typeof message.message === 'string') {
              msg = JSON.parse(message.message) as ArtistDiscoveryMsg;
            } else if (message.message && typeof message.message === 'object') {
              msg = message.message as ArtistDiscoveryMsg;
            } else {
              throw new Error(`Invalid message format: ${JSON.stringify(message)}`);
            }
            
            const messageId = message.id || message.msg_id;
            log("info", `Processing message ${messageId}`, { msg });
            
            // Create idempotency key based on artist ID or name
            const idempotencyKey = msg.artistId 
              ? `artist:id:${msg.artistId}` 
              : `artist:name:${msg.artistName}`;
            
            try {
              // Process message with basic deduplication 
              const result = await processQueueMessageSafely(
                supabase,
                "artist_discovery",
                messageId.toString(),
                async () => await processArtist(supabase, spotifyClient, msg, redis),
                idempotencyKey, 
                async () => {
                  // Check if this artist was already processed
                  if (msg.artistId) {
                    const { data } = await supabase
                      .from('artists')
                      .select('id')
                      .eq('spotify_id', msg.artistId)
                      .maybeSingle();
                    return !!data;
                  } else if (msg.artistName) {
                    // For names, we'll check Redis but errors shouldn't block
                    try {
                      const artistKey = `processed:artist:name:${msg.artistName.toLowerCase()}`;
                      return await redis.exists(artistKey) === 1;
                    } catch (redisError) {
                      log("warn", `Redis check failed for artist ${msg.artistName}`, { error: redisError.message });
                      return false;
                    }
                  }
                  return false;
                },
                {
                  maxRetries: 2,
                  deduplication: {
                    enabled: true,
                    redis,
                    ttlSeconds: 86400 // 24 hour deduplication window
                  }
                }
              );
              
              if (result) {
                if (typeof result === 'object' && result.deduplication) {
                  log("info", `Message ${messageId} was handled by deduplication`);
                } else {
                  successCount++;
                  log("info", `Successfully processed artist with ID: ${result.id}`, { artistId: result.id });
                  
                  // Ensure album discovery is queued
                  const albumQueueResult = await enqueueMessage(
                    supabase,
                    'album_discovery',
                    { artistId: result.id, offset: 0 },
                    `artist:${result.id}:albums:offset:0`,
                    deduplicationService
                  );
                  
                  if (albumQueueResult) {
                    albumsQueued++;
                    log("info", `Successfully queued album discovery for artist ID: ${result.id}`);
                  } else {
                    log("warn", `Failed to queue album discovery for artist ID: ${result.id}`);
                  }
                }
              } else {
                log("error", `Failed to process message ${messageId}`);
                errorCount++;
              }
            } catch (processError) {
              log("error", `Error processing artist message`, { error: processError.message, message: msg });
              errorCount++;
            }
          } catch (parseError) {
            log("error", `Error parsing message`, { error: parseError.message, message });
            errorCount++;
            
            // Try to delete the message to avoid reprocessing
            if (message.id || message.msg_id) {
              await deleteMessageWithRetries(supabase, "artist_discovery", (message.id || message.msg_id).toString());
            }
          }
        }

        log("info", `Background processing complete`, {
          successes: successCount,
          errors: errorCount,
          albumsQueued: albumsQueued,
          messageCount: messages.length
        });
      } catch (backgroundError) {
        log("error", "Error in background processing", { error: backgroundError.message, stack: backgroundError.stack });
      }
    })());
    
    return response;
  } catch (mainError) {
    log("error", "Major error in artistDiscovery function", { error: mainError.message, stack: mainError.stack });
    
    return new Response(JSON.stringify({ error: mainError.message }), { 
      status: 500, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
    });
  }
});

// Process an artist from the discovery queue
async function processArtist(
  supabase: any, 
  spotifyClient: any, 
  msg: ArtistDiscoveryMsg,
  redis: Redis
) {
  const { artistId, artistName } = msg;
  
  if (!artistId && !artistName) {
    throw new Error("Either artistId or artistName must be provided");
  }

  // Get the Spotify artist ID and details
  let artistData;
  
  if (artistId) {
    // Direct API call without caching
    const response = await fetch(
      `https://api.spotify.com/v1/artists/${artistId}`,
      {
        headers: {
          "Authorization": `Bearer ${await getSpotifyToken()}`,
          "Content-Type": "application/json"
        }
      }
    );
    
    if (!response.ok) {
      throw new Error(`Spotify API error: ${response.status} ${await response.text()}`);
    }
    
    artistData = await response.json();
  } else if (artistName) {
    console.log(`Searching for artist by name: ${artistName}`);
    
    // Search for artist by name - direct API call
    const response = await fetch(
      `https://api.spotify.com/v1/search?q=${encodeURIComponent(artistName)}&type=artist&limit=1`,
      {
        headers: {
          "Authorization": `Bearer ${await getSpotifyToken()}`,
          "Content-Type": "application/json"
        }
      }
    );
    
    if (!response.ok) {
      throw new Error(`Spotify API error: ${response.status} ${await response.text()}`);
    }
    
    const searchResponse = await response.json();
    
    if (!searchResponse || !searchResponse.artists || !searchResponse.artists.items.length) {
      throw new Error(`Artist not found: ${artistName}`);
    }
    
    // Use the first artist result
    artistData = searchResponse.artists.items[0];
    console.log(`Found artist: ${artistData.name} (${artistData.id})`);
  }
  
  if (!artistData) {
    throw new Error("Failed to fetch artist data");
  }

  // Store in database with explicit conflict handling
  const { data: artist, error } = await supabase
    .from('artists')
    .upsert({
      spotify_id: artistData.id,
      name: artistData.name,
      followers: artistData.followers?.total || 0,
      popularity: artistData.popularity,
      image_url: artistData.images?.[0]?.url,
      metadata: artistData
    }, {
      onConflict: 'spotify_id',
      ignoreDuplicates: false
    })
    .select('id')
    .single();

  if (error) {
    console.error("Error storing artist:", error);
    throw new Error(`Error storing artist: ${error.message}`);
  }

  console.log(`Stored artist in database with ID: ${artist.id}`);

  // Mark this artist as processed in Redis 
  if (artistName) {
    try {
      const artistKey = `processed:artist:name:${artistName.toLowerCase()}`;
      await redis.set(artistKey, 'true', { ex: 86400 });
      console.log(`Marked artist ${artistName} as processed in Redis`);
    } catch (redisError) {
      console.warn(`Failed to mark artist ${artistName} as processed in Redis:`, redisError);
      // Continue anyway
    }
  }

  // The album discovery will be queued outside this function
  // to ensure proper logging and error handling
  
  return artist;
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
