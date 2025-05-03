import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { SpotifyClient } from "../_shared/spotifyClient.ts";

interface ArtistDiscoveryMsg {
  artistId?: string;
  artistName?: string;
}

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Add a worker issue logging function
async function logWorkerIssue(
  supabase: any,
  workerName: string,
  issueType: string,
  message: string,
  details: any = {}
) {
  try {
    await supabase.from('worker_issues').insert({
      worker_name: workerName,
      issue_type: issueType,
      message: message,
      details: details,
      resolved: false
    });
    console.error(`[${workerName}] ${issueType}: ${message}`);
  } catch (error) {
    console.error("Failed to log worker issue:", error);
  }
}

// Helper function to delete messages with retries
async function deleteMessageWithRetries(
  supabase: any,
  queueName: string,
  messageId: string,
  maxRetries: number = 3
): Promise<boolean> {
  console.log(`Attempting to delete message ${messageId} from queue ${queueName} with up to ${maxRetries} retries`);
  
  let deleted = false;
  let attempts = 0;
  
  while (!deleted && attempts < maxRetries) {
    attempts++;
    
    try {
      // Try using the database function first
      const { data, error } = await supabase.rpc(
        'pg_delete_message',
        { 
          queue_name: queueName, 
          message_id: messageId.toString()
        }
      );
      
      if (error) {
        console.error(`Delete attempt ${attempts} failed with RPC error:`, error);
      } else if (data === true) {
        console.log(`Successfully deleted message ${messageId} from ${queueName} (attempt ${attempts})`);
        deleted = true;
        break;
      } else {
        console.warn(`Delete attempt ${attempts} returned false for message ${messageId}`);
      }

      // If the first method failed and this isn't the last attempt, try the Edge Function approach
      if (!deleted && attempts < maxRetries) {
        const deleteResponse = await fetch(
          `${Deno.env.get("SUPABASE_URL")}/functions/v1/deleteFromQueue`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "Authorization": `Bearer ${Deno.env.get("SUPABASE_ANON_KEY")}`
            },
            body: JSON.stringify({ queue_name: queueName, message_id: messageId })
          }
        );
        
        if (deleteResponse.ok) {
          const result = await deleteResponse.json();
          if (result.success) {
            console.log(`Successfully deleted message ${messageId} via Edge Function (attempt ${attempts})`);
            deleted = true;
            break;
          } else if (result.reset) {
            console.log(`Reset visibility timeout for message ${messageId} (attempt ${attempts})`);
            // We'll consider this a success in terms of handling the message
            deleted = true;
            break;
          }
        }
      }
      
      // Wait before retrying (exponential backoff with jitter)
      if (!deleted && attempts < maxRetries) {
        const baseDelay = Math.pow(2, attempts) * 100;
        const jitter = Math.floor(Math.random() * 100);
        const delayMs = baseDelay + jitter;
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    } catch (e) {
      console.error(`Error during deletion attempt ${attempts} for message ${messageId}:`, e);
      
      // Wait before retrying
      if (attempts < maxRetries) {
        const delayMs = Math.pow(2, attempts) * 100;
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
  }
  
  if (!deleted) {
    console.error(`Failed to delete message ${messageId} after ${maxRetries} attempts`);
  }
  
  return deleted;
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

  console.log("Starting artist discovery worker process...");

  try {
    // Process queue batch
    console.log("Attempting to dequeue from artist_discovery queue");
    const { data: queueData, error: queueError } = await supabase.rpc('pg_dequeue', { 
      queue_name: "artist_discovery",
      batch_size: 5,
      visibility_timeout: 180 // 3 minutes
    });

    if (queueError) {
      console.error("Error reading from queue:", queueError);
      await logWorkerIssue(
        supabase,
        "artistDiscovery", 
        "queue_error", 
        `Error reading from queue: ${queueError.message}`, 
        { error: queueError }
      );
      
      return new Response(JSON.stringify({ error: queueError }), { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      });
    }

    // Parse the JSONB result from pg_dequeue
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
      console.log("Raw queue data:", queueData);
      await logWorkerIssue(
        supabase,
        "artistDiscovery", 
        "queue_parsing", 
        `Error parsing queue data: ${e.message}`, 
        { queueData, error: e.message }
      );
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

    // Process messages in background to avoid CPU timeout
    EdgeRuntime.waitUntil((async () => {
      try {
        let spotifyClient = null;
        
        // Try to initialize Spotify client, but don't fail if it doesn't work
        try {
          spotifyClient = new SpotifyClient();
        } catch (spotifyError) {
          console.error("Failed to initialize Spotify client:", spotifyError);
          await logWorkerIssue(
            supabase,
            "artistDiscovery", 
            "spotify_init_error", 
            `Failed to initialize Spotify client: ${spotifyError.message}`, 
            { error: spotifyError }
          );
        }
        
        let successCount = 0;
        let errorCount = 0;

        for (const message of messages) {
          // Ensure the message is properly parsed
          try {
            let msg: ArtistDiscoveryMsg;
            
            // Handle potential message format issues
            console.log("Processing message:", JSON.stringify(message));
            
            if (typeof message.message === 'string') {
              msg = JSON.parse(message.message) as ArtistDiscoveryMsg;
            } else if (message.message && typeof message.message === 'object') {
              msg = message.message as ArtistDiscoveryMsg;
            } else {
              throw new Error(`Invalid message format: ${JSON.stringify(message)}`);
            }
            
            const messageId = message.id || message.msg_id;
            console.log(`Processing message ${messageId}: ${JSON.stringify(msg)}`);
            
            try {
              await processArtistWithoutCache(supabase, spotifyClient, msg);
              console.log(`Successfully processed artist with fallback method`);
              
              // Delete message using our improved function
              const deleteSuccess = await deleteMessageWithRetries(
                supabase, 
                "artist_discovery", 
                messageId.toString()
              );
              
              if (deleteSuccess) {
                console.log(`Successfully processed and deleted message ${messageId}`);
                successCount++;
              } else {
                console.error(`Failed to delete message ${messageId} after multiple attempts`);
                await logWorkerIssue(
                  supabase,
                  "artistDiscovery", 
                  "queue_delete_failure", 
                  `Failed to delete message ${messageId} after processing`, 
                  { messageId }
                );
                errorCount++;
              }
            } catch (processError) {
              console.error(`Error processing artist message:`, processError);
              await logWorkerIssue(
                supabase,
                "artistDiscovery", 
                "processing_error", 
                `Error processing artist: ${processError.message}`, 
                { message: msg, error: processError.message }
              );
              errorCount++;
            }
          } catch (parseError) {
            console.error(`Error parsing message:`, parseError);
            await logWorkerIssue(
              supabase,
              "artistDiscovery", 
              "message_parsing", 
              `Error parsing message: ${parseError.message}`, 
              { message, error: parseError.message }
            );
            errorCount++;
          }
        }

        console.log(`Background processing complete: ${successCount} successful, ${errorCount} failed`);
      } catch (backgroundError) {
        console.error("Error in background processing:", backgroundError);
        await logWorkerIssue(
          supabase,
          "artistDiscovery", 
          "background_error", 
          `Background processing error: ${backgroundError.message}`, 
          { error: backgroundError.message }
        );
      }
    })());
    
    return response;
  } catch (mainError) {
    console.error("Major error in artistDiscovery function:", mainError);
    await logWorkerIssue(
      supabase,
      "artistDiscovery", 
      "fatal_error", 
      `Major error: ${mainError.message}`, 
      { error: mainError.message, stack: mainError.stack }
    );
    
    return new Response(JSON.stringify({ error: mainError.message }), { 
      status: 500, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
    });
  }
});

// Updated fallback implementation to use direct API methods without Redis caching
async function processArtistWithoutCache(
  supabase: any, 
  spotifyClient: any, 
  msg: ArtistDiscoveryMsg
) {
  console.log("Processing artist without Redis cache:", msg);
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
    console.log(`Searching for artist by name (no cache): ${artistName}`);
    
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

  // Enqueue album discovery
  const { data: enqueueData, error: queueError } = await supabase.rpc('pg_enqueue', {
    queue_name: 'album_discovery',
    message_body: JSON.stringify({ 
      artistId: artist.id, 
      offset: 0 
    })
  });

  if (queueError) {
    console.error("Error enqueueing album discovery:", queueError);
    throw new Error(`Error enqueueing album discovery: ${queueError.message}`);
  }

  console.log(`Processed artist ${artistData.name}, enqueued album discovery for artist ID ${artist.id}`);
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
