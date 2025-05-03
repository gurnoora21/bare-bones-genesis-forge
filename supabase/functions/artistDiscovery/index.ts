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

// IMPROVED: Helper function to ensure message deletion with retries
async function ensureMessageDeleted(
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
      // Try using the deleteFromQueue edge function
      const deleteResponse = await fetch(
        `${Deno.env.get("SUPABASE_URL")}/functions/v1/deleteFromQueue`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${Deno.env.get("SUPABASE_ANON_KEY")}`
          },
          body: JSON.stringify({ 
            queue_name: queueName, 
            message_id: messageId 
          })
        }
      );
      
      const deleteResult = await deleteResponse.json();
      
      if (deleteResponse.ok && deleteResult.success) {
        console.log(`Successfully deleted message ${messageId} from ${queueName} (attempt ${attempts})`);
        deleted = true;
      } else {
        console.warn(`Delete attempt ${attempts} failed for message ${messageId}: ${JSON.stringify(deleteResult)}`);
        
        // Try alternate approach if previous attempt failed
        if (attempts === 1) {
          try {
            // Try direct pgmq.delete
            const { data, error } = await supabase.rpc('pgmq.delete', {
              queue_name: queueName,
              msg_id: messageId
            });
            
            if (!error) {
              console.log(`Successfully deleted message ${messageId} using direct pgmq.delete`);
              deleted = true;
              continue;
            }
          } catch (pgmqError) {
            console.error(`pgmq.delete failed for message ${messageId}:`, pgmqError);
          }
        }
        
        // Try another alternate approach
        if (attempts === 2) {
          try {
            // Try pg_delete_message RPC
            const { error } = await supabase.rpc('pg_delete_message', {
              queue_name: queueName,
              message_id: messageId
            });
            
            if (!error) {
              console.log(`Successfully deleted message ${messageId} using pg_delete_message RPC`);
              deleted = true;
              continue;
            }
          } catch (rpcError) {
            console.error(`pg_delete_message RPC failed for message ${messageId}:`, rpcError);
          }
        }
        
        // Wait before retrying (exponential backoff)
        if (!deleted && attempts < maxRetries) {
          const delayMs = Math.pow(2, attempts) * 100;
          await new Promise(resolve => setTimeout(resolve, delayMs));
        }
      }
      
      // Verify deletion if still not confirmed
      if (!deleted && attempts === maxRetries - 1) {
        try {
          // Check if the message is actually gone
          const { data: verifyData, error: verifyError } = await supabase.rpc('confirm_message_deletion', {
            queue_name: queueName,
            message_id: messageId
          });
          
          if (!verifyError && verifyData === true) {
            console.log(`Message ${messageId} verified as deleted despite error responses`);
            deleted = true;
          }
        } catch (verifyError) {
          console.error(`Error verifying message deletion:`, verifyError);
        }
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
      console.log("Raw queue data received:", typeof queueData, queueData ? JSON.stringify(queueData) : "null");
      
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
        // Initialize the Spotify client
        // IMPORTANT FIX: Use try/catch for Spotify client initialization
        // to prevent it from failing due to Redis connection issues
        let spotifyClient;
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
          return; // Exit early if we can't initialize Spotify client
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
              let processed = false;
              
              // IMPROVED ERROR HANDLING: Try direct processing first, then fall back to cache-less method
              try {
                // Try processing with caching first
                await processArtist(supabase, spotifyClient, msg);
                processed = true;
                console.log(`Successfully processed artist with normal method`);
              } catch (processingError) {
                // If the error is related to Redis formatting, try the fallback method
                if (processingError.message && (
                  processingError.message.includes("Redis") || 
                  processingError.message.includes("unsupported arg type")
                )) {
                  console.log("Falling back to processing without Redis cache due to:", processingError.message);
                  await processArtistWithoutCache(supabase, spotifyClient, msg);
                  processed = true;
                  console.log(`Successfully processed artist with fallback method`);
                } else {
                  // If it's not Redis-related, rethrow
                  throw processingError;
                }
              }
              
              // Only delete the message if processing was successful
              if (processed) {
                // IMPROVED: Use the ensureMessageDeleted helper for reliable deletion
                const deleteSuccess = await ensureMessageDeleted(supabase, "artist_discovery", messageId);
                
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

// The original processArtist function
async function processArtist(
  supabase: any, 
  spotifyClient: any, 
  msg: ArtistDiscoveryMsg
) {
  console.log("Processing artist:", msg);
  const { artistId, artistName } = msg;
  
  if (!artistId && !artistName) {
    throw new Error("Either artistId or artistName must be provided");
  }

  // Get the Spotify artist ID and details
  let artistData;
  
  if (artistId) {
    // Get artist by Spotify ID
    artistData = await spotifyClient.getArtistById(artistId);
  } else if (artistName) {
    // Search for artist by name
    console.log(`Searching for artist by name: ${artistName}`);
    const searchResponse = await spotifyClient.getArtistByName(artistName);
    
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
    // Get artist by Spotify ID - direct call without cache
    artistData = await spotifyClient.getArtistByIdDirect(artistId);
  } else if (artistName) {
    // Search for artist by name - direct call without cache
    console.log(`Searching for artist by name (no cache): ${artistName}`);
    const searchResponse = await spotifyClient.searchArtistDirect(artistName);
    
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
