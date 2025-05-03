
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { SpotifyClient } from "../_shared/spotifyClient.ts";

interface AlbumDiscoveryMsg {
  artistId: string;
  offset: number;
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
          
          try {
            await processAlbums(supabase, spotifyClient, msg);
            
            // Delete the message using our improved function
            const deleteSuccess = await deleteMessageWithRetries(
              supabase, 
              "album_discovery", 
              messageId.toString()
            );
            
            if (deleteSuccess) {
              console.log(`Successfully processed and deleted album message ${messageId}`);
              successCount++;
            } else {
              console.error(`Failed to delete album message ${messageId} after multiple attempts`);
              await logWorkerIssue(
                supabase,
                "albumDiscovery", 
                "queue_delete_failure", 
                `Failed to delete message ${messageId} after processing`, 
                { messageId }
              );
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
      
      console.log(`Completed background processing: ${successCount} successful, ${errorCount} failed`);
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
