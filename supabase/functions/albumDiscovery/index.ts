
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

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );

  // Process queue batch
  const { data: messages, error } = await supabase.functions.invoke("readQueue", {
    body: { 
      queue_name: "album_discovery",
      batch_size: 3, // Process fewer albums at once
      visibility_timeout: 300 // 5 minutes
    }
  });

  if (error) {
    console.error("Error reading from queue:", error);
    await logWorkerIssue(
      supabase,
      "albumDiscovery", 
      "queue_error", 
      "Error reading from queue", 
      { error }
    );
    return new Response(JSON.stringify({ error }), { status: 500, headers: corsHeaders });
  }

  if (!messages || messages.length === 0) {
    return new Response(JSON.stringify({ processed: 0, message: "No messages to process" }), { headers: corsHeaders });
  }

  // Create a quick response to avoid timeout
  const response = new Response(JSON.stringify({ 
    processing: true, 
    message_count: messages.length 
  }), { headers: corsHeaders });

  // Initialize the Spotify client
  const spotifyClient = new SpotifyClient();

  // Process messages with background tasks
  EdgeRuntime.waitUntil((async () => {
    let successCount = 0;
    let errorCount = 0;
    
    for (const message of messages) {
      try {
        // Ensure the message is properly typed
        const msg = typeof message.message === 'string' 
          ? JSON.parse(message.message) as AlbumDiscoveryMsg 
          : message.message as AlbumDiscoveryMsg;
          
        const messageId = message.id;
        
        try {
          await processAlbums(supabase, spotifyClient, msg);
          // Archive processed message
          await supabase.functions.invoke("deleteFromQueue", {
            body: { queue_name: "album_discovery", message_id: messageId }
          });
          console.log(`Successfully processed album message ${messageId}`);
          successCount++;
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
          // Message will return to queue after visibility timeout
        }
      } catch (messageError) {
        console.error(`Error parsing message:`, messageError);
        errorCount++;
      }
    }
    
    console.log(`Completed background processing: ${successCount} successful, ${errorCount} failed`);
  })());
  
  return response;
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
