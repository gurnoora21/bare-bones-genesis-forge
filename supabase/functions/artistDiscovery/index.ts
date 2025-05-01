
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
    const { data: queueData, error: queueError } = await supabase.rpc('pg_dequeue', { 
      queue_name: "artist_discovery",
      batch_size: 5,
      visibility_timeout: 180 // 3 minutes
    });

    if (queueError) {
      console.error("Error reading from queue:", queueError);
      return new Response(JSON.stringify({ error: queueError }), { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      });
    }

    // Parse the JSONB result from pg_dequeue
    let messages = [];
    try {
      // Handle either string or object formats
      if (typeof queueData === 'string') {
        messages = JSON.parse(queueData);
      } else if (queueData) {
        messages = queueData;
      }

      console.log("Raw queue data:", JSON.stringify(queueData));
    } catch (e) {
      console.error("Error parsing queue data:", e);
      console.log("Raw queue data:", queueData);
    }

    console.log(`Retrieved ${messages.length} messages from queue`);

    if (!messages || messages.length === 0) {
      return new Response(
        JSON.stringify({ processed: 0, message: "No messages to process" }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Initialize the Spotify client
    const spotifyClient = new SpotifyClient();

    // Process messages with background tasks
    const promises = messages.map(async (message) => {
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
        
        const messageId = message.id;
        console.log(`Processing message ${messageId}: ${JSON.stringify(msg)}`);
        
        await processArtist(supabase, spotifyClient, msg);
        
        // Archive processed message
        const { error: deleteError } = await supabase.rpc('pg_delete_message', {
          queue_name: "artist_discovery",
          message_id: messageId
        });
        
        if (deleteError) {
          console.error(`Error deleting message ${messageId}:`, deleteError);
        } else {
          console.log(`Successfully processed message ${messageId}`);
        }
      } catch (error) {
        console.error(`Error processing artist message:`, error);
        // Message will return to queue after visibility timeout
      }
    });

    // Wait for all background tasks in a background process
    try {
      EdgeRuntime.waitUntil(Promise.all(promises));
      console.log(`Processing ${messages.length} messages in the background`);
    } catch (error) {
      console.error("Error in background processing:", error);
    }
    
    return new Response(JSON.stringify({ 
      processed: messages.length,
      success: true
    }), { 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
    });
  } catch (mainError) {
    console.error("Major error in artistDiscovery function:", mainError);
    return new Response(JSON.stringify({ error: mainError.message }), { 
      status: 500, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
    });
  }
});

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

  // Store in database
  const { data: artist, error } = await supabase
    .from('artists')
    .upsert({
      spotify_id: artistData.id,
      name: artistData.name,
      followers: artistData.followers?.total || 0,
      popularity: artistData.popularity,
      image_url: artistData.images?.[0]?.url,
      metadata: artistData
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
