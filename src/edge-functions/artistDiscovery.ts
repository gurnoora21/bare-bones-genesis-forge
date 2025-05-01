
// Import DenoTypes for type compatibility
import '../lib/DenoTypes';

import { createClient } from "@supabase/supabase-js";
import { SpotifyClient } from "../lib/SpotifyClient";

interface ArtistDiscoveryMsg {
  artistId?: string;
  artistName?: string;
}

// Use a dynamic import for Deno http/server
async function serve(handler: (req: Request) => Promise<Response>) {
  if (typeof globalThis !== 'undefined' && 'Deno' in globalThis) {
    // Use dynamic import instead of static import to avoid TypeScript errors
    try {
      const { serve: denoServe } = await import("https://deno.land/std@0.177.0/http/server.ts");
      return denoServe(handler);
    } catch (error) {
      console.error("Failed to import Deno http/server:", error);
      return null;
    }
  }
  return null;
}

// Initialize the Spotify client
const spotifyClient = SpotifyClient.getInstance();

const handler = async (req: Request) => {
  // Get environment variables safely using a helper function
  const SUPABASE_URL = typeof globalThis !== 'undefined' && 'Deno' in globalThis 
    ? (globalThis as any).Deno.env.get("SUPABASE_URL")!
    : '';
  const SUPABASE_SERVICE_ROLE_KEY = typeof globalThis !== 'undefined' && 'Deno' in globalThis 
    ? (globalThis as any).Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    : '';
    
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  // Process queue batch
  const { data: messages, error } = await supabase.functions.invoke("read-queue", {
    body: { 
      queue_name: "artist_discovery",
      batch_size: 5,
      visibility_timeout: 180 // 3 minutes
    }
  });

  if (error) {
    console.error("Error reading from queue:", error);
    return new Response(JSON.stringify({ error }), { status: 500 });
  }

  if (!messages || messages.length === 0) {
    return new Response(JSON.stringify({ processed: 0, message: "No messages to process" }));
  }

  // Process messages with background tasks
  const promises = messages.map(async (message) => {
    // Ensure the message is properly typed
    const msg = typeof message.message === 'string' 
      ? JSON.parse(message.message) as ArtistDiscoveryMsg 
      : message.message as ArtistDiscoveryMsg;
      
    const messageId = message.id;
    
    try {
      await processArtist(supabase, spotifyClient, msg);
      // Archive processed message
      await supabase.functions.invoke("delete-from-queue", {
        body: { queue_name: "artist_discovery", message_id: messageId }
      });
      console.log(`Successfully processed message ${messageId}`);
    } catch (error) {
      console.error(`Error processing artist message ${messageId}:`, error);
      // Message will return to queue after visibility timeout
    }
  });

  // Wait for all background tasks in a background process
  if (typeof EdgeRuntime !== 'undefined') {
    EdgeRuntime.waitUntil(Promise.all(promises));
  } else {
    // In a non-Edge environment, wait synchronously
    await Promise.all(promises);
  }
  
  return new Response(JSON.stringify({ 
    processed: messages.length,
    success: true
  }));
};

async function processArtist(
  supabase: any, 
  spotifyClient: SpotifyClient, 
  msg: ArtistDiscoveryMsg
) {
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
    const artist = await spotifyClient.getArtistByName(artistName);
    if (!artist) {
      throw new Error(`Artist not found: ${artistName}`);
    }
    artistData = artist;
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
    throw new Error(`Error storing artist: ${error.message}`);
  }

  // Enqueue album discovery
  await supabase.functions.invoke("send-to-queue", {
    body: {
      queue_name: "album_discovery",
      message: JSON.stringify({ 
        artistId: artist.id, 
        offset: 0 
      })
    }
  });

  console.log(`Processed artist ${artistData.name}, enqueued album discovery for artist ID ${artist.id}`);
  return artist;
}

// Initialize the server in Deno environments
serve(handler);

// Export the handler for Node.js environments
export { handler };
