
// Import DenoTypes for type compatibility
import '../lib/DenoTypes';

import { createClient } from "@supabase/supabase-js";
import { SpotifyClient } from "../lib/SpotifyClient";

interface AlbumDiscoveryMsg {
  artistId: string;
  offset: number;
}

// Use a safe approach to handle Deno environments
async function serve(handler: (req: Request) => Promise<Response>) {
  if (typeof globalThis !== 'undefined' && 'Deno' in globalThis) {
    // Instead of importing from deno.land, check if Deno.serve is available
    if ('serve' in (globalThis as any).Deno) {
      return (globalThis as any).Deno.serve(handler);
    } else {
      console.error("Deno.serve is not available");
      return null;
    }
  }
  return null;
}

// Initialize the Spotify client
const spotifyClient = SpotifyClient.getInstance();

const handler = async (req: Request) => {
  // Get environment variables safely
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
      queue_name: "album_discovery",
      batch_size: 3, // Process fewer albums at once
      visibility_timeout: 300 // 5 minutes
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
      ? JSON.parse(message.message) as AlbumDiscoveryMsg 
      : message.message as AlbumDiscoveryMsg;
      
    const messageId = message.id;
    
    try {
      await processAlbums(supabase, spotifyClient, msg);
      // Archive processed message
      await supabase.functions.invoke("delete-from-queue", {
        body: { queue_name: "album_discovery", message_id: messageId }
      });
      console.log(`Successfully processed album message ${messageId}`);
    } catch (error) {
      console.error(`Error processing album message ${messageId}:`, error);
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

async function processAlbums(
  supabase: any, 
  spotifyClient: SpotifyClient,
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
    // Store album in database
    const { data: album, error } = await supabase
      .from('albums')
      .upsert({
        artist_id: artistId,
        spotify_id: fullAlbumData.id,
        name: fullAlbumData.name,
        release_date: formatReleaseDate(fullAlbumData.release_date),
        cover_url: fullAlbumData.images[0]?.url,
        metadata: fullAlbumData
      })
      .select()
      .single();

    if (error) {
      console.error(`Error storing album ${fullAlbumData.name}:`, error);
      continue; // Continue with other albums
    }

    // Enqueue track discovery
    await supabase.functions.invoke("send-to-queue", {
      body: {
        queue_name: "track_discovery",
        message: JSON.stringify({ 
          albumId: album.id,
          albumName: album.name,
          artistId
        })
      }
    });
    
    console.log(`Processed album ${album.name}, enqueued track discovery`);
  }

  // If there are more albums, enqueue the next batch
  if (albumsData.next) {
    await supabase.functions.invoke("send-to-queue", {
      body: {
        queue_name: "album_discovery",
        message: JSON.stringify({ 
          artistId, 
          offset: offset + albumsData.items.length 
        })
      }
    });
    console.log(`Enqueued next page of albums for artist ${artistId} with offset ${offset + albumsData.items.length}`);
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

// Initialize the server in Deno environments
serve(handler);

// Export the handler for Node.js environments
export { handler };
