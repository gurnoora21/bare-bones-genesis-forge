
// Import DenoTypes for type compatibility
import '../lib/DenoTypes';

import { createClient } from "@supabase/supabase-js";
import { SpotifyClient } from "../lib/SpotifyClient";

interface TrackDiscoveryMsg {
  albumId: string;
  albumName: string;
  artistId: string;
  offset?: number;
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
const spotifyClient = new SpotifyClient('', ''); // We'll get actual credentials later

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
      queue_name: "track_discovery",
      batch_size: 3,
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
      ? JSON.parse(message.message) as TrackDiscoveryMsg 
      : message.message as TrackDiscoveryMsg;
      
    const messageId = message.id;
    
    try {
      await processTracks(supabase, msg);
      // Archive processed message
      await supabase.functions.invoke("delete-from-queue", {
        body: { queue_name: "track_discovery", message_id: messageId }
      });
      console.log(`Successfully processed track message ${messageId}`);
    } catch (error) {
      console.error(`Error processing track message ${messageId}:`, error);
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

async function processTracks(
  supabase: any, 
  msg: TrackDiscoveryMsg
) {
  const { albumId, albumName, artistId, offset = 0 } = msg;
  
  // Get album's database ID
  const { data: album, error: albumError } = await supabase
    .from('albums')
    .select('id, spotify_id')
    .eq('id', albumId)
    .single();

  if (albumError || !album) {
    throw new Error(`Album not found with ID: ${albumId}`);
  }

  // Get artist's database ID for normalized tracks
  const { data: artist, error: artistError } = await supabase
    .from('artists')
    .select('id, name')
    .eq('id', artistId)
    .single();

  if (artistError || !artist) {
    throw new Error(`Artist not found with ID: ${artistId}`);
  }

  // Ensure spotify_id is a string
  if (typeof album.spotify_id !== 'string') {
    throw new Error(`Invalid Spotify ID for album ${albumId}`);
  }
  
  // Fetch tracks from Spotify
  const tracksData = await spotifyClient.getAlbumTracks(album.spotify_id, offset);
  console.log(`Found ${tracksData.items.length} tracks in album ${albumName}`);

  // Filter and normalize tracks
  const tracksToProcess = tracksData.items.filter(track => 
    isArtistPrimaryOnTrack(track, artist.id)
  );

  // Get detailed track info in batches of 50 (Spotify API limit)
  for (let i = 0; i < tracksToProcess.length; i += 50) {
    const batch = tracksToProcess.slice(i, i + 50);
    const trackIds = batch.map(t => t.id);
    
    // Get detailed track info (uses Get Several Tracks endpoint)
    const trackDetails = await spotifyClient.getTrackDetails(trackIds);
    
    // Process each track in batch
    for (const track of trackDetails) {
      // Normalize the track name for deduplication
      const normalizedName = normalizeTrackName(track.name);
      
      // Check if normalized track already exists
      const { data: existingNormalizedTrack } = await supabase
        .from('normalized_tracks')
        .select('id')
        .eq('artist_id', artist.id)
        .eq('normalized_name', normalizedName)
        .maybeSingle();
      
      // Store track in database
      const { data: insertedTrack, error } = await supabase
        .from('tracks')
        .upsert({
          album_id: album.id,
          artist_id: artist.id,  // Include artist ID directly for easier queries
          spotify_id: track.id,
          name: track.name,
          duration_ms: track.duration_ms,
          popularity: track.popularity,
          spotify_preview_url: track.preview_url,
          metadata: {
            disc_number: track.disc_number,
            track_number: track.track_number,
            artists: track.artists,
            updated_at: new Date().toISOString()
          },
          updated_at: new Date().toISOString()
        }, {
          onConflict: 'spotify_id'
        })
        .select('id')
        .single();

      if (error) {
        console.error(`Error upserting track ${track.name}:`, error);
        continue;
      }

      // Create normalized track entry if it doesn't exist
      if (!existingNormalizedTrack) {
        await supabase
          .from('normalized_tracks')
          .upsert({
            artist_id: artist.id,
            normalized_name: normalizedName,
            representative_track_id: insertedTrack.id
          }, {
            onConflict: 'artist_id,normalized_name'
          });
      }

      // Enqueue producer identification
      await supabase.functions.invoke("send-to-queue", {
        body: {
          queue_name: "producer_identification",
          message: { 
            trackId: insertedTrack.id,
            trackName: track.name,
            albumId: album.id,
            artistId: artist.id
          }
        }
      });
      
      console.log(`Processed track ${track.name}, enqueued producer identification`);
    }
  }

  // If there are more tracks, enqueue next page
  if (tracksData.items.length > 0 && offset + tracksData.items.length < tracksData.total) {
    const newOffset = offset + tracksData.items.length;
    await supabase.functions.invoke("send-to-queue", {
      body: {
        queue_name: "track_discovery",
        message: { 
          albumId, 
          albumName, 
          artistId, 
          offset: newOffset 
        }
      }
    });
    console.log(`Enqueued next page of tracks for album ${albumName} with offset ${newOffset}`);
  }
}

// Helper functions
function normalizeTrackName(name: string): string {
  return name
    .toLowerCase()
    .replace(/\(.*?\)/g, '') // Remove text in parentheses
    .replace(/\[.*?\]/g, '') // Remove text in brackets
    .replace(/feat\.|ft\./g, '') // Remove featured artist markers
    .replace(/[^a-z0-9À-ÿ\s]/g, '') // Remove special characters
    .trim()
    .replace(/\s+/g, ' '); // Normalize whitespace
}

function isArtistPrimaryOnTrack(track: any, artistId: string): boolean {
  return track.artists && 
         track.artists.length > 0 && 
         track.artists[0].id === artistId;
}

// Initialize the server in Deno environments
serve(handler);

// Export the handler for Node.js environments
export { handler };
