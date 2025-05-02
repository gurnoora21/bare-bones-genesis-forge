
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { SpotifyClient } from "../_shared/spotifyClient.ts";

interface TrackDiscoveryMsg {
  albumId: string;
  albumName: string;
  artistId: string;
  offset?: number;
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

  try {
    console.log("Starting track discovery process");
    
    // Process queue batch
    const { data: messages, error } = await supabase.functions.invoke("readQueue", {
      body: { 
        queue_name: "track_discovery",
        batch_size: 3,
        visibility_timeout: 300 // 5 minutes
      }
    });

    if (error) {
      console.error("Error reading from queue:", error);
      return new Response(JSON.stringify({ error }), { status: 500, headers: corsHeaders });
    }

    if (!messages || messages.length === 0) {
      console.log("No messages to process in track_discovery queue");
      return new Response(JSON.stringify({ processed: 0, message: "No messages to process" }), { headers: corsHeaders });
    }

    console.log(`Found ${messages.length} messages to process in track_discovery queue`);

    // Initialize the Spotify client
    const spotifyClient = new SpotifyClient();

    // Process messages in sequence to avoid overwhelming the database
    const processPromises = messages.map(async (message) => {
      // Ensure the message is properly typed
      const msg = typeof message.message === 'string' 
        ? JSON.parse(message.message) as TrackDiscoveryMsg 
        : message.message as TrackDiscoveryMsg;
        
      const messageId = message.id;
      
      try {
        console.log(`Processing track message for album: ${msg.albumName} (${msg.albumId})`);
        const result = await processTracks(supabase, spotifyClient, msg);
        
        // Archive processed message
        const deleteResult = await supabase.functions.invoke("deleteFromQueue", {
          body: { queue_name: "track_discovery", message_id: messageId }
        });
        
        console.log(`Successfully processed track message ${messageId}`, { deleteResult });
        return { success: true, messageId, result };
      } catch (error) {
        console.error(`Error processing track message ${messageId}:`, error);
        // Message will return to queue after visibility timeout
        return { success: false, messageId, error: error.message };
      }
    });

    // Process all messages and capture results
    const results = await Promise.all(processPromises);
    const successCount = results.filter(r => r.success).length;
    
    console.log(`Completed processing ${successCount}/${messages.length} track discovery messages`);

    return new Response(JSON.stringify({ 
      processed: messages.length,
      success: successCount,
      total: messages.length
    }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  } catch (error) {
    console.error("Unexpected error in track discovery worker:", error);
    return new Response(JSON.stringify({ error: error.message }), 
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }
});

async function processTracks(
  supabase: any, 
  spotifyClient: any,
  msg: TrackDiscoveryMsg
) {
  const { albumId, albumName, artistId, offset = 0 } = msg;
  console.log(`Processing tracks for album ${albumName} (ID: ${albumId}) at offset ${offset}`);
  
  try {
    // Get album's database ID
    const { data: album, error: albumError } = await supabase
      .from('albums')
      .select('id, spotify_id')
      .eq('id', albumId)
      .single();

    if (albumError || !album) {
      const errMsg = `Album not found with ID: ${albumId}`;
      console.error(errMsg);
      throw new Error(errMsg);
    }
    
    console.log(`Found album in database: ${album.id} with Spotify ID: ${album.spotify_id}`);

    // Get artist's database ID for normalized tracks
    const { data: artist, error: artistError } = await supabase
      .from('artists')
      .select('id, name')
      .eq('id', artistId)
      .single();

    if (artistError || !artist) {
      const errMsg = `Artist not found with ID: ${artistId}`;
      console.error(errMsg);
      throw new Error(errMsg);
    }
    
    console.log(`Found artist in database: ${artist.name} (ID: ${artist.id})`);

    // Ensure spotify_id is a string
    if (typeof album.spotify_id !== 'string') {
      const errMsg = `Invalid Spotify ID for album ${albumId}: ${album.spotify_id}`;
      console.error(errMsg);
      throw new Error(errMsg);
    }
    
    // Fetch tracks from Spotify
    console.log(`Fetching tracks from Spotify for album ${albumName} (ID: ${album.spotify_id})`);
    const tracksData = await spotifyClient.getAlbumTracks(album.spotify_id, offset);
    console.log(`Found ${tracksData.items.length} tracks in album ${albumName} (total: ${tracksData.total})`);

    if (!tracksData.items || tracksData.items.length === 0) {
      console.log(`No tracks found for album ${albumName}`);
      return { processed: 0 };
    }

    // Filter and normalize tracks
    const tracksToProcess = tracksData.items.filter(track => 
      isArtistPrimaryOnTrack(track, artist.id)
    );
    
    console.log(`${tracksToProcess.length} tracks have the artist as primary artist`);

    // Get detailed track info in batches of 50 (Spotify API limit)
    let processedCount = 0;
    
    for (let i = 0; i < tracksToProcess.length; i += 50) {
      const batch = tracksToProcess.slice(i, i + 50);
      const trackIds = batch.map(t => t.id);
      
      console.log(`Processing batch of ${batch.length} tracks, IDs: ${trackIds.slice(0, 3)}...`);
      
      try {
        // Get detailed track info (uses Get Several Tracks endpoint)
        const trackDetails = await spotifyClient.getTrackDetails(trackIds);
        console.log(`Received ${trackDetails.length} track details from Spotify`);
        
        // Process each track in batch
        for (const track of trackDetails) {
          try {
            // Debug track object
            console.log(`Processing track: ${track.name} (ID: ${track.id})`);
            
            if (!track || !track.id) {
              console.error(`Invalid track data received:`, track);
              continue;
            }
            
            // Normalize the track name for deduplication
            const normalizedName = normalizeTrackName(track.name);
            
            // Check if normalized track already exists
            const { data: existingNormalizedTrack } = await supabase
              .from('normalized_tracks')
              .select('id')
              .eq('artist_id', artist.id)
              .eq('normalized_name', normalizedName)
              .maybeSingle();
            
            // Prepare track data for insertion
            const trackData = {
              album_id: album.id,
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
            };
            
            console.log(`Upserting track data: ${JSON.stringify({
              id: track.id,
              name: track.name,
              album_id: album.id
            })}`);
            
            // Store track in database
            const { data: insertedTrack, error: insertError } = await supabase
              .from('tracks')
              .upsert(trackData, {
                onConflict: 'spotify_id'
              })
              .select('id')
              .single();

            if (insertError) {
              console.error(`Error upserting track ${track.name} (${track.id}):`, insertError);
              continue;
            }
            
            console.log(`Successfully inserted/updated track: ${track.name} (DB ID: ${insertedTrack.id})`);

            // Create normalized track entry if it doesn't exist
            if (!existingNormalizedTrack) {
              const { error: normalizedError } = await supabase
                .from('normalized_tracks')
                .upsert({
                  artist_id: artist.id,
                  normalized_name: normalizedName,
                  representative_track_id: insertedTrack.id
                }, {
                  onConflict: 'artist_id,normalized_name'
                });
                
              if (normalizedError) {
                console.error(`Error upserting normalized track for ${track.name}:`, normalizedError);
              } else {
                console.log(`Created normalized track entry for: ${normalizedName}`);
              }
            }

            // Enqueue producer identification
            await supabase.functions.invoke("sendToQueue", {
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
            
            console.log(`Enqueued producer identification for track: ${track.name}`);
            processedCount++;
            
          } catch (trackError) {
            console.error(`Error processing individual track ${track?.name || 'unknown'}:`, trackError);
            // Continue with next track
          }
        }
      } catch (batchError) {
        console.error(`Error processing batch of tracks:`, batchError);
        // Continue with next batch
      }
    }

    // If there are more tracks, enqueue next page
    if (tracksData.items.length > 0 && offset + tracksData.items.length < tracksData.total) {
      const newOffset = offset + tracksData.items.length;
      console.log(`Enqueueing next page of tracks for album ${albumName} with offset ${newOffset}`);
      
      await supabase.functions.invoke("sendToQueue", {
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
      
      console.log(`Successfully enqueued next batch with offset ${newOffset}`);
    }
    
    return { processed: processedCount };
  } catch (error) {
    console.error(`Failed to process tracks for album ${albumName}:`, error);
    throw error; // Re-throw to ensure proper error handling in the parent function
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
  if (!track.artists || !Array.isArray(track.artists) || track.artists.length === 0) {
    console.log(`Track has no artists or invalid artists array: ${JSON.stringify(track)}`);
    return false;
  }
  
  // The first artist in the array is typically considered the primary artist
  const isPrimary = track.artists[0].id === artistId;
  if (!isPrimary) {
    console.log(`Artist ${artistId} is not primary on track: ${track.name}`);
  }
  return isPrimary;
}
