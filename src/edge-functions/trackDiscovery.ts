
import { serve } from "https://deno.land/std/http/server.ts";
import { createClient } from "@supabase/supabase-js";
import { SpotifyClient } from "../lib/SpotifyClient.ts";

interface TrackDiscoveryMsg {
  albumId: string;
  albumName: string;
  artistId: string;
  offset?: number;
}

serve(async (req) => {
  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );

  // Initialize Spotify client
  const spotifyClient = new SpotifyClient(
    Deno.env.get("SPOTIFY_CLIENT_ID")!,
    Deno.env.get("SPOTIFY_CLIENT_SECRET")!
  );

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

  // Process messages with background tasks
  const promises = messages.map(async (message) => {
    const msg = message.message as TrackDiscoveryMsg;
    const messageId = message.id;
    
    try {
      await processTracks(supabase, spotifyClient, msg);
      // Archive processed message
      await supabase.functions.invoke("delete-from-queue", {
        body: { queue_name: "track_discovery", message_id: messageId }
      });
    } catch (error) {
      console.error(`Error processing track message:`, error);
      // Message will return to queue after visibility timeout
    }
  });

  // Wait for all background tasks in a background process
  if (typeof EdgeRuntime !== 'undefined') {
    EdgeRuntime.waitUntil(Promise.all(promises));
  } else {
    await Promise.all(promises);
  }
  
  return new Response(JSON.stringify({ processed: messages.length }));
});

async function processTracks(supabase, spotifyClient: SpotifyClient, msg: TrackDiscoveryMsg) {
  const { albumId, albumName, artistId, offset = 0 } = msg;
  
  // Get album's database ID
  const { data: album, error: albumError } = await supabase
    .from('albums')
    .select('id')
    .eq('spotify_id', albumId)
    .single();

  if (albumError || !album) {
    throw new Error(`Album ${albumId} not found in database`);
  }

  // Get artist's database ID for normalized tracks
  const { data: artist, error: artistError } = await supabase
    .from('artists')
    .select('id')
    .eq('spotify_id', artistId)
    .single();

  if (artistError || !artist) {
    throw new Error(`Artist ${artistId} not found in database`);
  }

  // Fetch tracks from Spotify
  const tracks = await spotifyClient.getAlbumTracks(albumId, offset);
  console.log(`Found ${tracks.items.length} tracks in album ${albumName}`);

  // Filter and normalize tracks
  const tracksToProcess = tracks.items.filter(track => 
    isArtistPrimaryOnTrack(track, artistId)
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
  if (tracks.items.length > 0 && offset + tracks.items.length < tracks.total) {
    const newOffset = offset + tracks.items.length;
    await supabase.functions.invoke("send-to-queue", {
      body: {
        queue_name: "track_discovery",
        message: { albumId, albumName, artistId, offset: newOffset }
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

function isArtistPrimaryOnTrack(track, artistId: string): boolean {
  return track.artists && 
         track.artists.length > 0 && 
         track.artists[0].id === artistId;
}

// Create a specific SpotifyClient wrapper for Edge Functions
class SpotifyClient {
  private token: string | null = null;
  private tokenExpiry: number = 0;
  private clientId: string;
  private clientSecret: string;

  constructor(clientId: string, clientSecret: string) {
    this.clientId = clientId;
    this.clientSecret = clientSecret;
  }

  private async getToken(): Promise<string> {
    if (this.token && this.tokenExpiry > Date.now()) {
      return this.token;
    }

    const response = await fetch('https://accounts.spotify.com/api/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': `Basic ${btoa(`${this.clientId}:${this.clientSecret}`)}`
      },
      body: 'grant_type=client_credentials'
    });

    if (!response.ok) {
      throw new Error(`Failed to get Spotify token: ${response.statusText}`);
    }

    const data = await response.json();
    this.token = data.access_token;
    this.tokenExpiry = Date.now() + (data.expires_in * 1000) - 60000; // Subtract 1 minute as buffer
    return this.token;
  }

  private async apiRequest<T>(url: string): Promise<T> {
    const token = await this.getToken();
    const headers = { 'Authorization': `Bearer ${token}` };
    
    // Handle rate limiting with retries
    let retries = 0;
    const maxRetries = 3;
    
    while (retries <= maxRetries) {
      try {
        const response = await fetch(url, { headers });
        
        // Handle rate limiting
        if (response.status === 429) {
          const retryAfter = parseInt(response.headers.get('Retry-After') || '1', 10);
          await new Promise(r => setTimeout(r, retryAfter * 1000));
          retries++;
          continue;
        }
        
        if (!response.ok) {
          throw new Error(`Spotify API error: ${response.status} ${response.statusText}`);
        }
        
        return await response.json();
      } catch (error) {
        if (retries >= maxRetries) throw error;
        
        // Exponential backoff
        await new Promise(r => setTimeout(r, 1000 * Math.pow(2, retries)));
        retries++;
      }
    }
    
    throw new Error('Maximum retries exceeded');
  }

  async getAlbumTracks(albumId: string, offset = 0, limit = 50): Promise<any> {
    return this.apiRequest(
      `https://api.spotify.com/v1/albums/${albumId}/tracks?limit=${limit}&offset=${offset}`
    );
  }

  async getTrackDetails(trackIds: string[]): Promise<any[]> {
    if (trackIds.length === 0) return [];
    
    // Handle up to 50 tracks at once (Spotify API limit)
    if (trackIds.length === 1) {
      const track = await this.apiRequest(`https://api.spotify.com/v1/tracks/${trackIds[0]}`);
      return [track];
    }
    
    const response = await this.apiRequest(
      `https://api.spotify.com/v1/tracks?ids=${trackIds.join(',')}`
    );
    
    return response.tracks || [];
  }
}
