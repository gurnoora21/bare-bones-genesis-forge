
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getQueueHelper } from "../_shared/queueHelper.ts";
import { getDeduplicationService } from "../_shared/deduplication.ts";
import { SpotifyClient } from "../_shared/spotifyClient.ts";

// Initialize clients
const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const supabase = createClient(supabaseUrl, supabaseKey);

const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

// Initialize the queue helper
const queueHelper = getQueueHelper(supabase, redis);

// Initialize deduplication service
const deduplicationService = getDeduplicationService(redis);

// Initialize Spotify client 
const spotifyClient = new SpotifyClient(
  Deno.env.get("SPOTIFY_CLIENT_ID") || "",
  Deno.env.get("SPOTIFY_CLIENT_SECRET") || ""
);

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Process a batch of album discovery messages
async function processAlbumDiscoveryBatch() {
  console.log("Starting album discovery batch processing");
  
  try {
    // Read messages from the queue
    const { data: messages, error } = await supabase.rpc(
      'pg_dequeue',
      { 
        queue_name: 'album_discovery',
        batch_size: 5,
        visibility_timeout: 300
      }
    );
    
    if (error) {
      console.error("Error reading from album_discovery queue:", error);
      return { error: error.message };
    }
    
    if (!messages || !Array.isArray(messages) || messages.length === 0) {
      console.log("No album discovery messages to process");
      return { processed: 0 };
    }
    
    console.log(`Processing ${messages.length} album discovery messages`);
    
    // Tracking metrics
    const results = {
      processed: 0,
      skipped: 0,
      errors: 0,
      albumsProcessed: 0,
      tracksQueued: 0,
      nextPagesQueued: 0
    };
    
    // Process each message
    for (const message of messages) {
      try {
        // Parse message
        let payload;
        try {
          payload = typeof message.message === 'string' 
            ? JSON.parse(message.message) 
            : message.message;
        } catch (parseError) {
          console.error(`Error parsing message ${message.id}:`, parseError);
          
          // Delete invalid message and continue
          await supabase.rpc('pg_delete_message', {
            queue_name: 'album_discovery',
            message_id: message.id
          });
          
          results.errors++;
          continue;
        }
        
        // Extract album job info
        const artistId = payload.artistId;
        const offset = payload.offset || 0;
        
        if (!artistId) {
          console.error(`Message ${message.id} missing artistId`);
          
          // Delete invalid message and continue
          await supabase.rpc('pg_delete_message', {
            queue_name: 'album_discovery',
            message_id: message.id
          });
          
          results.errors++;
          continue;
        }
        
        // Generate deduplication key
        const dedupKey = `artist:${artistId}:albums:offset:${offset}`;
        
        // Check if already processed
        const isDuplicate = await deduplicationService.isDuplicate(
          'album_discovery', 
          dedupKey,
          { logDetails: true },
          { entityId: artistId }
        );
        
        if (isDuplicate) {
          console.log(`Album page for artist ${artistId} at offset ${offset} already processed, skipping`);
          
          // Delete message for already processed album page
          await supabase.rpc('pg_delete_message', {
            queue_name: 'album_discovery',
            message_id: message.id
          });
          
          results.skipped++;
          continue;
        }
        
        // Get artist details from database
        const { data: artist, error: artistError } = await supabase
          .from('artists')
          .select('id, name, spotify_id')
          .eq('id', artistId)
          .single();
        
        if (artistError || !artist) {
          console.error(`Error fetching artist ${artistId}:`, artistError);
          
          // Delete message if artist doesn't exist (no point retrying)
          await supabase.rpc('pg_delete_message', {
            queue_name: 'album_discovery',
            message_id: message.id
          });
          
          results.errors++;
          continue;
        }
        
        if (!artist.spotify_id) {
          console.error(`Artist ${artistId} has no Spotify ID`);
          
          // Delete message if artist has no Spotify ID (can't fetch albums)
          await supabase.rpc('pg_delete_message', {
            queue_name: 'album_discovery',
            message_id: message.id
          });
          
          results.errors++;
          continue;
        }
        
        console.log(`Fetching albums for artist ${artist.name} (${artist.spotify_id}) at offset ${offset}`);
        
        // Get albums from Spotify
        const albumsData = await spotifyClient.getArtistAlbums(artist.spotify_id, {
          limit: 50,
          offset: offset,
          include_groups: "album,single,compilation"
        });
        
        if (!albumsData || !albumsData.items) {
          console.log(`No albums found for artist ${artist.name} at offset ${offset}`);
          
          // Mark as processed (no results is not an error)
          await deduplicationService.markAsProcessed(
            'album_discovery', 
            dedupKey,
            86400, // 24 hour TTL
            { entityId: artistId }
          );
          
          // Delete message - successfully processed with zero results
          await supabase.rpc('pg_delete_message', {
            queue_name: 'album_discovery',
            message_id: message.id
          });
          
          results.processed++;
          continue;
        }
        
        console.log(`Found ${albumsData.items.length} albums for artist ${artist.name}`);
        
        // Process each album
        for (const album of albumsData.items) {
          try {
            if (!album.id || !album.name) {
              console.warn(`Skipping album with missing data:`, album);
              continue;
            }
            
            // Prepare album data
            const albumData = {
              artist_id: artist.id,
              name: album.name,
              spotify_id: album.id,
              release_date: album.release_date || null,
              cover_url: album.images && album.images.length > 0 ? album.images[0].url : null,
              metadata: {
                album_type: album.album_type,
                total_tracks: album.total_tracks,
                spotify_url: album.external_urls?.spotify,
                label: album.label,
                copyrights: album.copyrights
              }
            };
            
            // Insert or update album
            const { data: savedAlbum, error: albumError } = await supabase
              .from('albums')
              .upsert(albumData, {
                onConflict: 'spotify_id',
                returning: 'minimal'
              })
              .select('id')
              .single();
            
            if (albumError) {
              console.error(`Error upserting album ${album.name}:`, albumError);
              continue; // Skip to next album, don't abort the whole batch
            }
            
            const dbAlbumId = savedAlbum?.id;
            
            if (!dbAlbumId) {
              console.error(`Failed to get album ID after upsert for ${album.name}`);
              continue;
            }
            
            console.log(`Successfully saved/updated album ${album.name} (${dbAlbumId})`);
            results.albumsProcessed++;
            
            // Queue track discovery for this album
            const trackEnqueued = await queueHelper.enqueue('track_discovery', {
              albumId: dbAlbumId,
              albumName: album.name,
              artistId: artist.id,
              offset: 0
            });
            
            if (trackEnqueued) {
              console.log(`Successfully queued track discovery for album ${dbAlbumId}`);
              results.tracksQueued++;
            } else {
              console.error(`Failed to queue track discovery for album ${dbAlbumId}`);
              // Don't fail the whole task if just track queue fails
            }
          } catch (albumError) {
            console.error(`Error processing album ${album.name}:`, albumError);
            // Continue to next album
          }
        }
        
        // Check if there are more albums to fetch (pagination)
        if (albumsData.next) {
          // Queue the next page
          const nextOffset = offset + albumsData.items.length;
          const nextPageKey = `artist:${artistId}:albums:offset:${nextOffset}`;
          
          const nextPageEnqueued = await queueHelper.enqueue('album_discovery', {
            artistId: artistId,
            offset: nextOffset
          }, nextPageKey);
          
          if (nextPageEnqueued) {
            console.log(`Queued next album page for artist ${artist.name} at offset ${nextOffset}`);
            results.nextPagesQueued++;
          } else {
            console.error(`Failed to queue next album page for artist ${artist.name}`);
            // Still consider the current page processed
          }
        }
        
        // Mark this page as processed
        await deduplicationService.markAsProcessed(
          'album_discovery', 
          dedupKey,
          86400, // 24 hour TTL
          { entityId: artistId }
        );
        
        // Delete the message from the queue
        await supabase.rpc('pg_delete_message', {
          queue_name: 'album_discovery',
          message_id: message.id
        });
        
        results.processed++;
      } catch (messageError) {
        console.error(`Error processing album message ${message.id}:`, messageError);
        
        // Don't delete message on error - allow retry
        results.errors++;
      }
    }
    
    console.log(`Album discovery batch completed: ${JSON.stringify(results)}`);
    return results;
  } catch (batchError) {
    console.error("Fatal error in album discovery batch:", batchError);
    return { error: batchError.message };
  }
}

// Handle HTTP requests
serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }
  
  try {
    // Process the batch in the background using EdgeRuntime.waitUntil
    const resultPromise = processAlbumDiscoveryBatch();
    
    if (typeof EdgeRuntime !== 'undefined' && EdgeRuntime.waitUntil) {
      EdgeRuntime.waitUntil(resultPromise);
      
      // Return immediately with acknowledgment
      return new Response(
        JSON.stringify({ message: "Album discovery batch processing started" }),
        { 
          headers: { 
            ...corsHeaders, 
            'Content-Type': 'application/json' 
          } 
        }
      );
    } else {
      // If EdgeRuntime.waitUntil is not available, wait for completion
      const result = await resultPromise;
      
      return new Response(
        JSON.stringify(result),
        { 
          headers: { 
            ...corsHeaders, 
            'Content-Type': 'application/json' 
          } 
        }
      );
    }
  } catch (error) {
    console.error("Error in album discovery handler:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        status: 500,
        headers: { 
          ...corsHeaders, 
          'Content-Type': 'application/json' 
        } 
      }
    );
  }
});
