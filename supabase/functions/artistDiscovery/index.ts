
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

// Process a batch of artist discovery messages
async function processArtistDiscoveryBatch() {
  console.log("Starting artist discovery batch processing");
  
  try {
    // Read messages from the queue
    const { data: messages, error } = await supabase.rpc(
      'pg_dequeue',
      { 
        queue_name: 'artist_discovery',
        batch_size: 5,
        visibility_timeout: 300
      }
    );
    
    if (error) {
      console.error("Error reading from artist_discovery queue:", error);
      return { error: error.message };
    }
    
    if (!messages || !Array.isArray(messages) || messages.length === 0) {
      console.log("No artist discovery messages to process");
      return { processed: 0 };
    }
    
    console.log(`Processing ${messages.length} artist discovery messages`);
    
    // Tracking metrics
    const results = {
      processed: 0,
      skipped: 0,
      errors: 0,
      albumsQueued: 0
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
            queue_name: 'artist_discovery',
            message_id: message.id
          });
          
          results.errors++;
          continue;
        }
        
        // Extract artist info
        const artistName = payload.artistName;
        const artistId = payload.artistId;
        
        if (!artistName && !artistId) {
          console.error(`Message ${message.id} missing both artistName and artistId`);
          
          // Delete invalid message and continue
          await supabase.rpc('pg_delete_message', {
            queue_name: 'artist_discovery',
            message_id: message.id
          });
          
          results.errors++;
          continue;
        }
        
        // Generate deduplication key
        const dedupKey = artistId 
          ? `artist:id:${artistId}` 
          : `artist:name:${artistName.toLowerCase()}`;
        
        // Check if already processed
        const isDuplicate = await deduplicationService.isDuplicate(
          'artist_discovery', 
          dedupKey,
          { logDetails: true }
        );
        
        if (isDuplicate) {
          console.log(`Artist ${artistId || artistName} already processed, skipping`);
          
          // Delete message for already processed artist
          await supabase.rpc('pg_delete_message', {
            queue_name: 'artist_discovery',
            message_id: message.id
          });
          
          results.skipped++;
          continue;
        }
        
        // Get artist details from Spotify
        let spotifyArtist;
        
        if (artistId) {
          console.log(`Fetching artist by ID: ${artistId}`);
          spotifyArtist = await spotifyClient.getArtist(artistId);
        } else {
          console.log(`Searching for artist: ${artistName}`);
          const searchResults = await spotifyClient.searchArtist(artistName);
          
          if (!searchResults || !searchResults.artists || searchResults.artists.items.length === 0) {
            console.log(`No Spotify results found for artist "${artistName}"`);
            
            // Delete message but count as processed (not an error)
            await supabase.rpc('pg_delete_message', {
              queue_name: 'artist_discovery',
              message_id: message.id
            });
            
            results.processed++;
            continue;
          }
          
          spotifyArtist = searchResults.artists.items[0];
        }
        
        if (!spotifyArtist || !spotifyArtist.id) {
          console.error(`Failed to get valid artist data for ${artistId || artistName}`);
          
          // Delete message and count as error
          await supabase.rpc('pg_delete_message', {
            queue_name: 'artist_discovery',
            message_id: message.id
          });
          
          results.errors++;
          continue;
        }
        
        // Convert genres array to JSONB-friendly format if present
        const metadata = {
          spotify_url: spotifyArtist.external_urls?.spotify,
          spotify_uri: spotifyArtist.uri,
          genres: spotifyArtist.genres || []
        };
        
        // Insert or update artist in database
        const { data: savedArtist, error: upsertError } = await supabase
          .from('artists')
          .upsert({
            name: spotifyArtist.name,
            spotify_id: spotifyArtist.id,
            followers: spotifyArtist.followers?.total || 0,
            popularity: spotifyArtist.popularity || 0,
            image_url: spotifyArtist.images && spotifyArtist.images.length > 0 
              ? spotifyArtist.images[0].url 
              : null,
            metadata
          }, {
            onConflict: 'spotify_id', // Only one record per Spotify artist
            returning: 'minimal'
          })
          .select('id')
          .single();
        
        if (upsertError) {
          console.error(`Error upserting artist ${spotifyArtist.name}:`, upsertError);
          
          // Don't delete message on DB error - allow retry
          results.errors++;
          continue;
        }
        
        const dbArtistId = savedArtist?.id;
        
        if (!dbArtistId) {
          console.error(`Failed to get artist ID after upsert for ${spotifyArtist.name}`);
          
          // Don't delete message - allow retry
          results.errors++;
          continue;
        }
        
        console.log(`Successfully saved/updated artist ${spotifyArtist.name} (${dbArtistId})`);
        
        // Queue album discovery ONLY AFTER successfully inserting the artist
        // This way, if album queue fails, we still have the artist record
        const albumEnqueued = await queueHelper.enqueue('album_discovery', {
          artistId: dbArtistId,
          offset: 0
        });
        
        if (albumEnqueued) {
          console.log(`Successfully queued album discovery for artist ${dbArtistId}`);
          results.albumsQueued++;
        } else {
          console.error(`Failed to queue album discovery for artist ${dbArtistId}`);
          // Don't fail the whole task if just album queue fails
        }
        
        // Mark artist as processed ONLY AFTER successfully enqueuing albums
        // This ensures failed album enqueues will be retried when the artist is reprocessed
        await deduplicationService.markAsProcessed(
          'artist_discovery', 
          dedupKey,
          86400, // 24 hour TTL
          { entityId: dbArtistId?.toString() }
        );
        
        // Success - delete the message from the queue
        await supabase.rpc('pg_delete_message', {
          queue_name: 'artist_discovery',
          message_id: message.id
        });
        
        results.processed++;
      } catch (messageError) {
        console.error(`Error processing artist message ${message.id}:`, messageError);
        
        // Don't delete message on error - allow retry
        results.errors++;
      }
    }
    
    console.log(`Artist discovery batch completed: ${JSON.stringify(results)}`);
    return results;
  } catch (batchError) {
    console.error("Fatal error in artist discovery batch:", batchError);
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
    const resultPromise = processArtistDiscoveryBatch();
    
    if (typeof EdgeRuntime !== 'undefined' && EdgeRuntime.waitUntil) {
      EdgeRuntime.waitUntil(resultPromise);
      
      // Return immediately with acknowledgment
      return new Response(
        JSON.stringify({ message: "Artist discovery batch processing started" }),
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
    console.error("Error in artist discovery handler:", error);
    
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
