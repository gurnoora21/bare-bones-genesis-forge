
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getSpotifyClient } from "../_shared/spotifyClient.ts";
import { createEnhancedWorker } from "../_shared/enhancedQueueWorker.ts";

// Initialize Redis client
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

// Initialize Supabase client
const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const supabase = createClient(supabaseUrl, supabaseKey);

// Common CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Define the artist worker implementation
const EnhancedWorker = createEnhancedWorker('artist_discovery', supabase, redis);

class ArtistDiscoveryWorker extends EnhancedWorker {
  async processMessage(message: any): Promise<any> {
    console.log("Processing artist discovery message:", message);
    
    // Extract artist name
    const artistName = message.artistName;
    
    if (!artistName) {
      throw new Error("Message missing artistName");
    }
    
    // Get Spotify client
    const spotifyClient = getSpotifyClient();
    
    // Generate deduplication key
    const dedupKey = `artist_discovery:artist:name:${artistName.toLowerCase()}`;
    
    // Lookup artist on Spotify using the correct function name
    const searchResponse = await spotifyClient.getArtistByName(artistName);
    
    if (!searchResponse || !searchResponse.artists || !searchResponse.artists.items || searchResponse.artists.items.length === 0) {
      console.log(`No results found for artist "${artistName}"`);
      return { status: 'completed', result: 'no_results' };
    }
    
    // Get the first result
    const artist = searchResponse.artists.items[0];
    
    // Process artist details
    try {
      // Check if artist already exists in database
      const { data: existingArtist } = await supabase
        .from('artists')
        .select('id')
        .eq('spotify_id', artist.id)
        .maybeSingle();
      
      let artistId;
      
      if (existingArtist) {
        console.log(`Artist ${artist.name} already exists, updating`);
        artistId = existingArtist.id;
        
        // Update the artist
        await supabase
          .from('artists')
          .update({
            name: artist.name,
            followers: artist.followers?.total || 0,
            popularity: artist.popularity || 0,
            image_url: artist.images?.[0]?.url,
            metadata: {
              genres: artist.genres,
              updated_at: new Date().toISOString()
            }
          })
          .eq('id', artistId);
      } else {
        console.log(`Creating new artist: ${artist.name}`);
        
        // Insert the artist
        const { data: newArtist, error: insertError } = await supabase
          .from('artists')
          .insert({
            name: artist.name,
            spotify_id: artist.id,
            followers: artist.followers?.total || 0,
            popularity: artist.popularity || 0,
            image_url: artist.images?.[0]?.url,
            metadata: {
              genres: artist.genres,
              created_at: new Date().toISOString()
            }
          })
          .select('id')
          .single();
          
        if (insertError) {
          console.error(`Error inserting artist ${artist.name}:`, insertError);
          throw insertError;
        }
        
        artistId = newArtist.id;
      }
      
      // Enqueue album discovery for this artist
      console.log(`Enqueueing album discovery for artist ${artist.name} (${artistId})`);
      
      const queueResult = await supabase.functions.invoke('sendToQueue', {
        body: {
          queue_name: 'album_discovery',
          message: {
            artistId: artistId,
            spotifyArtistId: artist.id,
            artistName: artist.name
          },
          idempotency_key: `album_discovery:artist:${artistId}`
        }
      });
      
      if (queueResult.error) {
        console.error(`Failed to enqueue album discovery:`, queueResult.error);
      }
      
      return {
        status: 'completed',
        artistId,
        spotifyArtistId: artist.id,
        name: artist.name
      };
      
    } catch (error) {
      console.error(`Error processing artist ${artist.name}:`, error);
      throw error; // Re-throw for retry logic
    }
  }
}

// Process a batch of artist discovery messages
async function processArtistDiscovery() {
  console.log("Starting artist discovery batch processing");
  
  try {
    const worker = new ArtistDiscoveryWorker();
    
    // Process multiple batches within the time limit
    const result = await worker.processBatch({
      maxBatches: 1,
      batchSize: 5,
      processorName: 'artist-discovery',
      timeoutSeconds: 50,
      visibilityTimeoutSeconds: 900, // 15 minutes
      logDetailedMetrics: true,
      sendToDlqOnMaxRetries: true,
      maxRetries: 3,
      deadLetterQueue: 'artist_discovery_dlq'
    });
    
    return {
      processed: result.processed,
      errors: result.errors,
      duplicates: result.duplicates || 0,
      skipped: result.skipped || 0,
      processingTimeMs: result.processingTimeMs || 0,
      success: result.errors === 0
    };
  } catch (batchError) {
    console.error("Fatal error in artist discovery batch:", batchError);
    return { 
      error: batchError.message,
      success: false
    };
  }
}

// Handle HTTP requests
serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }
  
  try {
    // Process the batch in the background using EdgeRuntime.waitUntil
    const resultPromise = processArtistDiscovery();
    
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
      JSON.stringify({ error: error.message, success: false }),
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
