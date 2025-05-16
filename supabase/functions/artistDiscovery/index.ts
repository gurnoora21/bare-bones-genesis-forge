import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { createEnhancedWorker } from "../_shared/enhancedQueueWorker.ts";
import { getQueueHelper } from "../_shared/queueHelper.ts";
import { SpotifyClient } from "../_shared/spotifyClient.ts";
import { getDeduplicationService } from "../_shared/deduplication.ts";

// Initialize clients
const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const supabase = createClient(supabaseUrl, supabaseKey);

const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

// Initialize services
const queueHelper = getQueueHelper(supabase, redis);
const deduplicationService = getDeduplicationService(redis);
const spotifyClient = new SpotifyClient(
  Deno.env.get("SPOTIFY_CLIENT_ID") || "",
  Deno.env.get("SPOTIFY_CLIENT_SECRET") || ""
);

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Define the artist worker implementation
class ArtistDiscoveryWorker extends createEnhancedWorker<any> {
  constructor() {
    super('artist_discovery', supabase, redis);
  }
  
  async processMessage(message: any): Promise<any> {
    console.log("Processing artist discovery message:", message);
    
    // Extract artist info
    const artistName = message.artistName;
    const artistId = message.artistId;
    
    if (!artistName && !artistId) {
      throw new Error("Message missing both artistName and artistId");
    }
    
    // Generate deduplication key
    const dedupKey = artistId 
      ? `artist:id:${artistId}` 
      : `artist:name:${artistName.toLowerCase()}`;
    
    // Check if already processed (extra safety check)
    const isDuplicate = await deduplicationService.isDuplicate(
      'artist_discovery', 
      dedupKey,
      { logDetails: true }
    );
    
    if (isDuplicate) {
      console.log(`Artist ${artistId || artistName} already processed, skipping`);
      return { status: 'skipped', reason: 'already_processed' };
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
        return { status: 'completed', result: 'no_spotify_results' };
      }
      
      spotifyArtist = searchResults.artists.items[0];
    }
    
    if (!spotifyArtist || !spotifyArtist.id) {
      throw new Error(`Failed to get valid artist data for ${artistId || artistName}`);
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
        onConflict: 'spotify_id',
        returning: 'minimal'
      })
      .select('id')
      .single();
    
    if (upsertError) {
      throw new Error(`Error upserting artist ${spotifyArtist.name}: ${upsertError.message}`);
    }
    
    const dbArtistId = savedArtist?.id;
    
    if (!dbArtistId) {
      throw new Error(`Failed to get artist ID after upsert for ${spotifyArtist.name}`);
    }
    
    console.log(`Successfully saved/updated artist ${spotifyArtist.name} (${dbArtistId})`);
    
    // Queue album discovery ONLY AFTER successfully inserting the artist
    // This way, if album queue fails, we still have the artist record
    try {
      const albumEnqueued = await queueHelper.enqueue('album_discovery', {
        artistId: dbArtistId,
        offset: 0
      });

      if (!albumEnqueued) {
        throw new Error(`Failed to queue album discovery for artist ${dbArtistId}`);
      }
      
      console.log(`Successfully queued album discovery for artist ${dbArtistId}`);
      
      // Only mark as processed AFTER successfully enqueuing albums
      // This ensures failures in queueing will be retried
      await deduplicationService.markAsProcessed(
        'artist_discovery', 
        dedupKey,
        86400, // 24 hour TTL
        { entityId: dbArtistId?.toString() }
      );
      
      return { 
        status: 'completed', 
        artistId: dbArtistId,
        spotifyId: spotifyArtist.id,
        name: spotifyArtist.name,
        albumEnqueued: true 
      };
    } catch (error) {
      console.error(`Error queueing album discovery: ${error.message}`);
      throw error; // Re-throw to ensure the message stays visible for retry
    }
  }
}

// Process a batch of artist discovery messages with self-draining
async function processArtistDiscovery() {
  console.log("Starting artist discovery batch processing");
  
  try {
    const worker = new ArtistDiscoveryWorker();
    
    // Process multiple batches within the time limit
    const result = await worker.drainQueue({
      maxBatches: 10,         // Process up to 10 batches in one invocation
      maxRuntimeMs: 240000,   // 4 minute runtime limit (below 5-min cron)
      batchSize: 5,           // 5 messages per batch
      processorName: 'artist-discovery',
      timeoutSeconds: 60,     // Timeout per message
      visibilityTimeoutSeconds: 900, // Increased to 15 minutes per fix #8
      logDetailedMetrics: true
    });
    
    return {
      processed: result.processed,
      errors: result.errors,
      duplicates: result.duplicates,
      skipped: result.skipped,
      processingTimeMs: result.processingTimeMs,
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
