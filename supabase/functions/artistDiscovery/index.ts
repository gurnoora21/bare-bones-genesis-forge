import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getSpotifyClient } from "../_shared/spotifyClient.ts";
import { createEnhancedWorker } from "../_shared/enhancedQueueWorker.ts";
import { StructuredLogger } from "../_shared/structuredLogger.ts";
import { EnhancedWorkerBase } from "../_shared/enhancedWorkerBase.ts";
import { QueueHelper, getQueueHelper } from "../_shared/queueHelper.ts";
import { safeStringify, logDebug } from "../_shared/debugHelper.ts";

// Initialize Redis client
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

// Initialize Supabase client
const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const supabase = createClient(supabaseUrl, supabaseKey);

// Initialize QueueHelper
const queueHelper = getQueueHelper(supabase, redis);

// Common CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Define the artist worker implementation
class ArtistDiscoveryWorker extends EnhancedWorkerBase {
  constructor() {
    super('artist_discovery', supabase, 'ArtistDiscovery');
  }

  /**
   * Process an artist discovery message
   */
  async processMessage(message: any): Promise<any> {
    const logger = new StructuredLogger({ service: 'artist_discovery' });
    
    // Handle different message formats more robustly
    let artistMessage = message;
    
    // Extract the message content from different possible formats
    if (message.message) {
      if (typeof message.message === 'object') {
        artistMessage = message.message;
      } else if (typeof message.message === 'string') {
        try {
          artistMessage = JSON.parse(message.message);
        } catch (e) {
          logger.warn("Failed to parse message string as JSON", { error: e.message });
          // Keep original message
        }
      }
    }
    
    // Log the extracted message for debugging
    logger.debug("Processing with extracted message", { extractedMessage: artistMessage });
    
    // Ensure we have the artistName
    if (!artistMessage?.artistName) {
      logger.error("Missing artistName in message", { message: artistMessage });
      throw new Error("Missing artistName in message");
    }
    
    const artistName = artistMessage.artistName;
    logger.info("Processing artist discovery message", { artistName });
    
    // Generate deduplication key with consistent format: artist_discovery:artist:name:{normalized name}
    const dedupKey = `artist_discovery:artist:name:${artistName.toLowerCase()}`;
    
    try {
      // Get Spotify client
      const spotifyClient = getSpotifyClient();
      
      // Lookup artist on Spotify
      const searchResponse = await spotifyClient.getArtistByName(artistName);
      
      if (!searchResponse || !searchResponse.artists || !searchResponse.artists.items || searchResponse.artists.items.length === 0) {
        logger.info(`No results found for artist "${artistName}"`);
        return { status: 'completed', result: 'no_results' };
      }
      
      // Get the first result
      const artist = searchResponse.artists.items[0];
      
      // Check if artist already exists in database
      const { data: existingArtist } = await this.supabase
        .from('artists')
        .select('id')
        .eq('spotify_id', artist.id)
        .maybeSingle();
      
      let artistId;
      
      if (existingArtist) {
        logger.info(`Artist ${artist.name} already exists, updating`);
        artistId = existingArtist.id;
        
        // Update the artist
        await this.supabase
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
        logger.info(`Creating new artist: ${artist.name}`);
        
        // Insert the artist
        const { data: newArtist, error: insertError } = await this.supabase
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
          logger.error(`Error inserting artist ${artist.name}:`, insertError);
          throw insertError;
        }
        
        artistId = newArtist.id;
      }
      
      // Enqueue album discovery for this artist using multiple approaches
      // with robust error handling to ensure it gets enqueued
      
      // Create a consistent deduplication key for album discovery
      const albumDedupKey = `album_discovery:artist:${artist.id}:offset:0`;
      logger.info(`Enqueueing album discovery for artist ${artist.name} (${artist.id})`);
      
      // Log the exact dedup key being used
      logDebug("AlbumDiscovery", "Using deduplication key", albumDedupKey);
      
      // Prepare message with more details for better debugging
      const albumMessage = {
        spotifyId: artist.id,
        artistName: artist.name,
        artistId: artistId,
        enqueueTime: new Date().toISOString(),
        requestId: `album-discovery-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`
      };
      
      let enqueueSuccess = false;
      
      // Approach 1: Try using the QueueHelper
      try {
        const enqueueResult = await queueHelper.enqueue(
          'album_discovery',
          albumMessage,
          albumDedupKey,
          { ttl: 86400 * 7 } // 7 day TTL to avoid duplicate processing
        );
        
        if (enqueueResult) {
          logger.info(`Album discovery enqueued successfully`, { 
            artistName: artist.name,
            artistId: artist.id,
            messageId: enqueueResult
          });
          
          enqueueSuccess = true;
        }
      } catch (enqueueError) {
        logger.error(`Failed to enqueue album discovery via queueHelper:`, {
          error: enqueueError.message,
          artistName: artist.name,
          artistId: artist.id,
          stack: enqueueError.stack
        });
        // Continue to next approach
      }
      
      // Approach 2: If QueueHelper failed, try direct database function
      if (!enqueueSuccess) {
        logger.warn(`Trying direct database method for album discovery enqueue`, {
          artistName: artist.name,
          artistId: artist.id
        });
        
        try {
          // Try direct pg_enqueue function
          const { data: pgData, error: pgError } = await this.supabase.rpc('pg_enqueue', {
            queue_name: 'album_discovery',
            message_body: albumMessage
          });
          
          if (pgError) {
            logger.error(`pg_enqueue failed:`, {
              error: pgError.message,
              artistName: artist.name,
              artistId: artist.id
            });
          } else {
            logger.info(`Album discovery enqueued via pg_enqueue`, {
              artistName: artist.name,
              artistId: artist.id,
              messageId: pgData
            });
            
            enqueueSuccess = true;
          }
        } catch (pgError) {
          logger.error(`Exception in pg_enqueue:`, {
            error: pgError.message,
            artistName: artist.name,
            artistId: artist.id
          });
          // Continue to next approach
        }
      }
      
      // Approach 3: Last resort - use raw SQL
      if (!enqueueSuccess) {
        logger.warn(`Trying raw SQL for album discovery enqueue`, {
          artistName: artist.name,
          artistId: artist.id
        });
        
        try {
          // Use raw SQL as the final fallback
          const { data: sqlData, error: sqlError } = await this.supabase.rpc('raw_sql_query', {
            sql_query: `SELECT pgmq.send($1, $2::jsonb) AS msg_id`,
            params: JSON.stringify(['album_discovery', JSON.stringify(albumMessage)])
          });
          
          if (sqlError) {
            logger.error(`Raw SQL enqueue failed:`, {
              error: sqlError.message,
              artistName: artist.name,
              artistId: artist.id
            });
          } else {
            logger.info(`Album discovery enqueued via raw SQL`, {
              artistName: artist.name,
              artistId: artist.id,
              result: sqlData
            });
            
            enqueueSuccess = true;
          }
        } catch (sqlError) {
          logger.error(`Exception in raw SQL enqueue:`, {
            error: sqlError.message,
            artistName: artist.name,
            artistId: artist.id
          });
        }
      }
      
      // Log final enqueue status
      if (!enqueueSuccess) {
        logger.error(`Failed to enqueue album discovery after all attempts`, {
          artistName: artist.name,
          artistId: artist.id
        });
      }
      
      // Return successful completion even if album enqueue failed
      // We don't want to redo artist discovery just because album discovery couldn't be enqueued
      return { 
        status: 'completed',
        result: 'success',
        artistId,
        spotifyId: artist.id,
        albumEnqueued: enqueueSuccess
      };
    } catch (error) {
      logger.error(`Error processing artist ${artistName}:`, error);
      throw error;
    }
  }

  /**
   * Required implementation of handleMessage from EnhancedWorkerBase
   */
  async handleMessage(message: any, logger: StructuredLogger): Promise<any> {
    return this.processMessage(message);
  }
}

// Process a batch of artist discovery messages
async function processArtistDiscovery() {
  const executionId = `exec_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
  console.log(`[${executionId}] Starting artist discovery batch processing`);
  
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
    
    console.log(`[${executionId}] Total messages processed: ${result.processed}, errors: ${result.errors}, duplicates: ${result.duplicates || 0}, skipped: ${result.skipped || 0}`);
    
    return {
      processed: result.processed,
      errors: result.errors,
      duplicates: result.duplicates || 0,
      skipped: result.skipped || 0,
      processingTimeMs: result.processingTimeMs || 0,
      success: result.errors === 0
    };
  } catch (batchError) {
    console.error(`[${executionId}] Fatal error in artist discovery batch:`, batchError);
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
