
/**
 * Producer Identification Edge Function
 * 
 * Processes track messages to identify producers via Genius API
 * Uses enhanced worker base class for scaling and reliability
 */

import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { createEnhancedWorker } from "../_shared/enhancedQueueWorker.ts";

// Initialize Supabase client
const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const supabase = createClient(supabaseUrl, supabaseKey);

// Initialize Redis client
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

// Import Genius client factory
import { createEnhancedGeniusClient } from "../_shared/enhancedGeniusClient.ts";

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Producer Identification Worker class
class ProducerIdentificationWorker extends createEnhancedWorker<any> {
  private geniusClient: any;
  
  constructor() {
    super('producer_identification', supabase, redis);
    
    // Initialize the Genius client with API key
    this.geniusClient = createEnhancedGeniusClient(
      Deno.env.get("GENIUS_ACCESS_TOKEN") || ""
    );
  }
  
  // Validate message format
  validateMessage(message: any): boolean {
    return Boolean(message && (message.trackId || message.track_id));
  }
  
  // Create idempotency key for this operation
  createIdempotencyKey(message: any): string {
    return message._idempotencyKey || 
      `producer_identification:track:${message.trackId || message.track_id}`;
  }
  
  // Process a single message
  async processMessage(message: any): Promise<any> {
    console.log("Processing producer identification for track:", 
      message.trackId || message.track_id);
    
    // Get the normalized track ID
    const trackId = message.trackId || message.track_id;
    
    if (!trackId) {
      throw new Error("Message missing trackId");
    }
    
    // Acquire a direct DB lock for this track
    const lockAcquired = await this.acquireDirectDbLock(trackId);
    
    if (!lockAcquired) {
      console.log(`Track ${trackId} is already being processed, skipping`);
      return { 
        status: 'skipped', 
        reason: 'already_processing'
      };
    }
    
    try {
      // Fetch track details from database
      const { data: track, error } = await supabase
        .from('tracks')
        .select(`
          id, 
          name, 
          spotify_id, 
          albums!inner (
            id, 
            name, 
            artists!inner (
              id, 
              name
            )
          )
        `)
        .eq('id', trackId)
        .single();
      
      if (error || !track) {
        throw new Error(`Failed to fetch track ${trackId}: ${error?.message || 'Track not found'}`);
      }
      
      // Extract track and artist names
      const trackName = track.name;
      const artistName = track.albums?.artists?.[0]?.name;
      
      if (!trackName || !artistName) {
        throw new Error(`Missing track name or artist name for track ${trackId}`);
      }
      
      // Search for the song on Genius
      console.log(`Searching Genius for "${trackName} ${artistName}"`);
      const searchResults = await this.geniusClient.searchSong(`${trackName} ${artistName}`);
      
      // If no results found, log and return success with no producers
      if (!searchResults || !searchResults.hits || searchResults.hits.length === 0) {
        console.log(`No Genius results found for "${trackName} ${artistName}"`);
        return { 
          status: 'completed', 
          found: false,
          message: 'No Genius results found' 
        };
      }
      
      console.log(`Found ${searchResults.hits.length} potential matches on Genius`);
      
      // Take the first search result as the most likely match
      const firstHit = searchResults.hits[0];
      if (!firstHit || !firstHit.result || !firstHit.result.id) {
        console.log(`No valid hit in search results for "${trackName} ${artistName}"`);
        return { 
          status: 'completed', 
          found: false,
          message: 'Invalid search results structure' 
        };
      }
      
      // Get detailed song info to extract producer credits
      const songId = firstHit.result.id;
      console.log(`Fetching detailed info for Genius song ID: ${songId}`);
      const songDetails = await this.geniusClient.getSongById(songId);
      
      if (!songDetails || !songDetails.song) {
        console.log(`No song details returned from Genius API for ID ${songId}`);
        return { 
          status: 'completed', 
          found: false,
          message: 'Failed to get song details' 
        };
      }
      
      // Extract producers from song details
      const producers = songDetails.song.producer_artists || [];
      
      if (producers.length === 0) {
        console.log(`No producers found for "${trackName}" (ID: ${songId})`);
        return { 
          status: 'completed', 
          found: true,
          producers_found: 0,
          message: 'No producers listed on Genius' 
        };
      }
      
      console.log(`Found ${producers.length} producers for "${trackName}"`);
      
      // Process each producer
      const processedProducers = await this.processProducers(producers, trackId);
      
      return {
        status: 'completed',
        found: true,
        producers_found: producers.length,
        producers_processed: processedProducers.length,
        producers: processedProducers
      };
    } finally {
      // Always release the lock when done
      await this.releaseDirectDbLock(trackId);
    }
  }
  
  // Process a list of producers for a specific track
  private async processProducers(producers: any[], trackId: string): Promise<any[]> {
    const results = [];
    
    // Use transaction to ensure both producer insert and relationship are atomic
    const { data: transactionResult, error: transactionError } = await supabase.rpc(
      'execute_in_transaction',
      {
        p_sql: `
          WITH producer_inserts AS (
            INSERT INTO producers (name, normalized_name, metadata)
            VALUES 
              ${producers.map((p, i) => 
                `($${i*3 + 1}, $${i*3 + 2}, $${i*3 + 3})`
              ).join(', ')}
            ON CONFLICT (normalized_name) DO UPDATE SET
              metadata = COALESCE(producers.metadata, '{}'::jsonb) || EXCLUDED.metadata
            RETURNING id, name, normalized_name
          ),
          relation_inserts AS (
            INSERT INTO track_producers (track_id, producer_id, confidence, source)
            SELECT 
              $${producers.length*3 + 1}::uuid, 
              id, 
              0.9, 
              'genius'
            FROM producer_inserts
            ON CONFLICT (track_id, producer_id) DO NOTHING
            RETURNING producer_id
          )
          SELECT json_agg(pi.*) FROM producer_inserts pi
        `,
        p_params: [
          // Build the parameter array dynamically
          ...producers.flatMap(p => [
            p.name, 
            this.normalizeProducerName(p.name),
            JSON.stringify({
              genius_id: p.id,
              genius_url: p.url
            })
          ]),
          trackId
        ]
      }
    );
    
    if (transactionError) {
      throw new Error(`Failed to process producers: ${transactionError.message}`);
    }
    
    // Return the list of processed producers
    return transactionResult?.result || [];
  }
  
  // Normalize a producer name for matching
  private normalizeProducerName(name: string): string {
    return name.toLowerCase()
      .replace(/[^\w\s]/g, '') // Remove special characters
      .replace(/\s+/g, '');    // Remove spaces
  }
  
  // Acquire a direct DB lock for a track
  private async acquireDirectDbLock(trackId: string): Promise<boolean> {
    try {
      const { data, error } = await supabase.rpc('acquire_processing_lock', {
        p_entity_type: 'track',
        p_entity_id: trackId,
        p_timeout_minutes: 10,
        p_correlation_id: `producer_identification:${new Date().toISOString()}`
      });
      
      if (error) {
        console.error(`Error acquiring lock for track ${trackId}: ${error.message}`);
        return false;
      }
      
      return data === true;
    } catch (error) {
      console.error(`Exception acquiring lock for track ${trackId}: ${error.message}`);
      return false;
    }
  }
  
  // Release a direct DB lock for a track
  private async releaseDirectDbLock(trackId: string): Promise<boolean> {
    try {
      const { data, error } = await supabase.rpc('release_processing_lock', {
        p_entity_type: 'track',
        p_entity_id: trackId
      });
      
      if (error) {
        console.error(`Error releasing lock for track ${trackId}: ${error.message}`);
        return false;
      }
      
      return data === true;
    } catch (error) {
      console.error(`Exception releasing lock for track ${trackId}: ${error.message}`);
      return false;
    }
  }
}

// Process producer identification messages
async function processProducerIdentification(req: Request): Promise<any> {
  // Extract params from request if any
  let params: any = {};
  if (req.method === 'POST') {
    try {
      params = await req.json();
    } catch (error) {
      console.log("No valid JSON body provided");
    }
  }
  
  try {
    // Create worker instance
    const worker = new ProducerIdentificationWorker();
    
    // Process with self-draining batch
    const result = await worker.drainQueue({
      maxBatches: params.maxBatches || 3,  // Process up to 3 batches
      batchSize: params.batchSize || 5,    // 5 messages per batch
      maxRuntimeMs: 240000,                // 4 minute runtime max
      processorName: 'producer-identification',
      logDetailedMetrics: true,
      // Configure to use DLQ for failed messages
      deadLetterQueueName: 'producer_identification_dlq'
    });
    
    return {
      processed: result.processed,
      errors: result.errors,
      processingTimeMs: result.processingTimeMs,
      success: result.errors === 0
    };
  } catch (error) {
    console.error("Error processing producer identification:", error);
    return {
      error: error.message,
      success: false
    };
  }
}

// Handle HTTP requests
serve(async (req) => {
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }
  
  try {
    // Process the batch in the background using EdgeRuntime.waitUntil
    const resultPromise = processProducerIdentification(req);
    
    if (typeof EdgeRuntime !== 'undefined' && EdgeRuntime.waitUntil) {
      EdgeRuntime.waitUntil(resultPromise);
      
      // Return immediately with acknowledgment
      return new Response(
        JSON.stringify({ message: "Producer identification processing started" }),
        { 
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        }
      );
    } else {
      // If EdgeRuntime.waitUntil is not available, wait for completion
      const result = await resultPromise;
      
      return new Response(
        JSON.stringify(result),
        { 
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        }
      );
    }
  } catch (error) {
    console.error("Error in producer identification handler:", error);
    
    return new Response(
      JSON.stringify({ error: error.message, success: false }),
      { 
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        }
    );
  }
});
