
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { createEnhancedWorker } from "../_shared/enhancedQueueWorker.ts";
import { getGeniusClient } from "../_shared/geniusClient.ts";

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

// Define the producer worker implementation
const EnhancedWorker = createEnhancedWorker('producer_identification', supabase, redis);

class ProducerIdentificationWorker extends EnhancedWorker {
  async processMessage(message: any): Promise<any> {
    console.log("Processing producer identification message:", message);
    
    // Extract track info
    const { trackId, trackName, artistId } = message;
    
    if (!trackId) {
      throw new Error("Message missing trackId");
    }
    
    // Generate deduplication key
    const dedupKey = `producer:track:${trackId}`;
    
    // Get track details from database
    const { data: track, error: trackError } = await supabase
      .from('tracks')
      .select('id, name, spotify_id')
      .eq('id', trackId)
      .single();
    
    if (trackError || !track) {
      throw new Error(`Track not found with ID: ${trackId}`);
    }
    
    try {
      // Initialize genius client
      const genius = getGeniusClient();
      
      // Search for track on Genius
      const searchResults = await genius.search(trackName, track.name);
      
      if (!searchResults || !searchResults.hits || searchResults.hits.length === 0) {
        console.log(`No genius results found for track "${trackName}"`);
        return { status: 'completed', result: 'no_genius_results' };
      }
      
      // Get first result
      const songId = searchResults.hits[0].result.id;
      
      // Get detailed song info
      const songDetails = await genius.getSong(songId);
      
      if (!songDetails) {
        console.log(`No song details found for song ID ${songId}`);
        return { status: 'completed', result: 'no_song_details' };
      }
      
      // Extract producers
      const producers = genius.extractProducers(songDetails);
      
      // Save producers to database
      if (producers.length > 0) {
        console.log(`Found ${producers.length} producers for track ${trackName}`);
        
        // Process each producer
        for (const producer of producers) {
          // Check if producer already exists
          const { data: existingProducer } = await supabase
            .from('producers')
            .select('id')
            .eq('name', producer.name)
            .maybeSingle();
          
          let producerId;
          
          if (existingProducer) {
            producerId = existingProducer.id;
          } else {
            // Insert new producer
            const { data: newProducer, error: insertError } = await supabase
              .from('producers')
              .insert({
                name: producer.name,
                genius_id: producer.id,
                metadata: {
                  genius_url: producer.url,
                  image_url: producer.image_url,
                  updated_at: new Date().toISOString()
                }
              })
              .select('id')
              .single();
              
            if (insertError) {
              console.error(`Error inserting producer ${producer.name}:`, insertError);
              continue;
            }
            
            producerId = newProducer.id;
          }
          
          // Create track-producer relationship
          const { error: relationError } = await supabase
            .from('track_producers')
            .insert({
              track_id: trackId,
              producer_id: producerId,
              source: 'genius',
              confidence: producer.confidence || 0.8
            })
            .onConflict(['track_id', 'producer_id'])
            .ignore();
            
          if (relationError) {
            console.error(`Error creating track-producer relationship:`, relationError);
          }
          
          // Enqueue social enrichment for this producer
          // Note: We'll use Redis to deduplicate across workers
          const enrichmentKey = `enqueued:enrichment:${producerId}`;
          const alreadyEnqueued = await redis.get(enrichmentKey);
          
          if (!alreadyEnqueued) {
            // Enqueue social enrichment task
            const queueResult = await supabase.functions.invoke('sendToQueue', {
              body: {
                queue_name: 'social_enrichment',
                message: {
                  producerId: producerId,
                  producerName: producer.name
                },
                idempotency_key: `producer:${producerId}`
              }
            });
            
            if (queueResult.error) {
              console.error(`Failed to enqueue social enrichment:`, queueResult.error);
            } else {
              // Mark as enqueued in Redis
              await redis.set(enrichmentKey, 'true', { ex: 86400 }); // 24 hour TTL
            }
          }
        }
      } else {
        console.log(`No producers found for track ${trackName}`);
      }
      
      return {
        status: 'completed',
        trackId,
        producersCount: producers.length
      };
      
    } catch (error) {
      console.error(`Error processing track ${trackName}:`, error);
      throw error; // Re-throw for retry logic
    }
  }
}

// Process a batch of producer identification messages
async function processProducerIdentification() {
  console.log("Starting producer identification batch processing");
  
  try {
    const worker = new ProducerIdentificationWorker();
    
    // Process multiple batches within the time limit
    const result = await worker.processBatch({
      maxBatches: 5,
      batchSize: 3,
      processorName: 'producer-identification',
      timeoutSeconds: 60,
      visibilityTimeoutSeconds: 900, // 15 minutes
      logDetailedMetrics: true
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
    console.error("Fatal error in producer identification batch:", batchError);
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
    const resultPromise = processProducerIdentification();
    
    if (typeof EdgeRuntime !== 'undefined' && EdgeRuntime.waitUntil) {
      EdgeRuntime.waitUntil(resultPromise);
      
      // Return immediately with acknowledgment
      return new Response(
        JSON.stringify({ message: "Producer identification batch processing started" }),
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
    console.error("Error in producer identification handler:", error);
    
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
