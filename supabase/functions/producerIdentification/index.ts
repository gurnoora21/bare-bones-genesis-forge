import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { GeniusClient } from "../_shared/geniusClient.ts";
import { 
  deleteMessageWithRetries, 
  logWorkerIssue,
  checkTrackProcessed,
  processQueueMessageSafely,
  acquireProcessingLock,
  safeMessageIdString
} from "../_shared/queueHelper.ts";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

// Initialize Redis client for distributed locking and caching
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

interface ProducerIdentificationMsg {
  trackId: string;
  trackName: string;
  albumId: string;
  artistId: string;
}

interface ProducerCandidate {
  name: string;
  confidence: number;
  source: string;
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
    console.log("Starting producer identification process");
    
    // Process queue batch
    const { data: messages, error } = await supabase.functions.invoke("readQueue", {
      body: { 
        queue_name: "producer_identification",
        batch_size: 5,
        visibility_timeout: 300 // 5 minutes
      }
    });

    if (error) {
      console.error("Error reading from queue:", error);
      await logWorkerIssue(
        supabase,
        "producerIdentification", 
        "queue_error", 
        "Error reading from queue", 
        { error }
      );
      return new Response(JSON.stringify({ error }), { status: 500, headers: corsHeaders });
    }

    if (!messages || messages.length === 0) {
      return new Response(JSON.stringify({ processed: 0, message: "No messages to process" }), { headers: corsHeaders });
    }
    
    console.log(`Found ${messages.length} messages to process in producer_identification queue`);
    
    // Create a quick response to avoid timeout
    const response = new Response(JSON.stringify({ 
      processing: true, 
      message_count: messages.length 
    }), { 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
    });

    // Initialize the Genius client
    const geniusClient = new GeniusClient();

    // Process messages with background tasks
    EdgeRuntime.waitUntil((async () => {
      // Track overall metrics
      let successCount = 0;
      let errorCount = 0;
      const processingResults = [];
      
      try {
        // Process messages in sequence to avoid overwhelming external APIs
        for (const message of messages) {
          try {
            // Ensure the message is properly typed
            console.log(`Raw producer message: ${JSON.stringify(message)}`);
            
            let msg: ProducerIdentificationMsg;
            if (typeof message.message === 'string') {
              msg = JSON.parse(message.message) as ProducerIdentificationMsg;
            } else {
              msg = message.message as ProducerIdentificationMsg;
            }
            
            // Safely get the message ID 
            const messageId = safeMessageIdString(message.id);
            console.log(`Processing producer identification for track: ${msg.trackName} (${msg.trackId})`);
            
            // Use our safer processing function with idempotency check
            const success = await processQueueMessageSafely(
              supabase,
              "producer_identification",
              messageId,
              async () => await identifyProducers(supabase, geniusClient, msg, redis),
              `track_producers:${msg.trackId}`,
              async () => await checkTrackProcessed(redis, msg.trackId, "producer_identification"),
              { maxRetries: 2, circuitBreaker: true }
            );
            
            if (success) {
              console.log(`Successfully processed producer identification message ${messageId}`);
              successCount++;
            } else {
              console.warn(`Message ${messageId} was not successfully processed`);
              errorCount++;
            }
            
            processingResults.push({
              messageId,
              trackId: msg.trackId,
              trackName: msg.trackName,
              success
            });
          } catch (messageError) {
            console.error(`Error parsing producer message:`, messageError);
            await logWorkerIssue(
              supabase,
              "producerIdentification", 
              "message_error", 
              `Error parsing message: ${messageError.message}`, 
              { 
                message, 
                error: messageError.message 
              }
            );
            errorCount++;
          }
        }
      } catch (batchError) {
        console.error("Error in batch processing producers:", batchError);
        await logWorkerIssue(
          supabase,
          "producerIdentification", 
          "batch_error", 
          `Batch processing error: ${batchError.message}`, 
          { error: batchError.stack }
        );
      } finally {
        // Record metrics
        try {
          await supabase.from('queue_metrics').insert({
            queue_name: "producer_identification",
            operation: "batch_processing",
            started_at: new Date().toISOString(),
            finished_at: new Date().toISOString(),
            processed_count: messages.length,
            success_count: successCount,
            error_count: errorCount,
            details: { results: processingResults }
          });
        } catch (metricsError) {
          console.error("Failed to record metrics:", metricsError);
        }
        
        console.log(`Completed producer identification background processing: ${successCount} successful, ${errorCount} failed`);
      }
    })());
    
    return response;
  } catch (error) {
    console.error("Unexpected error in producer identification worker:", error);
    await logWorkerIssue(
      supabase,
      "producerIdentification", 
      "fatal_error", 
      `Unexpected error: ${error.message}`,
      { stack: error.stack }
    );
    
    return new Response(JSON.stringify({ error: error.message }), 
      { status: 500, headers: corsHeaders });
  }
});

// Keep existing implementation of identifyProducers function
async function identifyProducers(
  supabase: any, 
  geniusClient: any,
  msg: ProducerIdentificationMsg,
  redis: Redis
) {
  const { trackId, trackName, albumId, artistId } = msg;
  
  try {
    // Acquire lock for this track to prevent concurrent processing
    const hasLock = await acquireProcessingLock('track_producers', trackId);
    
    if (!hasLock) {
      console.log(`Another process is handling producers for track ${trackName} (${trackId}), skipping`);
      return { skipped: true, reason: "concurrent_processing" };
    }
    
    // Mark as being processed in Redis
    try {
      const processingKey = `processing:track_producers:${trackId}`;
      await redis.set(processingKey, 'true', { ex: 300 }); // 5 minute TTL
    } catch (redisError) {
      console.warn("Failed to set processing flag in Redis:", redisError);
    }
    
    // Quick double-check if track is already processed
    // FIX: Updated to pass redis client and proper queue name
    const alreadyProcessed = await checkTrackProcessed(redis, trackId, "producer_identification");
    if (alreadyProcessed) {
      console.log(`Track ${trackName} (${trackId}) already has producers, skipping`);
      return { skipped: true, reason: "already_processed" };
    }
    
    // Get track details from database
    const { data: trackData, error: trackError } = await supabase
      .from('tracks')
      .select('id, name, metadata')
      .eq('id', trackId)
      .single();

    if (trackError || !trackData) {
      const errMsg = `Track not found with ID: ${trackId}`;
      console.error(errMsg);
      throw new Error(errMsg);
    }

    // Get artist details for Genius search
    const { data: artistData, error: artistError } = await supabase
      .from('artists')
      .select('id, name')
      .eq('id', artistId)
      .single();

    if (artistError || !artistData) {
      const errMsg = `Artist not found with ID: ${artistId}`;
      console.error(errMsg);
      throw new Error(errMsg);
    }

    // Extract producers from multiple sources
    const producers: ProducerCandidate[] = [];
    
    // 1. Extract from Spotify metadata if available
    if (trackData.metadata) {
      // Look for producer credits in metadata
      try {
        if (trackData.metadata.producers) {
          trackData.metadata.producers.forEach((producer: string) => {
            producers.push({
              name: producer,
              confidence: 0.9,
              source: 'spotify_metadata'
            });
          });
        }
        
        // Extract from artists list (collaborators)
        if (trackData.metadata.artists) {
          const collaborators = trackData.metadata.artists.filter((artist: any) => 
            artist.id !== artistId // Skip the main artist
          );
          
          collaborators.forEach((artist: any) => {
            producers.push({
              name: artist.name,
              confidence: 0.7, // Lower confidence for mere collaborators
              source: 'spotify_collaboration'
            });
          });
        }
        
        // Look for producer information in track credits
        if (trackData.metadata.credits) {
          const producerCredits = trackData.metadata.credits.filter((credit: any) => 
            credit.role?.toLowerCase().includes('produc') || 
            credit.role?.toLowerCase().includes('beat') ||
            credit.role?.toLowerCase().includes('instrumental')
          );
          
          producerCredits.forEach((credit: any) => {
            producers.push({
              name: credit.name,
              confidence: 0.85,
              source: 'spotify_credits'
            });
          });
        }
      } catch (error) {
        console.warn('Error extracting Spotify producers:', error);
      }
    }
    
    // 2. Search Genius for producer information
    try {
      // Use Redis cache to avoid duplicate Genius API calls
      const geniusKey = `genius:${trackName}:${artistData.name}`;
      let songDetails: any = null;
      
      try {
        const cachedData = await redis.get(geniusKey);
        if (cachedData) {
          songDetails = JSON.parse(cachedData);
          console.log(`Using cached Genius data for ${trackName}`);
        }
      } catch (redisError) {
        console.warn(`Redis cache get failed for Genius data:`, redisError);
      }
      
      if (!songDetails) {
        // First, search for the song on Genius
        const searchResult = await geniusClient.search(trackName, artistData.name);
        
        if (searchResult) {
          // Get song details
          songDetails = await geniusClient.getSong(searchResult.id);
          
          // Cache the result
          try {
            await redis.set(geniusKey, JSON.stringify(songDetails), { ex: 86400 }); // 24 hour TTL
          } catch (redisError) {
            console.warn(`Failed to cache Genius data:`, redisError);
          }
        }
      }
      
      if (songDetails) {
        // Extract producers from song details
        const geniusProducers = geniusClient.extractProducers(songDetails);
        
        // Add to our producers list
        geniusProducers.forEach((producer: any) => {
          producers.push({
            name: producer.name,
            confidence: producer.confidence,
            source: producer.source
          });
        });
      }
    } catch (error) {
      console.warn(`Genius search failed for ${trackName}:`, error);
    }

    // Deduplicate producers
    const uniqueProducers = deduplicateProducers(producers);
    console.log(`Found ${uniqueProducers.length} producers for track "${trackName}"`);
    
    // No producers found is not an error - it just means the track has no identified producers
    if (uniqueProducers.length === 0) {
      // Create an empty record to mark this track as processed
      await supabase.from('track_producers').insert({
        track_id: trackId,
        producer_id: '00000000-0000-0000-0000-000000000000', // Placeholder UUID
        confidence: 0,
        source: 'no_producers_found'
      }).select();
      
      // Mark as processed in Redis
      try {
        const processedKey = `processed:track_producers:${trackId}`;
        await redis.set(processedKey, 'true', { ex: 86400 }); // 24 hour TTL
      } catch (redisError) {
        console.warn("Failed to set processed flag in Redis:", redisError);
      }
      
      return { processed: 0, noProducersFound: true };
    }
    
    const successfulProducers = [];
    
    // Process each producer
    for (const producer of uniqueProducers) {
      try {
        // Normalize producer name for database
        const normalizedName = normalizeProducerName(producer.name);
        
        if (!normalizedName) {
          console.warn(`Normalized name is empty for producer ${producer.name}, skipping`);
          continue;
        }
        
        // Upsert producer in database
        const { data: dbProducer, error } = await supabase
          .from('producers')
          .upsert({
            name: producer.name,
            normalized_name: normalizedName,
            metadata: {
              source: producer.source,
              updated_at: new Date().toISOString()
            }
          }, {
            onConflict: 'normalized_name'
          })
          .select('id, enriched_at, enrichment_failed')
          .single();

        if (error) {
          console.error(`Error upserting producer ${producer.name}:`, error);
          continue;
        }
        
        // Make sure producer exists and has an id
        if (!dbProducer || !dbProducer.id) {
          console.error(`Missing producer data for ${producer.name}`);
          continue;
        }
        
        // Check if this producer is already linked to this track
        const producerTrackKey = `producer_track:${dbProducer.id}:${trackId}`;
        let alreadyLinked = false;
        
        try {
          alreadyLinked = await redis.exists(producerTrackKey) === 1;
        } catch (redisError) {
          console.warn(`Redis check failed for producer-track link:`, redisError);
        }
        
        if (!alreadyLinked) {
          // Associate producer with track
          const { error: linkError } = await supabase
            .from('track_producers')
            .upsert({
              track_id: trackId,
              producer_id: dbProducer.id,
              confidence: producer.confidence,
              source: producer.source
            }, {
              onConflict: 'track_id,producer_id'
            });
          
          if (linkError) {
            console.error(`Error linking producer ${producer.name} to track:`, linkError);
            continue;
          }
          
          console.log(`Linked producer ${producer.name} to track "${trackName}"`);
          
          // Mark this association in Redis
          try {
            await redis.set(producerTrackKey, 'true', { ex: 86400 }); // 24 hour TTL
          } catch (redisError) {
            console.warn(`Failed to mark producer-track link in Redis:`, redisError);
          }
          
          successfulProducers.push({
            name: producer.name,
            id: dbProducer.id,
            confidence: producer.confidence
          });
        } else {
          console.log(`Producer ${producer.name} already linked to track "${trackName}", skipping`);
        }

        // If producer hasn't been enriched yet, enqueue social enrichment
        if (!dbProducer.enriched_at && !dbProducer.enrichment_failed) {
          // Check if social enrichment was already enqueued
          const enrichmentKey = `enqueued:enrichment:${dbProducer.id}`;
          let enrichmentEnqueued = false;
          
          try {
            enrichmentEnqueued = await redis.exists(enrichmentKey) === 1;
          } catch (redisError) {
            console.warn(`Redis check failed for social enrichment:`, redisError);
          }
          
          if (!enrichmentEnqueued) {
            try {
              await supabase.functions.invoke("sendToQueue", {
                body: {
                  queue_name: "social_enrichment",
                  message: { 
                    producerId: dbProducer.id,
                    producerName: producer.name
                  }
                }
              });
              
              console.log(`Enqueued social enrichment for producer ${producer.name}`);
              
              // Mark as enqueued in Redis
              try {
                await redis.set(enrichmentKey, 'true', { ex: 86400 }); // 24 hour TTL
              } catch (redisError) {
                console.warn(`Failed to mark social enrichment as enqueued:`, redisError);
              }
            } catch (enqueueError) {
              console.error(`Error enqueueing social enrichment for ${producer.name}:`, enqueueError);
            }
          } else {
            console.log(`Social enrichment already enqueued for ${producer.name}, skipping`);
          }
        }
      } catch (producerError) {
        console.error(`Error processing producer ${producer.name}:`, producerError);
      }
    }
    
    // Mark track as processed in Redis
    try {
      const processedKey = `processed:track_producers:${trackId}`;
      await redis.set(processedKey, 'true', { ex: 86400 }); // 24 hour TTL
    } catch (redisError) {
      console.warn("Failed to set processed flag in Redis:", redisError);
    }
    
    // Remove processing flag
    try {
      const processingKey = `processing:track_producers:${trackId}`;
      await redis.del(processingKey);
    } catch (redisError) {
      console.warn("Failed to remove processing flag:", redisError);
    }
    
    return { 
      processed: successfulProducers.length,
      producers: successfulProducers
    };
  } catch (error) {
    console.error(`Failed to identify producers for track ${trackName}:`, error);
    
    // Remove processing flag on error
    try {
      const processingKey = `processing:track_producers:${trackId}`;
      await redis.del(processingKey);
    } catch (redisError) {
      console.warn("Failed to remove processing flag:", redisError);
    }
    
    throw error;
  }
}

// Helper functions
function normalizeProducerName(name: string): string {
  if (!name) return '';
  
  // Remove extraneous information
  let normalized = name
    .replace(/\([^)]*\)/g, '') // Remove text in parentheses
    .replace(/\[[^\]]*\]/g, '') // Remove text in brackets
    
  // Remove special characters and trim
  normalized = normalized
    .replace(/[^\w\s]/g, ' ') // Replace special chars with space
    .replace(/\s+/g, ' ')     // Replace multiple spaces with single space
    .trim()
    .toLowerCase();
    
  return normalized;
}

function deduplicateProducers(producers: ProducerCandidate[]): ProducerCandidate[] {
  const producerMap = new Map<string, ProducerCandidate>();
  
  for (const producer of producers) {
    if (!producer.name) continue;
    
    const normalizedName = normalizeProducerName(producer.name);
    
    if (!normalizedName) continue;
    
    const existing = producerMap.get(normalizedName);
    
    if (!existing || producer.confidence > existing.confidence) {
      producerMap.set(normalizedName, producer);
    }
  }
  
  return Array.from(producerMap.values());
}
