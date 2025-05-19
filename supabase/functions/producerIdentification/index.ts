import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.7.1";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { createEnhancedWorker } from "../_shared/enhancedQueueWorker.ts";
import { getGeniusClient } from "../_shared/geniusClient.ts";
import { EnhancedWorkerBase } from "../_shared/enhancedWorkerBase.ts";
import { StructuredLogger } from "../_shared/structuredLogger.ts";
import { corsHeaders } from "../_shared/cors.ts";
import { validateMessage, ProducerIdentificationMessageSchema, type ProducerIdentificationMessage } from "../_shared/types/queueMessages.ts";
import { QueueHelper, getQueueHelper } from "../_shared/queueHelper.ts";
import { readQueueMessages, deleteQueueMessage } from "../_shared/pgmqBridge.ts";

// Initialize Redis client
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

// Initialize Supabase client
const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
const supabase = createClient(supabaseUrl, supabaseServiceKey);

const QUEUE_NAME = "producer_identification";
const BATCH_SIZE = 10;
const VISIBILITY_TIMEOUT = 30; // seconds

// Define the producer worker implementation
class ProducerIdentificationWorker extends EnhancedWorkerBase {
  constructor() {
    super('producer_identification', supabase, 'ProducerIdentification');
  }

  async processMessage(message: ProducerIdentificationMessage): Promise<any> {
    const logger = new StructuredLogger({ service: 'producer_identification' });
    logger.info("Processing producer identification message:", { message });
    
    // Extract track info
    const { trackId, trackName, artistId } = message;
    
    if (!trackId) {
      logger.error("Message missing trackId");
      throw new Error("Message missing trackId");
    }
    
    // Generate deduplication key
    const dedupKey = `producer:track:${trackId}`;
    
    // Get track details from database
    const { data: track, error: trackError } = await this.supabase
      .from('tracks')
      .select('id, name, spotify_id')
      .eq('id', trackId)
      .single();
    
    if (trackError || !track) {
      logger.error(`Track not found with ID: ${trackId}`);
      throw new Error(`Track not found with ID: ${trackId}`);
    }
    
    try {
      // Initialize genius client
      const genius = getGeniusClient();
      
      // Search for track on Genius
      const searchResults = await genius.search(trackName, track.name);
      
      if (!searchResults || !searchResults.hits || searchResults.hits.length === 0) {
        logger.info(`No genius results found for track "${trackName}"`);
        return { status: 'completed', result: 'no_genius_results' };
      }
      
      // Get first result
      const songId = searchResults.hits[0].result.id;
      
      // Get detailed song info
      const songDetails = await genius.getSong(songId);
      
      if (!songDetails) {
        logger.info(`No song details found for song ID ${songId}`);
        return { status: 'completed', result: 'no_song_details' };
      }
      
      // Extract producers
      const producers = genius.extractProducers(songDetails);
      
      // Save producers to database
      if (producers.length > 0) {
        logger.info(`Found ${producers.length} producers for track ${trackName}`);
        
        // Process each producer
        for (const producer of producers) {
          // Check if producer already exists
          const { data: existingProducer } = await this.supabase
            .from('producers')
            .select('id')
            .eq('name', producer.name)
            .maybeSingle();
          
          let producerId;
          
          if (existingProducer) {
            producerId = existingProducer.id;
          } else {
            // Insert new producer
            const { data: newProducer, error: insertError } = await this.supabase
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
              logger.error(`Error inserting producer ${producer.name}:`, insertError);
              continue;
            }
            
            producerId = newProducer.id;
          }
          
          // Create track-producer relationship
          const { error: relationError } = await this.supabase
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
            logger.error(`Error creating track-producer relationship:`, relationError);
          }
          
      // Enqueue social enrichment for this producer
      // Note: We'll use Redis to deduplicate across workers
      const enrichmentKey = `enqueued:enrichment:${producerId}`;
      const alreadyEnqueued = await this.redis.get(enrichmentKey);
      
      if (!alreadyEnqueued) {
        // Get queue helper
        const queueHelper = getQueueHelper(this.supabase, this.redis);
        
        // Enqueue social enrichment task directly
        const messageId = await queueHelper.enqueue(
          'social_enrichment',
          {
            producerId: producerId,
            producerName: producer.name
          },
          `producer:${producerId}`,
          { ttl: 86400 } // 24 hour TTL
        );
        
        if (messageId) {
          // Mark as enqueued in Redis
          await this.redis.set(enrichmentKey, 'true', { ex: 86400 }); // 24 hour TTL
          logger.info(`Enqueued social enrichment for producer ${producer.name}`);
        } else {
          logger.error(`Failed to enqueue social enrichment for producer ${producer.name}`);
        }
      }
        }
      } else {
        logger.info(`No producers found for track ${trackName}`);
      }
      
      return {
        status: 'completed',
        trackId,
        producersCount: producers.length
      };
      
    } catch (error) {
      logger.error(`Error processing track ${trackName}:`, error);
      throw error;
    }
  }

  /**
   * Required implementation of handleMessage from EnhancedWorkerBase
   */
  async handleMessage(message: ProducerIdentificationMessage, logger: StructuredLogger): Promise<any> {
    return this.processMessage(message);
  }
}

async function processMessage(message: ProducerIdentificationMessage) {
  try {
    // Your existing message processing logic here
    console.log("Processing producer:", message.producerName);
    
    // Example processing steps:
    // 1. Identify producer from track metadata
    // 2. Store producer information
    // 3. Enqueue for social enrichment if needed
    
    return { success: true };
  } catch (error) {
    console.error("Error processing message:", error);
    return { success: false, error: error.message };
  }
}

// Handle HTTP requests
serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    // Use our bridge function to read messages from the queue
    console.log(`Reading messages from ${QUEUE_NAME} queue using bridge function`);
    const messages = await readQueueMessages(
      supabase,
      QUEUE_NAME,
      BATCH_SIZE,
      VISIBILITY_TIMEOUT
    );

    if (!messages || messages.length === 0) {
      console.log("No messages to process");
      return new Response(
        JSON.stringify({ message: "No messages to process" }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    console.log(`Retrieved ${messages.length} messages to process`);

    const results = [];
    const errors = [];

    for (const message of messages) {
      try {
        // Validate the message format
        console.log(`Validating message format for message ID: ${message.id}`);
        const messageBody = typeof message.message === 'string' 
          ? JSON.parse(message.message) 
          : message.message;
        
        if (!messageBody) {
          throw new Error(`Invalid message format: message body is empty or null`);
        }
        
        const validatedMessage = validateMessage(ProducerIdentificationMessageSchema, messageBody);
        
        // Process the validated message
        console.log(`Processing producer identification for track: ${validatedMessage.trackName}`);
        const result = await processMessage(validatedMessage);
        
        if (result.success) {
          // Debug message structure to identify the correct ID field
          console.log(`Message structure:`, {
            id: message.id,
            msgId: message.msgId,
            msg_id: message.msg_id,
            messageId: message.messageId,
            fullMessage: JSON.stringify(message).substring(0, 200) // Log first 200 chars to avoid huge logs
          });
          
          // Try to find the message ID from various possible properties
          const messageId = message.id || message.msgId || message.msg_id || message.messageId;
          
          // Delete the message from the queue after successful processing using our bridge function
          console.log(`Processing successful, deleting message ID: ${messageId}`);
          await deleteQueueMessage(supabase, QUEUE_NAME, messageId);
          
          results.push({ id: messageId, status: "success", ...result });
        } else {
          errors.push({ id: message.id || message.msgId || message.msg_id || "unknown", error: result.error });
        }
      } catch (error) {
        console.error(`Error processing message ${message.id}:`, error);
        errors.push({ id: message.id, error: error.message });
        
        // If it's a validation error, send to DLQ
        if (error.message.includes("Invalid message format") || error.message.includes("Required")) {
          console.log(`Validation error, sending message ${message.id} to DLQ`);
          try {
            // Use QueueHelper to send to DLQ
            const queueHelper = getQueueHelper(supabase, redis);
            await queueHelper.sendToDLQ(
              QUEUE_NAME,
              message.id,
              message,
              error.message,
              { timestamp: new Date().toISOString() }
            );
          } catch (dlqError) {
            console.error(`Failed to send to DLQ: ${dlqError.message}`);
          }
        }
      }
    }

    return new Response(
      JSON.stringify({
        processed: results.length,
        errors: errors.length,
        results,
        errors,
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  } catch (error) {
    console.error("Error:", error);
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        status: 500,
        headers: { ...corsHeaders, "Content-Type": "application/json" }
      }
    );
  }
});
