
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { IdempotentWorker, ProcessingResult, WorkerContext } from "../_shared/idempotentWorker.ts";
import { createEnhancedError, ErrorCategory, ErrorSource } from "../_shared/errorHandling.ts";
import { EntityType } from "../_shared/stateManager.ts";
import { getGeniusClient } from "../_shared/enhancedGeniusClient.ts";

// Message structure for producer identification
interface ProducerIdentificationMsg {
  trackId: string;
  trackName: string;
  artistName: string;
  idempotencyKey?: string;
}

// Worker implementation for producer identification
class ProducerIdentificationWorker extends IdempotentWorker<ProducerIdentificationMsg, any> {
  constructor() {
    super({
      queueName: "producer_identification",
      deadLetterQueueName: "producer_identification_dlq",
      batchSize: 5,
      visibilityTimeoutSeconds: 180,
      maxRetryAttempts: 3,
      useTransactions: true
    });
  }
  
  /**
   * Extract entity info from message for state tracking
   */
  protected extractEntityInfo(message: ProducerIdentificationMsg): { entityType: string; entityId: string } | null {
    if (!message || !message.trackId) {
      return null;
    }
    
    return {
      entityType: EntityType.TRACK,
      entityId: message.trackId
    };
  }
  
  /**
   * Validate incoming message structure
   */
  protected validateMessage(message: ProducerIdentificationMsg): boolean {
    if (!message) return false;
    if (!message.trackId || typeof message.trackId !== 'string') return false;
    if (!message.trackName || typeof message.trackName !== 'string') return false;
    if (!message.artistName || typeof message.artistName !== 'string') return false;
    
    return true;
  }
  
  /**
   * Create idempotency key from message
   */
  protected createIdempotencyKey(message: ProducerIdentificationMsg): string {
    if (message.idempotencyKey) {
      return message.idempotencyKey;
    }
    
    return `track:${message.trackId}:producer-identification`;
  }
  
  /**
   * Process the message to identify producers
   */
  protected async processMessage(
    msg: ProducerIdentificationMsg,
    context: WorkerContext
  ): Promise<ProcessingResult> {
    const { correlationId } = context;
    const { trackId, trackName, artistName } = msg;
    
    console.log(`[${correlationId}] Identifying producers for track "${trackName}" by ${artistName}`);
    
    try {
      // Get track information from database to double-check
      const { data: track, error: trackError } = await this.supabase
        .from('tracks')
        .select('id, name')
        .eq('id', trackId)
        .maybeSingle();
      
      if (trackError) {
        throw createEnhancedError(
          `Failed to fetch track from database: ${trackError.message}`,
          ErrorSource.DATABASE,
          ErrorCategory.TRANSIENT_SERVICE
        );
      }
      
      if (!track) {
        throw createEnhancedError(
          `Track with ID ${trackId} not found in database`,
          ErrorSource.DATABASE,
          ErrorCategory.PERMANENT_NOT_FOUND
        );
      }
      
      // Initialize Genius client
      const geniusClient = getGeniusClient();
      
      // Step 1: Search for the song on Genius
      console.log(`[${correlationId}] Searching Genius for "${trackName}" by ${artistName}`);
      
      const song = await geniusClient.search(trackName, artistName);
      
      if (!song) {
        console.log(`[${correlationId}] No song found on Genius for "${trackName}" by ${artistName}`);
        return {
          success: true,
          metadata: { trackId, found: false }
        };
      }
      
      console.log(`[${correlationId}] Found song on Genius: "${song.title}" (ID: ${song.id})`);
      
      // Step 2: Get detailed song information
      const songDetails = await geniusClient.getSong(song.id);
      
      if (!songDetails) {
        throw createEnhancedError(
          `Failed to get details for song ID ${song.id}`,
          ErrorSource.GENIUS_API,
          ErrorCategory.TRANSIENT_SERVICE
        );
      }
      
      // Step 3: Extract producers from song details
      const producers = geniusClient.extractProducers(songDetails);
      
      console.log(`[${correlationId}] Found ${producers.length} producers for "${trackName}"`);
      
      if (producers.length === 0) {
        return {
          success: true,
          metadata: { trackId, found: true, producersFound: 0 }
        };
      }
      
      // Step 4: Process each producer within the same transaction
      const producerResults = [];
      
      for (const producer of producers) {
        try {
          // Normalize producer name
          const normalizedName = this.normalizeProducerName(producer.name);
          
          // Check if producer already exists using upsert pattern
          const { data: producerRecord, error: producerError } = await this.supabase
            .from('producers')
            .upsert({
              name: producer.name,
              normalized_name: normalizedName,
              metadata: {
                genius_extraction: {
                  song_id: song.id,
                  extraction_source: producer.source,
                  extraction_confidence: producer.confidence
                },
                transaction_id: correlationId
              }
            }, {
              onConflict: 'normalized_name',
              returning: true
            })
            .select('id, name')
            .maybeSingle();
          
          if (producerError) {
            throw createEnhancedError(
              `Failed to upsert producer: ${producerError.message}`,
              ErrorSource.DATABASE,
              ErrorCategory.TRANSIENT_SERVICE
            );
          }
          
          // Create track-producer association using upsert
          const { error: associationError } = await this.supabase
            .from('track_producers')
            .upsert({
              track_id: trackId,
              producer_id: producerRecord.id,
              confidence: producer.confidence,
              source: producer.source
            }, {
              onConflict: 'track_id,producer_id',
              ignoreDuplicates: false
            });
          
          if (associationError) {
            throw createEnhancedError(
              `Failed to create track-producer association: ${associationError.message}`,
              ErrorSource.DATABASE,
              ErrorCategory.TRANSIENT_SERVICE
            );
          }
          
          // Queue producer for social enrichment with idempotency key
          const idempotencyKey = `social_enrichment:producer:${producerRecord.id}`;
          
          await this.supabase.rpc('pg_enqueue', {
            queue_name: 'social_enrichment',
            message_body: {
              producerId: producerRecord.id,
              producerName: producer.name,
              idempotencyKey
            }
          });
          
          producerResults.push({
            name: producer.name,
            id: producerRecord.id,
            confidence: producer.confidence
          });
          
          console.log(`[${correlationId}] Processed producer ${producer.name} (${producerRecord.id})`);
        } catch (producerError) {
          console.warn(`[${correlationId}] Error processing producer ${producer.name}:`, producerError);
          // Continue with other producers - this is safe because we're in a transaction
        }
      }
      
      // Return results
      return {
        success: true,
        metadata: {
          trackId,
          trackName,
          found: true,
          producersFound: producerResults.length,
          producers: producerResults
        }
      };
    } catch (error) {
      console.error(`[${correlationId}] Error identifying producers:`, error);
      return {
        success: false,
        error
      };
    }
  }
  
  /**
   * Normalize producer name for consistent comparison
   */
  private normalizeProducerName(name: string): string {
    return name
      .toLowerCase()
      .replace(/[^\w\s]/g, '') // Remove special chars
      .replace(/\s+/g, ' ')    // Normalize whitespace
      .trim();
  }
}

// Create handler that serves the worker
const producerIdentificationWorker = new ProducerIdentificationWorker();

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const response = await producerIdentificationWorker.process(req);
    
    // Add CORS headers to response
    const responseHeaders = new Headers(response.headers);
    Object.entries(corsHeaders).forEach(([k, v]) => responseHeaders.set(k, v));
    
    return new Response(response.body, {
      status: response.status,
      headers: responseHeaders
    });
  } catch (error) {
    console.error("Critical worker error:", error);
    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
});
