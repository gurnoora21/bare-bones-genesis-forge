import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { IdempotentWorker, ProcessingResult, WorkerContext } from "../_shared/idempotentWorker.ts";
import { createEnhancedError, ErrorCategory, ErrorSource } from "../_shared/errorHandling.ts";
import { EntityType } from "../_shared/stateManager.ts";
import { getGeniusClient } from "../_shared/enhancedGeniusClient.ts";

// Message structure for producer identification
interface ProducerIdentificationMsg {
  trackId: string;
  trackName?: string;     // Made optional
  artistName?: string;    // Made optional
  idempotencyKey?: string;
  // Also support alternative field names that might be in use
  track_id?: string;
  track_name?: string;
  artist_name?: string;
  artist_id?: string;     // Added potential field
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
      useTransactions: true,
      allowPartialFailures: true // FIX: Allow partial failures
    });
  }
  
  /**
   * Extract entity info from message for state tracking
   */
  protected extractEntityInfo(message: ProducerIdentificationMsg): { entityType: string; entityId: string } | null {
    // Get trackId from any available field
    const trackId = message?.trackId || message?.track_id;
    
    if (!trackId) {
      console.warn("Cannot extract entity info: no track ID found in message", message);
      return null;
    }
    
    return {
      entityType: EntityType.TRACK,
      entityId: trackId
    };
  }
  
  /**
   * Normalize message fields to ensure consistent access patterns
   */
  private normalizeMessage(message: ProducerIdentificationMsg): ProducerIdentificationMsg {
    if (!message) return message;
    
    // Ensure trackId is available
    if (!message.trackId && message.track_id) {
      message.trackId = message.track_id;
    }
    
    // Ensure trackName is available
    if (!message.trackName && message.track_name) {
      message.trackName = message.track_name;
    }
    
    // Ensure artistName is available
    if (!message.artistName && message.artist_name) {
      message.artistName = message.artist_name;
    }
    
    return message;
  }
  
  /**
   * Validate incoming message structure
   */
  protected validateMessage(originalMessage: ProducerIdentificationMsg): boolean {
    if (!originalMessage) {
      console.warn("Message validation failed: message is null or undefined");
      return false;
    }
    
    // Normalize the message to ensure all needed fields are consistently available
    const message = this.normalizeMessage(originalMessage);
    
    // The only truly required field is trackId
    const trackId = message.trackId;
    if (!trackId || typeof trackId !== 'string') {
      console.warn("Message validation failed: missing or invalid trackId", { message });
      return false;
    }
    
    return true;
  }
  
  /**
   * Create idempotency key from message
   */
  protected createIdempotencyKey(message: ProducerIdentificationMsg): string {
    // Use existing idempotency key if provided
    if (message.idempotencyKey) {
      return message.idempotencyKey;
    }
    
    // Get trackId from normalized message
    const trackId = message.trackId || message.track_id;
    
    if (!trackId) {
      // Fallback using message attributes as a string to create a consistent key
      const msgStr = JSON.stringify(message);
      return `producer-identification:${msgStr.length}:${msgStr.substring(0, 40)}`;
    }
    
    return `track:${trackId}:producer-identification`;
  }
  
  /**
   * Process the message to identify producers
   */
  protected async processMessage(
    originalMsg: ProducerIdentificationMsg,
    context: WorkerContext
  ): Promise<ProcessingResult> {
    const { correlationId } = context;
    
    // Normalize message to ensure consistent field access
    const msg = this.normalizeMessage(originalMsg);
    const trackId = msg.trackId;
    const trackName = msg.trackName;
    const artistName = msg.artistName;
    
    console.log(`[${correlationId}] Processing producer identification for track ID: ${trackId}`);
    
    if (!trackName || !artistName) {
      console.log(`[${correlationId}] Track name or artist name is missing, fetching from database`);
      
      try {
        // Get track information from database if trackName or artistName is missing
        const { data: track, error: trackError } = await this.supabase
          .from('tracks')
          .select('id, name, album_id')
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
        
        // If we're missing artist information, query the album to get the artist
        if (!artistName && track.album_id) {
          console.log(`[${correlationId}] Getting artist from track's album ${track.album_id}`);
          
          const { data: album, error: albumError } = await this.supabase
            .from('albums')
            .select('artist_id')
            .eq('id', track.album_id)
            .maybeSingle();
          
          if (albumError) {
            throw createEnhancedError(
              `Failed to fetch album from database: ${albumError.message}`,
              ErrorSource.DATABASE,
              ErrorCategory.TRANSIENT_SERVICE
            );
          }
          
          if (album && album.artist_id) {
            const { data: artist, error: artistError } = await this.supabase
              .from('artists')
              .select('name')
              .eq('id', album.artist_id)
              .maybeSingle();
            
            if (!artistError && artist) {
              msg.artistName = artist.name;
            }
          }
        }
        
        // Use track name from database if missing in message
        if (!msg.trackName && track) {
          msg.trackName = track.name;
        }
        
        // If we still don't have everything we need, log and abort
        if (!msg.trackName || !msg.artistName) {
          console.warn(`[${correlationId}] Insufficient data to proceed with producer identification`);
          console.warn(`[${correlationId}] trackId: ${trackId}, trackName: ${msg.trackName}, artistName: ${msg.artistName}`);
          
          return {
            success: false,
            error: createEnhancedError(
              "Missing required data for producer identification",
              ErrorSource.VALIDATION,
              ErrorCategory.PERMANENT_INVALID_INPUT
            ),
            metadata: { 
              trackId,
              incomplete: true,
              message: "Track name or artist name could not be determined" 
            }
          };
        }
      } catch (error) {
        console.error(`[${correlationId}] Error fetching track data:`, error);
        return {
          success: false,
          error
        };
      }
    }
    
    // Now proceed with producer identification using the complete data
    try {
      // Initialize Genius client
      const geniusClient = getGeniusClient();
      
      // Search for the song on Genius
      console.log(`[${correlationId}] Searching Genius for "${msg.trackName}" by ${msg.artistName}`);
      
      const song = await geniusClient.search(msg.trackName!, msg.artistName!);
      
      if (!song) {
        console.log(`[${correlationId}] No song found on Genius for "${msg.trackName}" by ${msg.artistName}`);
        return {
          success: true,
          metadata: { trackId, found: false }
        };
      }
      
      console.log(`[${correlationId}] Found song on Genius: "${song.title}" (ID: ${song.id})`);
      
      // Get detailed song information
      const songDetails = await geniusClient.getSong(song.id);
      
      if (!songDetails) {
        throw createEnhancedError(
          `Failed to get details for song ID ${song.id}`,
          ErrorSource.GENIUS_API,
          ErrorCategory.TRANSIENT_SERVICE
        );
      }
      
      // Extract producers from song details
      const producers = geniusClient.extractProducers(songDetails);
      
      console.log(`[${correlationId}] Found ${producers.length} producers for "${msg.trackName}"`);
      
      if (producers.length === 0) {
        return {
          success: true,
          metadata: { trackId, found: true, producersFound: 0 }
        };
      }
      
      // Process each producer within the same transaction
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
          trackName: msg.trackName,
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
   * Enhanced direct lock acquisition method that connects to the database directly
   * This bypasses the Redis lock mechanism if it's failing
   */
  protected async directLockAcquisition(entityType: string, entityId: string, options: any = {}): Promise<boolean> {
    try {
      console.log(`Attempting direct lock acquisition for ${entityType}:${entityId}`);
      
      // Try to insert a row directly in the processing_locks table
      const { data, error } = await this.supabase
        .from('processing_locks')
        .upsert({
          entity_type: entityType,
          entity_id: entityId,
          worker_id: Deno.env.get("WORKER_ID") || `worker_${Math.random().toString(36).substring(2, 10)}`,
          correlation_id: options.correlationId || `direct_${Date.now()}`,
          acquired_at: new Date().toISOString(),
          last_heartbeat: new Date().toISOString(),
          metadata: {
            direct_acquisition: true,
            acquisition_reason: "Redis lock fallback",
            attempt_time: new Date().toISOString()
          }
        }, {
          onConflict: 'entity_type,entity_id',
          ignoreDuplicates: true // Don't update if it already exists
        });
      
      if (error) {
        console.error(`Failed direct lock acquisition: ${error.message}`);
        
        // Try a more aggressive approach if the regular upsert fails
        try {
          // First check if the lock is stale
          const { data: staleLock } = await this.supabase
            .from('processing_locks')
            .select('*')
            .eq('entity_type', entityType)
            .eq('entity_id', entityId)
            .single();
          
          if (staleLock && new Date(staleLock.last_heartbeat) < new Date(Date.now() - 5 * 60 * 1000)) {
            // Lock exists but is stale (no heartbeat for 5+ minutes), force update it
            const { error: updateError } = await this.supabase
              .from('processing_locks')
              .update({
                worker_id: Deno.env.get("WORKER_ID") || `worker_${Math.random().toString(36).substring(2, 10)}`,
                correlation_id: options.correlationId || `direct_${Date.now()}`,
                acquired_at: new Date().toISOString(),
                last_heartbeat: new Date().toISOString(),
                metadata: {
                  force_acquisition: true,
                  acquisition_reason: "Stale lock takeover",
                  previous_worker: staleLock.worker_id,
                  previous_heartbeat: staleLock.last_heartbeat,
                  attempt_time: new Date().toISOString()
                }
              })
              .eq('entity_type', entityType)
              .eq('entity_id', entityId);
              
            if (!updateError) {
              console.log(`Successfully took over stale lock for ${entityType}:${entityId}`);
              return true;
            }
          }
        } catch (staleError) {
          console.error(`Error checking for stale lock: ${staleError.message}`);
        }
        
        return false;
      }
      
      return true;
    } catch (error) {
      console.error(`Error in direct lock acquisition: ${error.message}`);
      return false;
    }
  }
  
  /**
   * Override acquireProcessingLock to implement a fallback mechanism
   */
  protected async acquireProcessingLock(entityType: string, entityId: string, options: any = {}): Promise<boolean> {
    // First try normal lock acquisition 
    try {
      const lockAcquired = await this.stateManager.acquireProcessingLock(
        entityType,
        entityId,
        options
      );
      
      if (lockAcquired) {
        return true;
      }
    } catch (error) {
      console.warn(`Redis lock acquisition failed: ${error.message}, trying fallback`);
    }
    
    // If Redis lock failed, try direct database lock as fallback
    try {
      // FIX: For producer identification, we'll use a more aggressive lock acquisition strategy
      // For important processes like this, we prefer proceeding with the work over waiting for locks
      if (entityType === EntityType.TRACK && this.options.queueName === 'producer_identification') {
        // For tracks being processed in producer identification, we'll use the direct approach
        return await this.directLockAcquisition(entityType, entityId, options);
      }
    } catch (fallbackError) {
      console.error(`Fallback lock acquisition failed: ${fallbackError.message}`);
    }
    
    // Check for and claim stale locks as a last resort
    try {
      const { data: staleLockCheck } = await this.supabase.rpc(
        'claim_stale_lock',
        {
          p_entity_type: entityType,
          p_entity_id: entityId,
          p_new_worker_id: Deno.env.get("WORKER_ID") || `worker_${Math.random().toString(36).substring(2, 10)}`,
          p_correlation_id: options.correlationId || `claim_${Date.now()}`,
          p_stale_threshold_seconds: 300 // 5 minutes
        }
      );
      
      if (staleLockCheck && staleLockCheck.claimed) {
        console.log(`Successfully claimed stale lock for ${entityType}:${entityId}`);
        return true;
      }
    } catch (claimError) {
      console.error(`Error claiming stale lock: ${claimError.message}`);
    }
    
    return false;
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
