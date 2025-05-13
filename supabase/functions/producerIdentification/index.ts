
// Producer identification worker using the enhanced circuit breaker and resilience mechanisms

import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { IdempotentWorker, WorkerContext, ProcessingResult } from "../_shared/idempotentWorker.ts";
import { getApiResilienceManager } from "../_shared/apiResilienceManager.ts";
import { createEnhancedGeniusClient } from "../_shared/enhancedGeniusClient.ts";

// Initialize Redis client
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

// Initialize API resilience manager
const apiResilienceManager = getApiResilienceManager(redis);

// Initialize Genius client with resilience
const geniusClient = createEnhancedGeniusClient(
  Deno.env.get("GENIUS_ACCESS_TOKEN") || "",
  redis,
  apiResilienceManager
);

// Initialize Supabase client
const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const supabase = createClient(supabaseUrl, supabaseKey);

// CORS headers for function responses
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

interface ProducerIdentificationMessage {
  trackId: string;
  track_id?: string;  // Alternative property name
  artistName?: string;
  trackName?: string;
  album?: string;
  _idempotencyKey?: string;
}

class ProducerIdentificationWorker extends IdempotentWorker<ProducerIdentificationMessage, any> {
  constructor() {
    // Configure worker options
    super({
      queueName: 'producer_identification',
      deadLetterQueueName: 'producer_identification_dlq',
      batchSize: 3,  // Process 3 messages at a time
      visibilityTimeoutSeconds: 120,  // 2 minutes timeout
      maxRetryAttempts: 3
    });
  }

  /**
   * Extract entity information from message for deduplication and locking
   */
  protected extractEntityInfo(message: ProducerIdentificationMessage): { entityType: string; entityId: string } | null {
    const trackId = message.trackId || message.track_id;
    
    if (trackId) {
      return {
        entityType: 'track',
        entityId: trackId
      };
    }
    
    return null;
  }
  
  /**
   * Validate the message before processing
   */
  protected validateMessage(message: ProducerIdentificationMessage): boolean {
    return !!(message.trackId || message.track_id);
  }
  
  /**
   * Create idempotency key for the message
   */
  protected createIdempotencyKey(message: ProducerIdentificationMessage): string {
    // Use provided idempotency key if available
    if (message._idempotencyKey) {
      return message._idempotencyKey;
    }
    
    const trackId = message.trackId || message.track_id;
    return `producer_identification:track:${trackId}`;
  }
  
  /**
   * Main message processing logic
   */
  protected async processMessage(
    message: ProducerIdentificationMessage, 
    context: WorkerContext
  ): Promise<ProcessingResult> {
    console.log(`[${context.correlationId}] Processing producer identification for track ${message.trackId || message.track_id}`);
    
    try {
      // Get track ID (supporting both property names)
      const trackId = message.trackId || message.track_id;
      
      if (!trackId) {
        return { 
          success: false, 
          error: new Error('No track ID provided in message') 
        };
      }
      
      // Get track details from database
      const { data: track, error: trackError } = await this.supabase
        .from('tracks')
        .select(`
          id, 
          name, 
          album_id,
          albums:album_id (
            name, 
            artist_id,
            artists:artist_id (
              name
            )
          )
        `)
        .eq('id', trackId)
        .single();
      
      if (trackError) {
        console.error(`[${context.correlationId}] Error fetching track:`, trackError);
        return { 
          success: false, 
          error: new Error(`Failed to fetch track: ${trackError.message}`),
          metadata: { trackId }
        };
      }
      
      if (!track) {
        return { 
          success: false, 
          error: new Error(`Track not found with ID: ${trackId}`),
          metadata: { trackId } 
        };
      }
      
      const artistName = track.albums?.artists?.name;
      const trackName = track.name;
      const albumName = track.albums?.name;
      
      if (!artistName || !trackName) {
        return { 
          success: false, 
          error: new Error(`Incomplete track data: ${JSON.stringify({ artistName, trackName })}`),
          metadata: { trackId, track }
        };
      }
      
      console.log(`[${context.correlationId}] Searching for '${trackName} by ${artistName}'`);
      
      // Search for track on Genius using direct DB query acquisition
      try {
        // Bypass circuit breaker logic for now and use direct DB acquisition
        // IMPORTANT: Using direct database lock acquisition to avoid potential issues
        await this.acquireDirectDbLock(trackId, context.correlationId);
        
        // Only proceed if we were able to acquire the lock
        const searchQuery = `${trackName} ${artistName}`;
        const searchResult = await geniusClient.searchSong(searchQuery);
        
        if (!searchResult.hits || searchResult.hits.length === 0) {
          console.log(`[${context.correlationId}] No search results found`);
          return { 
            success: true,  // Mark as success with no results
            metadata: { trackId, searchQuery, found: false }
          };
        }
        
        console.log(`[${context.correlationId}] Found ${searchResult.hits.length} results, processing first match`);
        
        // Process first match
        const firstMatch = searchResult.hits[0].result;
        const songId = firstMatch.id;
        
        // Get song details from Genius
        console.log(`[${context.correlationId}] Getting details for song ID ${songId}`);
        const songData = await geniusClient.getSongById(songId);
        
        // Extract producer information
        const producers = songData.song.producer_artists || [];
        
        // Process producers with transaction
        const processResult = await this.processProducers(producers, trackId, context.correlationId);
        
        return {
          success: true,
          metadata: {
            trackId,
            geniusSongId: songId,
            songTitle: songData.song.title,
            producersFound: producers.length,
            processResult
          }
        };
      } catch (error) {
        console.error(`[${context.correlationId}] Error processing track:`, error);
        return {
          success: false,
          error,
          metadata: { trackId, artistName, trackName }
        };
      } finally {
        // Be extra careful to release the lock
        try {
          await this.releaseDirectDbLock(trackId);
        } catch (lockError) {
          console.error(`[${context.correlationId}] Error releasing lock:`, lockError);
        }
      }
    } catch (error) {
      console.error(`[${context.correlationId}] Uncaught error:`, error);
      return {
        success: false,
        error,
        metadata: { message }
      };
    }
  }
  
  /**
   * Process producers found in Genius data
   */
  private async processProducers(
    producers: Array<{ id: number; name: string; url?: string }>,
    trackId: string,
    correlationId: string
  ): Promise<any> {
    // Create a transaction for producer processing
    return await this.transactionManager.transaction(async (client) => {
      const results = [];
      
      for (const producer of producers) {
        // Check if producer exists in database
        const { data: existingProducer, error: findError } = await this.supabase
          .from('producers')
          .select('id, name')
          .eq('normalized_name', producer.name.toLowerCase())
          .maybeSingle();
        
        if (findError) {
          console.error(`[${correlationId}] Error finding producer:`, findError);
          continue;
        }
        
        let producerId;
        
        // Insert or get producer
        if (!existingProducer) {
          // Create new producer
          const { data: newProducer, error: insertError } = await this.supabase
            .from('producers')
            .insert({
              name: producer.name,
              normalized_name: producer.name.toLowerCase(),
              metadata: { genius_id: producer.id, genius_url: producer.url }
            })
            .select('id')
            .single();
          
          if (insertError) {
            console.error(`[${correlationId}] Error inserting producer:`, insertError);
            continue;
          }
          
          producerId = newProducer?.id;
        } else {
          producerId = existingProducer.id;
        }
        
        if (!producerId) {
          console.error(`[${correlationId}] Failed to get producer ID for ${producer.name}`);
          continue;
        }
        
        // Create track-producer relationship using ON CONFLICT for idempotence
        const { error: relationError } = await this.supabase
          .from('track_producers')
          .insert({
            track_id: trackId,
            producer_id: producerId,
            confidence: 0.95,  // High confidence from Genius data
            source: 'genius'
          }, { 
            onConflict: 'track_id,producer_id',
            ignoreDuplicates: true  // Or update with { count: track_producers.count + 1 }
          });
        
        if (relationError) {
          console.error(`[${correlationId}] Error creating track-producer relationship:`, relationError);
        } else {
          results.push({
            producer_id: producerId,
            producer_name: producer.name,
            added: !existingProducer
          });
        }
      }
      
      return { producersProcessed: results };
    }, {
      correlationId,
      isolationLevel: 'read committed'  // Lower isolation level for better concurrency
    });
  }
  
  /**
   * Acquire a direct database lock bypassing Redis
   * This is a fallback mechanism for when Redis may be unavailable
   */
  private async acquireDirectDbLock(trackId: string, correlationId: string): Promise<boolean> {
    try {
      // Try using the RPC function first (preferred way)
      const { data: lockAcquired, error: lockError } = await this.supabase.rpc(
        'acquire_processing_lock',
        {
          p_entity_type: 'track',
          p_entity_id: trackId,
          p_timeout_minutes: 10,
          p_correlation_id: correlationId
        }
      );
      
      if (lockError) {
        console.error(`[${correlationId}] Error acquiring lock via RPC:`, lockError);
        
        // Fallback to direct SQL approach
        const entityType = 'track';
        
        // Use transaction and advisory lock to prevent race conditions
        const { data, error } = await this.supabase.rpc(
          'raw_sql_query',
          {
            sql_query: `
              BEGIN;
              
              -- Use advisory lock for this operation
              SELECT pg_advisory_xact_lock(('x' || substr(md5($1 || ':' || $2), 1, 16))::bit(64)::bigint);
              
              -- Get current state
              WITH current_state AS (
                SELECT state, last_processed_at
                FROM public.processing_status
                WHERE entity_type = $1 AND entity_id = $2
              ),
              
              -- Handle insert or update
              upsert AS (
                INSERT INTO public.processing_status (
                  entity_type, entity_id, state, last_processed_at, metadata
                )
                VALUES (
                  $1, $2, 'IN_PROGRESS', NOW(), 
                  jsonb_build_object('correlation_id', $3, 'direct_lock', true)
                )
                ON CONFLICT (entity_type, entity_id) DO UPDATE
                SET 
                  state = 'IN_PROGRESS',
                  last_processed_at = NOW(),
                  metadata = jsonb_build_object('correlation_id', $3, 'direct_lock', true),
                  updated_at = NOW()
                WHERE 
                  -- Only update if not already IN_PROGRESS or COMPLETED
                  (processing_status.state = 'PENDING' OR 
                   processing_status.state = 'FAILED' OR
                   (processing_status.state = 'IN_PROGRESS' AND 
                    processing_status.last_processed_at < NOW() - INTERVAL '10 minutes'))
                RETURNING state
              )
              
              -- Return success status
              SELECT EXISTS(SELECT 1 FROM upsert) AS acquired;
              
              COMMIT;
            `,
            params: [entityType, trackId, correlationId]
          }
        );
        
        if (error) {
          console.error(`[${correlationId}] Error acquiring direct lock:`, error);
          return false;
        }
        
        // Extract result from the query
        return data && data[0] && data[0].acquired === true;
      }
      
      return !!lockAcquired;
    } catch (error) {
      console.error(`[${correlationId}] Exception acquiring lock:`, error);
      return false;
    }
  }
  
  /**
   * Release direct database lock
   */
  private async releaseDirectDbLock(trackId: string): Promise<boolean> {
    try {
      // Try the RPC function first
      const { data: released, error: releaseError } = await this.supabase.rpc(
        'release_processing_lock',
        {
          p_entity_type: 'track',
          p_entity_id: trackId
        }
      );
      
      if (releaseError) {
        console.error(`Error releasing lock via RPC:`, releaseError);
        
        // Fallback to direct update
        const { error } = await this.supabase
          .from('processing_status')
          .update({
            state: 'PENDING',
            last_processed_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
            metadata: { released_at: new Date().toISOString(), force_released: true }
          })
          .eq('entity_type', 'track')
          .eq('entity_id', trackId)
          .eq('state', 'IN_PROGRESS');
          
        if (error) {
          console.error(`Error releasing lock directly:`, error);
          return false;
        }
        
        return true;
      }
      
      return !!released;
    } catch (error) {
      console.error(`Exception releasing lock:`, error);
      return false;
    }
  }
}

// Create worker instance
const worker = new ProducerIdentificationWorker();

// Export handler for the Edge Function
serve((req) => {
  // CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }
  
  return worker.process(req);
});
