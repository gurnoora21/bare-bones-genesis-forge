
/**
 * Social Enrichment Edge Function
 * 
 * This function enriches producer profiles with social media information
 * and relevant metadata using the Genius API and other sources.
 */

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getRedis } from "../_shared/upstashRedis.ts";
import { IdempotentWorker } from "../_shared/idempotentWorker.ts";

// Define interface for the message we receive
interface SocialEnrichmentMessage {
  producerId: string;
  producerName: string;
  artistIds?: string[];
  trackIds?: string[];
  recursive?: boolean;
  priority?: number;
  _idempotencyKey?: string;
}

// Create a worker for social enrichment processing
class SocialEnrichmentWorker extends IdempotentWorker<SocialEnrichmentMessage> {
  constructor() {
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL") || "",
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || ""
    );
    const redis = getRedis().redis;
    
    // Initialize with the queue name
    super("social_enrichment", supabase, redis);
  }
  
  /**
   * Process a social enrichment message
   */
  async processMessage(message: SocialEnrichmentMessage): Promise<any> {
    console.log(`Processing social enrichment for producer: ${message.producerName} (${message.producerId})`);
    
    try {
      const startTime = Date.now();
      
      // 1. Fetch producer details from database
      const { data: producer, error: producerError } = await this.supabase
        .from('producers')
        .select('*')
        .eq('id', message.producerId)
        .single();
      
      if (producerError || !producer) {
        console.error(`Producer not found: ${message.producerId}`);
        throw new Error(`Producer not found: ${producerError?.message}`);
      }
      
      // 2. Enrich with social media information if not already present
      if (!producer.social_media || Object.keys(producer.social_media || {}).length === 0) {
        const socialMedia = await this.discoverSocialMedia(message.producerName);
        
        // Update producer with social media info
        const { error: updateError } = await this.supabase
          .from('producers')
          .update({ 
            social_media: socialMedia,
            updated_at: new Date().toISOString(),
            last_enriched_at: new Date().toISOString()
          })
          .eq('id', message.producerId);
        
        if (updateError) {
          console.error(`Failed to update producer: ${updateError.message}`);
          throw new Error(`Failed to update producer: ${updateError.message}`);
        }
      }
      
      // 3. Process recursive discovery if requested
      if (message.recursive && message.artistIds?.length) {
        await this.discoverRelatedProducers(message.artistIds, message.trackIds || []);
      }
      
      // Return success with processing time
      return { 
        success: true, 
        producerId: message.producerId,
        processingTime: Date.now() - startTime
      };
    } catch (error) {
      console.error(`Social enrichment error: ${error.message}`);
      throw error;
    }
  }
  
  /**
   * Discover social media profiles for a producer
   * In a real implementation, this would use Genius API, MusicBrainz, etc.
   */
  private async discoverSocialMedia(producerName: string): Promise<Record<string, string>> {
    // Simulate API calls and processing time
    await new Promise(resolve => setTimeout(resolve, 500));
    
    // Return simulated social media profiles
    // In a real implementation, this would call external APIs
    const normalizedName = producerName.toLowerCase().replace(/[^a-z0-9]/g, '');
    
    return {
      twitter: `https://twitter.com/${normalizedName}`,
      instagram: `https://instagram.com/${normalizedName}`,
      spotify: `https://open.spotify.com/artist/${normalizedName}`,
      website: `https://${normalizedName}.com`
    };
  }
  
  /**
   * Discover related producers from artists and tracks
   */
  private async discoverRelatedProducers(
    artistIds: string[], 
    trackIds: string[]
  ): Promise<void> {
    try {
      // Get tracks by artists that weren't specified
      const { data: artistTracks, error: tracksError } = await this.supabase
        .from('tracks')
        .select('id')
        .in('artist_id', artistIds)
        .not('id', 'in', trackIds);
      
      if (tracksError) {
        console.error(`Error fetching artist tracks: ${tracksError.message}`);
        return;
      }
      
      // Get all track IDs to process
      const allTrackIds = [
        ...trackIds,
        ...(artistTracks?.map(t => t.id) || [])
      ];
      
      // Get producers associated with these tracks who haven't been enriched yet
      const { data: producersToEnrich, error: producersError } = await this.supabase
        .from('track_producers')
        .select(`
          producer_id,
          producers:producer_id (
            id, 
            name,
            last_enriched_at
          )
        `)
        .in('track_id', allTrackIds)
        .is('producers.last_enriched_at', null);
      
      if (producersError) {
        console.error(`Error fetching producers: ${producersError.message}`);
        return;
      }
      
      // Queue producers for enrichment
      for (const relation of (producersToEnrich || [])) {
        if (relation.producers && relation.producer_id) {
          await this.supabase.rpc('pgmq_send', {
            queue_name: 'social_enrichment',
            message: JSON.stringify({
              producerId: relation.producer_id,
              producerName: relation.producers.name,
              priority: 5,
              _idempotencyKey: `social_enrichment:${relation.producer_id}`
            })
          });
        }
      }
    } catch (error) {
      console.error(`Error discovering related producers: ${error.message}`);
    }
  }
  
  /**
   * Process a message from the queue
   */
  async processBatch(options = {}): Promise<any> {
    return super.processBatch({
      processorName: 'social-enrichment',
      batchSize: 5,
      timeoutSeconds: 25,
      ...options
    });
  }
}

// Create worker instance
const socialEnrichmentWorker = new SocialEnrichmentWorker();

// Set up CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Serve the function
serve(async (req) => {
  // Handle OPTIONS for CORS
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }
  
  try {
    // Check if this is a direct batch processing request
    if (req.method === "POST") {
      try {
        const body = await req.json();
        
        // Process directly or queue
        if (body.action === "process") {
          // Process a batch of messages
          const result = await socialEnrichmentWorker.processBatch({
            batchSize: body.batchSize || 5,
            timeoutSeconds: body.timeoutSeconds || 25
          });
          
          return new Response(JSON.stringify(result), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
            status: 200
          });
        }
        else if (body.action === "queue") {
          // Check required fields
          if (!body.producerId || !body.producerName) {
            return new Response(
              JSON.stringify({ error: "Missing required fields: producerId, producerName" }),
              { 
                headers: { ...corsHeaders, 'Content-Type': 'application/json' },
                status: 400
              }
            );
          }
          
          // Queue the enrichment task
          const message: SocialEnrichmentMessage = {
            producerId: body.producerId,
            producerName: body.producerName,
            artistIds: body.artistIds,
            trackIds: body.trackIds,
            recursive: body.recursive,
            priority: body.priority || 3,
            _idempotencyKey: `social_enrichment:${body.producerId}`
          };
          
          const result = await socialEnrichmentWorker.supabase.rpc('pgmq_send', {
            queue_name: 'social_enrichment',
            message: JSON.stringify(message)
          });
          
          return new Response(
            JSON.stringify({ success: true, queued: message }),
            { 
              headers: { ...corsHeaders, 'Content-Type': 'application/json' },
              status: 200
            }
          );
        }
        else {
          return new Response(
            JSON.stringify({ error: "Invalid action. Use 'process' or 'queue'." }),
            { 
              headers: { ...corsHeaders, 'Content-Type': 'application/json' },
              status: 400
            }
          );
        }
      } catch (error) {
        console.error("Error processing request:", error);
        return new Response(
          JSON.stringify({ error: error.message }),
          { 
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
            status: 500
          }
        );
      }
    }
    
    // When invoked by queue processor or scheduler
    // Use waitUntil to handle long-running tasks
    EdgeRuntime.waitUntil(
      socialEnrichmentWorker.processBatch({
        batchSize: 5,
        timeoutSeconds: 25
      })
    );
    
    return new Response(
      JSON.stringify({ message: "Social enrichment processor started" }),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 202
      }
    );
  } catch (error) {
    console.error("Critical worker error:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 500
      }
    );
  }
});

export { socialEnrichmentWorker };
