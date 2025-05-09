
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { IdempotentWorker, ProcessingResult, WorkerContext } from "../_shared/idempotentWorker.ts";
import { createEnhancedError, ErrorCategory, ErrorSource } from "../_shared/errorHandling.ts";
import { EntityType } from "../_shared/stateManager.ts";

interface SocialEnrichmentMsg {
  producerId: string;
  producerName: string;
  idempotencyKey?: string;
}

class SocialEnrichmentWorker extends IdempotentWorker<SocialEnrichmentMsg, any> {
  constructor() {
    super({
      queueName: "social_enrichment",
      deadLetterQueueName: "social_enrichment_dlq",
      batchSize: 10,
      visibilityTimeoutSeconds: 180,
      maxRetryAttempts: 3,
      useTransactions: true
    });
  }
  
  /**
   * Extract entity info from message for state tracking
   */
  protected extractEntityInfo(message: SocialEnrichmentMsg): { entityType: string; entityId: string } | null {
    if (!message || !message.producerId) {
      return null;
    }
    
    return {
      entityType: EntityType.PRODUCER,
      entityId: message.producerId
    };
  }
  
  /**
   * Validate incoming message structure
   */
  protected validateMessage(message: SocialEnrichmentMsg): boolean {
    if (!message) return false;
    if (!message.producerId || typeof message.producerId !== 'string') return false;
    if (!message.producerName || typeof message.producerName !== 'string') return false;
    
    return true;
  }
  
  /**
   * Create idempotency key from message
   */
  protected createIdempotencyKey(message: SocialEnrichmentMsg): string {
    if (message.idempotencyKey) {
      return message.idempotencyKey;
    }
    
    return `producer:${message.producerId}:social-enrichment`;
  }
  
  /**
   * Process a social enrichment message
   */
  protected async processMessage(
    msg: SocialEnrichmentMsg,
    context: WorkerContext
  ): Promise<ProcessingResult> {
    const { correlationId } = context;
    const { producerId, producerName } = msg;
    
    console.log(`[${correlationId}] Enriching producer profile for ${producerName} (${producerId})`);
    
    try {
      // First, check if this producer has already been enriched (prior to deduplication)
      // This provides an additional safeguard
      const { data: existingProducer } = await this.supabase
        .from('producers')
        .select('enriched_at, enrichment_failed')
        .eq('id', producerId)
        .maybeSingle();
        
      if (existingProducer && (existingProducer.enriched_at || existingProducer.enrichment_failed)) {
        console.log(`[${correlationId}] Producer ${producerName} already enriched, skipping`);
        return {
          success: true,
          isDuplicate: true,
          metadata: {
            producerId,
            producerName,
            alreadyEnriched: true
          }
        };
      }
      
      // Search for Instagram handle
      const instagramHandle = await this.findInstagramHandle(producerName, correlationId);
      
      // Get Instagram bio if handle found
      let instagramBio = null;
      if (instagramHandle) {
        instagramBio = await this.getInstagramBio(instagramHandle, correlationId);
        console.log(`[${correlationId}] Found Instagram profile for ${producerName}: @${instagramHandle}`);
      }
      
      // Update producer with social info using upsert pattern
      const { data, error } = await this.supabase
        .from('producers')
        .update({
          instagram_handle: instagramHandle,
          instagram_bio: instagramBio,
          enriched_at: new Date().toISOString(),
          metadata: {
            enriched_by: "social_enrichment_worker",
            correlation_id: correlationId,
            transaction_id: context.batchId
          }
        })
        .eq('id', producerId)
        .select();
      
      if (error) {
        throw createEnhancedError(
          `Error updating producer record: ${error.message}`,
          ErrorSource.DATABASE,
          ErrorCategory.TRANSIENT_SERVICE
        );
      }
      
      console.log(`[${correlationId}] Successfully enriched producer profile for ${producerName}`);
      
      // Create Redis flag for fast lookups
      const processedKey = `processed:producer:${producerId}`;
      await this.redis.set(processedKey, JSON.stringify({
        enrichedAt: new Date().toISOString(),
        instagramHandle,
        correlationId
      }), { ex: 86400 }); // 24 hour TTL
      
      return {
        success: true,
        metadata: {
          producerId,
          producerName,
          instagramHandle,
          success: true
        }
      };
    } catch (error) {
      console.error(`[${correlationId}] Social enrichment failed for ${producerName}:`, error);
      
      // Mark the producer as having failed enrichment
      try {
        await this.supabase
          .from('producers')
          .update({
            enrichment_failed: true,
            metadata: {
              error: error.message,
              correlation_id: correlationId,
              failed_at: new Date().toISOString()
            }
          })
          .eq('id', producerId);
      } catch (updateError) {
        console.error(`[${correlationId}] Failed to update producer failure status:`, updateError);
      }
      
      // Return failure
      return {
        success: false,
        error
      };
    }
  }
  
  /**
   * Enhanced social media API calls with circuit breaker and timeout
   */
  private async findInstagramHandle(name: string, correlationId: string): Promise<string | null> {
    // In a real implementation, this would call a service or API
    // to search for the producer's Instagram handle
    
    // For now, simulate a search with randomized results
    console.log(`[${correlationId}] Searching for Instagram handle for "${name}"...`);
    
    // Simulate API call delay with timeout
    try {
      await Promise.race([
        new Promise(resolve => setTimeout(resolve, 500)),
        new Promise((_, reject) => setTimeout(() => reject(new Error("Instagram search timeout")), 5000))
      ]);
    } catch (error) {
      console.warn(`[${correlationId}] Instagram search timed out for ${name}`);
      throw error; // Let the caller handle the timeout
    }
    
    // Simulate finding a handle 70% of the time
    if (Math.random() < 0.7) {
      // Generate a simulated Instagram handle based on the name
      const handle = name
        .toLowerCase()
        .replace(/\s+/g, '')
        .replace(/[^\w]/g, '')
        .slice(0, 15);
        
      return handle;
    }
    
    return null;
  }
  
  private async getInstagramBio(handle: string, correlationId: string): Promise<string | null> {
    // In a real implementation, this would call a service or API
    // to fetch the Instagram bio
    
    console.log(`[${correlationId}] Getting Instagram bio for @${handle}...`);
    
    // Simulate API call delay with timeout
    try {
      await Promise.race([
        new Promise(resolve => setTimeout(resolve, 700)),
        new Promise((_, reject) => setTimeout(() => reject(new Error("Instagram bio fetch timeout")), 5000))
      ]);
    } catch (error) {
      console.warn(`[${correlationId}] Instagram bio fetch timed out for ${handle}`);
      throw error; // Let the caller handle the timeout
    }
    
    // Generate a simulated bio
    const possibleBios = [
      "Music Producer | Beat Maker",
      "Producing hits since 2015",
      `Producer & Songwriter | @${handle}`,
      "Creating sounds that move you | DM for collabs",
      `The official page of ${handle}`,
      "Grammy-nominated producer",
      "Making beats and breaking records"
    ];
    
    const randomIndex = Math.floor(Math.random() * possibleBios.length);
    return possibleBios[randomIndex];
  }
}

// Create handler that serves the worker
const socialEnrichmentWorker = new SocialEnrichmentWorker();

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
    const response = await socialEnrichmentWorker.process(req);
    
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
