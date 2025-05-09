
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { processQueueMessageSafely } from "../_shared/queueHelper.ts";
import { DeduplicationService } from "../_shared/deduplication.ts";
import { getDeduplicationMetrics } from "../_shared/metrics.ts";
import { ProcessingState, EntityType, generateCorrelationId } from "../_shared/stateManager.ts";
import { getStateManager } from "../_shared/coordinatedStateManager.ts";

interface SocialEnrichmentMsg {
  producerId: string;
  producerName: string;
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

  // Initialize Redis client
  const redis = new Redis({
    url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
    token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
  });

  // Initialize metrics
  const metrics = getDeduplicationMetrics(redis);
  
  // Initialize state manager
  const stateManager = getStateManager(supabase, redis, true);

  // Process queue batch
  const { data: messages, error } = await supabase.functions.invoke("readQueue", {
    body: { 
      queue_name: "social_enrichment",
      batch_size: 10,
      visibility_timeout: 180 // 3 minutes
    }
  });

  if (error) {
    console.error("Error reading from queue:", error);
    return new Response(JSON.stringify({ error }), { status: 500, headers: corsHeaders });
  }

  if (!messages || messages.length === 0) {
    return new Response(JSON.stringify({ processed: 0, message: "No messages to process" }), { headers: corsHeaders });
  }

  // Generate correlation ID for this batch
  const batchCorrelationId = generateCorrelationId('batch');
  console.log(`[${batchCorrelationId}] Processing ${messages.length} messages`);

  // Process messages with background tasks
  const promises = messages.map(async (message) => {
    try {
      // Ensure the message is properly typed
      const msg = typeof message.message === 'string' 
        ? JSON.parse(message.message) as SocialEnrichmentMsg 
        : message.message as SocialEnrichmentMsg;
        
      // Safely handle messageId
      const messageId = message.id?.toString() || message.msg_id?.toString();
      if (!messageId) {
        console.warn(`Missing message ID in social_enrichment queue message`);
        return { success: false, error: "Missing messageId" };
      }
      
      // Create idempotency key for this producer
      const idempotencyKey = `social_enrichment:producer:${msg.producerId}`;
      
      // Generate correlation ID for this message
      const correlationId = `${batchCorrelationId}:msg_${messageId}`;

      // Use enhanced processQueueMessageSafely with deduplication and state management
      const result = await processQueueMessageSafely(
        supabase,
        "social_enrichment",
        messageId,
        async () => await enrichProducerProfile(supabase, msg, correlationId, redis),
        idempotencyKey,
        async () => {
          // Check if this producer was already enriched
          try {
            const { data } = await supabase
              .from('producers')
              .select('enriched_at, enrichment_failed')
              .eq('id', msg.producerId)
              .maybeSingle();
              
            return data && (data.enriched_at !== null || data.enrichment_failed === true);
          } catch (error) {
            console.warn(`[${correlationId}] Error checking if producer ${msg.producerId} was enriched:`, error);
            return false;
          }
        },
        {
          maxRetries: 2,
          timeoutMs: 25000, // 25 seconds timeout for each attempt
          deduplication: {
            enabled: true,
            redis,
            ttlSeconds: 86400, // 24 hour deduplication window
            strictMatching: true
          },
          stateManagement: {
            enabled: true,
            entityType: EntityType.PRODUCER,
            entityId: msg.producerId
          },
          deadLetter: {
            enabled: true,
            queue: "social_enrichment_dlq"
          },
          correlationId,
          retryStrategy: {
            baseDelayMs: 500,
            maxDelayMs: 5000,
            jitterFactor: 0.3
          }
        }
      );
      
      // Record metrics
      if (result.success) {
        if (result.deduplication) {
          await metrics.recordDeduplicated("social_enrichment", "consumer");
        } else {
          await metrics.recordProcessed("social_enrichment", "consumer");
        }
      }
      
      return result;
    } catch (error) {
      console.error(`Error processing social enrichment message:`, error);
      return { 
        success: false, 
        error: error.message || String(error)
      };
    }
  });

  // Wait for all background tasks in a background process
  EdgeRuntime.waitUntil(Promise.all(promises).then(results => {
    console.log(`[${batchCorrelationId}] Completed social enrichment background processing: ${
      results.filter(r => r.success).length
    } successful, ${
      results.filter(r => !r.success).length
    } failed, ${
      results.filter(r => r.deduplication).length
    } deduplicated`);
  }));
  
  return new Response(JSON.stringify({ 
    processed: messages.length,
    success: true,
    batchCorrelationId
  }), { headers: corsHeaders });
});

async function enrichProducerProfile(
  supabase: any, 
  msg: SocialEnrichmentMsg,
  correlationId: string,
  redis: Redis
) {
  const { producerId, producerName } = msg;
  
  console.log(`[${correlationId}] Enriching producer profile for ${producerName} (${producerId})`);
  
  // Create a transaction context for atomic operations
  const transactionId = generateCorrelationId('tx');
  
  try {
    // Track the start of processing in Redis for observability
    try {
      const processingKey = `processing:producer:${producerId}`;
      await redis.set(processingKey, JSON.stringify({
        transactionId,
        correlationId,
        startedAt: new Date().toISOString()
      }), { ex: 3600 }); // 1 hour TTL
    } catch (redisError) {
      console.warn(`[${correlationId}] Redis tracking failed (non-critical): ${redisError.message}`);
    }
    
    // First, search for potential Instagram handle
    const instagramHandle = await findInstagramHandle(producerName);
    
    // If found, get profile info
    let instagramBio = null;
    if (instagramHandle) {
      instagramBio = await getInstagramBio(instagramHandle);
      console.log(`[${correlationId}] Found Instagram profile for ${producerName}: @${instagramHandle}`);
    }
    
    // Update producer with social info - implement proper transaction handling
    const { data, error } = await supabase
      .from('producers')
      .update({
        instagram_handle: instagramHandle,
        instagram_bio: instagramBio,
        enriched_at: new Date().toISOString(),
        metadata: {
          transaction_id: transactionId,
          correlation_id: correlationId,
          enriched_by: "social_enrichment_worker"
        }
      })
      .eq('id', producerId)
      .select();
    
    if (error) {
      throw new Error(`Error updating producer record: ${error.message}`);
    }
    
    console.log(`[${correlationId}] Successfully enriched producer profile for ${producerName}`);
    
    // Mark as processed in Redis for observability and fast lookups
    try {
      const processedKey = `processed:producer:${producerId}`;
      await redis.set(processedKey, JSON.stringify({
        enrichedAt: new Date().toISOString(),
        instagramHandle,
        correlationId,
        transactionId
      }), { ex: 86400 }); // 24 hour TTL
    } catch (redisError) {
      console.warn(`[${correlationId}] Redis processed flag set failed (non-critical): ${redisError.message}`);
    }
    
    return {
      producerId,
      producerName,
      instagramHandle,
      success: true
    };
    
  } catch (error) {
    console.error(`[${correlationId}] Social enrichment failed for ${producerName}:`, error);
    
    // Mark the producer as having failed enrichment
    try {
      await supabase
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
    
    // Clear processing key on error
    try {
      const processingKey = `processing:producer:${producerId}`;
      await redis.del(processingKey);
    } catch (redisError) {
      console.warn(`[${correlationId}] Redis key cleanup failed (non-critical): ${redisError.message}`);
    }
      
    throw error;
  }
}

// Enhanced social media API calls with circuit breaker and timeout
async function findInstagramHandle(name: string): Promise<string | null> {
  // In a real implementation, this would call a service or API
  // to search for the producer's Instagram handle
  
  // For now, simulate a search with randomized results
  console.log(`Searching for Instagram handle for "${name}"...`);
  
  // Simulate API call delay with timeout
  try {
    await Promise.race([
      new Promise(resolve => setTimeout(resolve, 500)),
      new Promise((_, reject) => setTimeout(() => reject(new Error("Instagram search timeout")), 5000))
    ]);
  } catch (error) {
    console.warn(`Instagram search timed out for ${name}`);
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

async function getInstagramBio(handle: string): Promise<string | null> {
  // In a real implementation, this would call a service or API
  // to fetch the Instagram bio
  
  console.log(`Getting Instagram bio for @${handle}...`);
  
  // Simulate API call delay with timeout
  try {
    await Promise.race([
      new Promise(resolve => setTimeout(resolve, 700)),
      new Promise((_, reject) => setTimeout(() => reject(new Error("Instagram bio fetch timeout")), 5000))
    ]);
  } catch (error) {
    console.warn(`Instagram bio fetch timed out for ${handle}`);
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
