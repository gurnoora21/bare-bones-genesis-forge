
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { processQueueMessageSafely } from "../_shared/queueHelper.ts";
import { DeduplicationService } from "../_shared/deduplication.ts";
import { getDeduplicationMetrics } from "../_shared/metrics.ts";
import { StateManager, EntityType, ProcessingState } from "../_shared/stateManager.ts";

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
  const stateManager = new StateManager(supabase, redis, true);

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
  const batchCorrelationId = `batch_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
  console.log(`[${batchCorrelationId}] Processing ${messages.length} messages`);

  // Process messages with background tasks
  const promises = messages.map(async (message) => {
    // Ensure the message is properly typed
    const msg = typeof message.message === 'string' 
      ? JSON.parse(message.message) as SocialEnrichmentMsg 
      : message.message as SocialEnrichmentMsg;
      
    const messageId = message.id;
    
    // Create idempotency key for this producer
    const idempotencyKey = `social_enrichment:producer:${msg.producerId}`;
    
    // Generate correlation ID for this message
    const correlationId = `${batchCorrelationId}:msg_${messageId}`;

    // Use enhanced processQueueMessageSafely with deduplication and state management
    const result = await processQueueMessageSafely(
      supabase,
      "social_enrichment",
      messageId.toString(),
      async () => await enrichProducerProfile(supabase, msg, correlationId),
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
        deduplication: {
          enabled: true,
          redis,
          strictMatching: true
        },
        stateManagement: {
          enabled: true,
          entityType: EntityType.PRODUCER,
          entityId: msg.producerId
        },
        correlationId
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
  correlationId: string
) {
  const { producerId, producerName } = msg;
  
  console.log(`[${correlationId}] Enriching producer profile for ${producerName} (${producerId})`);
  
  try {
    // First, search for potential Instagram handle
    const instagramHandle = await findInstagramHandle(producerName);
    
    // If found, get profile info
    let instagramBio = null;
    if (instagramHandle) {
      instagramBio = await getInstagramBio(instagramHandle);
      console.log(`[${correlationId}] Found Instagram profile for ${producerName}: @${instagramHandle}`);
    }
    
    // Update producer with social info
    const { error } = await supabase
      .from('producers')
      .update({
        instagram_handle: instagramHandle,
        instagram_bio: instagramBio,
        enriched_at: new Date().toISOString()
      })
      .eq('id', producerId);
    
    if (error) {
      throw new Error(`Error updating producer record: ${error.message}`);
    }
    
    console.log(`[${correlationId}] Successfully enriched producer profile for ${producerName}`);
    
    return {
      producerId,
      producerName,
      instagramHandle,
      success: true
    };
    
  } catch (error) {
    console.error(`[${correlationId}] Social enrichment failed for ${producerName}:`, error);
    
    // Mark the producer as having failed enrichment
    await supabase
      .from('producers')
      .update({
        enrichment_failed: true
      })
      .eq('id', producerId);
      
    throw error;
  }
}

// Simulated social media API calls
async function findInstagramHandle(name: string): Promise<string | null> {
  // In a real implementation, this would call a service or API
  // to search for the producer's Instagram handle
  
  // For now, simulate a search with randomized results
  console.log(`Searching for Instagram handle for "${name}"...`);
  
  // Simulate API call delay
  await new Promise(resolve => setTimeout(resolve, 500));
  
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
  
  // Simulate API call delay
  await new Promise(resolve => setTimeout(resolve, 700));
  
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
