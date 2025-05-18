
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { TrackDiscoveryWorker } from "../_shared/workers/TrackDiscoveryWorker.ts";
import { logDebug } from "../_shared/debugHelper.ts";

// Initialize Redis client for distributed locking and idempotency
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

// CORS headers for API responses
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  const executionId = `track_disc_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
  
  // Initialize Supabase client
  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );
  
  try {
    logDebug("TrackDiscovery", `Starting track discovery process`, { executionId });
    
    // Initialize the TrackDiscoveryWorker
    const worker = new TrackDiscoveryWorker(supabase, redis);
    
    // Process messages in background to avoid CPU timeout
    const response = new Response(
      JSON.stringify({ processing: true, message: "Starting batch process" }), 
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
    // Process queue in background to avoid edge function timeout
    EdgeRuntime.waitUntil((async () => {
      try {
        // Process up to 5 batches of 3 messages each
        const result = await worker.processBatch({
          batchSize: 3,
          processorName: 'track_discovery',
          maxBatches: 5,
          timeoutSeconds: 60,
          visibilityTimeoutSeconds: 900, // 15 minutes
          sendToDlqOnMaxRetries: true,
          maxRetries: 3,
          logDetailedMetrics: true,
          deadLetterQueue: 'track_discovery_dlq'
        });
        
        logDebug("TrackDiscovery", `Completed background processing: ${result.processed} processed, ${result.errors} errors`, { executionId });
      } catch (error) {
        logDebug("TrackDiscovery", `Error in background queue processing:`, { 
          executionId, 
          error: error.message,
          stack: error.stack 
        });
      }
    })());
    
    return response;
  } catch (error) {
    logDebug("TrackDiscovery", `Unexpected error in track discovery worker:`, {
      executionId,
      error: error.message,
      stack: error.stack
    });
    
    return new Response(
      JSON.stringify({ error: error.message }), 
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
