
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { DeduplicationService } from "../_shared/deduplication.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Default deduplication settings by queue type
const queueDeduplicationSettings = {
  // Default settings, applied when specific queue settings are not found
  default: {
    enabled: true,
    ttlSeconds: 300, // 5 minutes
    strictMatching: false
  },
  // Queue-specific settings
  artist_discovery: {
    enabled: true,
    ttlSeconds: 3600, // 1 hour
    strictMatching: false
  },
  album_discovery: {
    enabled: true,
    ttlSeconds: 1800, // 30 minutes
    strictMatching: false
  },
  track_discovery: {
    enabled: true,
    ttlSeconds: 1800, // 30 minutes
    strictMatching: false
  },
  producer_identification: {
    enabled: true,
    ttlSeconds: 3600, // 1 hour
    strictMatching: true
  },
  social_enrichment: {
    enabled: true,
    ttlSeconds: 3600 * 24, // 24 hours
    strictMatching: true
  }
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const { queue_name, message, deduplication_options } = await req.json();
    
    if (!queue_name || !message) {
      throw new Error("queue_name and message are required");
    }
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    // Initialize Redis for deduplication
    const redis = new Redis({
      url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
      token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
    });
    
    // Set up deduplication service
    const deduplicationService = new DeduplicationService(redis);
    
    // Get deduplication settings for this queue
    const queueSettings = queueDeduplicationSettings[queue_name] || queueDeduplicationSettings.default;
    
    // Merge with any custom options provided in the request
    const dedupOptions = {
      ...queueSettings,
      ...(deduplication_options || {})
    };

    // Check for duplicate message if deduplication is enabled
    if (dedupOptions.enabled) {
      try {
        const isDuplicate = await deduplicationService.isDuplicate(
          queue_name, 
          message,
          {
            ttlSeconds: dedupOptions.ttlSeconds,
            useStrictPayloadMatch: dedupOptions.strictMatching
          }
        );
        
        if (isDuplicate) {
          console.log(`Duplicate detected, skipping enqueue to ${queue_name}: ${JSON.stringify(message).substring(0, 100)}...`);
          
          return new Response(
            JSON.stringify({
              success: true,
              messageId: null,
              duplicate: true,
              message: `Duplicate message detected and skipped for queue ${queue_name}`
            }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
      } catch (dedupError) {
        // Log but continue if deduplication check fails
        console.warn(`Deduplication check failed for ${queue_name}, proceeding with enqueue:`, dedupError);
      }
    }
    
    console.log(`Sending message to queue ${queue_name}: ${JSON.stringify(message)}`);
    
    const { data: messageId, error } = await supabase.rpc('pg_enqueue', {
      queue_name,
      message_body: JSON.stringify(message)
    });
    
    if (error) {
      console.error(`Error sending to queue ${queue_name}:`, error);
      throw error;
    }
    
    console.log(`Successfully sent message to queue ${queue_name} with ID: ${messageId}`);
    
    return new Response(
      JSON.stringify({ success: true, messageId }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Error sending to queue:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
