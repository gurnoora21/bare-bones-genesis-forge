import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Initialize Supabase client
    const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
    const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
    const supabase = createClient(supabaseUrl, supabaseKey);
    
    // Initialize Redis client
    const redis = new Redis({
      url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
      token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
    });
    
    let clearedKeys = 0;
    let preservedKeys = 0;
    
    // Get all keys with the dedup prefix
    try {
      let cursor = '0';
      do {
        // Scan for all deduplication keys
        const [nextCursor, keys] = await redis.scan(
          cursor,
          "MATCH",
          "dedup:*",
          "COUNT",
          100
        );
        
        cursor = nextCursor;
        
        if (keys && keys.length > 0) {
          // Delete in batches to avoid huge commands
          for (let i = 0; i < keys.length; i += 50) {
            const batch = keys.slice(i, i + 50);
            if (batch.length > 0) {
              try {
                const deleteCount = await redis.del(...batch);
                clearedKeys += deleteCount;
              } catch (deleteError) {
                console.warn(`Error deleting batch: ${deleteError.message}`);
              }
            }
          }
        }
      } while (cursor !== '0');
      
      console.log(`Successfully cleared ${clearedKeys} deduplication keys`);
      
      return new Response(
        JSON.stringify({ 
          clearedKeys,
          preservedKeys,
          message: `Successfully cleared ${clearedKeys} deduplication keys`
        }),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } catch (redisError) {
      console.error(`Redis operation failed: ${redisError.message}`);
      return new Response(
        JSON.stringify({ 
          error: 'Failed to clear deduplication keys',
          details: redisError.message
        }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
  } catch (error) {
    console.error("Error clearing deduplication keys:", error);
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
