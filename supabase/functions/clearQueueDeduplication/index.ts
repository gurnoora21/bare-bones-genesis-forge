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
    
    // Parse request data
    let queueName: string;
    let pattern: string | undefined;
    let force: boolean = false;
    
    if (req.method === 'POST') {
      const body = await req.json();
      queueName = body.queueName || body.queue_name;
      pattern = body.pattern;
      force = !!body.force;
    } else {
      // Parse URL query parameters
      const url = new URL(req.url);
      queueName = url.searchParams.get('queue') || '';
      pattern = url.searchParams.get('pattern') || undefined;
      force = url.searchParams.get('force') === 'true';
    }
    
    if (!queueName) {
      return new Response(
        JSON.stringify({ error: 'Queue name is required' }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    let clearedKeys = 0;
    
    // Simple operation: just clear all Redis keys related to this queue
    try {
      // Use the more direct KEYS command for simplicity
      const keys = await redis.keys(`dedup:${queueName}:*`);
      
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
    } catch (redisError) {
      console.error(`Redis operation failed: ${redisError.message}`);
      // Continue anyway to try the DB operation - this is crucial
    }
    
    // For album and artist discovery, always try to re-queue some jobs
    // This is key to ensuring the pipeline keeps running even if Redis fails
    if (queueName === 'album_discovery' || queueName === 'artist_discovery') {
      // Get recent artists that might need album discovery
      const { data: artists, error } = await supabase
        .from('artists')
        .select('id')
        .order('created_at', { ascending: false })
        .limit(10);
      
      if (error) {
        return new Response(
          JSON.stringify({ 
            clearedKeys, 
            error: 'Failed to fetch recent artists',
            details: error.message
          }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      // Re-queue album discovery for these artists
      const requeued = [];
      for (const artist of artists) {
        try {
          const { data, error } = await supabase.rpc(
            'start_album_discovery',
            { 
              artist_id: artist.id, 
              offset_val: 0 
            }
          );
          
          if (!error) {
            requeued.push({
              artist_id: artist.id,
              message_id: data
            });
          }
        } catch (err) {
          console.error(`Error re-queuing album discovery for artist ${artist.id}:`, err);
        }
      }
      
      return new Response(
        JSON.stringify({ 
          clearedKeys, 
          message: `Successfully cleared ${clearedKeys} deduplication keys for ${queueName}`,
          requeued_artists: requeued 
        }),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    return new Response(
      JSON.stringify({ 
        clearedKeys, 
        message: `Successfully cleared ${clearedKeys} deduplication keys for ${queueName}`
      }),
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error("Error clearing deduplication keys:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
