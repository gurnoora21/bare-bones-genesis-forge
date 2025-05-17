
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
    let preserveInProgress: boolean = true; // Default to true to preserve in-progress items
    
    if (req.method === 'POST') {
      const body = await req.json();
      queueName = body.queueName || body.queue_name;
      pattern = body.pattern;
      force = !!body.force;
      // Allow explicit override of preserveInProgress
      preserveInProgress = body.preserveInProgress === false ? false : true;
    } else {
      // Parse URL query parameters
      const url = new URL(req.url);
      queueName = url.searchParams.get('queue') || '';
      pattern = url.searchParams.get('pattern') || undefined;
      force = url.searchParams.get('force') === 'true';
      preserveInProgress = url.searchParams.get('preserveInProgress') !== 'false'; // Default to true unless explicitly false
    }
    
    if (!queueName) {
      return new Response(
        JSON.stringify({ error: 'Queue name is required' }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    let clearedKeys = 0;
    let preservedKeys = 0;
    
    // Check if this is album_discovery queue - we need to be more careful
    const isAlbumDiscovery = queueName === 'album_discovery';
    
    // Get keys but don't delete them yet
    try {
      let keys = await redis.keys(`dedup:${queueName}:*`);
      
      if (keys && keys.length > 0) {
        // If album_discovery queue and preserveInProgress is true, check for keys that might be in progress
        if (isAlbumDiscovery && preserveInProgress) {
          // Get all messages that are currently being processed (have visibility timeout)
          const { data: activeMessages } = await supabase.rpc('pg_dequeue', {
            queue_name: queueName,
            batch_size: 0, // We just want to peek, not actually dequeue
            visibility_timeout: 0 
          });
          
          // Parse the active messages
          let activeIds = [];
          if (activeMessages) {
            try {
              const messages = typeof activeMessages === 'string' ? 
                JSON.parse(activeMessages) : activeMessages;
              
              // Extract all message IDs and related deduplication keys
              if (Array.isArray(messages)) {
                activeIds = messages.map(msg => {
                  try {
                    const message = typeof msg.message === 'string' ? JSON.parse(msg.message) : msg.message;
                    // For album discovery, extract keys in the format artist:${spotifyId}:offset:${offset}
                    if (message.spotifyId) {
                      return `artist:${message.spotifyId}:offset:${message.offset || 0}`;
                    }
                    return null;
                  } catch (e) {
                    console.warn("Failed to parse message", e);
                    return null;
                  }
                }).filter(id => id !== null);
              }
            } catch (e) {
              console.warn("Failed to parse active messages", e);
            }
          }
          
          console.log(`Found ${activeIds.length} active messages in the ${queueName} queue`);
          
          // Filter out keys that are for active messages
          const keysToDelete = keys.filter(key => {
            // Extract the relevant part of the dedup key (after "dedup:queueName:")
            const relevantPart = key.substring(`dedup:${queueName}:`.length);
            const shouldPreserve = activeIds.some(id => relevantPart.includes(id));
            
            if (shouldPreserve) {
              preservedKeys++;
              return false; // Don't delete this key
            }
            return true; // Delete this key
          });
          
          console.log(`Preserving ${preservedKeys} keys for in-progress messages`);
          keys = keysToDelete;
        }
      
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
            preservedKeys,
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
          preservedKeys,
          message: `Successfully cleared ${clearedKeys} deduplication keys for ${queueName} (preserved ${preservedKeys} active keys)`,
          requeued_artists: requeued 
        }),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    return new Response(
      JSON.stringify({ 
        clearedKeys, 
        preservedKeys,
        message: `Successfully cleared ${clearedKeys} deduplication keys for ${queueName} (preserved ${preservedKeys} active keys)`
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
