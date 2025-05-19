
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
    let matchedKeys = 0;
    const keysByPattern = {}; // Track keys by pattern for better debugging
    const oldFormatKeys = []; // Track old format keys specifically
    
    // Define patterns to match both old and new format keys
    const patterns = [
      "dedup:*", // Standard pattern for all deduplication keys
      "dedup:track_discovery:track:*", // Old format track keys
      "dedup:track_discovery:album:*", // Old format album keys
      "dedup:producer_identification:track:*" // Old format producer keys
    ];
    
    for (const pattern of patterns) {
      console.log(`Processing pattern: ${pattern}`);
      
      // Get all keys with the specified pattern
      try {
        let cursor = 0;
        do {
          console.log(`Starting scan for pattern ${pattern} with cursor: ${cursor}`);
          
          // Use scan with proper parameters for Upstash
          const scanResult = await redis.scan(cursor, {
            match: pattern,
            count: 100
          });
          
          if (!scanResult || !Array.isArray(scanResult) || scanResult.length !== 2) {
            console.warn(`Unexpected scan result format for pattern ${pattern}: ${JSON.stringify(scanResult)}`);
            break;
          }
          
          const [nextCursor, keys] = scanResult;
          console.log(`Scan for pattern ${pattern} returned ${keys?.length || 0} keys. Next cursor: ${nextCursor}`);
          matchedKeys += keys?.length || 0;
          
          cursor = parseInt(nextCursor);
          
          if (keys && Array.isArray(keys) && keys.length > 0) {
            // Filter out any null or undefined keys
            const validKeys = keys.filter(key => {
              if (key == null) {
                console.warn('Found null/undefined key in scan results');
                return false;
              }
              return true;
            });
            
            console.log(`After filtering, processing ${validKeys.length} valid keys for pattern ${pattern}`);
            
            // Categorize keys by pattern for better debugging
            validKeys.forEach(key => {
              // Extract pattern from key for reporting
              const keyPattern = key.split(':').slice(0, 2).join(':');
              if (!keysByPattern[keyPattern]) {
                keysByPattern[keyPattern] = 0;
              }
              keysByPattern[keyPattern]++;
              
              // Identify old format keys
              if (key.includes('track_discovery:track:') || 
                  key.includes('track_discovery:album:') || 
                  key.includes('producer_identification:track:')) {
                oldFormatKeys.push(key);
              }
            });
            
            // Delete in batches to avoid huge commands
            for (let i = 0; i < validKeys.length; i += 50) {
              const batch = validKeys.slice(i, i + 50);
              console.log(`Processing batch of ${batch.length} keys for pattern ${pattern}`);
              
              if (batch.length > 0) {
                try {
                  // Process each key individually to avoid null arguments
                  for (const key of batch) {
                    try {
                      if (typeof key !== 'string' || !key) {
                        console.warn(`Skipping invalid key: ${key}`);
                        continue;
                      }
                      
                      console.log(`Attempting to delete key: ${key}`);
                      await redis.del(key);
                      clearedKeys++;
                      console.log(`Successfully deleted key: ${key}`);
                    } catch (keyError) {
                      console.error(`Error deleting key ${key}:`, {
                        error: keyError.message,
                        stack: keyError.stack,
                        key: key
                      });
                    }
                  }
                } catch (batchError) {
                  console.error('Error processing batch:', {
                    error: batchError.message,
                    stack: batchError.stack,
                    batchSize: batch.length
                  });
                }
              }
            }
          }
        } while (cursor !== 0);
      } catch (patternError) {
        console.error(`Error processing pattern ${pattern}:`, {
          error: patternError.message,
          stack: patternError.stack
        });
      }
    }
    
    console.log(`Successfully cleared ${clearedKeys} deduplication keys`);
    console.log(`Keys by pattern: ${JSON.stringify(keysByPattern, null, 2)}`);
    console.log(`Old format keys found: ${oldFormatKeys.length}`);
    console.log(`Old format keys: ${JSON.stringify(oldFormatKeys.slice(0, 10), null, 2)}${oldFormatKeys.length > 10 ? '...' : ''}`);
    
    return new Response(
      JSON.stringify({ 
        clearedKeys,
        preservedKeys,
        matchedKeys,
        keysByPattern,
        oldFormatKeysFound: oldFormatKeys.length,
        oldFormatKeysSample: oldFormatKeys.slice(0, 10),
        message: `Successfully cleared ${clearedKeys} deduplication keys (including ${oldFormatKeys.length} old format keys)`
      }),
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (redisError) {
    console.error('Redis operation failed:', {
      error: redisError.message,
      stack: redisError.stack,
      name: redisError.name
    });
    return new Response(
      JSON.stringify({ 
        error: 'Failed to clear deduplication keys',
        details: redisError.message,
        stack: redisError.stack
      }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error("Error clearing deduplication keys:", {
      error: error.message,
      stack: error.stack,
      name: error.name
    });
    return new Response(
      JSON.stringify({ 
        error: error.message,
        stack: error.stack
      }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
