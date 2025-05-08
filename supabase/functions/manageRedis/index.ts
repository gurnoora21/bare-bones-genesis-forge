
// Redis management edge function
// Provides operations to view, delete, and manage Redis keys

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getRedis } from "../_shared/upstashRedis.ts";

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
    // Initialize Redis client
    const redis = new Redis({
      url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
      token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
    });
    
    // Parse the request URL
    const url = new URL(req.url);
    const pathname = url.pathname.split('/').filter(Boolean);
    
    // Extract the operation from the path
    // Format: /manageRedis/{operation}
    const operation = pathname[1] || '';
    
    // Handle operation: clear-idempotency
    if (operation === 'clear-idempotency' && req.method === 'DELETE') {
      const pattern = "dedup:*";
      let cursor = 0;
      let totalDeleted = 0;
      
      console.log(`Scanning for keys matching pattern: ${pattern}`);
      
      // SCAN and DELETE in batches (Redis SCAN pattern)
      do {
        // Get keys in batches of 100
        const [nextCursor, keys] = await redis.scan(cursor, {
          match: pattern,
          count: 100
        });
        
        cursor = nextCursor;
        
        if (keys.length > 0) {
          console.log(`Found ${keys.length} keys to delete in this batch`);
          
          // Delete found keys
          const deleteCount = await redis.del(keys);
          totalDeleted += deleteCount;
          
          console.log(`Deleted ${deleteCount} keys in this batch`);
        }
      } while (cursor !== 0);
      
      return new Response(
        JSON.stringify({ 
          success: true, 
          message: `Successfully cleared ${totalDeleted} idempotency keys`,
          keysDeleted: totalDeleted
        }),
        { 
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          status: 200 
        }
      );
    }
    
    // Handle operation: clear-by-pattern
    if (operation === 'clear-by-pattern' && req.method === 'DELETE') {
      const { pattern } = await req.json();
      
      if (!pattern) {
        return new Response(
          JSON.stringify({ error: "Pattern is required" }),
          { 
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
            status: 400 
          }
        );
      }
      
      let cursor = 0;
      let totalDeleted = 0;
      
      console.log(`Scanning for keys matching pattern: ${pattern}`);
      
      // SCAN and DELETE in batches
      do {
        const [nextCursor, keys] = await redis.scan(cursor, {
          match: pattern,
          count: 100
        });
        
        cursor = nextCursor;
        
        if (keys.length > 0) {
          console.log(`Found ${keys.length} keys to delete in this batch`);
          
          // Delete found keys
          const deleteCount = await redis.del(keys);
          totalDeleted += deleteCount;
          
          console.log(`Deleted ${deleteCount} keys in this batch`);
        }
      } while (cursor !== 0);
      
      return new Response(
        JSON.stringify({ 
          success: true, 
          message: `Successfully cleared ${totalDeleted} keys matching pattern: ${pattern}`,
          pattern,
          keysDeleted: totalDeleted
        }),
        { 
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          status: 200 
        }
      );
    }
    
    // Handle operation: list-patterns
    if (operation === 'list-patterns' && req.method === 'GET') {
      // Get all keys
      let cursor = 0;
      const allKeys = [];
      
      do {
        const [nextCursor, keys] = await redis.scan(cursor, {
          count: 1000
        });
        
        cursor = nextCursor;
        allKeys.push(...keys);
      } while (cursor !== 0);
      
      // Group keys by pattern
      const patterns = {};
      for (const key of allKeys) {
        const keyParts = key.split(':');
        const prefix = keyParts[0];
        
        if (!patterns[prefix]) {
          patterns[prefix] = 0;
        }
        patterns[prefix]++;
      }
      
      return new Response(
        JSON.stringify({ 
          success: true,
          totalKeys: allKeys.length,
          patterns
        }),
        { 
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          status: 200 
        }
      );
    }
    
    // If operation not recognized
    return new Response(
      JSON.stringify({ 
        error: "Invalid operation", 
        availableOperations: [
          "/manageRedis/clear-idempotency (DELETE)",
          "/manageRedis/clear-by-pattern (DELETE)",
          "/manageRedis/list-patterns (GET)"
        ]
      }),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 400 
      }
    );
  } catch (error) {
    console.error("Error managing Redis:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      }
    );
  }
});
