
// Redis management edge function
// Provides operations to view, delete, and manage Redis keys

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getRedis } from "../_shared/upstashRedis.ts";
import { getEnvironmentTTL } from "../_shared/stateManager.ts";

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
    
    // Check if we're receiving body data or path parameters
    let operation = '';
    let queueName = '';
    let age = 0;
    
    // Parse the request URL for path-based operations
    const url = new URL(req.url);
    const pathname = url.pathname.split('/').filter(Boolean);
    
    // Try to get operation from URL path first
    if (pathname.length > 1) {
      operation = pathname[1];
      
      // Check for queue name in the URL (e.g., /manageRedis/clear-queue/artist_discovery)
      if (pathname.length > 2) {
        queueName = pathname[2];
      }
    } 
    
    // If not found in URL, try to get from request body
    if (!operation && req.body) {
      try {
        const body = await req.json();
        operation = body.operation || '';
        queueName = body.queueName || body.queue || '';
        age = body.age || body.ageMinutes || 0;
      } catch (e) {
        console.log("No valid JSON body or no operation specified");
      }
    }
    
    console.log(`Processing operation: ${operation}, queue: ${queueName}, age: ${age}`);
    
    // Handle operation: clear-idempotency
    if (operation === 'clear-idempotency' && req.method === 'DELETE') {
      const pattern = queueName ? `dedup:${queueName}:*` : "dedup:*";
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
          
          // If age filter is provided, only delete keys older than the specified age
          if (age > 0) {
            const keysToDelete = [];
            
            // For each key, check when it was created
            for (const key of keys) {
              const ttl = await redis.ttl(key);
              const defaultTtl = getEnvironmentTTL();
              
              // If TTL is less than (default - age), it's older than the specified age
              if (ttl < 0 || defaultTtl - ttl > age * 60) {
                keysToDelete.push(key);
              }
            }
            
            if (keysToDelete.length > 0) {
              const deleteCount = await redis.del(keysToDelete);
              totalDeleted += deleteCount;
              console.log(`Deleted ${deleteCount} keys older than ${age} minutes in this batch`);
            }
          } else {
            // Delete all keys in this batch
            const deleteCount = await redis.del(keys);
            totalDeleted += deleteCount;
            console.log(`Deleted ${deleteCount} keys in this batch`);
          }
        }
      } while (cursor !== 0);
      
      return new Response(
        JSON.stringify({ 
          success: true, 
          message: `Successfully cleared ${totalDeleted} idempotency keys`,
          keysDeleted: totalDeleted,
          pattern,
          queueName: queueName || "all queues",
          ageFilter: age > 0 ? `${age} minutes` : "none"
        }),
        { 
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          status: 200 
        }
      );
    }
    
    // Handle operation: clear-by-pattern
    if (operation === 'clear-by-pattern' && req.method === 'DELETE') {
      let pattern = '';
      
      try {
        const body = await req.json();
        pattern = body.pattern || '';
      } catch (e) {
        return new Response(
          JSON.stringify({ error: "Pattern is required in the request body" }),
          { 
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
            status: 400 
          }
        );
      }
      
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
      
      // Group deduplication keys by queue
      const deduplicationQueues = {};
      for (const key of allKeys) {
        if (key.startsWith('dedup:')) {
          const keyParts = key.split(':');
          if (keyParts.length >= 2) {
            const queueName = keyParts[1];
            if (!deduplicationQueues[queueName]) {
              deduplicationQueues[queueName] = 0;
            }
            deduplicationQueues[queueName]++;
          }
        }
      }
      
      return new Response(
        JSON.stringify({ 
          success: true,
          totalKeys: allKeys.length,
          patterns,
          deduplicationQueues
        }),
        { 
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          status: 200 
        }
      );
    }
    
    // Handle operation: get-key-info
    if (operation === 'get-key-info' && req.method === 'GET') {
      try {
        const body = await req.json();
        const keyPattern = body.pattern || '';
        
        if (!keyPattern) {
          return new Response(
            JSON.stringify({ error: "Key pattern is required" }),
            { 
              headers: { ...corsHeaders, 'Content-Type': 'application/json' },
              status: 400 
            }
          );
        }
        
        // Get all keys matching the pattern
        let cursor = 0;
        const matchingKeys = [];
        
        do {
          const [nextCursor, keys] = await redis.scan(cursor, {
            match: keyPattern,
            count: 100
          });
          
          cursor = nextCursor;
          matchingKeys.push(...keys);
        } while (cursor !== 0);
        
        // Get info for each key
        const keyInfoPromises = matchingKeys.map(async (key) => {
          const ttl = await redis.ttl(key);
          const value = await redis.get(key);
          
          return {
            key,
            ttl,
            value,
            expiresAt: ttl > 0 ? new Date(Date.now() + ttl * 1000).toISOString() : null
          };
        });
        
        const keyInfo = await Promise.all(keyInfoPromises);
        
        return new Response(
          JSON.stringify({ 
            success: true,
            keyCount: matchingKeys.length,
            keyInfo
          }),
          { 
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
            status: 200 
          }
        );
      } catch (error) {
        return new Response(
          JSON.stringify({ error: error.message }),
          { 
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
            status: 500 
          }
        );
      }
    }
    
    // If operation not recognized
    return new Response(
      JSON.stringify({ 
        error: "Invalid operation", 
        availableOperations: [
          "/manageRedis/clear-idempotency (DELETE)",
          "/manageRedis/clear-by-pattern (DELETE)",
          "/manageRedis/list-patterns (GET)",
          "/manageRedis/get-key-info (GET)"
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
