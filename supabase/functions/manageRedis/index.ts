
// Edge function to manage Redis operations for idempotency and state coordination

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getEnvironmentTTL } from "../_shared/stateManager.ts";
import { DeduplicationService, getDeduplicationService } from "../_shared/deduplication.ts";

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

    // Initialize deduplication service
    const deduplicationService = getDeduplicationService(redis);
    
    // Parse request
    const requestData = await req.json();
    
    // Extract operation
    const operation = requestData.operation;
    
    if (!operation) {
      return new Response(
        JSON.stringify({ error: "Operation is required" }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    // Process based on operation
    switch (operation) {
      case 'clear-idempotency': {
        // Extract parameters
        const queueName = requestData.queueName || requestData.queue;
        const entityType = requestData.entityType;
        const pattern = requestData.pattern;
        const age = requestData.age || 0;
        
        // Determine which pattern to use for clearing keys
        let namespace;
        if (queueName) {
          namespace = queueName;
        } else if (entityType) {
          namespace = entityType;
        }
        
        console.log(`Clearing idempotency keys with: namespace=${namespace}, pattern=${pattern}, age=${age}`);
        
        // Convert age to seconds if provided
        const ageSeconds = age ? age * 60 : undefined;
        
        // Clear the keys
        const deletedCount = await deduplicationService.clearKeys(
          namespace,
          pattern,
          ageSeconds
        );
        
        return new Response(
          JSON.stringify({ 
            success: true,
            keysDeleted: deletedCount,
            operation: 'clear-idempotency',
            parameters: {
              namespace,
              pattern,
              age
            }
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      case 'get-key-count': {
        // Count keys matching a pattern
        const pattern = requestData.pattern || 'dedup:*';
        
        let cursor = '0';
        let keyCount = 0;
        
        do {
          const [nextCursor, keys] = await redis.scan(
            cursor,
            "MATCH",
            pattern,
            "COUNT",
            1000
          );
          
          cursor = nextCursor;
          keyCount += keys.length;
        } while (cursor !== '0');
        
        return new Response(
          JSON.stringify({ 
            success: true,
            keyCount,
            pattern
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      case 'redis-health-check': {
        // Simple health check
        const pingResult = await redis.ping();
        const now = new Date().toISOString();
        const ttl = getEnvironmentTTL();
        
        return new Response(
          JSON.stringify({ 
            success: true,
            ping: pingResult === 'PONG',
            timestamp: now,
            environmentTtl: ttl
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      default:
        return new Response(
          JSON.stringify({ error: `Unknown operation: ${operation}` }),
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
    }
  } catch (error) {
    console.error("Error managing Redis operations:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
