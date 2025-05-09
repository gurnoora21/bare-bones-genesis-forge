
// Edge function to manage Redis operations for idempotency and state coordination

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getEnvironmentTTL } from "../_shared/stateManager.ts";
import { DeduplicationService, getDeduplicationService } from "../_shared/deduplication.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

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
    
    // Initialize Supabase client for database operations
    const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
    const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
    const supabase = createClient(supabaseUrl, supabaseKey);

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
    
    // Generate correlation ID for tracing
    const correlationId = `redis_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
    
    // Process based on operation
    switch (operation) {
      case 'clear-idempotency': {
        // Extract parameters
        const queueName = requestData.queueName || requestData.queue;
        const entityType = requestData.entityType;
        const pattern = requestData.pattern;
        const age = requestData.age || 0;
        const resetDatabase = !!requestData.resetDatabase;
        
        // Determine which pattern to use for clearing keys
        let namespace;
        if (queueName) {
          namespace = queueName;
        } else if (entityType) {
          namespace = entityType;
        }
        
        console.log(`[${correlationId}] Clearing idempotency keys with: namespace=${namespace}, pattern=${pattern}, age=${age}`);
        
        // Convert age to seconds if provided
        const ageSeconds = age ? age * 60 : undefined;
        
        // Clear the Redis keys
        const deletedCount = await deduplicationService.clearKeys(
          namespace,
          pattern,
          ageSeconds
        );
        
        // If database reset was requested, also reset entity processing state
        let dbResetCount = 0;
        if (resetDatabase && (entityType || namespace)) {
          const dbEntityType = entityType || namespace;
          const ageMinutes = age || 60;
          
          try {
            // Call database function to reset processing state
            const { data, error } = await supabase.rpc(
              'reset_entity_processing_state',
              {
                p_entity_type: dbEntityType,
                p_older_than_minutes: ageMinutes,
                p_target_states: ['COMPLETED', 'FAILED']
              }
            );
            
            if (error) {
              console.warn(`[${correlationId}] Database reset error: ${error.message}`);
            } else {
              dbResetCount = data?.length || 0;
              console.log(`[${correlationId}] Reset ${dbResetCount} database entities`);
            }
          } catch (dbError) {
            console.error(`[${correlationId}] Database reset exception: ${dbError.message}`);
          }
        }
        
        return new Response(
          JSON.stringify({ 
            success: true,
            keysDeleted: deletedCount,
            dbEntitiesReset: dbResetCount,
            operation: 'clear-idempotency',
            parameters: {
              namespace,
              pattern,
              age
            },
            correlationId
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
            pattern,
            correlationId
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      case 'check-inconsistencies': {
        // Check for inconsistencies between Redis and database
        const entityType = requestData.entityType;
        const minutesOld = requestData.minutes || 60;
        
        console.log(`[${correlationId}] Checking for inconsistencies: entityType=${entityType}, minutesOld=${minutesOld}`);
        
        // Get potentially inconsistent states from database
        const { data: dbStates, error: dbError } = await supabase.rpc(
          'find_inconsistent_states',
          {
            p_entity_type: entityType || null,
            p_older_than_minutes: minutesOld
          }
        );
        
        if (dbError) {
          console.error(`[${correlationId}] Database error: ${dbError.message}`);
          return new Response(
            JSON.stringify({ 
              success: false,
              error: dbError.message,
              correlationId
            }),
            { 
              status: 500,
              headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
            }
          );
        }
        
        // Check Redis state for each entity
        const inconsistencies = [];
        
        for (const entity of dbStates) {
          try {
            const stateKey = `state:${entity.entity_type}:${entity.entity_id}`;
            const redisState = await redis.get(stateKey);
            
            let redisStateValue = null;
            if (redisState) {
              try {
                const parsed = JSON.parse(redisState as string);
                redisStateValue = parsed.state;
              } catch (e) {
                redisStateValue = redisState;
              }
            }
            
            // If states don't match, it's an inconsistency
            if (redisStateValue !== entity.db_state) {
              inconsistencies.push({
                entityType: entity.entity_type,
                entityId: entity.entity_id,
                dbState: entity.db_state,
                redisState: redisStateValue,
                minutesSinceUpdate: entity.minutes_since_update
              });
            }
          } catch (redisError) {
            console.warn(`[${correlationId}] Redis error for ${entity.entity_type}:${entity.entity_id}: ${redisError.message}`);
            
            // Include in inconsistencies if Redis check fails
            inconsistencies.push({
              entityType: entity.entity_type,
              entityId: entity.entity_id,
              dbState: entity.db_state,
              redisState: null,
              minutesSinceUpdate: entity.minutes_since_update,
              error: redisError.message
            });
          }
        }
        
        console.log(`[${correlationId}] Found ${inconsistencies.length} inconsistencies out of ${dbStates.length} entities checked`);
        
        // Fix inconsistencies if requested
        let fixedCount = 0;
        if (requestData.fix && inconsistencies.length > 0) {
          for (const item of inconsistencies) {
            try {
              const stateKey = `state:${item.entityType}:${item.entityId}`;
              
              // Update Redis to match database
              await redis.set(stateKey, JSON.stringify({
                state: item.dbState,
                timestamp: new Date().toISOString(),
                metadata: {
                  autoFixed: true,
                  previousState: item.redisState
                }
              }), { ex: getEnvironmentTTL() });
              
              fixedCount++;
            } catch (fixError) {
              console.error(`[${correlationId}] Failed to fix inconsistency: ${fixError.message}`);
            }
          }
        }
        
        return new Response(
          JSON.stringify({ 
            success: true,
            inconsistenciesFound: inconsistencies.length,
            inconsistenciesFixed: fixedCount,
            entitiesChecked: dbStates.length,
            inconsistencies,
            correlationId
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      case 'redis-health-check': {
        // Simple health check
        const pingResult = await redis.ping();
        const now = new Date().toISOString();
        const ttl = getEnvironmentTTL();
        
        // Check memory usage
        let memoryInfo = {};
        try {
          const info = await redis.info("memory");
          memoryInfo = {
            info
          };
        } catch (memErr) {
          memoryInfo = {
            error: memErr.message
          };
        }
        
        return new Response(
          JSON.stringify({ 
            success: true,
            ping: pingResult === 'PONG',
            timestamp: now,
            environmentTtl: ttl,
            memory: memoryInfo,
            correlationId
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
