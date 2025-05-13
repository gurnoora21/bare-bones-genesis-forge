
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

// CORS headers for cross-origin requests
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

/**
 * System Reset Edge Function
 * 
 * This function provides emergency reset capabilities for:
 * 1. Circuit breakers (for API resilience)
 * 2. Stuck locks (both in Redis and database)
 * 3. Processing state cleanup
 */
serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }
  
  try {
    // Create a Supabase client
    const supabaseUrl = Deno.env.get('SUPABASE_URL') || '';
    const supabaseKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') || '';
    const supabase = createClient(supabaseUrl, supabaseKey);
    
    // Create Redis client for direct operations
    const redisUrl = Deno.env.get('UPSTASH_REDIS_REST_URL');
    const redisToken = Deno.env.get('UPSTASH_REDIS_REST_TOKEN');
    let redis = null;
    
    if (redisUrl && redisToken) {
      redis = new Redis({
        url: redisUrl,
        token: redisToken,
      });
    }
    
    // Parse request data
    const { action, entityType, entityId, worker, service, all } = await req.json();
    
    // Results object to track actions performed
    const results = {
      circuitBreaker: null,
      redisLocks: null,
      dbLocks: null,
      processingStates: null
    };
    
    // 1. Reset circuit breakers in Redis
    if (action === 'reset-circuit-breakers' || all) {
      try {
        if (redis) {
          const circuitBreakerKeys = await redis.keys('circuit:*');
          let deletedCount = 0;
          
          if (service) {
            // Reset only keys for a specific service
            const serviceKeys = circuitBreakerKeys.filter(key => key.includes(`:${service}:`));
            for (const key of serviceKeys) {
              await redis.del(key);
              deletedCount++;
            }
          } else {
            // Reset all circuit breaker keys
            for (const key of circuitBreakerKeys) {
              await redis.del(key);
              deletedCount++;
            }
          }
          
          results.circuitBreaker = {
            success: true,
            reset: deletedCount,
            message: `Reset ${deletedCount} circuit breaker(s)`
          };
        } else {
          results.circuitBreaker = {
            success: false,
            message: "Redis client not available"
          };
        }
      } catch (error) {
        results.circuitBreaker = {
          success: false,
          error: error.message
        };
      }
    }
    
    // 2. Clean up Redis locks
    if (action === 'clean-redis-locks' || all) {
      try {
        if (redis) {
          const lockPattern = entityType ? `lock:${entityType}:*` : 'lock:*';
          const lockKeys = await redis.keys(lockPattern);
          let deletedCount = 0;
          
          if (entityId && entityType) {
            // Delete specific lock
            const specificKey = `lock:${entityType}:${entityId}`;
            await redis.del(specificKey);
            deletedCount = 1;
          } else {
            // Delete all matching locks
            for (const key of lockKeys) {
              await redis.del(key);
              deletedCount++;
            }
          }
          
          results.redisLocks = {
            success: true,
            cleaned: deletedCount,
            message: `Removed ${deletedCount} Redis lock(s)`
          };
        } else {
          results.redisLocks = {
            success: false,
            message: "Redis client not available"
          };
        }
      } catch (error) {
        results.redisLocks = {
          success: false,
          error: error.message
        };
      }
    }
    
    // 3. Clean up database locks
    if (action === 'clean-db-locks' || all) {
      try {
        let query = supabase.from('processing_locks').delete();
        
        if (entityType) {
          query = query.eq('entity_type', entityType);
        }
        
        if (entityId) {
          query = query.eq('entity_id', entityId);
        }
        
        if (worker) {
          query = query.eq('worker_id', worker);
        }
        
        const { data, error, count } = await query.select('*');
        
        if (error) {
          throw error;
        }
        
        results.dbLocks = {
          success: true,
          cleaned: count,
          locks: data,
          message: `Removed ${count} database lock(s)`
        };
      } catch (error) {
        results.dbLocks = {
          success: false,
          error: error.message
        };
      }
    }
    
    // 4. Reset processing states
    if (action === 'reset-processing-states' || all) {
      try {
        let query = supabase
          .from('processing_status')
          .update({
            state: 'PENDING',
            last_processed_at: new Date().toISOString(),
            metadata: { reset_reason: 'manual_reset', reset_time: new Date().toISOString() }
          });
        
        if (entityType) {
          query = query.eq('entity_type', entityType);
        }
        
        if (entityId) {
          query = query.eq('entity_id', entityId);
        } else {
          // Only reset IN_PROGRESS states if no specific entity is targeted
          query = query.eq('state', 'IN_PROGRESS');
        }
        
        const { data, error, count } = await query.select('*');
        
        if (error) {
          throw error;
        }
        
        results.processingStates = {
          success: true,
          reset: count,
          states: data,
          message: `Reset ${count} processing state(s)`
        };
      } catch (error) {
        results.processingStates = {
          success: false,
          error: error.message
        };
      }
    }
    
    // Return the results
    return new Response(JSON.stringify({
      success: true,
      action: action || 'all',
      timestamp: new Date().toISOString(),
      results
    }), {
      headers: {
        ...corsHeaders,
        'Content-Type': 'application/json'
      }
    });
  } catch (error) {
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: {
        ...corsHeaders,
        'Content-Type': 'application/json'
      }
    });
  }
});
