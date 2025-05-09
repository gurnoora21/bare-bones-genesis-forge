
/**
 * Lock Manager Edge Function
 * 
 * Provides distributed lock acquisition and management capabilities
 * for coordinating worker processes.
 */

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

// Allow CORS
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Initialize Redis client
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  // Initialize Supabase client
  const supabaseClient = createClient(
    Deno.env.get("SUPABASE_URL") || "",
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || ""
  );

  try {
    const { operation, entityType, entityId, options } = await req.json();
    
    // Default options
    const lockOptions = {
      timeoutMinutes: options?.timeoutMinutes || 30,
      ttlSeconds: options?.ttlSeconds || 1800,
      workerId: options?.workerId || `worker_${Math.random().toString(36).substring(2, 10)}`,
      correlationId: options?.correlationId || `lock_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`,
    };
    
    // Validate required parameters
    if (!entityType || !entityId) {
      return new Response(
        JSON.stringify({ 
          error: "Missing required parameters: entityType and entityId are required" 
        }),
        { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    let result;
    let statusCode = 200;

    // Switch based on the requested operation
    switch (operation) {
      case "acquire":
        result = await acquireLock(supabaseClient, redis, entityType, entityId, lockOptions);
        if (!result.acquired) {
          statusCode = 409; // Conflict - lock already held
        }
        break;
        
      case "release":
        result = await releaseLock(supabaseClient, redis, entityType, entityId, lockOptions);
        break;
        
      case "heartbeat":
        result = await updateHeartbeat(supabaseClient, redis, entityType, entityId, lockOptions);
        if (!result.updated) {
          statusCode = 404; // Not Found - lock doesn't exist or isn't held by this worker
        }
        break;
        
      case "check":
        result = await checkLock(supabaseClient, redis, entityType, entityId);
        break;
        
      case "steal":
        result = await stealStaleLock(supabaseClient, redis, entityType, entityId, lockOptions);
        if (!result.stolen) {
          statusCode = 409; // Conflict - lock not stale or race condition
        }
        break;
        
      default:
        return new Response(
          JSON.stringify({ error: "Invalid operation. Supported operations: acquire, release, heartbeat, check, steal" }),
          { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
        );
    }

    // Return the result
    return new Response(
      JSON.stringify(result),
      { 
        status: statusCode,
        headers: { 
          ...corsHeaders,
          "Content-Type": "application/json" 
        } 
      }
    );
  } catch (error) {
    console.error("Error handling lock operation:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        status: 500, 
        headers: { 
          ...corsHeaders,
          "Content-Type": "application/json" 
        } 
      }
    );
  }
});

/**
 * Acquire a lock for an entity
 */
async function acquireLock(supabase: any, redis: Redis, entityType: string, entityId: string, options: any): Promise<any> {
  const lockKey = `lock:${entityType}:${entityId}`;
  const redisLockTtl = options.ttlSeconds;
  
  // Try Redis lock first (faster)
  try {
    const lockData = {
      acquiredAt: new Date().toISOString(),
      workerId: options.workerId,
      correlationId: options.correlationId,
      heartbeatEnabled: true
    };
    
    // Try to set the lock with NX (only if it doesn't exist)
    const result = await redis.set(lockKey, JSON.stringify(lockData), {
      nx: true,
      ex: redisLockTtl
    });

    if (result === "OK") {
      // Also try to acquire DB lock for dual-system approach
      try {
        // Try to acquire a lock in the database
        const { data: dbLockResult, error: dbLockError } = await supabase.rpc(
          'acquire_processing_lock',
          {
            p_entity_type: entityType,
            p_entity_id: entityId,
            p_timeout_minutes: Math.ceil(options.timeoutMinutes),
            p_correlation_id: options.correlationId
          }
        );
        
        if (dbLockError) {
          console.warn("Warning: Redis lock acquired but DB lock failed:", dbLockError.message);
        }
        
        // Return success even if DB lock fails - Redis is the primary
        return {
          acquired: true,
          method: "redis",
          correlationId: options.correlationId,
          dbLockAcquired: !dbLockError && dbLockResult === true,
          lockData
        };
      } catch (dbError) {
        console.warn("Database lock error:", dbError.message);
        // Still return success if Redis lock was acquired
        return {
          acquired: true,
          method: "redis",
          correlationId: options.correlationId,
          dbLockAcquired: false,
          dbError: dbError.message,
          lockData
        };
      }
    } else {
      // Check if we should try database as fallback
      // Try to acquire a lock in the database
      try {
        const { data: dbLockResult, error: dbLockError } = await supabase.rpc(
          'acquire_processing_lock',
          {
            p_entity_type: entityType,
            p_entity_id: entityId,
            p_timeout_minutes: Math.ceil(options.timeoutMinutes),
            p_correlation_id: options.correlationId
          }
        );
        
        if (dbLockError || !dbLockResult) {
          // Both Redis and DB locks failed
          return { 
            acquired: false,
            reason: "Both Redis and database locks failed",
            redisError: "Lock already exists",
            dbError: dbLockError ? dbLockError.message : "Lock acquisition returned false"
          };
        }
        
        // DB lock succeeded as fallback
        return {
          acquired: true,
          method: "database",
          correlationId: options.correlationId,
          redisError: "Lock already exists, but database lock succeeded as fallback"
        };
      } catch (dbError) {
        // Both Redis and database locks failed
        return {
          acquired: false,
          reason: "Both Redis and database locks failed",
          redisError: "Lock already exists",
          dbError: dbError.message
        };
      }
    }
  } catch (redisError) {
    console.error("Redis error during lock acquisition:", redisError);
    
    // Try database as fallback
    try {
      // Try to acquire a lock in the database
      const { data: dbLockResult, error: dbLockError } = await supabase.rpc(
        'acquire_processing_lock',
        {
          p_entity_type: entityType,
          p_entity_id: entityId,
          p_timeout_minutes: Math.ceil(options.timeoutMinutes),
          p_correlation_id: options.correlationId
        }
      );
      
      if (dbLockError || !dbLockResult) {
        // Both Redis and DB locks failed
        return { 
          acquired: false,
          reason: "Both Redis and database locks failed",
          redisError: redisError.message,
          dbError: dbLockError ? dbLockError.message : "Lock acquisition returned false"
        };
      }
      
      // DB lock succeeded as fallback
      return {
        acquired: true,
        method: "database",
        correlationId: options.correlationId,
        redisError: redisError.message
      };
    } catch (dbError) {
      // Both Redis and DB locks failed
      return {
        acquired: false,
        reason: "Both Redis and database locks failed",
        redisError: redisError.message,
        dbError: dbError.message
      };
    }
  }
}

/**
 * Release a lock for an entity
 */
async function releaseLock(supabase: any, redis: Redis, entityType: string, entityId: string, options: any): Promise<any> {
  const lockKey = `lock:${entityType}:${entityId}`;
  const results = { redisReleased: false, dbReleased: false };
  let errors: any = {};
  
  // Try to release Redis lock
  try {
    // Get current lock data to check if it's owned by this worker
    const lockData = await redis.get(lockKey);
    
    if (lockData) {
      try {
        const parsedLock = JSON.parse(lockData as string);
        
        // Only delete if lock is owned by this worker, or workerId not specified
        if (!options.workerId || parsedLock.workerId === options.workerId) {
          await redis.del(lockKey);
          results.redisReleased = true;
        } else {
          errors.redis = "Lock is owned by a different worker";
        }
      } catch (parseError) {
        // If parsing fails, attempt to delete anyway
        await redis.del(lockKey);
        results.redisReleased = true;
        errors.redisWarning = "Lock data could not be parsed, deleted anyway";
      }
    } else {
      results.redisReleased = true; // Lock doesn't exist, consider it released
      errors.redisWarning = "Lock did not exist";
    }
  } catch (redisError) {
    errors.redis = redisError.message;
  }
  
  // Try to release database lock
  try {
    const { data: dbReleaseResult, error: dbReleaseError } = await supabase.rpc(
      'release_processing_lock',
      {
        p_entity_type: entityType,
        p_entity_id: entityId
      }
    );
    
    if (dbReleaseError) {
      errors.db = dbReleaseError.message;
    } else {
      results.dbReleased = dbReleaseResult === true;
      if (!results.dbReleased) {
        errors.dbWarning = "Database lock was not released, it may not have existed or was in wrong state";
      }
    }
  } catch (dbError) {
    errors.db = dbError.message;
  }
  
  // Check if both systems released the lock
  const success = results.redisReleased || results.dbReleased;
  
  return {
    released: success,
    results,
    errors: Object.keys(errors).length > 0 ? errors : undefined
  };
}

/**
 * Update the heartbeat for a lock
 */
async function updateHeartbeat(supabase: any, redis: Redis, entityType: string, entityId: string, options: any): Promise<any> {
  const lockKey = `lock:${entityType}:${entityId}`;
  const heartbeatKey = `heartbeat:${entityType}:${entityId}`;
  const results = { redisUpdated: false, dbUpdated: false };
  let errors: any = {};
  
  // Try to update Redis lock heartbeat
  try {
    // Get current lock data
    const lockData = await redis.get(lockKey);
    
    if (lockData) {
      try {
        const parsedLock = JSON.parse(lockData as string);
        
        // Only update if lock is owned by this worker
        if (parsedLock.workerId === options.workerId) {
          // Update the lock with new heartbeat info
          parsedLock.lastHeartbeat = new Date().toISOString();
          parsedLock.heartbeatCount = (parsedLock.heartbeatCount || 0) + 1;
          
          // Extend TTL on heartbeat
          await redis.set(lockKey, JSON.stringify(parsedLock), {
            xx: true, // Only set if key exists
            ex: options.ttlSeconds // Reset TTL with each heartbeat
          });
          
          // Also set a separate heartbeat key
          await redis.set(heartbeatKey, JSON.stringify({
            workerId: options.workerId,
            correlationId: options.correlationId,
            timestamp: new Date().toISOString()
          }), {
            ex: options.ttlSeconds
          });
          
          results.redisUpdated = true;
        } else {
          errors.redis = "Lock is owned by a different worker";
        }
      } catch (parseError) {
        errors.redis = "Failed to parse lock data: " + parseError.message;
      }
    } else {
      errors.redis = "Lock does not exist";
    }
  } catch (redisError) {
    errors.redis = redisError.message;
  }
  
  // Try to update database lock heartbeat
  try {
    const { data: dbHeartbeatResult, error: dbHeartbeatError } = await supabase.rpc(
      'update_lock_heartbeat',
      {
        p_entity_type: entityType,
        p_entity_id: entityId,
        p_worker_id: options.workerId,
        p_correlation_id: options.correlationId
      }
    );
    
    if (dbHeartbeatError) {
      errors.db = dbHeartbeatError.message;
    } else {
      results.dbUpdated = dbHeartbeatResult === true;
      if (!results.dbUpdated) {
        errors.dbWarning = "Database heartbeat was not updated, lock may not exist or is owned by different worker";
      }
    }
  } catch (dbError) {
    errors.db = dbError.message;
  }
  
  // Consider success if either Redis or DB heartbeat was updated
  const success = results.redisUpdated || results.dbUpdated;
  
  return {
    updated: success,
    results,
    errors: Object.keys(errors).length > 0 ? errors : undefined
  };
}

/**
 * Check if a lock exists and get its details
 */
async function checkLock(supabase: any, redis: Redis, entityType: string, entityId: string): Promise<any> {
  const lockKey = `lock:${entityType}:${entityId}`;
  const results: any = { redisLock: null, dbLock: null };
  let errors: any = {};
  
  // Check Redis lock
  try {
    const lockData = await redis.get(lockKey);
    
    if (lockData) {
      try {
        results.redisLock = JSON.parse(lockData as string);
        results.redisLock.source = "redis";
        results.exists = true;
      } catch (parseError) {
        results.redisLock = { raw: lockData, parseError: parseError.message };
        errors.redisWarning = "Failed to parse lock data";
      }
    }
  } catch (redisError) {
    errors.redis = redisError.message;
  }
  
  // Check database lock
  try {
    const { data: processingStatus, error: dbLookupError } = await supabase
      .from('processing_status')
      .select('*')
      .eq('entity_type', entityType)
      .eq('entity_id', entityId)
      .maybeSingle();
    
    if (dbLookupError) {
      errors.db = dbLookupError.message;
    } else if (processingStatus) {
      results.dbLock = {
        state: processingStatus.state,
        lastProcessedAt: processingStatus.last_processed_at,
        metadata: processingStatus.metadata,
        source: "database"
      };
      
      if (processingStatus.state === 'IN_PROGRESS') {
        results.exists = true;
      }
    }
  } catch (dbError) {
    errors.db = dbError.message;
  }
  
  // Also check the processing_locks table if it exists
  try {
    const { data: processingLock, error: lockLookupError } = await supabase
      .from('processing_locks')
      .select('*')
      .eq('entity_type', entityType)
      .eq('entity_id', entityId)
      .maybeSingle();
    
    if (!lockLookupError && processingLock) {
      results.dbLock = {
        ...results.dbLock,
        lockDetails: processingLock,
        locksTableExists: true
      };
      results.exists = true;
    }
  } catch (lockError) {
    // This might fail if the table doesn't exist, which is fine
    errors.dbLockTableWarning = "Processing_locks table might not exist or is inaccessible";
  }
  
  return {
    exists: results.exists === true,
    locks: results,
    errors: Object.keys(errors).length > 0 ? errors : undefined
  };
}

/**
 * Attempt to steal a stale lock
 */
async function stealStaleLock(supabase: any, redis: Redis, entityType: string, entityId: string, options: any): Promise<any> {
  const lockKey = `lock:${entityType}:${entityId}`;
  const results = { redisStolen: false, dbStolen: false };
  let errors: any = {};
  const staleThresholdSeconds = options.staleThresholdSeconds || 300; // Default 5 minutes
  
  // Try to steal Redis lock if it's stale
  try {
    // Get current lock data
    const lockData = await redis.get(lockKey);
    
    if (lockData) {
      try {
        const parsedLock = JSON.parse(lockData as string);
        const acquiredAt = new Date(parsedLock.acquiredAt);
        const lastHeartbeat = parsedLock.lastHeartbeat 
          ? new Date(parsedLock.lastHeartbeat)
          : acquiredAt;
        
        const now = new Date();
        const secondsSinceHeartbeat = (now.getTime() - lastHeartbeat.getTime()) / 1000;
        
        // Check if lock is stale based on heartbeat
        if (secondsSinceHeartbeat > staleThresholdSeconds) {
          // The lock is stale, try to steal it
          const newLockData = {
            acquiredAt: now.toISOString(),
            workerId: options.workerId,
            correlationId: options.correlationId,
            heartbeatEnabled: true,
            stolenAt: now.toISOString(),
            stolenFrom: parsedLock.workerId || 'unknown',
            previousHeartbeat: parsedLock.lastHeartbeat
          };
          
          // Set new lock data, but only if the key still exists
          const stealResult = await redis.set(lockKey, JSON.stringify(newLockData), {
            xx: true, // Only set if key exists
            ex: options.ttlSeconds
          });
          
          if (stealResult === "OK") {
            results.redisStolen = true;
          } else {
            errors.redis = "Failed to steal lock, it may have been released or updated concurrently";
          }
        } else {
          errors.redis = `Lock is not stale. Last heartbeat was ${secondsSinceHeartbeat.toFixed(1)} seconds ago, threshold is ${staleThresholdSeconds} seconds`;
        }
      } catch (parseError) {
        errors.redis = "Failed to parse lock data: " + parseError.message;
      }
    } else {
      errors.redis = "Lock does not exist";
    }
  } catch (redisError) {
    errors.redis = redisError.message;
  }
  
  // Try to steal database lock if it's stale
  try {
    // Check if claim_stale_lock function exists
    const { data: dbStealResult, error: dbStealError } = await supabase.rpc(
      'claim_stale_lock',
      {
        p_entity_type: entityType,
        p_entity_id: entityId,
        p_new_worker_id: options.workerId,
        p_correlation_id: options.correlationId,
        p_stale_threshold_seconds: staleThresholdSeconds
      }
    );
    
    if (dbStealError) {
      // Fall back to the processing_status approach
      try {
        // First check if the lock is stale
        const { data: processingStatus, error: lookupError } = await supabase
          .from('processing_status')
          .select('state, last_processed_at')
          .eq('entity_type', entityType)
          .eq('entity_id', entityId)
          .maybeSingle();
        
        if (lookupError) {
          errors.db = lookupError.message;
        } else if (processingStatus && processingStatus.state === 'IN_PROGRESS') {
          const lastProcessed = new Date(processingStatus.last_processed_at);
          const now = new Date();
          const secondsSinceUpdate = (now.getTime() - lastProcessed.getTime()) / 1000;
          
          if (secondsSinceUpdate > staleThresholdSeconds) {
            // The lock is stale, try to update it
            const { data: updateResult, error: updateError } = await supabase
              .from('processing_status')
              .update({
                last_processed_at: now.toISOString(),
                metadata: {
                  stolen_by: options.workerId,
                  stolen_at: now.toISOString(),
                  previous_update: processingStatus.last_processed_at,
                  correlation_id: options.correlationId
                }
              })
              .eq('entity_type', entityType)
              .eq('entity_id', entityId)
              .eq('last_processed_at', processingStatus.last_processed_at); // Optimistic concurrency control
            
            if (updateError) {
              errors.db = "Failed to steal lock: " + updateError.message;
            } else {
              results.dbStolen = true;
            }
          } else {
            errors.db = `Lock is not stale. Last update was ${secondsSinceUpdate.toFixed(1)} seconds ago, threshold is ${staleThresholdSeconds} seconds`;
          }
        } else {
          errors.db = "Lock does not exist or is not in IN_PROGRESS state";
        }
      } catch (fallbackError) {
        errors.db = "Both claim_stale_lock and fallback approach failed: " + fallbackError.message;
      }
    } else if (dbStealResult && dbStealResult.claimed === true) {
      results.dbStolen = true;
    } else {
      errors.db = dbStealResult?.reason || "Failed to steal lock";
    }
  } catch (dbError) {
    errors.db = dbError.message;
  }
  
  // Consider success if either Redis or DB lock was stolen
  const success = results.redisStolen || results.dbStolen;
  
  return {
    stolen: success,
    results,
    errors: Object.keys(errors).length > 0 ? errors : undefined
  };
}
