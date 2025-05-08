
/**
 * State Manager for database-backed processing state
 * Provides formal state machine transitions and idempotency controls
 * 
 * DUAL SYSTEM ROLES:
 * 
 * DATABASE:
 * - Primary source of truth for processing state
 * - Durable record of entity processing
 * - Manages state transitions through the processing lifecycle
 * - Provides transactional integrity
 * - Permanent historical record
 * 
 * REDIS:
 * - Performance optimization layer
 * - Fast lookups for idempotency checks
 * - Temporary deduplication to prevent duplicate processing
 * - Fallback mechanism during database unavailability
 * - Caching layer with TTL-based expiry
 */

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

// Processing state constants
export enum ProcessingState {
  PENDING = 'PENDING',
  IN_PROGRESS = 'IN_PROGRESS',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED'
}

// Environment detection
export enum Environment {
  DEVELOPMENT = 'development',
  STAGING = 'staging',
  PRODUCTION = 'production'
}

// Entity types for processing
export enum EntityType {
  ARTIST = 'artist',
  ALBUM = 'album',
  TRACK = 'track',
  PRODUCER = 'producer'
}

// Environment-specific Redis TTL settings (in seconds)
const TTL_SETTINGS = {
  [Environment.DEVELOPMENT]: 10 * 60,     // 10 minutes
  [Environment.STAGING]: 60 * 60,         // 60 minutes
  [Environment.PRODUCTION]: 24 * 60 * 60  // 24 hours
}

// Default Redis TTL if environment can't be detected
const DEFAULT_TTL = 3600; // 1 hour

/**
 * Detects the current environment based on configuration
 */
export function detectEnvironment(): Environment {
  const env = Deno.env.get("ENV") || Deno.env.get("ENVIRONMENT") || "development";
  
  if (env.toLowerCase().includes('prod')) {
    return Environment.PRODUCTION;
  } else if (env.toLowerCase().includes('stag') || env.toLowerCase().includes('test')) {
    return Environment.STAGING;
  } else {
    return Environment.DEVELOPMENT;
  }
}

/**
 * Gets the appropriate TTL based on the environment
 */
export function getEnvironmentTTL(): number {
  const environment = detectEnvironment();
  return TTL_SETTINGS[environment] || DEFAULT_TTL;
}

/**
 * StateManager class for handling entity processing state
 */
export class StateManager {
  private supabase: any;
  private redis: Redis | null = null;
  private useRedisBackup: boolean = false;
  
  constructor(supabaseClient?: any, redisClient?: Redis, useRedisBackup = true) {
    // Initialize Supabase client if not provided
    if (supabaseClient) {
      this.supabase = supabaseClient;
    } else {
      this.supabase = createClient(
        Deno.env.get("SUPABASE_URL") || "",
        Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || ""
      );
    }
    
    // Initialize Redis client if provided
    this.redis = redisClient || null;
    this.useRedisBackup = useRedisBackup && !!this.redis;
  }

  /**
   * Attempts to acquire a processing lock for an entity
   * DATABASE FIRST: Try to set the database state first as the source of truth
   * REDIS SECOND: If successful, update Redis for fast lookups
   * REDIS FALLBACK: If database fails, try Redis as a temporary fallback
   */
  async acquireProcessingLock(
    entityType: EntityType | string,
    entityId: string,
    timeoutMinutes: number = 30,
    correlationId?: string
  ): Promise<boolean> {
    try {
      // Step 1: Try database first (source of truth)
      const { data, error } = await this.supabase.rpc(
        'acquire_processing_lock',
        {
          p_entity_type: entityType,
          p_entity_id: entityId,
          p_timeout_minutes: timeoutMinutes,
          p_correlation_id: correlationId || null
        }
      );
      
      if (error) {
        console.error(`[${entityType}:${entityId}] Database lock error: ${error.message}`);
        // Fall back to Redis if database fails
        if (this.useRedisBackup && this.redis) {
          return await this.acquireRedisLock(entityType, entityId, timeoutMinutes, correlationId);
        }
        return false;
      }
      
      // Step 2: If database lock was successful, update Redis for fast lookups
      if (data === true) {
        if (this.useRedisBackup && this.redis) {
          try {
            const lockKey = `lock:${entityType}:${entityId}`;
            const stateKey = `state:${entityType}:${entityId}`;
            
            // Set the lock in Redis with TTL
            await this.redis.set(lockKey, new Date().toISOString(), {
              ex: timeoutMinutes * 60
            });
            
            // Store the state for fast lookups
            await this.redis.set(stateKey, JSON.stringify({
              state: ProcessingState.IN_PROGRESS,
              timestamp: new Date().toISOString(),
              correlationId: correlationId || null
            }), {
              ex: getEnvironmentTTL() // Use environment-specific TTL
            });
          } catch (redisErr) {
            // Redis errors are non-critical since database is source of truth
            console.warn(`[${entityType}:${entityId}] Redis lock update failed (non-critical): ${redisErr.message}`);
          }
        }
        return true;
      }
      
      return false;
    } catch (error) {
      console.error(`[${entityType}:${entityId}] Failed to acquire processing lock: ${error.message}`);
      
      // Fall back to Redis if database fails entirely
      if (this.useRedisBackup && this.redis) {
        return await this.acquireRedisLock(entityType, entityId, timeoutMinutes, correlationId);
      }
      
      return false;
    }
  }
  
  /**
   * Backup method to acquire a Redis-based lock when database is unavailable
   * This is a FALLBACK mechanism when the database is down
   */
  private async acquireRedisLock(
    entityType: EntityType | string,
    entityId: string,
    timeoutMinutes: number,
    correlationId?: string
  ): Promise<boolean> {
    if (!this.redis) return false;
    
    const lockKey = `lock:${entityType}:${entityId}`;
    const stateKey = `state:${entityType}:${entityId}`;
    const ttlSeconds = timeoutMinutes * 60;
    
    try {
      console.log(`[${entityType}:${entityId}] Attempting Redis fallback lock`);
      
      // Try to set the lock key with NX (only if it doesn't exist)
      const result = await this.redis.set(lockKey, new Date().toISOString(), {
        nx: true,
        ex: ttlSeconds
      });
      
      if (result === "OK") {
        // Also store state for consistency
        await this.redis.set(stateKey, JSON.stringify({
          state: ProcessingState.IN_PROGRESS,
          timestamp: new Date().toISOString(),
          correlationId: correlationId || null,
          fallback: true // Mark as fallback for awareness
        }), {
          ex: getEnvironmentTTL()
        });
        
        console.log(`[${entityType}:${entityId}] Redis fallback lock acquired`);
        return true;
      }
      
      console.log(`[${entityType}:${entityId}] Redis fallback lock failed (already locked)`);
      return false;
    } catch (error) {
      console.error(`[${entityType}:${entityId}] Error acquiring Redis fallback lock: ${error.message}`);
      return false;
    }
  }
  
  /**
   * Updates the state of an entity
   * DATABASE FIRST: Update the database as the source of truth
   * REDIS SECOND: Update Redis for fast lookups
   */
  async updateEntityState(
    entityType: EntityType | string,
    entityId: string,
    state: ProcessingState,
    errorMessage: string | null = null,
    metadata: Record<string, any> = {}
  ): Promise<boolean> {
    try {
      // Step 1: Update database first (source of truth)
      const { error } = await this.supabase
        .from('processing_status')
        .update({
          state: state,
          last_processed_at: new Date().toISOString(),
          last_error: errorMessage,
          metadata: metadata,
          updated_at: new Date().toISOString()
        })
        .match({ entity_type: entityType, entity_id: entityId });
      
      if (error) {
        console.error(`[${entityType}:${entityId}] Error updating entity state: ${error.message}`);
        return false;
      }
      
      // Step 2: Update Redis for fast lookups
      if (this.useRedisBackup && this.redis) {
        try {
          const stateKey = `state:${entityType}:${entityId}`;
          const lockKey = `lock:${entityType}:${entityId}`;
          
          // Set state in Redis
          await this.redis.set(stateKey, JSON.stringify({
            state: state,
            timestamp: new Date().toISOString(),
            errorMessage,
            metadata
          }), {
            ex: getEnvironmentTTL()
          });
          
          // If completed or failed, remove the lock
          if (state === ProcessingState.COMPLETED || state === ProcessingState.FAILED) {
            await this.redis.del(lockKey);
          }
        } catch (redisErr) {
          // Redis errors are non-critical
          console.warn(`[${entityType}:${entityId}] Redis state update failed (non-critical): ${redisErr.message}`);
        }
      }
      
      return true;
    } catch (error) {
      console.error(`[${entityType}:${entityId}] Failed to update entity state: ${error.message}`);
      return false;
    }
  }
  
  /**
   * Marks an entity as complete
   */
  async markAsCompleted(
    entityType: EntityType | string,
    entityId: string,
    metadata: Record<string, any> = {}
  ): Promise<boolean> {
    return await this.updateEntityState(
      entityType,
      entityId,
      ProcessingState.COMPLETED,
      null,
      metadata
    );
  }
  
  /**
   * Marks an entity as failed
   */
  async markAsFailed(
    entityType: EntityType | string,
    entityId: string,
    errorMessage: string,
    metadata: Record<string, any> = {}
  ): Promise<boolean> {
    return await this.updateEntityState(
      entityType,
      entityId,
      ProcessingState.FAILED,
      errorMessage,
      metadata
    );
  }
  
  /**
   * Checks if an entity has already been processed
   * REDIS FIRST: Try Redis for fast lookups
   * DATABASE FALLBACK: Fall back to database when Redis misses
   */
  async isProcessed(
    entityType: EntityType | string,
    entityId: string
  ): Promise<boolean> {
    // Step 1: Try Redis first for performance
    if (this.useRedisBackup && this.redis) {
      try {
        const stateKey = `state:${entityType}:${entityId}`;
        const stateData = await this.redis.get(stateKey);
        
        if (stateData) {
          try {
            const parsedState = JSON.parse(stateData as string);
            if (parsedState.state === ProcessingState.COMPLETED) {
              return true;
            }
          } catch (parseErr) {
            console.warn(`[${entityType}:${entityId}] Failed to parse Redis state: ${parseErr.message}`);
          }
        }
      } catch (redisErr) {
        console.warn(`[${entityType}:${entityId}] Redis check failed: ${redisErr.message}`);
      }
    }
    
    // Step 2: Fall back to database (source of truth)
    try {
      const { data, error } = await this.supabase
        .from('processing_status')
        .select('state')
        .eq('entity_type', entityType)
        .eq('entity_id', entityId)
        .maybeSingle();
      
      if (error) {
        console.error(`[${entityType}:${entityId}] Error checking entity state: ${error.message}`);
        return false;
      }
      
      return data && data.state === ProcessingState.COMPLETED;
    } catch (error) {
      console.error(`[${entityType}:${entityId}] Failed to check entity state: ${error.message}`);
      return false;
    }
  }
  
  /**
   * Checks if an entity is in a specific state
   */
  async isInState(
    entityType: EntityType | string,
    entityId: string,
    state: ProcessingState
  ): Promise<boolean> {
    // Try Redis first for performance
    if (this.useRedisBackup && this.redis) {
      try {
        const stateKey = `state:${entityType}:${entityId}`;
        const stateData = await this.redis.get(stateKey);
        
        if (stateData) {
          try {
            const parsedState = JSON.parse(stateData as string);
            if (parsedState.state === state) {
              return true;
            }
          } catch (parseErr) {
            console.warn(`[${entityType}:${entityId}] Failed to parse Redis state: ${parseErr.message}`);
          }
        }
      } catch (redisErr) {
        console.warn(`[${entityType}:${entityId}] Redis check failed: ${redisErr.message}`);
      }
    }
    
    // Fall back to database (source of truth)
    try {
      const { data, error } = await this.supabase
        .from('processing_status')
        .select('state')
        .eq('entity_type', entityType)
        .eq('entity_id', entityId)
        .maybeSingle();
      
      if (error) {
        console.error(`[${entityType}:${entityId}] Error checking entity state: ${error.message}`);
        return false;
      }
      
      return data && data.state === state;
    } catch (error) {
      console.error(`[${entityType}:${entityId}] Failed to check entity state: ${error.message}`);
      return false;
    }
  }
}
