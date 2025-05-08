
/**
 * State Manager for database-backed processing state
 * Provides formal state machine transitions and idempotency controls
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
   */
  async acquireProcessingLock(
    entityType: EntityType | string,
    entityId: string,
    timeoutMinutes: number = 30
  ): Promise<boolean> {
    try {
      // First try the database function for optimal reliability
      const { data, error } = await this.supabase.rpc(
        'acquire_processing_lock',
        {
          p_entity_type: entityType,
          p_entity_id: entityId,
          p_timeout_minutes: timeoutMinutes
        }
      );
      
      if (error) {
        console.error(`Error acquiring processing lock: ${error.message}`);
        throw error;
      }
      
      // If we got a lock, return success
      if (data === true) {
        return true;
      }
      
      // If we failed to get a lock, use Redis as backup if enabled
      if (this.useRedisBackup && this.redis) {
        return await this.acquireRedisLock(entityType, entityId, timeoutMinutes);
      }
      
      return false;
    } catch (error) {
      console.error(`Failed to acquire processing lock: ${error.message}`);
      
      // Use Redis as fallback if enabled
      if (this.useRedisBackup && this.redis) {
        return await this.acquireRedisLock(entityType, entityId, timeoutMinutes);
      }
      
      return false;
    }
  }
  
  /**
   * Backup method to acquire a Redis-based lock
   */
  private async acquireRedisLock(
    entityType: EntityType | string,
    entityId: string,
    timeoutMinutes: number
  ): Promise<boolean> {
    if (!this.redis) return false;
    
    const lockKey = `lock:${entityType}:${entityId}`;
    const ttlSeconds = timeoutMinutes * 60;
    
    try {
      const result = await this.redis.set(lockKey, new Date().toISOString(), {
        nx: true,
        ex: ttlSeconds
      });
      
      return result === "OK";
    } catch (error) {
      console.error(`Error acquiring Redis lock: ${error.message}`);
      return false;
    }
  }
  
  /**
   * Updates the state of an entity
   */
  async updateEntityState(
    entityType: EntityType | string,
    entityId: string,
    state: ProcessingState,
    errorMessage: string | null = null,
    metadata: Record<string, any> = {}
  ): Promise<boolean> {
    try {
      // Update the processing_status record
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
        console.error(`Error updating entity state: ${error.message}`);
        return false;
      }
      
      return true;
    } catch (error) {
      console.error(`Failed to update entity state: ${error.message}`);
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
   */
  async isProcessed(
    entityType: EntityType | string,
    entityId: string
  ): Promise<boolean> {
    try {
      const { data, error } = await this.supabase
        .from('processing_status')
        .select('state')
        .eq('entity_type', entityType)
        .eq('entity_id', entityId)
        .maybeSingle();
      
      if (error) {
        console.error(`Error checking entity state: ${error.message}`);
        return false;
      }
      
      return data && data.state === ProcessingState.COMPLETED;
    } catch (error) {
      console.error(`Failed to check entity state: ${error.message}`);
      return false;
    }
  }
}
