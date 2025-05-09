
/**
 * Coordinated State Manager
 * Implements the dual-system approach with Database as source of truth and Redis for performance
 */

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { ProcessingState, StateTransitionResult, LockOptions, StateCheckOptions } from "./stateManager.ts";
import { DbStateManager } from "./dbStateManager.ts"; 
import { RedisStateManager } from "./redisStateManager.ts";

export class CoordinatedStateManager {
  private dbStateManager: DbStateManager;
  private redisStateManager: RedisStateManager | null = null;
  private useRedisBackup: boolean = false;
  
  constructor(supabaseClient?: any, redisClient?: Redis, useRedisBackup = true) {
    // Initialize database state manager
    this.dbStateManager = new DbStateManager(supabaseClient);
    
    // Initialize Redis state manager if provided
    if (redisClient) {
      this.redisStateManager = new RedisStateManager(redisClient);
      this.useRedisBackup = useRedisBackup;
    }
  }

  /**
   * Attempts to acquire a processing lock for an entity
   * DATABASE FIRST: Try to set the database state first as the source of truth
   * REDIS SECOND: If successful, update Redis for fast lookups
   */
  async acquireProcessingLock(
    entityType: string,
    entityId: string,
    options: LockOptions = {}
  ): Promise<boolean> {
    const {
      correlationId = `lock_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`
    } = options;
    
    console.log(`[${correlationId}] Attempting to acquire lock for ${entityType}:${entityId}`);
    
    // Step 1: Try database first (source of truth)
    const dbLockAcquired = await this.dbStateManager.acquireProcessingLock(
      entityType,
      entityId,
      options
    );
    
    // Step 2: If database lock was successful, update Redis for fast lookups
    if (dbLockAcquired && this.useRedisBackup && this.redisStateManager) {
      try {
        await this.redisStateManager.acquireProcessingLock(
          entityType,
          entityId,
          options
        );
      } catch (redisErr) {
        // Redis errors are non-critical since database is source of truth
        console.warn(`[${correlationId}] Redis lock update failed (non-critical): ${redisErr.message}`);
      }
      
      return true;
    }
    
    return dbLockAcquired;
  }
  
  /**
   * Updates the state of an entity
   * DATABASE FIRST: Update the database as the source of truth
   * REDIS SECOND: Update Redis for fast lookups
   */
  async updateEntityState(
    entityType: string,
    entityId: string,
    state: ProcessingState,
    errorMessage: string | null = null,
    metadata: Record<string, any> = {}
  ): Promise<StateTransitionResult> {
    // Step 1: Update database first (source of truth)
    const dbResult = await this.dbStateManager.updateEntityState(
      entityType,
      entityId,
      state,
      errorMessage,
      metadata
    );
    
    // Step 2: Update Redis for fast lookups if database update succeeded
    if (dbResult.success && this.useRedisBackup && this.redisStateManager) {
      try {
        await this.redisStateManager.updateEntityState(
          entityType,
          entityId,
          state,
          errorMessage,
          metadata
        );
      } catch (redisErr) {
        // Redis errors are non-critical
        console.warn(`[${entityType}:${entityId}] Redis state update failed (non-critical): ${redisErr.message}`);
      }
    }
    
    return dbResult;
  }
  
  /**
   * Marks an entity as complete
   */
  async markAsCompleted(
    entityType: string,
    entityId: string,
    metadata: Record<string, any> = {}
  ): Promise<StateTransitionResult> {
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
    entityType: string,
    entityId: string,
    errorMessage: string,
    metadata: Record<string, any> = {}
  ): Promise<StateTransitionResult> {
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
    entityType: string,
    entityId: string,
    options: StateCheckOptions = {}
  ): Promise<boolean> {
    const { useRedisFallback = true } = options;
    
    // Step 1: Try Redis first for performance
    if (useRedisFallback && this.useRedisBackup && this.redisStateManager) {
      try {
        const isProcessedInRedis = await this.redisStateManager.isProcessed(
          entityType,
          entityId
        );
        
        if (isProcessedInRedis) {
          return true;
        }
      } catch (redisErr) {
        console.warn(`[${entityType}:${entityId}] Redis check failed: ${redisErr.message}`);
      }
    }
    
    // Step 2: Fall back to database (source of truth)
    return await this.dbStateManager.isProcessed(entityType, entityId);
  }
  
  /**
   * Checks if an entity is in a specific state
   */
  async isInState(
    entityType: string,
    entityId: string,
    state: ProcessingState,
    options: StateCheckOptions = {}
  ): Promise<boolean> {
    const { useRedisFallback = true } = options;
    
    // Try Redis first for performance
    if (useRedisFallback && this.useRedisBackup && this.redisStateManager) {
      try {
        const isInStateInRedis = await this.redisStateManager.isInState(
          entityType,
          entityId,
          state
        );
        
        if (isInStateInRedis) {
          return true;
        }
      } catch (redisErr) {
        console.warn(`[${entityType}:${entityId}] Redis check failed: ${redisErr.message}`);
      }
    }
    
    // Fall back to database (source of truth)
    return await this.dbStateManager.isInState(entityType, entityId, state);
  }
  
  /**
   * Release a processing lock
   */
  async releaseLock(
    entityType: string,
    entityId: string
  ): Promise<boolean> {
    let success = true;
    
    // Release in Redis first (non-critical)
    if (this.useRedisBackup && this.redisStateManager) {
      try {
        await this.redisStateManager.releaseLock(entityType, entityId);
      } catch (redisErr) {
        console.warn(`[${entityType}:${entityId}] Redis lock release failed (non-critical): ${redisErr.message}`);
      }
    }
    
    // Update database state - source of truth
    const dbSuccess = await this.dbStateManager.releaseLock(entityType, entityId);
    if (!dbSuccess) {
      success = false;
    }
    
    return success;
  }
}

// Singleton factory function
let stateManagerInstance: CoordinatedStateManager | null = null;

export function getStateManager(
  supabase?: any,
  redis?: Redis,
  useRedisBackup = true
): CoordinatedStateManager {
  if (!stateManagerInstance) {
    stateManagerInstance = new CoordinatedStateManager(supabase, redis, useRedisBackup);
  }
  return stateManagerInstance;
}
