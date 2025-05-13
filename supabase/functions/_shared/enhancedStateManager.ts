
/**
 * Enhanced State Manager
 * Provides a reliable and efficient way to manage distributed state and locks
 * using PostgreSQL advisory locks as the source of truth
 */

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { ProcessingState, StateTransitionResult, LockOptions, generateCorrelationId } from "./stateManager.ts";
import { DbStateManager } from "./dbStateManager.ts";
import { PgLockManager } from "./pgLockManager.ts";
import { RedisStateManager } from "./redisStateManager.ts";

interface TransactionContext {
  transactionId: string;
  startTime: number;
  entityType: string;
  entityId: string;
  correlationId?: string;
}

export class EnhancedStateManager {
  private dbStateManager: DbStateManager;
  private pgLockManager: PgLockManager;
  private redisStateManager: RedisStateManager | null = null;
  private useRedisCache: boolean = false;
  private activeTransactions: Map<string, TransactionContext> = new Map();
  
  constructor(supabaseClient?: any, redisClient?: Redis, useRedisCache = false) {
    // Initialize database state manager
    this.dbStateManager = new DbStateManager(supabaseClient);
    
    // Initialize PostgreSQL advisory lock manager
    this.pgLockManager = new PgLockManager(supabaseClient);
    
    // Initialize Redis state manager if provided (for caching only)
    if (redisClient) {
      this.redisStateManager = new RedisStateManager(redisClient);
      this.useRedisCache = useRedisCache;
    }
  }

  /**
   * Begins a transaction context for atomic operations
   */
  beginTransaction(entityType: string, entityId: string, correlationId?: string): string {
    const transactionId = generateCorrelationId('tx');
    
    this.activeTransactions.set(transactionId, {
      transactionId,
      startTime: Date.now(),
      entityType,
      entityId,
      correlationId: correlationId || transactionId
    });
    
    console.log(`[${transactionId}] Beginning transaction context for ${entityType}:${entityId}`);
    
    return transactionId;
  }
  
  /**
   * Commits a transaction by updating the state in Redis cache if configured
   */
  async commitTransaction(
    transactionId: string, 
    dbResult: StateTransitionResult
  ): Promise<StateTransitionResult> {
    const transactionContext = this.activeTransactions.get(transactionId);
    
    if (!transactionContext) {
      console.warn(`[${transactionId}] Cannot commit transaction - no active context found`);
      return {
        success: false,
        error: "No active transaction context found"
      };
    }
    
    const { entityType, entityId, correlationId } = transactionContext;
    
    console.log(`[${correlationId}] Committing transaction ${transactionId} for ${entityType}:${entityId}`);
    
    // If database update was successful and Redis cache is enabled, update Redis as well
    if (dbResult.success && this.useRedisCache && this.redisStateManager) {
      try {
        // Update Redis cache
        if (dbResult.newState) {
          await this.redisStateManager.updateEntityState(
            entityType,
            entityId,
            dbResult.newState,
            dbResult.error || null,
            { 
              transactionId,
              correlationId,
              committedAt: new Date().toISOString(),
              source: 'transaction_commit'
            }
          );
        }
      } catch (redisErr) {
        // Redis errors are non-critical since database is source of truth
        console.warn(`[${correlationId}] Redis cache update failed (non-critical): ${redisErr.message}`);
      }
    }
    
    // Clean up transaction context
    this.activeTransactions.delete(transactionId);
    
    return dbResult;
  }
  
  /**
   * Rolls back a transaction context (primarily cleanup)
   */
  rollbackTransaction(transactionId: string): void {
    const transactionContext = this.activeTransactions.get(transactionId);
    
    if (transactionContext) {
      const { entityType, entityId, correlationId } = transactionContext;
      console.log(`[${correlationId}] Rolling back transaction ${transactionId} for ${entityType}:${entityId}`);
    }
    
    this.activeTransactions.delete(transactionId);
  }

  /**
   * Attempts to acquire a processing lock for an entity
   * ADVISORY LOCK FIRST: Uses PostgreSQL advisory locks as the source of truth
   * DATABASE SECOND: Updates processing_status table to maintain visibility
   * REDIS THIRD: If configured, updates Redis cache for fast lookups
   */
  async acquireProcessingLock(
    entityType: string,
    entityId: string,
    options: LockOptions = {}
  ): Promise<boolean> {
    const {
      correlationId = generateCorrelationId('lock'),
      retries = 1,
      timeoutSeconds = 0,
      timeoutMinutes
    } = options;
    
    // Convert timeoutMinutes to seconds if provided
    const effectiveTimeoutSeconds = timeoutMinutes 
      ? timeoutMinutes * 60 
      : timeoutSeconds;
    
    console.log(`[${correlationId}] Attempting to acquire lock for ${entityType}:${entityId}`);
    
    // Begin a transaction context
    const transactionId = this.beginTransaction(entityType, entityId, correlationId);
    
    try {
      // Step 1: Acquire PostgreSQL advisory lock (source of truth)
      const pgLockResult = await this.pgLockManager.acquireLock(
        entityType,
        entityId,
        {
          ...options,
          timeoutSeconds: effectiveTimeoutSeconds
        }
      );
      
      if (!pgLockResult.success) {
        console.warn(`[${correlationId}] Failed to acquire advisory lock: ${pgLockResult.error}`);
        this.rollbackTransaction(transactionId);
        return false;
      }
      
      // Step 2: Update the database state
      const dbLockAcquired = await this.dbStateManager.acquireProcessingLock(
        entityType,
        entityId,
        {
          ...options,
          allowRetry: false // No need to retry, we already have the advisory lock
        }
      );
      
      // Even if the database update fails, we still have the advisory lock
      // which is our source of truth. Log the issue but continue.
      if (!dbLockAcquired) {
        console.warn(`[${correlationId}] Advisory lock acquired but database state update failed`);
      }
      
      // Step 3: If Redis cache is enabled, update it for fast lookups
      if (this.useRedisCache && this.redisStateManager) {
        try {
          await this.redisStateManager.updateEntityState(
            entityType,
            entityId,
            ProcessingState.IN_PROGRESS,
            null,
            { 
              transactionId,
              correlationId,
              locked_at: new Date().toISOString(),
              lock_source: 'pg_advisory'
            }
          );
        } catch (redisErr) {
          // Redis errors are non-critical since advisory lock is source of truth
          console.warn(`[${correlationId}] Redis cache update failed (non-critical): ${redisErr.message}`);
        }
      }
      
      // Commit the transaction
      await this.commitTransaction(transactionId, { 
        success: true,
        newState: ProcessingState.IN_PROGRESS,
        source: 'advisory_lock'
      });
      
      return true;
    } catch (error) {
      // If an error occurred, roll back the transaction
      this.rollbackTransaction(transactionId);
      console.error(`[${correlationId}] Error acquiring lock for ${entityType}:${entityId}: ${error.message}`);
      return false;
    }
  }
  
  /**
   * Updates the state of an entity
   * DATABASE FIRST: Update the database as the source of truth
   * REDIS SECOND: If configured, update Redis cache for fast lookups
   */
  async updateEntityState(
    entityType: string,
    entityId: string,
    newState: ProcessingState,
    errorMessage: string | null = null,
    metadata: Record<string, any> = {}
  ): Promise<StateTransitionResult> {
    const correlationId = metadata.correlationId || generateCorrelationId('state');
    
    // Begin a transaction context
    const transactionId = this.beginTransaction(entityType, entityId, correlationId);
    
    try {
      // Update database state - source of truth
      const dbResult = await this.dbStateManager.updateEntityState(
        entityType,
        entityId,
        newState,
        errorMessage,
        {
          ...metadata,
          transactionId
        }
      );
      
      // If database update succeeded and Redis cache is enabled, update it
      if (dbResult.success) {
        // Commit the transaction (also updates Redis if configured)
        return await this.commitTransaction(transactionId, dbResult);
      }
      
      // If database update failed, roll back the transaction
      this.rollbackTransaction(transactionId);
      return dbResult;
    } catch (error) {
      // If an error occurred, roll back the transaction
      this.rollbackTransaction(transactionId);
      
      return {
        success: false,
        error: `Error updating state: ${error.message}`,
        previousState: undefined,
        newState
      };
    }
  }
  
  /**
   * Marks an entity as complete
   */
  async markAsCompleted(
    entityType: string,
    entityId: string,
    metadata: Record<string, any> = {}
  ): Promise<StateTransitionResult> {
    // First release the advisory lock
    await this.releaseLock(entityType, entityId);
    
    // Then update the state
    return await this.updateEntityState(
      entityType,
      entityId,
      ProcessingState.COMPLETED,
      null,
      {
        ...metadata,
        completedAt: new Date().toISOString()
      }
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
    // First release the advisory lock
    await this.releaseLock(entityType, entityId);
    
    return await this.updateEntityState(
      entityType,
      entityId,
      ProcessingState.FAILED,
      errorMessage,
      {
        ...metadata,
        failedAt: new Date().toISOString()
      }
    );
  }
  
  /**
   * Checks if an entity has already been processed
   * REDIS FIRST: If configured, try Redis for fast lookups
   * DATABASE FALLBACK: Fall back to database when Redis misses
   */
  async isProcessed(
    entityType: string,
    entityId: string
  ): Promise<boolean> {
    // Try Redis first for performance (if configured)
    if (this.useRedisCache && this.redisStateManager) {
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
    
    // Fall back to database (source of truth)
    return await this.dbStateManager.isProcessed(entityType, entityId);
  }
  
  /**
   * Checks if an entity is in a specific state
   */
  async isInState(
    entityType: string,
    entityId: string,
    state: ProcessingState
  ): Promise<boolean> {
    // Try Redis first for performance (if configured)
    if (this.useRedisCache && this.redisStateManager) {
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
   * Checks if entity is locked using advisory lock
   */
  async isLocked(entityType: string, entityId: string): Promise<boolean> {
    return await this.pgLockManager.isLocked(entityType, entityId);
  }
  
  /**
   * Release a processing lock
   */
  async releaseLock(
    entityType: string,
    entityId: string
  ): Promise<boolean> {
    const correlationId = generateCorrelationId('release');
    let success = true;
    
    // Begin a transaction context
    const transactionId = this.beginTransaction(entityType, entityId, correlationId);
    
    try {
      // Step 1: Release the advisory lock (source of truth)
      const pgSuccess = await this.pgLockManager.releaseLock(entityType, entityId);
      
      if (!pgSuccess) {
        console.warn(`[${correlationId}] Advisory lock release failed`);
        success = false;
      }
      
      // Step 2: Update database state even if advisory release failed
      // This ensures the entity can be reprocessed in the database state
      const dbSuccess = await this.dbStateManager.releaseLock(entityType, entityId);
      
      if (!dbSuccess) {
        console.warn(`[${correlationId}] Database state update for lock release failed`);
        // Don't set success to false here since advisory lock is source of truth
      }
      
      // Step 3: Update Redis cache if configured
      if (this.useRedisCache && this.redisStateManager) {
        try {
          await this.redisStateManager.updateEntityState(
            entityType,
            entityId,
            ProcessingState.PENDING,
            null,
            {
              transactionId,
              correlationId,
              released_at: new Date().toISOString(),
              release_source: 'pg_advisory'
            }
          );
        } catch (redisErr) {
          console.warn(`[${correlationId}] Redis cache update for lock release failed (non-critical): ${redisErr.message}`);
        }
      }
      
      // Commit or roll back the transaction
      if (success) {
        await this.commitTransaction(transactionId, {
          success: true,
          newState: ProcessingState.PENDING,
          source: 'advisory_unlock'
        });
      } else {
        this.rollbackTransaction(transactionId);
      }
      
      return success;
    } catch (error) {
      // If an error occurred, roll back the transaction
      this.rollbackTransaction(transactionId);
      console.error(`[${correlationId}] Error releasing lock: ${error.message}`);
      return false;
    }
  }
  
  /**
   * Clean up stale locks
   */
  async cleanupStaleLocks(olderThanMinutes: number = 30): Promise<number> {
    let cleanedCount = 0;
    
    try {
      // Use the pgLockManager to clean up stale locks in the tracking table
      cleanedCount = await this.pgLockManager.cleanupStaleLocks(olderThanMinutes);
      
      // Also reset any stuck IN_PROGRESS states in the processing_status table
      const { data, error } = await this.dbStateManager.resetStuckProcessingStates(olderThanMinutes);
      
      if (error) {
        console.error(`Failed to reset stuck processing states: ${error}`);
      } else if (data) {
        cleanedCount += data.length;
      }
      
      return cleanedCount;
    } catch (error) {
      console.error(`Error during stale lock cleanup: ${error.message}`);
      return cleanedCount;
    }
  }
}

// Singleton factory function
let stateManagerInstance: EnhancedStateManager | null = null;

export function getStateManager(
  supabase?: any,
  redis?: Redis,
  useRedisCache = false
): EnhancedStateManager {
  if (!stateManagerInstance) {
    stateManagerInstance = new EnhancedStateManager(supabase, redis, useRedisCache);
  }
  return stateManagerInstance;
}
