
/**
 * Coordinated State Manager
 * Implements the dual-system approach with Database as source of truth and Redis for performance
 */

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { ProcessingState, StateTransitionResult, LockOptions, StateCheckOptions, isValidStateTransition, generateCorrelationId } from "./stateManager.ts";
import { DbStateManager } from "./dbStateManager.ts"; 
import { RedisStateManager } from "./redisStateManager.ts";

interface TransactionContext {
  transactionId: string;
  startTime: number;
  entityType: string;
  entityId: string;
  correlationId?: string;
}

export class CoordinatedStateManager {
  private dbStateManager: DbStateManager;
  private redisStateManager: RedisStateManager | null = null;
  private useRedisBackup: boolean = false;
  private activeTransactions: Map<string, TransactionContext> = new Map();
  
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
   * Begins a transaction context for atomic operations
   * This doesn't start an actual database transaction, but tracks related operations
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
   * Commits a transaction by updating the state in Redis if the database operation was successful
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
    
    // If database update was successful and Redis is available, update Redis as well
    if (dbResult.success && this.useRedisBackup && this.redisStateManager) {
      try {
        // Update Redis state
        if (dbResult.newState) {
          await this.redisStateManager.updateEntityState(
            entityType,
            entityId,
            dbResult.newState,
            dbResult.error || null,
            { 
              transactionId,
              correlationId,
              committedAt: new Date().toISOString()
            }
          );
        }
      } catch (redisErr) {
        // Redis errors are non-critical since database is source of truth
        console.warn(`[${correlationId}] Redis commit failed for transaction ${transactionId} (non-critical): ${redisErr.message}`);
      } finally {
        // Clean up transaction context
        this.activeTransactions.delete(transactionId);
      }
    } else {
      // Clean up transaction context
      this.activeTransactions.delete(transactionId);
    }
    
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
   * DATABASE FIRST: Try to set the database state first as the source of truth
   * REDIS SECOND: If successful, update Redis for fast lookups
   */
  async acquireProcessingLock(
    entityType: string,
    entityId: string,
    options: LockOptions = {}
  ): Promise<boolean> {
    const {
      correlationId = generateCorrelationId('lock'),
      heartbeatIntervalSeconds = 15
    } = options;
    
    console.log(`[${correlationId}] Attempting to acquire lock for ${entityType}:${entityId}`);
    
    // Begin a transaction context
    const transactionId = this.beginTransaction(entityType, entityId, correlationId);
    
    try {
      // Step 1: Try database first (source of truth)
      const dbLockAcquired = await this.dbStateManager.acquireProcessingLock(
        entityType,
        entityId,
        options
      );
      
      // Step 2: If database lock was successful, update Redis for fast lookups
      if (dbLockAcquired && this.useRedisBackup && this.redisStateManager) {
        try {
          // Add heartbeat support if specified
          const redisOptions: LockOptions = {
            ...options,
            correlationId,
            heartbeatIntervalSeconds
          };
          
          await this.redisStateManager.acquireProcessingLock(
            entityType,
            entityId,
            redisOptions
          );
        } catch (redisErr) {
          // Redis errors are non-critical since database is source of truth
          console.warn(`[${correlationId}] Redis lock update failed (non-critical): ${redisErr.message}`);
        }
        
        // Commit the transaction
        await this.commitTransaction(transactionId, { 
          success: true,
          newState: ProcessingState.IN_PROGRESS
        });
        
        return true;
      }
      
      // If database lock failed, roll back the transaction
      if (!dbLockAcquired) {
        this.rollbackTransaction(transactionId);
      }
      
      return dbLockAcquired;
    } catch (error) {
      // If an error occurred, roll back the transaction
      this.rollbackTransaction(transactionId);
      console.error(`[${correlationId}] Error acquiring lock for ${entityType}:${entityId}: ${error.message}`);
      return false;
    }
  }
  
  /**
   * Updates the state of an entity with validation
   * DATABASE FIRST: Update the database as the source of truth
   * REDIS SECOND: Update Redis for fast lookups
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
      // Step 1: Get current state from database
      const currentState = await this.dbStateManager.getEntityState(entityType, entityId);
      
      // Step 2: Validate state transition
      const isValid = isValidStateTransition(currentState, newState);
      
      if (!isValid) {
        console.error(
          `[${correlationId}] Invalid state transition for ${entityType}:${entityId}: ` +
          `${currentState || 'NULL'} -> ${newState}`
        );
        
        const result: StateTransitionResult = {
          success: false,
          previousState: currentState || undefined,
          newState,
          error: `Invalid state transition: ${currentState || 'NULL'} -> ${newState}`,
          validationDetails: {
            isValid: false,
            reason: `Transition from ${currentState || 'NULL'} to ${newState} is not allowed`
          }
        };
        
        // Roll back the transaction
        this.rollbackTransaction(transactionId);
        
        return result;
      }
      
      // Step 3: Update database first (source of truth)
      const dbResult = await this.dbStateManager.updateEntityState(
        entityType,
        entityId,
        newState,
        errorMessage,
        {
          ...metadata,
          transactionId,
          validatedTransition: true
        }
      );
      
      // Step 4: If database update succeeded, update Redis for fast lookups
      if (dbResult.success && this.useRedisBackup && this.redisStateManager) {
        // Commit the transaction (updates Redis)
        return await this.commitTransaction(transactionId, dbResult);
      }
      
      // If database update failed, roll back the transaction
      if (!dbResult.success) {
        this.rollbackTransaction(transactionId);
      }
      
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
   * Moves an entity to the dead-letter queue
   */
  async markAsDeadLetter(
    entityType: string,
    entityId: string,
    errorMessage: string,
    metadata: Record<string, any> = {}
  ): Promise<StateTransitionResult> {
    return await this.updateEntityState(
      entityType,
      entityId,
      ProcessingState.DEAD_LETTER,
      errorMessage,
      {
        ...metadata,
        deadLetteredAt: new Date().toISOString()
      }
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
    const correlationId = generateCorrelationId('release');
    let success = true;
    
    // Begin a transaction context
    const transactionId = this.beginTransaction(entityType, entityId, correlationId);
    
    try {
      // Release in Redis first (non-critical)
      if (this.useRedisBackup && this.redisStateManager) {
        try {
          await this.redisStateManager.releaseLock(entityType, entityId);
        } catch (redisErr) {
          console.warn(`[${correlationId}] Redis lock release failed (non-critical): ${redisErr.message}`);
        }
      }
      
      // Update database state - source of truth
      const dbSuccess = await this.dbStateManager.releaseLock(entityType, entityId);
      if (!dbSuccess) {
        success = false;
      }
      
      // Commit or roll back the transaction
      if (success) {
        await this.commitTransaction(transactionId, {
          success: true,
          newState: ProcessingState.PENDING
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
   * Get active heartbeats
   */
  async getActiveHeartbeats() {
    if (this.redisStateManager) {
      return await this.redisStateManager.getHeartbeats();
    }
    return [];
  }
  
  /**
   * Finds and resolves inconsistent states between database and Redis
   */
  async reconcileInconsistentStates(
    entityType?: string,
    olderThanMinutes: number = 60
  ): Promise<number> {
    if (!this.redisStateManager) return 0;
    
    try {
      // Find potentially inconsistent states in the database
      const inconsistentStates = await this.dbStateManager.findInconsistentStates(
        entityType,
        olderThanMinutes
      );
      
      let fixedCount = 0;
      
      // Process each inconsistent state
      for (const state of inconsistentStates) {
        try {
          // Check Redis state
          const stateKey = `state:${state.entityType}:${state.entityId}`;
          const redisData = await this.redisStateManager.redis.get(stateKey);
          
          let redisState: ProcessingState | null = null;
          if (redisData) {
            try {
              const parsedState = JSON.parse(redisData as string);
              redisState = parsedState.state as ProcessingState;
            } catch (parseErr) {
              console.warn(`Failed to parse Redis state: ${parseErr.message}`);
            }
          }
          
          // If states are inconsistent, update Redis to match database
          if (redisState !== state.dbState) {
            console.log(
              `State inconsistency found for ${state.entityType}:${state.entityId}. ` +
              `DB: ${state.dbState}, Redis: ${redisState || 'NULL'}. Fixing...`
            );
            
            // Update Redis to match database
            await this.redisStateManager.updateEntityState(
              state.entityType,
              state.entityId,
              state.dbState as ProcessingState,
              null,
              { 
                reconciled: true,
                reconciledAt: new Date().toISOString(),
                previousRedisState: redisState
              }
            );
            
            fixedCount++;
          }
        } catch (error) {
          console.error(`Error reconciling state for ${state.entityType}:${state.entityId}: ${error.message}`);
        }
      }
      
      return fixedCount;
    } catch (error) {
      console.error(`Error reconciling inconsistent states: ${error.message}`);
      return 0;
    }
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
