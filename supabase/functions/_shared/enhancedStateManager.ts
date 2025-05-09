
/**
 * Enhanced state management system
 * Provides stronger consistency guarantees and better transaction handling
 */

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { 
  ProcessingState, 
  EntityType, 
  LockOptions, 
  StateTransitionResult, 
  StateCheckOptions,
  isValidStateTransition,
  generateCorrelationId
} from "./stateManager.ts";
import { 
  ErrorCategory, 
  ErrorSource, 
  createEnhancedError, 
  trackError,
  retryWithBackoff
} from "./errorHandling.ts";

// Transaction context for atomic operations
interface TransactionContext {
  id: string;
  entityType: string;
  entityId: string;
  startTime: number;
  correlationId?: string;
  operations: Array<{
    type: 'db' | 'redis';
    operation: string;
    successful?: boolean;
    error?: Error;
  }>;
}

/**
 * Enhanced state manager with stronger consistency guarantees
 */
export class EnhancedStateManager {
  private supabase: any;
  private redis: Redis;
  private useRedisBackup: boolean;
  private transactions: Map<string, TransactionContext> = new Map();
  private stats = {
    dbOps: 0,
    redisOps: 0,
    successfulTransactions: 0,
    failedTransactions: 0,
    stateTransitions: {
      [ProcessingState.PENDING]: 0,
      [ProcessingState.IN_PROGRESS]: 0,
      [ProcessingState.COMPLETED]: 0,
      [ProcessingState.FAILED]: 0,
      [ProcessingState.DEAD_LETTER]: 0,
    },
    locks: {
      acquired: 0,
      failed: 0,
      released: 0
    },
    errors: {
      db: 0,
      redis: 0,
      validation: 0
    }
  };
  private lastStatSync = 0;
  private statSyncInterval = 60000; // 1 minute
  
  constructor(supabase: any, redis: Redis, useRedisBackup = true) {
    this.supabase = supabase;
    this.redis = redis;
    this.useRedisBackup = useRedisBackup;
  }
  
  /**
   * Begin a transaction for atomic operations
   */
  beginTransaction(entityType: string, entityId: string, correlationId?: string): string {
    const transactionId = generateCorrelationId('tx');
    
    const transaction: TransactionContext = {
      id: transactionId,
      entityType,
      entityId,
      startTime: Date.now(),
      correlationId: correlationId || transactionId,
      operations: []
    };
    
    this.transactions.set(transactionId, transaction);
    
    console.log(`[${transaction.correlationId}] Beginning transaction ${transactionId} for ${entityType}:${entityId}`);
    
    return transactionId;
  }
  
  /**
   * Commit a transaction
   */
  async commitTransaction(transactionId: string): Promise<boolean> {
    const transaction = this.transactions.get(transactionId);
    
    if (!transaction) {
      console.warn(`Cannot commit transaction ${transactionId} - no active transaction found`);
      return false;
    }
    
    const { entityType, entityId, correlationId, operations } = transaction;
    
    console.log(`[${correlationId}] Committing transaction ${transactionId} for ${entityType}:${entityId} with ${operations.length} operations`);
    
    // Check for any failed operations
    const failedOps = operations.filter(op => op.successful === false);
    
    if (failedOps.length > 0) {
      console.error(`[${correlationId}] Transaction ${transactionId} has ${failedOps.length} failed operations`);
      
      // Clean up transaction
      this.transactions.delete(transactionId);
      this.stats.failedTransactions++;
      
      return false;
    }
    
    // All operations succeeded, clean up
    this.transactions.delete(transactionId);
    this.stats.successfulTransactions++;
    
    // Sync stats periodically
    await this.maybeSyncStats();
    
    return true;
  }
  
  /**
   * Roll back a transaction
   */
  async rollbackTransaction(transactionId: string): Promise<void> {
    const transaction = this.transactions.get(transactionId);
    
    if (!transaction) {
      console.warn(`Cannot rollback transaction ${transactionId} - no active transaction found`);
      return;
    }
    
    const { entityType, entityId, correlationId, operations } = transaction;
    
    console.log(`[${correlationId}] Rolling back transaction ${transactionId} for ${entityType}:${entityId}`);
    
    // Perform rollback operations if needed
    // For now, we just clean up transaction state
    
    this.transactions.delete(transactionId);
    this.stats.failedTransactions++;
    
    // Sync stats periodically
    await this.maybeSyncStats();
  }
  
  /**
   * Record an operation in the current transaction
   */
  private recordOperation(
    transactionId: string,
    type: 'db' | 'redis',
    operation: string,
    successful: boolean,
    error?: Error
  ): void {
    const transaction = this.transactions.get(transactionId);
    
    if (!transaction) {
      console.warn(`Cannot record operation for transaction ${transactionId} - no active transaction found`);
      return;
    }
    
    transaction.operations.push({
      type,
      operation,
      successful,
      error
    });
    
    // Update stats
    if (type === 'db') {
      this.stats.dbOps++;
      if (!successful) this.stats.errors.db++;
    } else {
      this.stats.redisOps++;
      if (!successful) this.stats.errors.redis++;
    }
  }
  
  /**
   * Acquire a processing lock with enhanced consistency
   */
  async acquireProcessingLock(
    entityType: EntityType | string,
    entityId: string,
    options: LockOptions = {}
  ): Promise<boolean> {
    const {
      timeoutMinutes = 30,
      correlationId = generateCorrelationId('lock'),
      heartbeatIntervalSeconds = 15,
      retries = 1,
      allowRetry = true
    } = options;
    
    console.log(`[${correlationId}] Attempting to acquire lock for ${entityType}:${entityId}`);
    
    // Begin a transaction
    const transactionId = this.beginTransaction(entityType, entityId, correlationId);
    
    try {
      // Try to acquire lock in database first
      let dbLockAcquired = false;
      
      try {
        // First attempt with database RPC function
        const { data: lockResult, error: lockError } = await this.supabase.rpc(
          'acquire_processing_lock',
          {
            p_entity_type: entityType,
            p_entity_id: entityId,
            p_timeout_minutes: timeoutMinutes,
            p_correlation_id: correlationId
          }
        );
        
        if (lockError) {
          console.warn(`[${correlationId}] Error acquiring DB lock via RPC: ${lockError.message}`);
          
          // Fallback to SQL-based lock
          const { data: statusData, error: statusError } = await this.supabase
            .from('processing_status')
            .select('state, last_processed_at')
            .eq('entity_type', entityType)
            .eq('entity_id', entityId)
            .maybeSingle();
          
          if (statusError) {
            throw createEnhancedError(
              `Failed to check entity status: ${statusError.message}`,
              ErrorSource.DATABASE,
              ErrorCategory.TRANSIENT_SERVICE
            );
          }
          
          if (!statusData) {
            // Entity doesn't exist yet, create it
            const { error: insertError } = await this.supabase
              .from('processing_status')
              .insert({
                entity_type: entityType,
                entity_id: entityId,
                state: ProcessingState.IN_PROGRESS,
                last_processed_at: new Date().toISOString(),
                metadata: { correlation_id: correlationId }
              });
            
            if (insertError) {
              throw createEnhancedError(
                `Failed to create entity status: ${insertError.message}`,
                ErrorSource.DATABASE,
                ErrorCategory.TRANSIENT_SERVICE
              );
            }
            
            dbLockAcquired = true;
          } else if (statusData.state === ProcessingState.PENDING || statusData.state === ProcessingState.FAILED) {
            // Entity exists and is in PENDING or FAILED state, transition to IN_PROGRESS
            const { error: updateError } = await this.supabase
              .from('processing_status')
              .update({
                state: ProcessingState.IN_PROGRESS,
                last_processed_at: new Date().toISOString(),
                metadata: { correlation_id: correlationId },
                attempts: this.supabase.sql`attempts + 1`,
                updated_at: new Date().toISOString()
              })
              .eq('entity_type', entityType)
              .eq('entity_id', entityId);
            
            if (updateError) {
              throw createEnhancedError(
                `Failed to update entity status: ${updateError.message}`,
                ErrorSource.DATABASE,
                ErrorCategory.TRANSIENT_SERVICE
              );
            }
            
            dbLockAcquired = true;
          } else if (statusData.state === ProcessingState.IN_PROGRESS) {
            // Check if the lock has timed out
            const lastProcessed = new Date(statusData.last_processed_at);
            const timeoutThreshold = new Date(Date.now() - (timeoutMinutes * 60 * 1000));
            
            if (lastProcessed < timeoutThreshold) {
              // Lock has timed out, reset to IN_PROGRESS
              const { error: updateError } = await this.supabase
                .from('processing_status')
                .update({
                  state: ProcessingState.IN_PROGRESS,
                  last_processed_at: new Date().toISOString(),
                  metadata: {
                    timeout_recovery: {
                      previous_lock_time: statusData.last_processed_at,
                      timeout_minutes: timeoutMinutes,
                      recovered_at: new Date().toISOString(),
                      correlation_id: correlationId
                    }
                  },
                  attempts: this.supabase.sql`attempts + 1`,
                  updated_at: new Date().toISOString()
                })
                .eq('entity_type', entityType)
                .eq('entity_id', entityId);
              
              if (updateError) {
                throw createEnhancedError(
                  `Failed to update entity status: ${updateError.message}`,
                  ErrorSource.DATABASE,
                  ErrorCategory.TRANSIENT_SERVICE
                );
              }
              
              dbLockAcquired = true;
            } else {
              // Lock is still valid, cannot acquire
              dbLockAcquired = false;
              
              // Record operation
              this.recordOperation(transactionId, 'db', 'check_lock', true);
            }
          } else if (statusData.state === ProcessingState.COMPLETED) {
            // Already completed, no need to process again
            dbLockAcquired = false;
            
            // Record operation
            this.recordOperation(transactionId, 'db', 'check_lock', true);
          }
        } else {
          // RPC call succeeded
          dbLockAcquired = !!lockResult;
          
          // Record operation
          this.recordOperation(transactionId, 'db', 'acquire_lock_rpc', true);
        }
      } catch (dbError) {
        // Record operation failure
        this.recordOperation(transactionId, 'db', 'acquire_lock', false, dbError);
        
        // Roll back transaction
        await this.rollbackTransaction(transactionId);
        
        // Track error
        await trackError(dbError, {
          entityType: entityType as EntityType,
          entityId,
          operationName: 'acquireProcessingLock',
          correlationId
        });
        
        throw dbError;
      }
      
      // Record operation success
      this.recordOperation(transactionId, 'db', 'acquire_lock', true);
      
      // If database lock was not acquired, return false
      if (!dbLockAcquired) {
        // Just roll back transaction, not an error
        await this.rollbackTransaction(transactionId);
        return false;
      }
      
      // If using Redis backup, also try to acquire Redis lock
      if (this.useRedisBackup) {
        try {
          const lockKey = `lock:${entityType}:${entityId}`;
          const stateKey = `state:${entityType}:${entityId}`;
          const ttlSeconds = timeoutMinutes * 60;
          const workerId = Deno.env.get("WORKER_ID") || `worker_${Math.random().toString(36).substring(2, 10)}`;
          
          // Try to set the lock key with NX (only if it doesn't exist)
          const lockData = {
            acquiredAt: new Date().toISOString(),
            correlationId,
            workerId,
            heartbeatEnabled: heartbeatIntervalSeconds > 0
          };
          
          const result = await this.redis.set(lockKey, JSON.stringify(lockData), {
            nx: true,
            ex: ttlSeconds
          });
          
          // If we got the Redis lock too, update the state
          if (result === "OK") {
            await this.redis.set(stateKey, JSON.stringify({
              state: ProcessingState.IN_PROGRESS,
              timestamp: new Date().toISOString(),
              correlationId,
              workerId
            }), {
              ex: ttlSeconds
            });
            
            // Start heartbeat if enabled
            if (heartbeatIntervalSeconds > 0) {
              this.startHeartbeat(
                entityType,
                entityId,
                workerId,
                correlationId,
                heartbeatIntervalSeconds,
                ttlSeconds
              );
            }
            
            // Record operation success
            this.recordOperation(transactionId, 'redis', 'acquire_lock', true);
          } else {
            // We couldn't acquire the Redis lock, but that's OK
            console.warn(`[${correlationId}] DB lock acquired but Redis lock failed for ${entityType}:${entityId}`);
            
            // Record operation "success" (it's OK if Redis fails)
            this.recordOperation(transactionId, 'redis', 'acquire_lock', true);
          }
        } catch (redisError) {
          // Record Redis operation failure, but it's not critical
          this.recordOperation(transactionId, 'redis', 'acquire_lock', false, redisError);
          
          console.warn(`[${correlationId}] Redis lock error (non-critical): ${redisError.message}`);
        }
      }
      
      // Commit transaction
      const committed = await this.commitTransaction(transactionId);
      
      // Update stats
      this.stats.locks.acquired++;
      
      // If lock was acquired, we're done
      return committed && dbLockAcquired;
    } catch (error) {
      // Roll back transaction
      await this.rollbackTransaction(transactionId);
      
      // Track error
      await trackError(error, {
        entityType: entityType as EntityType,
        entityId,
        operationName: 'acquireProcessingLock',
        correlationId
      });
      
      // Update stats
      this.stats.locks.failed++;
      
      // Retry if allowed and attempts remain
      if (allowRetry && retries > 0) {
        console.log(`[${correlationId}] Retrying lock acquisition for ${entityType}:${entityId}`);
        
        return this.acquireProcessingLock(
          entityType,
          entityId,
          {
            ...options,
            retries: retries - 1
          }
        );
      }
      
      // Re-throw error
      throw error;
    }
  }
  
  /**
   * Release a processing lock
   */
  async releaseLock(
    entityType: EntityType | string,
    entityId: string,
    options: {
      correlationId?: string;
      force?: boolean;
      retries?: number;
    } = {}
  ): Promise<boolean> {
    const {
      correlationId = generateCorrelationId('release'),
      force = false,
      retries = 1
    } = options;
    
    console.log(`[${correlationId}] Releasing lock for ${entityType}:${entityId}`);
    
    // Begin a transaction
    const transactionId = this.beginTransaction(entityType, entityId, correlationId);
    
    try {
      // First, update in the database
      let dbReleased = false;
      
      try {
        // Try using the RPC function first
        const { data: releaseResult, error: releaseError } = await this.supabase.rpc(
          'release_processing_lock',
          {
            p_entity_type: entityType,
            p_entity_id: entityId
          }
        );
        
        if (releaseError) {
          console.warn(`[${correlationId}] Error releasing lock via RPC: ${releaseError.message}`);
          
          // Fallback to direct update
          if (force) {
            // Force release by setting state to PENDING
            const { error: updateError } = await this.supabase
              .from('processing_status')
              .update({
                state: ProcessingState.PENDING,
                last_processed_at: new Date().toISOString(),
                updated_at: new Date().toISOString(),
                metadata: this.supabase.sql`
                  jsonb_set(
                    coalesce(metadata, '{}'::jsonb),
                    '{forced_release}',
                    jsonb_build_object(
                      'released_at', now()::text,
                      'correlation_id', ${correlationId}
                    )
                  )
                `
              })
              .eq('entity_type', entityType)
              .eq('entity_id', entityId);
            
            if (updateError) {
              throw createEnhancedError(
                `Failed to force release lock: ${updateError.message}`,
                ErrorSource.DATABASE,
                ErrorCategory.TRANSIENT_SERVICE
              );
            }
            
            dbReleased = true;
          } else {
            // Check current state first
            const { data: statusData, error: statusError } = await this.supabase
              .from('processing_status')
              .select('state')
              .eq('entity_type', entityType)
              .eq('entity_id', entityId)
              .maybeSingle();
            
            if (statusError) {
              throw createEnhancedError(
                `Failed to check entity status: ${statusError.message}`,
                ErrorSource.DATABASE,
                ErrorCategory.TRANSIENT_SERVICE
              );
            }
            
            // Only release if in IN_PROGRESS state
            if (statusData && statusData.state === ProcessingState.IN_PROGRESS) {
              const { error: updateError } = await this.supabase
                .from('processing_status')
                .update({
                  state: ProcessingState.PENDING,
                  last_processed_at: new Date().toISOString(),
                  updated_at: new Date().toISOString()
                })
                .eq('entity_type', entityType)
                .eq('entity_id', entityId);
              
              if (updateError) {
                throw createEnhancedError(
                  `Failed to release lock: ${updateError.message}`,
                  ErrorSource.DATABASE,
                  ErrorCategory.TRANSIENT_SERVICE
                );
              }
              
              dbReleased = true;
            } else {
              // Not in IN_PROGRESS state, can't release
              dbReleased = false;
            }
          }
        } else {
          // RPC call succeeded
          dbReleased = !!releaseResult;
          
          // Record operation
          this.recordOperation(transactionId, 'db', 'release_lock_rpc', true);
        }
      } catch (dbError) {
        // Record operation failure
        this.recordOperation(transactionId, 'db', 'release_lock', false, dbError);
        
        // Roll back transaction
        await this.rollbackTransaction(transactionId);
        
        // Track error
        await trackError(dbError, {
          entityType: entityType as EntityType,
          entityId,
          operationName: 'releaseLock',
          correlationId
        });
        
        throw dbError;
      }
      
      // Record operation success
      this.recordOperation(transactionId, 'db', 'release_lock', true);
      
      // If using Redis backup, also release Redis lock
      if (this.useRedisBackup) {
        try {
          const lockKey = `lock:${entityType}:${entityId}`;
          const heartbeatKey = `heartbeat:${entityType}:${entityId}`;
          
          // Delete the lock key
          await this.redis.del(lockKey);
          
          // Delete the heartbeat key
          await this.redis.del(heartbeatKey);
          
          // Stop heartbeat if active
          this.stopHeartbeat(entityType, entityId);
          
          // Record operation success
          this.recordOperation(transactionId, 'redis', 'release_lock', true);
        } catch (redisError) {
          // Record Redis operation failure, but it's not critical
          this.recordOperation(transactionId, 'redis', 'release_lock', false, redisError);
          
          console.warn(`[${correlationId}] Redis lock release error (non-critical): ${redisError.message}`);
        }
      }
      
      // Commit transaction
      const committed = await this.commitTransaction(transactionId);
      
      // Update stats
      if (dbReleased) {
        this.stats.locks.released++;
      }
      
      // If lock was released, we're done
      return committed && dbReleased;
    } catch (error) {
      // Roll back transaction
      await this.rollbackTransaction(transactionId);
      
      // Track error
      await trackError(error, {
        entityType: entityType as EntityType,
        entityId,
        operationName: 'releaseLock',
        correlationId
      });
      
      // Retry if attempts remain
      if (retries > 0) {
        console.log(`[${correlationId}] Retrying lock release for ${entityType}:${entityId}`);
        
        return this.releaseLock(
          entityType,
          entityId,
          {
            correlationId,
            force,
            retries: retries - 1
          }
        );
      }
      
      // Re-throw error
      throw error;
    }
  }
  
  /**
   * Update entity processing state with proper validation
   */
  async updateEntityState(
    entityType: EntityType | string,
    entityId: string,
    newState: ProcessingState,
    errorMessage: string | null = null,
    metadata: Record<string, any> = {}
  ): Promise<StateTransitionResult> {
    const correlationId = metadata.correlationId || generateCorrelationId('state');
    
    console.log(`[${correlationId}] Updating state for ${entityType}:${entityId} to ${newState}`);
    
    // Begin a transaction
    const transactionId = this.beginTransaction(entityType, entityId, correlationId);
    
    try {
      // Get current state from the database
      const { data: statusData, error: statusError } = await this.supabase
        .from('processing_status')
        .select('state')
        .eq('entity_type', entityType)
        .eq('entity_id', entityId)
        .maybeSingle();
      
      if (statusError) {
        throw createEnhancedError(
          `Failed to check entity status: ${statusError.message}`,
          ErrorSource.DATABASE,
          ErrorCategory.TRANSIENT_SERVICE
        );
      }
      
      const currentState = statusData?.state || null;
      
      // Validate state transition
      const isValid = isValidStateTransition(currentState, newState);
      
      if (!isValid) {
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
        
        // Roll back transaction
        await this.rollbackTransaction(transactionId);
        
        return result;
      }
      
      // Update state in the database
      try {
        // Prepare update data
        const updateData: Record<string, any> = {
          state: newState,
          last_processed_at: new Date().toISOString(),
          updated_at: new Date().toISOString()
        };
        
        // Add error message if provided
        if (errorMessage) {
          updateData.last_error = errorMessage;
        }
        
        // Handle special state transitions
        if (newState === ProcessingState.DEAD_LETTER) {
          updateData.dead_lettered = true;
          updateData.metadata = {
            ...metadata,
            dead_lettered_at: new Date().toISOString()
          };
        } else if (newState === ProcessingState.COMPLETED) {
          updateData.metadata = {
            ...metadata,
            completed_at: new Date().toISOString()
          };
        } else if (newState === ProcessingState.FAILED) {
          updateData.metadata = {
            ...metadata,
            failed_at: new Date().toISOString()
          };
        } else {
          updateData.metadata = metadata;
        }
        
        // Perform the update
        const { error: updateError } = await this.supabase
          .from('processing_status')
          .update(updateData)
          .eq('entity_type', entityType)
          .eq('entity_id', entityId);
        
        if (updateError) {
          throw createEnhancedError(
            `Failed to update entity state: ${updateError.message}`,
            ErrorSource.DATABASE,
            ErrorCategory.TRANSIENT_SERVICE
          );
        }
        
        // Record operation success
        this.recordOperation(transactionId, 'db', 'update_state', true);
      } catch (dbError) {
        // Record operation failure
        this.recordOperation(transactionId, 'db', 'update_state', false, dbError);
        
        // Roll back transaction
        await this.rollbackTransaction(transactionId);
        
        // Track error
        await trackError(dbError, {
          entityType: entityType as EntityType,
          entityId,
          operationName: 'updateEntityState',
          correlationId
        });
        
        throw dbError;
      }
      
      // If using Redis backup, also update Redis state
      if (this.useRedisBackup) {
        try {
          const stateKey = `state:${entityType}:${entityId}`;
          const lockKey = `lock:${entityType}:${entityId}`;
          
          // Update state in Redis
          await this.redis.set(stateKey, JSON.stringify({
            state: newState,
            timestamp: new Date().toISOString(),
            errorMessage,
            correlationId,
            metadata: {
              ...metadata,
              updatedAt: new Date().toISOString()
            }
          }), {
            ex: 86400 // 24 hour TTL
          });
          
          // If completed or failed, remove the lock and stop heartbeat
          if (
            newState === ProcessingState.COMPLETED || 
            newState === ProcessingState.FAILED || 
            newState === ProcessingState.DEAD_LETTER
          ) {
            await this.redis.del(lockKey);
            this.stopHeartbeat(entityType, entityId);
          }
          
          // Record operation success
          this.recordOperation(transactionId, 'redis', 'update_state', true);
        } catch (redisError) {
          // Record Redis operation failure, but it's not critical
          this.recordOperation(transactionId, 'redis', 'update_state', false, redisError);
          
          console.warn(`[${correlationId}] Redis state update error (non-critical): ${redisError.message}`);
        }
      }
      
      // Commit transaction
      const committed = await this.commitTransaction(transactionId);
      
      // Update stats
      this.stats.stateTransitions[newState]++;
      
      // Return success result
      return {
        success: committed,
        previousState: currentState || undefined,
        newState,
        source: 'database',
        validationDetails: {
          isValid: true
        }
      };
    } catch (error) {
      // Roll back transaction
      await this.rollbackTransaction(transactionId);
      
      // Track error
      await trackError(error, {
        entityType: entityType as EntityType,
        entityId,
        operationName: 'updateEntityState',
        correlationId
      });
      
      // Return error result
      return {
        success: false,
        previousState: undefined,
        newState,
        error: `Error updating state: ${error.message}`,
        source: 'database'
      };
    }
  }
  
  /**
   * Check if an entity is in a specific state
   */
  async isInState(
    entityType: EntityType | string,
    entityId: string,
    state: ProcessingState,
    options: StateCheckOptions = {}
  ): Promise<boolean> {
    const { useRedisFallback = true } = options;
    
    // Try Redis first for performance
    if (useRedisFallback && this.useRedisBackup) {
      try {
        const stateKey = `state:${entityType}:${entityId}`;
        const stateData = await this.redis.get(stateKey);
        
        if (stateData) {
          try {
            const parsedState = JSON.parse(stateData as string);
            return parsedState.state === state;
          } catch (parseErr) {
            console.warn(`Failed to parse Redis state: ${parseErr.message}`);
          }
        }
      } catch (redisErr) {
        console.warn(`Redis state check failed: ${redisErr.message}`);
      }
    }
    
    // Fall back to database
    try {
      const { data, error } = await this.supabase
        .from('processing_status')
        .select('state')
        .eq('entity_type', entityType)
        .eq('entity_id', entityId)
        .maybeSingle();
      
      if (error) {
        console.warn(`Database state check failed: ${error.message}`);
        return false;
      }
      
      return data?.state === state;
    } catch (dbError) {
      console.warn(`Database state check failed: ${dbError.message}`);
      return false;
    }
  }
  
  /**
   * Check if an entity has already been processed (i.e., is in COMPLETED state)
   */
  async isProcessed(
    entityType: EntityType | string,
    entityId: string,
    options: StateCheckOptions = {}
  ): Promise<boolean> {
    return this.isInState(entityType, entityId, ProcessingState.COMPLETED, options);
  }
  
  /**
   * Mark an entity as completed
   */
  async markAsCompleted(
    entityType: EntityType | string,
    entityId: string,
    metadata: Record<string, any> = {}
  ): Promise<StateTransitionResult> {
    return this.updateEntityState(
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
   * Mark an entity as failed
   */
  async markAsFailed(
    entityType: EntityType | string,
    entityId: string,
    errorMessage: string,
    metadata: Record<string, any> = {}
  ): Promise<StateTransitionResult> {
    return this.updateEntityState(
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
   * Move an entity to the dead-letter queue
   */
  async markAsDeadLetter(
    entityType: EntityType | string,
    entityId: string,
    errorMessage: string,
    metadata: Record<string, any> = {}
  ): Promise<StateTransitionResult> {
    return this.updateEntityState(
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
   * Sync stats to Redis
   */
  private async maybeSyncStats(): Promise<void> {
    const now = Date.now();
    
    // Only sync periodically
    if (now - this.lastStatSync < this.statSyncInterval) {
      return;
    }
    
    this.lastStatSync = now;
    
    try {
      // Update stats in Redis
      await this.redis.set('state_manager:stats', JSON.stringify({
        ...this.stats,
        updated_at: new Date().toISOString()
      }), {
        ex: 86400 // 24 hour TTL
      });
    } catch (error) {
      console.warn(`Failed to sync stats to Redis: ${error.message}`);
    }
  }
  
  // === Heartbeat mechanism ===
  
  // Track active heartbeats
  private activeHeartbeats = new Map<string, number>();
  
  /**
   * Start heartbeat mechanism for a lock
   */
  private startHeartbeat(
    entityType: string,
    entityId: string,
    workerId: string,
    correlationId: string,
    intervalSeconds: number,
    ttlSeconds: number
  ): void {
    const heartbeatKey = `${entityType}:${entityId}`;
    
    // Stop existing heartbeat if any
    this.stopHeartbeat(entityType, entityId);
    
    // Start new heartbeat interval
    const intervalId = setInterval(async () => {
      try {
        const lockKey = `lock:${entityType}:${entityId}`;
        const heartbeatKey = `heartbeat:${entityType}:${entityId}`;
        
        // Get existing lock data
        const existingLockData = await this.redis.get(lockKey);
        if (!existingLockData) {
          // Lock no longer exists, stop heartbeat
          this.stopHeartbeat(entityType, entityId);
          return;
        }
        
        try {
          const parsedLock = JSON.parse(existingLockData as string);
          
          // Only update if we are the lock owner
          if (parsedLock.workerId === workerId) {
            parsedLock.lastHeartbeat = new Date().toISOString();
            
            // Update lock with heartbeat info
            await this.redis.set(lockKey, JSON.stringify(parsedLock), {
              xx: true, // Only set if key exists
              ex: ttlSeconds // Reset TTL with each heartbeat
            });
            
            // Also set a separate heartbeat key
            await this.redis.set(heartbeatKey, JSON.stringify({
              workerId,
              correlationId,
              timestamp: new Date().toISOString()
            }), {
              ex: ttlSeconds
            });
            
            console.log(`Heartbeat sent for ${entityType}:${entityId}`);
          } else {
            // We no longer own this lock, stop heartbeat
            console.log(`Lock for ${entityType}:${entityId} is now owned by ${parsedLock.workerId}, stopping heartbeat`);
            this.stopHeartbeat(entityType, entityId);
          }
        } catch (parseError) {
          console.warn(`Failed to parse lock data during heartbeat: ${parseError.message}`);
        }
      } catch (error) {
        console.error(`Error sending heartbeat for ${entityType}:${entityId}: ${error.message}`);
      }
    }, intervalSeconds * 1000);
    
    // Store interval ID for later cleanup
    this.activeHeartbeats.set(heartbeatKey, intervalId);
  }
  
  /**
   * Stop heartbeat for an entity
   */
  private stopHeartbeat(entityType: string, entityId: string): void {
    const heartbeatKey = `${entityType}:${entityId}`;
    const intervalId = this.activeHeartbeats.get(heartbeatKey);
    
    if (intervalId) {
      clearInterval(intervalId);
      this.activeHeartbeats.delete(heartbeatKey);
      console.log(`Heartbeat stopped for ${entityType}:${entityId}`);
    }
  }
}

// Singleton instance for consistent access
let enhancedStateManagerInstance: EnhancedStateManager | null = null;

/**
 * Get the singleton instance of the enhanced state manager
 */
export function getEnhancedStateManager(
  supabase: any,
  redis: Redis,
  useRedisBackup = true
): EnhancedStateManager {
  if (!enhancedStateManagerInstance) {
    enhancedStateManagerInstance = new EnhancedStateManager(supabase, redis, useRedisBackup);
  }
  return enhancedStateManagerInstance;
}
