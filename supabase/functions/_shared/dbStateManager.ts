
/**
 * Database State Manager
 * Implements the database side of our state management approach
 */

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { ProcessingState, LockOptions, StateTransitionResult } from "./stateManager.ts";

export class DbStateManager {
  private supabase: any;
  private circuitBreakerState = {
    failures: 0,
    lastFailure: 0,
    isOpen: false
  };
  
  constructor(supabaseClient?: any) {
    // Initialize Supabase client if not provided
    if (supabaseClient) {
      this.supabase = supabaseClient;
    } else {
      this.supabase = createClient(
        Deno.env.get("SUPABASE_URL") || "",
        Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || ""
      );
    }
  }
  
  /**
   * Attempts to acquire a processing lock in database
   * This updates the processing_status table but not the actual advisory lock
   * Used for visibility and tracking purposes
   */
  async acquireProcessingLock(
    entityType: string,
    entityId: string,
    options: LockOptions = {}
  ): Promise<boolean> {
    const {
      timeoutMinutes = 30,
      allowRetry = true,
      correlationId = `lock_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`,
      retries = 1
    } = options;
    
    // Check if circuit breaker is open
    if (this.isCircuitOpen()) {
      console.warn(`[${correlationId}] Database circuit breaker open, skipping lock acquisition`);
      return false;
    }
    
    let attemptCount = 0;
    
    while (attemptCount <= retries) {
      try {
        // Try to update processing status
        const { data, error } = await this.supabase.rpc(
          'acquire_processing_lock',
          {
            p_entity_type: entityType,
            p_entity_id: entityId,
            p_timeout_minutes: timeoutMinutes,
            p_correlation_id: correlationId
          }
        );
        
        if (error) {
          console.error(`[${correlationId}] Database lock error: ${error.message}`);
          this.incrementCircuitFailure();
          
          // Try again if retries are allowed
          if (allowRetry && attemptCount < retries) {
            attemptCount++;
            await new Promise(resolve => setTimeout(resolve, 500 * attemptCount));
            continue;
          }
          
          return false;
        }
        
        // Reset circuit breaker on success
        this.resetCircuitBreaker();
        
        // Return lock acquisition result
        return data === true;
      } catch (error) {
        console.error(`[${correlationId}] Exception acquiring db lock: ${error.message}`);
        this.incrementCircuitFailure();
        
        // Try again if retries are allowed
        if (allowRetry && attemptCount < retries) {
          attemptCount++;
          await new Promise(resolve => setTimeout(resolve, 500 * attemptCount));
          continue;
        }
        
        return false;
      }
    }
    
    return false;
  }

  /**
   * Get entity state from database
   */
  async getEntityState(entityType: string, entityId: string): Promise<ProcessingState | null> {
    try {
      const { data, error } = await this.supabase
        .from('processing_status')
        .select('state')
        .eq('entity_type', entityType)
        .eq('entity_id', entityId)
        .maybeSingle();
      
      if (error) {
        console.error(`Error getting entity state: ${error.message}`);
        return null;
      }
      
      return data && data.state ? data.state as ProcessingState : null;
    } catch (error) {
      console.error(`Failed to get entity state: ${error.message}`);
      return null;
    }
  }
  
  /**
   * Updates entity state in database
   */
  async updateEntityState(
    entityType: string,
    entityId: string,
    state: ProcessingState,
    errorMessage: string | null = null,
    metadata: Record<string, any> = {}
  ): Promise<StateTransitionResult> {
    // Check if circuit breaker is open
    if (this.isCircuitOpen()) {
      return { 
        success: false, 
        error: "Database circuit breaker open" 
      };
    }
    
    try {
      // Add timestamp to metadata
      const enhancedMetadata = {
        ...metadata,
        updated_at: new Date().toISOString()
      };
      
      // Get current state first to return for change tracking
      const currentState = await this.getEntityState(entityType, entityId);
      
      // Update database state
      const { data, error } = await this.supabase
        .from('processing_status')
        .upsert({
          entity_type: entityType,
          entity_id: entityId,
          state: state,
          last_processed_at: new Date().toISOString(),
          last_error: errorMessage,
          metadata: enhancedMetadata,
          updated_at: new Date().toISOString()
        }, {
          onConflict: 'entity_type,entity_id'
        })
        .select('state');
      
      if (error) {
        console.error(`Error updating entity state: ${error.message}`);
        this.incrementCircuitFailure();
        return {
          success: false,
          error: error.message
        };
      }
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      return {
        success: true,
        previousState: currentState || undefined,
        newState: state,
        source: 'database'
      };
    } catch (error) {
      console.error(`Failed to update entity state: ${error.message}`);
      this.incrementCircuitFailure();
      
      return {
        success: false,
        error: error.message
      };
    }
  }
  
  /**
   * Checks if entity is in specific state in database
   */
  async isInState(
    entityType: string,
    entityId: string,
    state: ProcessingState
  ): Promise<boolean> {
    // Check if circuit breaker is open
    if (this.isCircuitOpen()) {
      return false;
    }
    
    try {
      const { data, error } = await this.supabase
        .from('processing_status')
        .select('state')
        .eq('entity_type', entityType)
        .eq('entity_id', entityId)
        .maybeSingle();
      
      if (error) {
        console.error(`Error checking entity state: ${error.message}`);
        this.incrementCircuitFailure();
        return false;
      }
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      return data && data.state === state;
    } catch (error) {
      console.error(`Failed to check entity state: ${error.message}`);
      this.incrementCircuitFailure();
      return false;
    }
  }
  
  /**
   * Checks if entity is already processed
   */
  async isProcessed(
    entityType: string,
    entityId: string
  ): Promise<boolean> {
    return await this.isInState(entityType, entityId, ProcessingState.COMPLETED);
  }
  
  /**
   * Marks an entity as completed
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
   * Releases a processing lock (updates database state only)
   */
  async releaseLock(
    entityType: string,
    entityId: string
  ): Promise<boolean> {
    // Check if circuit breaker is open
    if (this.isCircuitOpen()) {
      return false;
    }
    
    try {
      const { error } = await this.supabase.rpc(
        'release_processing_lock',
        {
          p_entity_type: entityType,
          p_entity_id: entityId
        }
      );
      
      if (error) {
        console.error(`Database lock release error: ${error.message}`);
        this.incrementCircuitFailure();
        return false;
      }
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      return true;
    } catch (error) {
      console.error(`Failed to release processing lock: ${error.message}`);
      this.incrementCircuitFailure();
      return false;
    }
  }
  
  /**
   * Find inconsistent or stale states in the database
   */
  async findInconsistentStates(
    entityType?: string,
    olderThanMinutes: number = 60
  ) {
    // Check if circuit breaker is open
    if (this.isCircuitOpen()) {
      return [];
    }
    
    try {
      const { data, error } = await this.supabase.rpc(
        'find_inconsistent_states',
        {
          p_entity_type: entityType || null,
          p_older_than_minutes: olderThanMinutes
        }
      );
      
      if (error) {
        console.error(`Error finding inconsistent states: ${error.message}`);
        this.incrementCircuitFailure();
        return [];
      }
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      return data || [];
    } catch (error) {
      console.error(`Failed to find inconsistent states: ${error.message}`);
      this.incrementCircuitFailure();
      return [];
    }
  }
  
  /**
   * Reset stuck processing states
   */
  async resetStuckProcessingStates(olderThanMinutes: number = 60) {
    // Check if circuit breaker is open
    if (this.isCircuitOpen()) {
      return { success: false, error: "Database circuit breaker open" };
    }
    
    try {
      const { data, error } = await this.supabase
        .from('processing_status')
        .update({
          state: ProcessingState.PENDING,
          metadata: {
            auto_reset: {
              reset_at: new Date().toISOString(),
              reason: 'Stuck in IN_PROGRESS state'
            }
          },
          updated_at: new Date().toISOString(),
          last_processed_at: new Date().toISOString()
        })
        .eq('state', ProcessingState.IN_PROGRESS)
        .lt('last_processed_at', new Date(Date.now() - (olderThanMinutes * 60 * 1000)).toISOString())
        .select();
      
      if (error) {
        console.error(`Error resetting stuck states: ${error.message}`);
        this.incrementCircuitFailure();
        return { success: false, error: error.message };
      }
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      return { success: true, data };
    } catch (error) {
      console.error(`Failed to reset stuck states: ${error.message}`);
      this.incrementCircuitFailure();
      return { success: false, error: error.message };
    }
  }
  
  /**
   * Circuit breaker implementation
   */
  private incrementCircuitFailure(): void {
    const now = Date.now();
    
    // Reset counter if last failure was more than 60 seconds ago
    if (now - this.circuitBreakerState.lastFailure > 60000) {
      this.circuitBreakerState.failures = 1;
    } else {
      this.circuitBreakerState.failures++;
    }
    
    this.circuitBreakerState.lastFailure = now;
    
    // Trip circuit breaker after 5 consecutive failures
    if (this.circuitBreakerState.failures >= 5 && !this.circuitBreakerState.isOpen) {
      this.circuitBreakerState.isOpen = true;
      
      // Auto-reset after 30 seconds
      setTimeout(() => {
        console.log("Database circuit breaker auto-reset");
        this.resetCircuitBreaker();
      }, 30000);
      
      console.warn(`Database circuit breaker tripped. Will auto-reset in 30 seconds`);
    }
  }
  
  private isCircuitOpen(): boolean {
    return this.circuitBreakerState.isOpen;
  }
  
  private resetCircuitBreaker(): void {
    this.circuitBreakerState.failures = 0;
    this.circuitBreakerState.isOpen = false;
  }
}
