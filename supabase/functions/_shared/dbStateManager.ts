/**
 * Database State Manager
 * Implements the database side of state management
 */

import { ProcessingState, StateTransitionResult, LockOptions, StateCheckOptions } from "./stateManager.ts";

export class DbStateManager {
  private supabase: any;
  
  constructor(supabaseClient: any) {
    this.supabase = supabaseClient;
  }

  /**
   * Acquires a processing lock in the database
   */
  async acquireProcessingLock(
    entityType: string,
    entityId: string,
    options: LockOptions = {}
  ): Promise<boolean> {
    const { allowRetry = true } = options;
    
    let attempts = 0;
    const maxRetries = allowRetry ? 3 : 1;
    
    while (attempts < maxRetries) {
      attempts++;
      
      try {
        const { data, error } = await this.supabase.rpc('acquire_processing_lock', {
          p_entity_type: entityType,
          p_entity_id: entityId
        });
        
        if (!error && data === true) {
          return true; // Lock acquired successfully
        } else {
          console.warn(`Attempt ${attempts}: Failed to acquire lock for ${entityType}:${entityId}: ${error?.message || 'Unknown error'}`);
          
          // If retry is allowed, wait before retrying
          if (allowRetry && attempts < maxRetries) {
            await new Promise(resolve => setTimeout(resolve, 200 * attempts));
          }
        }
      } catch (err) {
        console.error(`Attempt ${attempts}: Error acquiring lock for ${entityType}:${entityId}: ${err.message}`);
        
        // If retry is allowed, wait before retrying
        if (allowRetry && attempts < maxRetries) {
          await new Promise(resolve => setTimeout(resolve, 200 * attempts));
        }
      }
    }
    
    return false; // Failed to acquire lock after all attempts
  }
  
  /**
   * Updates a lock's heartbeat timestamp
   * Used to keep track of active locks
   */
  async updateHeartbeat(
    entityType: string,
    entityId: string
  ): Promise<boolean> {
    try {
      // Call RPC to update the lock's heartbeat
      const { data, error } = await this.supabase.rpc('update_lock_heartbeat', {
        p_entity_type: entityType,
        p_entity_id: entityId,
        p_heartbeat_time: new Date().toISOString()
      });
      
      if (error) {
        console.warn(`Failed to update heartbeat for ${entityType}:${entityId}: ${error.message}`);
        return false;
      }
      
      return true;
    } catch (err) {
      console.error(`Error updating lock heartbeat: ${err.message}`);
      return false;
    }
  }
  
  /**
   * Check if a lock is stale based on its last heartbeat
   */
  async isLockStale(
    entityType: string,
    entityId: string,
    staleCutoffMinutes: number
  ): Promise<boolean> {
    try {
      // Call RPC to check if lock is stale
      const { data, error } = await this.supabase.rpc('is_lock_stale', {
        p_entity_type: entityType,
        p_entity_id: entityId,
        p_cutoff_minutes: staleCutoffMinutes
      });
      
      if (error) {
        console.warn(`Failed to check stale lock for ${entityType}:${entityId}: ${error.message}`);
        return false;
      }
      
      return data === true;
    } catch (err) {
      console.error(`Error checking stale lock: ${err.message}`);
      return false;
    }
  }
  
  /**
   * Find all stale locks in the system
   */
  async findStaleLocks(staleCutoffMinutes: number = 30): Promise<{ data?: any[], error?: any }> {
    try {
      return await this.supabase.rpc('find_stale_locks', {
        p_cutoff_minutes: staleCutoffMinutes
      });
    } catch (err) {
      return { error: err };
    }
  }
  
  /**
   * Force release a lock without checking ownership
   * Used for stale lock cleanup
   */
  async forceReleaseLock(
    entityType: string,
    entityId: string
  ): Promise<boolean> {
    try {
      const { data, error } = await this.supabase.rpc('force_release_lock', {
        p_entity_type: entityType,
        p_entity_id: entityId
      });
      
      if (error) {
        console.error(`Failed to force release lock for ${entityType}:${entityId}: ${error.message}`);
        return false;
      }
      
      return true;
    } catch (err) {
      console.error(`Error forcing lock release: ${err.message}`);
      return false;
    }
  }

  /**
   * Updates the state of an entity in the database
   */
  async updateEntityState(
    entityType: string,
    entityId: string,
    newState: ProcessingState,
    errorMessage: string | null = null,
    metadata: Record<string, any> = {}
  ): Promise<StateTransitionResult> {
    try {
      const { data, error } = await this.supabase.from('processing_status').upsert(
        {
          entity_type: entityType,
          entity_id: entityId,
          state: newState,
          last_updated: new Date().toISOString(),
          error_message: errorMessage,
          metadata: metadata
        },
        { onConflict: 'entity_type,entity_id', returning: 'minimal' }
      ).select();
      
      if (error) {
        console.error(`Failed to update state for ${entityType}:${entityId} to ${newState}: ${error.message}`);
        return {
          success: false,
          error: error.message,
          previousState: undefined,
          newState
        };
      }
      
      return {
        success: true,
        newState
      };
    } catch (error) {
      console.error(`Error updating state for ${entityType}:${entityId}: ${error.message}`);
      return {
        success: false,
        error: `Error updating state: ${error.message}`,
        previousState: undefined,
        newState
      };
    }
  }
  
  /**
   * Release a processing lock
   */
  async releaseLock(
    entityType: string,
    entityId: string
  ): Promise<boolean> {
    try {
      const { data, error } = await this.supabase.rpc('release_processing_lock', {
        p_entity_type: entityType,
        p_entity_id: entityId
      });
      
      if (error) {
        console.error(`Failed to release lock for ${entityType}:${entityId}: ${error.message}`);
        return false;
      }
      
      return true;
    } catch (err) {
      console.error(`Error releasing lock: ${err.message}`);
      return false;
    }
  }
  
  /**
   * Reset stuck processing states
   */
  async resetStuckProcessingStates(
    olderThanMinutes: number = 30
  ): Promise<{ data?: any[], error?: any }> {
    try {
      const { data, error } = await this.supabase.rpc('reset_stuck_states', {
        p_older_than_minutes: olderThanMinutes
      });
      
      if (error) {
        console.error(`Failed to reset stuck processing states: ${error.message}`);
        return { error };
      }
      
      return { data };
    } catch (err) {
      console.error(`Error resetting stuck processing states: ${err.message}`);
      return { error: err };
    }
  }
  
  /**
   * Finds inconsistent states between database and Redis
   */
  async findInconsistentStates(
    entityType?: string,
    olderThanMinutes: number = 60
  ): Promise<any[]> {
    try {
      const { data, error } = await this.supabase.rpc('find_inconsistent_states', {
        p_entity_type: entityType,
        p_older_than_minutes: olderThanMinutes
      });
      
      if (error) {
        console.error(`Failed to find inconsistent states: ${error.message}`);
        return [];
      }
      
      return data || [];
    } catch (err) {
      console.error(`Error finding inconsistent states: ${err.message}`);
      return [];
    }
  }
  
  /**
   * Get current state of an entity
   */
  async getEntityState(
    entityType: string,
    entityId: string
  ): Promise<ProcessingState | null> {
    try {
      const { data, error } = await this.supabase
        .from('processing_status')
        .select('state')
        .eq('entity_type', entityType)
        .eq('entity_id', entityId)
        .single();
      
      if (error) {
        // If no row is found, return null
        if (error.message.includes('No rows found')) {
          return null;
        }
        
        console.error(`Error getting state for ${entityType}:${entityId}: ${error.message}`);
        return null;
      }
      
      return data?.state || null;
    } catch (err) {
      console.error(`Error getting state for ${entityType}:${entityId}: ${err.message}`);
      return null;
    }
  }
  
  /**
   * Checks if entity is in specific state
   */
  async isInState(
    entityType: string,
    entityId: string,
    state: ProcessingState
  ): Promise<boolean> {
    try {
      const { data, error } = await this.supabase
        .from('processing_status')
        .select('state')
        .eq('entity_type', entityType)
        .eq('entity_id', entityId)
        .eq('state', state)
        .single();
      
      if (error) {
        // If no row is found, it's not in the state
        if (error.message.includes('No rows found')) {
          return false;
        }
        
        console.error(`Error checking state for ${entityType}:${entityId}: ${error.message}`);
        return false;
      }
      
      return !!data;
    } catch (err) {
      console.error(`Error checking state for ${entityType}:${entityId}: ${err.message}`);
      return false;
    }
  }
  
  /**
   * Checks if entity has been processed
   */
  async isProcessed(
    entityType: string,
    entityId: string
  ): Promise<boolean> {
    return this.isInState(entityType, entityId, ProcessingState.COMPLETED);
  }
}
