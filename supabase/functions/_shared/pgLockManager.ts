/**
 * PostgreSQL Advisory Lock Manager
 * Implementation using Postgres advisory locks for distributed locking
 */

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { LockOptions } from "./stateManager.ts";

interface PgLockResult {
  success: boolean;
  lockId?: string;
  error?: string;
}

export class PgLockManager {
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
   * Generate a 64-bit lock integer from entity type and id
   */
  private generateLockId(entityType: string, entityId: string): string {
    // Create a consistent hash from the entity type and ID
    const combinedKey = `${entityType}:${entityId}`;
    const hash = this.hashString(combinedKey);
    return hash;
  }
  
  /**
   * Create a simple string hash function
   */
  private hashString(str: string): string {
    // Using a simple hash algorithm to generate a numeric hash
    let hash = 5381;
    for (let i = 0; i < str.length; i++) {
      hash = ((hash << 5) + hash) + str.charCodeAt(i);
    }
    return Math.abs(hash).toString();
  }
  
  /**
   * Acquires an advisory lock from PostgreSQL
   */
  async acquireLock(
    entityType: string, 
    entityId: string, 
    options: LockOptions = {}
  ): Promise<PgLockResult> {
    const {
      timeoutSeconds = 0, // 0 means non-blocking
      correlationId = `lock_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`,
      retries = 1
    } = options;
    
    // Check if circuit breaker is open
    if (this.isCircuitOpen()) {
      return {
        success: false, 
        error: "Database circuit breaker open"
      };
    }
    
    const lockId = this.generateLockId(entityType, entityId);
    
    try {
      let attemptCount = 0;
      
      while (attemptCount <= retries) {
        // If timeout is 0, use pg_try_advisory_lock (non-blocking)
        // Otherwise use pg_advisory_lock_timeout (blocking with timeout)
        const functionName = timeoutSeconds > 0 
          ? 'pg_advisory_lock_timeout' 
          : 'pg_try_advisory_lock';
        
        const params: any = timeoutSeconds > 0 
          ? { p_key: lockId, p_timeout_ms: timeoutSeconds * 1000 }
          : { p_key: lockId };
        
        const { data, error } = await this.supabase.rpc(functionName, params);
        
        if (error) {
          console.error(`[${correlationId}] Advisory lock error: ${error.message}`);
          
          if (attemptCount < retries) {
            attemptCount++;
            await new Promise(resolve => setTimeout(resolve, 500 * attemptCount));
            continue;
          }
          
          this.incrementCircuitFailure();
          return {
            success: false,
            error: error.message
          };
        }
        
        // Reset circuit breaker on success
        this.resetCircuitBreaker();
        
        // For pg_try_advisory_lock, data is true/false
        // For pg_advisory_lock_timeout, success is indicated by not throwing an error
        const lockAcquired = data === true || (timeoutSeconds > 0);
        
        if (lockAcquired) {
          console.log(`[${correlationId}] Acquired advisory lock for ${entityType}:${entityId} (${lockId})`);
          
          // Also record in the processing_locks table for visibility
          await this.recordLockAcquisition(entityType, entityId, lockId, correlationId);
          
          return {
            success: true,
            lockId
          };
        } else if (attemptCount < retries) {
          // Retry acquiring the lock
          attemptCount++;
          await new Promise(resolve => setTimeout(resolve, 500 * attemptCount));
          continue;
        } else {
          // We've exhausted our retries
          console.warn(`[${correlationId}] Could not acquire advisory lock for ${entityType}:${entityId} after ${retries} attempts`);
          return {
            success: false,
            error: "Lock acquisition failed, resource is locked by another process"
          };
        }
      }
      
      // This should be unreachable, but satisfies TypeScript
      return {
        success: false,
        error: "Unknown error acquiring lock"
      };
    } catch (error) {
      console.error(`[${correlationId}] Exception acquiring advisory lock: ${error.message}`);
      this.incrementCircuitFailure();
      
      return {
        success: false,
        error: error.message
      };
    }
  }
  
  /**
   * Records lock acquisition in the processing_locks table for visibility
   * This is separate from the actual lock mechanism (advisory lock)
   */
  private async recordLockAcquisition(
    entityType: string,
    entityId: string,
    lockId: string,
    correlationId: string
  ): Promise<void> {
    try {
      const workerId = Deno.env.get("WORKER_ID") || 
        `worker_${Math.random().toString(36).substring(2, 10)}`;
        
      await this.supabase.from('processing_locks').upsert({
        entity_type: entityType,
        entity_id: entityId,
        worker_id: workerId,
        correlation_id: correlationId,
        acquired_at: new Date().toISOString(),
        last_heartbeat: new Date().toISOString(),
        metadata: {
          lock_type: 'advisory',
          lock_id: lockId,
          pg_session_id: correlationId
        }
      }, {
        onConflict: 'entity_type,entity_id'
      });
    } catch (error) {
      // Non-critical error - the real lock is the advisory lock
      console.warn(`Failed to record lock acquisition: ${error.message}`);
    }
  }

  /**
   * Releases an advisory lock from PostgreSQL
   */
  async releaseLock(
    entityType: string,
    entityId: string
  ): Promise<boolean> {
    // Check if circuit breaker is open
    if (this.isCircuitOpen()) {
      return false;
    }
    
    const lockId = this.generateLockId(entityType, entityId);
    
    try {
      // Call pg_advisory_unlock RPC
      const { data, error } = await this.supabase.rpc('pg_advisory_unlock', {
        p_key: lockId
      });
      
      if (error) {
        console.error(`Advisory lock release error: ${error.message}`);
        this.incrementCircuitFailure();
        return false;
      }
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      // Also remove from the processing_locks table
      try {
        await this.supabase.from('processing_locks')
          .delete()
          .eq('entity_type', entityType)
          .eq('entity_id', entityId);
      } catch (e) {
        // Non-critical error
        console.warn(`Failed to remove lock record: ${e.message}`);
      }
      
      return data === true;
    } catch (error) {
      console.error(`Failed to release advisory lock: ${error.message}`);
      this.incrementCircuitFailure();
      return false;
    }
  }
  
  /**
   * Checks if a lock is held by someone
   */
  async isLocked(entityType: string, entityId: string): Promise<boolean> {
    // Check if circuit breaker is open
    if (this.isCircuitOpen()) {
      return false;
    }
    
    const lockId = this.generateLockId(entityType, entityId);
    
    try {
      // Check if a lock exists by trying to acquire it in non-blocking mode
      const { data, error } = await this.supabase.rpc('pg_try_advisory_lock', {
        p_key: lockId
      });
      
      if (error) {
        console.error(`Lock check error: ${error.message}`);
        this.incrementCircuitFailure();
        return false;
      }
      
      if (data === true) {
        // We got the lock, which means it wasn't locked
        // So we need to release it immediately
        await this.supabase.rpc('pg_advisory_unlock', { p_key: lockId });
        return false;
      }
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      // We couldn't get the lock, which means it is locked by someone
      return true;
    } catch (error) {
      console.error(`Error checking lock state: ${error.message}`);
      this.incrementCircuitFailure();
      return false;
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
  
  /**
   * Clean up stale locks
   */
  async cleanupStaleLocks(olderThanMinutes: number = 30): Promise<number> {
    try {
      const { data, error } = await this.supabase
        .from('processing_locks')
        .delete()
        .lt('last_heartbeat', new Date(Date.now() - (olderThanMinutes * 60 * 1000)).toISOString())
        .select();
      
      if (error) {
        console.error(`Failed to clean up stale locks: ${error.message}`);
        return 0;
      }
      
      return data?.length || 0;
    } catch (error) {
      console.error(`Error in stale lock cleanup: ${error.message}`);
      return 0;
    }
  }
}
