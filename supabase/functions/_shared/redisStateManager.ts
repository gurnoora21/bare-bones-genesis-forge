
/**
 * Redis State Manager
 * Implements the Redis side of our dual-system approach
 */

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { ProcessingState, LockOptions, getEnvironmentTTL } from "./stateManager.ts";

export class RedisStateManager {
  private redis: Redis;
  private circuitBreakerState = {
    failures: 0,
    lastFailure: 0,
    isOpen: false
  };
  
  constructor(redis: Redis) {
    this.redis = redis;
  }
  
  /**
   * Attempts to acquire a processing lock in Redis
   */
  async acquireProcessingLock(
    entityType: string,
    entityId: string,
    options: LockOptions = {}
  ): Promise<boolean> {
    const {
      timeoutMinutes = 30,
      correlationId = `lock_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`
    } = options;
    
    // Check if circuit breaker is open
    if (this.isCircuitOpen()) {
      return false;
    }
    
    const lockKey = `lock:${entityType}:${entityId}`;
    const stateKey = `state:${entityType}:${entityId}`;
    const ttlSeconds = timeoutMinutes * 60;
    
    try {
      // Try to set the lock key with NX (only if it doesn't exist)
      const result = await this.redis.set(lockKey, JSON.stringify({
        acquiredAt: new Date().toISOString(),
        correlationId
      }), {
        nx: true,
        ex: ttlSeconds
      });
      
      if (result === "OK") {
        // Also store state for consistency
        await this.redis.set(stateKey, JSON.stringify({
          state: ProcessingState.IN_PROGRESS,
          timestamp: new Date().toISOString(),
          correlationId
        }), {
          ex: getEnvironmentTTL()
        });
        
        // Reset circuit breaker on success
        this.resetCircuitBreaker();
        
        return true;
      }
      
      return false;
    } catch (error) {
      console.error(`Error acquiring Redis lock: ${error.message}`);
      this.incrementCircuitFailure();
      return false;
    }
  }
  
  /**
   * Updates entity state in Redis
   */
  async updateEntityState(
    entityType: string,
    entityId: string,
    state: ProcessingState,
    errorMessage: string | null = null,
    metadata: Record<string, any> = {}
  ): Promise<boolean> {
    // Check if circuit breaker is open
    if (this.isCircuitOpen()) {
      return false;
    }
    
    try {
      const stateKey = `state:${entityType}:${entityId}`;
      const lockKey = `lock:${entityType}:${entityId}`;
      
      // Set state in Redis
      await this.redis.set(stateKey, JSON.stringify({
        state,
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
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      return true;
    } catch (error) {
      console.error(`Redis state update failed: ${error.message}`);
      this.incrementCircuitFailure();
      return false;
    }
  }
  
  /**
   * Checks if entity is in specific state in Redis
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
      const stateKey = `state:${entityType}:${entityId}`;
      const stateData = await this.redis.get(stateKey);
      
      if (stateData) {
        try {
          const parsedState = JSON.parse(stateData as string);
          
          // Reset circuit breaker on success
          this.resetCircuitBreaker();
          
          return parsedState.state === state;
        } catch (parseErr) {
          console.warn(`Failed to parse Redis state: ${parseErr.message}`);
        }
      }
      
      // Reset circuit breaker on success (even if key not found)
      this.resetCircuitBreaker();
      
      return false;
    } catch (error) {
      console.warn(`Redis check failed: ${error.message}`);
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
   * Releases a processing lock in Redis
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
      const lockKey = `lock:${entityType}:${entityId}`;
      await this.redis.del(lockKey);
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      return true;
    } catch (error) {
      console.error(`Redis lock release failed: ${error.message}`);
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
        console.log("Redis circuit breaker auto-reset");
        this.resetCircuitBreaker();
      }, 30000);
      
      console.warn(`Redis circuit breaker tripped. Will auto-reset in 30 seconds`);
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
