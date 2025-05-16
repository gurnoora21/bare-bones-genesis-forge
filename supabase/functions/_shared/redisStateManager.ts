/**
 * Redis State Manager
 * Implements the Redis side of our dual-system approach
 */

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { ProcessingState, LockOptions, getEnvironmentTTL, HeartbeatDetails, generateCorrelationId } from "./stateManager.ts";

// Circuit breaker configuration
interface CircuitBreakerConfig {
  failureThreshold: number;
  resetTimeoutMs: number;
  healthCheckIntervalMs: number;
  maxConsecutiveFailures: number;
  // New: Track current backoff duration
  currentBackoffMs: number;
  maxBackoffMs: number;
  initialBackoffMs: number;
}

export class RedisStateManager {
  private redis: Redis;
  // Enhanced circuit breaker with more detailed state
  private circuitBreakerState = {
    failures: 0,
    consecutiveFailures: 0,
    lastFailure: 0,
    isOpen: false,
    lastHealthCheck: 0,
    healthChecksPassed: 0
  };
  // Default circuit breaker configuration with exponential backoff
  private circuitBreakerConfig: CircuitBreakerConfig = {
    failureThreshold: 5,
    resetTimeoutMs: 30000,
    healthCheckIntervalMs: 5000,
    maxConsecutiveFailures: 3,
    // New: Initial 60s timeout, max 15min timeout
    initialBackoffMs: 60000,
    currentBackoffMs: 60000,
    maxBackoffMs: 900000 // 15 minutes
  };
  // Map to track active heartbeat intervals
  private activeHeartbeats = new Map<string, number>();
  
  constructor(redis: Redis, circuitBreakerConfig?: Partial<CircuitBreakerConfig>) {
    this.redis = redis;
    // Apply custom circuit breaker config if provided
    if (circuitBreakerConfig) {
      this.circuitBreakerConfig = {
        ...this.circuitBreakerConfig,
        ...circuitBreakerConfig
      };
    }
    
    // Auto-check health periodically
    this.setupPeriodicHealthCheck();
  }
  
  /**
   * Access to Redis instance
   */
  get redisClient(): Redis {
    return this.redis;
  }
  
  /**
   * Set up periodic health check to auto-recover circuit breaker
   */
  private setupPeriodicHealthCheck(): void {
    // Check health every minute
    setInterval(async () => {
      if (this.circuitBreakerState.isOpen) {
        const healthy = await this.performHealthCheck();
        if (healthy) {
          this.circuitBreakerState.healthChecksPassed++;
          if (this.circuitBreakerState.healthChecksPassed >= 2) {
            console.log("Redis health check passed, auto-resetting circuit breaker");
            this.resetCircuitBreaker();
          }
        }
      }
    }, 60000); // Check every minute
  }
  
  /**
   * Attempts to acquire a processing lock in Redis with heartbeat support
   */
  async acquireProcessingLock(
    entityType: string,
    entityId: string,
    options: LockOptions = {}
  ): Promise<boolean> {
    const {
      timeoutMinutes = 30,
      correlationId = generateCorrelationId('lock'),
      heartbeatIntervalSeconds = 15 // Default heartbeat interval
    } = options;
    
    // Check if circuit breaker is open
    if (this.isCircuitOpen()) {
      console.log(`[Redis] Circuit breaker open, skipping lock acquisition for ${entityType}:${entityId}`);
      return false;
    }
    
    const lockKey = `lock:${entityType}:${entityId}`;
    const stateKey = `state:${entityType}:${entityId}`;
    const ttlSeconds = timeoutMinutes * 60;
    const workerId = Deno.env.get("WORKER_ID") || `worker_${Math.random().toString(36).substring(2, 10)}`;
    
    try {
      // Try to set the lock key with NX (only if it doesn't exist)
      const lockData = {
        acquiredAt: new Date().toISOString(),
        correlationId,
        workerId,
        heartbeatEnabled: true
      };
      
      const result = await this.redis.set(lockKey, JSON.stringify(lockData), {
        nx: true,
        ex: ttlSeconds
      });
      
      if (result === "OK") {
        // Also store state for consistency
        await this.redis.set(stateKey, JSON.stringify({
          state: ProcessingState.IN_PROGRESS,
          timestamp: new Date().toISOString(),
          correlationId,
          workerId
        }), {
          ex: getEnvironmentTTL()
        });
        
        // Start heartbeat mechanism if enabled
        if (heartbeatIntervalSeconds > 0) {
          this.startHeartbeat(entityType, entityId, workerId, correlationId, heartbeatIntervalSeconds, ttlSeconds);
        }
        
        // Reset circuit breaker on success
        this.resetCircuitBreaker();
        
        return true;
      }
      
      // FIX: Improved stale lock detection and recovery
      // Check if we can "steal" a stale lock by checking its heartbeat
      const existingLockData = await this.redis.get(lockKey);
      if (existingLockData) {
        try {
          const parsedLock = JSON.parse(existingLockData as string);
          const acquiredAt = new Date(parsedLock.acquiredAt);
          const staleCutoff = new Date(Date.now() - (timeoutMinutes * 60 * 1000) / 2); // Reduce stale threshold by half
          
          // If the lock is stale (older than timeout with no heartbeat)
          if (acquiredAt < staleCutoff) {
            console.log(`[Redis] Stealing stale lock for ${entityType}:${entityId} from ${parsedLock.workerId || 'unknown'}`);
            
            // Update the lock with our data
            const newLockData = {
              acquiredAt: new Date().toISOString(),
              correlationId,
              workerId,
              heartbeatEnabled: true,
              stolenAt: new Date().toISOString(),
              stolenFrom: parsedLock.workerId || 'unknown'
            };
            
            await this.redis.set(lockKey, JSON.stringify(newLockData), {
              xx: true, // Only set if key exists
              ex: ttlSeconds
            });
            
            // Also update state for consistency
            await this.redis.set(stateKey, JSON.stringify({
              state: ProcessingState.IN_PROGRESS,
              timestamp: new Date().toISOString(),
              correlationId,
              workerId,
              lockStolen: true
            }), {
              ex: getEnvironmentTTL()
            });
            
            // Start heartbeat mechanism
            if (heartbeatIntervalSeconds > 0) {
              this.startHeartbeat(entityType, entityId, workerId, correlationId, heartbeatIntervalSeconds, ttlSeconds);
            }
            
            return true;
          }
        } catch (parseError) {
          console.warn(`Failed to parse existing lock data: ${parseError.message}`);
        }
      }
      
      return false;
    } catch (error) {
      console.error(`Error acquiring Redis lock: ${error.message}`);
      this.incrementCircuitFailure();
      return false;
    }
  }
  
  /**
   * Extend the TTL of an existing lock
   * Used for heartbeat functionality
   */
  async extendLock(
    entityType: string,
    entityId: string,
    ttlSeconds: number = 60
  ): Promise<boolean> {
    // Check if circuit breaker is open
    if (this.isCircuitOpen()) {
      return false;
    }
    
    try {
      const lockKey = `lock:${entityType}:${entityId}`;
      const heartbeatKey = `heartbeat:${entityType}:${entityId}`;
      
      // Check if the lock exists first
      const exists = await this.redis.exists(lockKey);
      if (exists !== 1) {
        return false;
      }
      
      // Extend the TTL of the existing lock
      const extended = await this.redis.expire(lockKey, ttlSeconds);
      
      // Also extend the heartbeat key if it exists
      await this.redis.expire(heartbeatKey, ttlSeconds);
      
      // Update the lastHeartbeat timestamp in the lock data
      try {
        const lockData = await this.redis.get(lockKey);
        if (lockData) {
          const parsedData = JSON.parse(lockData as string);
          parsedData.lastHeartbeat = new Date().toISOString();
          await this.redis.set(lockKey, JSON.stringify(parsedData), { xx: true, ex: ttlSeconds });
        }
      } catch (parseErr) {
        console.warn(`Failed to update lock heartbeat timestamp: ${parseErr.message}`);
      }
      
      // Reset circuit breaker on success
      this.resetCircuitBreaker();
      
      return extended === 1;
    } catch (error) {
      console.error(`Error extending Redis lock TTL: ${error.message}`);
      this.incrementCircuitFailure();
      return false;
    }
  }
  
  /**
   * Force release a lock that might be stale
   * Used by cleanup procedures
   */
  async forceReleaseLock(
    entityType: string,
    entityId: string
  ): Promise<boolean> {
    try {
      const lockKey = `lock:${entityType}:${entityId}`;
      const heartbeatKey = `heartbeat:${entityType}:${entityId}`;
      
      // Delete the lock keys
      await this.redis.del(lockKey);
      await this.redis.del(heartbeatKey);
      
      // Also update the state to indicate no longer in progress
      const stateKey = `state:${entityType}:${entityId}`;
      await this.redis.set(stateKey, JSON.stringify({
        state: ProcessingState.PENDING,
        timestamp: new Date().toISOString(),
        metadata: {
          forceReleased: true,
          releasedAt: new Date().toISOString()
        }
      }), { ex: getEnvironmentTTL() });
      
      return true;
    } catch (error) {
      console.error(`Error force releasing Redis lock: ${error.message}`);
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
      const workerId = Deno.env.get("WORKER_ID") || `worker_${Math.random().toString(36).substring(2, 10)}`;
      
      // Set state in Redis
      await this.redis.set(stateKey, JSON.stringify({
        state,
        timestamp: new Date().toISOString(),
        errorMessage,
        workerId,
        metadata: {
          ...metadata,
          updatedAt: new Date().toISOString()
        }
      }), {
        ex: getEnvironmentTTL()
      });
      
      // If completed or failed, remove the lock and stop heartbeat
      if (state === ProcessingState.COMPLETED || state === ProcessingState.FAILED || state === ProcessingState.DEAD_LETTER) {
        await this.redis.del(lockKey);
        this.stopHeartbeat(entityType, entityId);
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
      
      // Stop heartbeat for this entity
      this.stopHeartbeat(entityType, entityId);
      
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
  
  /**
   * Get active heartbeat details
   */
  async getHeartbeats(): Promise<HeartbeatDetails[]> {
    // Check if circuit breaker is open
    if (this.isCircuitOpen()) {
      return [];
    }
    
    try {
      const heartbeats: HeartbeatDetails[] = [];
      const keys = await this.redis.keys("heartbeat:*");
      
      for (const key of keys) {
        try {
          const data = await this.redis.get(key);
          if (data) {
            const [, entityType, entityId] = key.split(":");
            const heartbeatData = JSON.parse(data as string);
            
            heartbeats.push({
              entityType,
              entityId,
              workerId: heartbeatData.workerId,
              correlationId: heartbeatData.correlationId,
              lastHeartbeat: new Date(heartbeatData.timestamp),
              lockAcquiredAt: new Date(heartbeatData.acquiredAt || heartbeatData.timestamp)
            });
          }
        } catch (error) {
          console.warn(`Failed to parse heartbeat data for ${key}: ${error.message}`);
        }
      }
      
      return heartbeats;
    } catch (error) {
      console.error(`Error getting heartbeats: ${error.message}`);
      return [];
    }
  }
  
  /**
   * Perform a health check on Redis
   */
  async performHealthCheck(): Promise<boolean> {
    try {
      // Simple ping-pong check
      const result = await this.redis.ping();
      return result === "PONG";
    } catch (error) {
      console.error(`Redis health check failed: ${error.message}`);
      return false;
    }
  }
  
  /**
   * Force reset the circuit breaker
   */
  forceResetCircuitBreaker(): void {
    this.resetCircuitBreaker();
    console.log("Redis circuit breaker manually reset");
  }
  
  /**
   * Enhanced circuit breaker implementation with exponential backoff
   */
  private incrementCircuitFailure(): void {
    const now = Date.now();
    const config = this.circuitBreakerConfig;
    
    // Reset counter if last failure was more than reset timeout ago
    if (now - this.circuitBreakerState.lastFailure > config.resetTimeoutMs) {
      this.circuitBreakerState.failures = 1;
      this.circuitBreakerState.consecutiveFailures = 1;
    } else {
      this.circuitBreakerState.failures++;
      this.circuitBreakerState.consecutiveFailures++;
    }
    
    this.circuitBreakerState.lastFailure = now;
    
    // Trip circuit breaker on consecutive failures or total failures threshold
    const shouldTrip = 
      this.circuitBreakerState.consecutiveFailures >= config.maxConsecutiveFailures ||
      this.circuitBreakerState.failures >= config.failureThreshold;
    
    if (shouldTrip && !this.circuitBreakerState.isOpen) {
      this.circuitBreakerState.isOpen = true;
      
      // Calculate backoff timeout (doubles each time the circuit trips again recently)
      if (now - this.circuitBreakerState.lastFailure < config.resetTimeoutMs * 5) {
        // Double the backoff if we're retripping quickly
        config.currentBackoffMs = Math.min(
          config.maxBackoffMs,
          config.currentBackoffMs * 2
        );
      } else {
        // Reset to initial backoff if it's been a while since last trip
        config.currentBackoffMs = config.initialBackoffMs;
      }
      
      // Log circuit breaker state change with timeout
      console.warn(
        `Redis circuit breaker OPEN at ${new Date().toISOString()} ` +
        `for ${config.currentBackoffMs / 1000}s after ` +
        `${this.circuitBreakerState.consecutiveFailures} consecutive failures`
      );
      
      // Log to worker_issues table if available
      try {
        this.logCircuitEvent('OPEN', config.currentBackoffMs / 1000);
      } catch (logErr) {
        // Ignore logging errors
      }
      
      // Schedule health checks
      this.scheduleHealthCheck();
    }
  }
  
  /**
   * Log circuit breaker events to worker_issues
   */
  private async logCircuitEvent(state: string, timeoutSeconds: number): Promise<void> {
    // This method would call the worker_issues table logging if available
    // Implementation depends on how worker_issues logging is set up
    console.log(
      `CircuitBreaker ${state} at ${new Date().toISOString()} for ${timeoutSeconds}s`
    );
  }
  
  /**
   * Schedule Redis health checks when circuit is open
   */
  private scheduleHealthCheck(): void {
    if (!this.circuitBreakerState.isOpen) return;
    
    const now = Date.now();
    
    // Only schedule if we haven't done a health check recently
    if (now - this.circuitBreakerState.lastHealthCheck < this.circuitBreakerConfig.healthCheckIntervalMs) {
      return;
    }
    
    this.circuitBreakerState.lastHealthCheck = now;
    
    // Schedule health check
    setTimeout(async () => {
      if (this.circuitBreakerState.isOpen) {
        const healthy = await this.performHealthCheck();
        
        if (healthy) {
          this.circuitBreakerState.healthChecksPassed++;
          
          // Reset circuit breaker after several successful health checks
          if (this.circuitBreakerState.healthChecksPassed >= 3) {
            console.log(`Redis circuit breaker reset after ${this.circuitBreakerState.healthChecksPassed} successful health checks`);
            this.resetCircuitBreaker();
            
            // Log state change
            this.logCircuitEvent('CLOSED', 0);
          } else {
            // Schedule another health check
            this.scheduleHealthCheck();
          }
        } else {
          // Failed health check - reset counter and schedule another
          this.circuitBreakerState.healthChecksPassed = 0;
          this.scheduleHealthCheck();
        }
      }
    }, this.circuitBreakerConfig.healthCheckIntervalMs);
  }
  
  private isCircuitOpen(): boolean {
    return this.circuitBreakerState.isOpen;
  }
  
  private resetCircuitBreaker(): void {
    // Log state change if was previously open
    if (this.circuitBreakerState.isOpen) {
      this.logCircuitEvent('CLOSED', 0);
    }
    
    this.circuitBreakerState.failures = 0;
    this.circuitBreakerState.consecutiveFailures = 0;
    this.circuitBreakerState.isOpen = false;
    this.circuitBreakerState.healthChecksPassed = 0;
    
    // Note: We don't reset currentBackoffMs here, so it maintains its value
    // for the next time the circuit trips (exponential backoff strategy)
  }
}
