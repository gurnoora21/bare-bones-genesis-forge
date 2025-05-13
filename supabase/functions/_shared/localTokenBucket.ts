
/**
 * Local token bucket implementation with periodic Redis sync
 * Serves as a memory-based rate limiter with Redis coordination
 * for distributed environments
 */

import { getRedis } from "./upstashRedis.ts";

export interface TokenBucketOptions {
  tokensPerInterval: number;
  interval: number; // in seconds
  redisKey?: string;
  syncInterval?: number; // in milliseconds
}

export class LocalTokenBucket {
  private tokens: number;
  private tokensPerInterval: number;
  private interval: number; // in seconds
  private lastRefill: number; // in milliseconds
  private redisKey: string | null;
  private redis: ReturnType<typeof getRedis> | null;
  private lastSync: number;
  private syncIntervalMs: number;
  
  constructor(options: TokenBucketOptions) {
    this.tokens = options.tokensPerInterval;
    this.tokensPerInterval = options.tokensPerInterval;
    this.interval = options.interval;
    this.lastRefill = Date.now();
    this.redisKey = options.redisKey || null;
    this.redis = this.redisKey ? getRedis() : null;
    this.lastSync = Date.now();
    this.syncIntervalMs = options.syncInterval || 30000; // Default: sync every 30 seconds
  }
  
  /**
   * Try to consume tokens from the bucket
   * Returns true if tokens were consumed, false otherwise
   */
  async consume(tokensToConsume = 1): Promise<boolean> {
    this.refill();
    
    // If we need to sync with Redis
    if (this.redis && this.redisKey && 
        (Date.now() - this.lastSync > this.syncIntervalMs)) {
      await this.syncWithRedis();
    }
    
    if (this.tokens >= tokensToConsume) {
      this.tokens -= tokensToConsume;
      return true;
    }
    
    return false;
  }
  
  /**
   * Force sync with Redis to ensure global rate limiting
   * across multiple workers/instances
   */
  async syncWithRedis(): Promise<void> {
    if (!this.redis || !this.redisKey) return;
    
    try {
      // Get the current state from Redis
      const now = Math.floor(Date.now() / 1000);
      const result = await this.redis.pipelineExec([
        ["HMGET", this.redisKey, "tokens", "last_refill"],
        ["EXPIRE", this.redisKey, String(this.interval * 2)]
      ]);
      
      if (result && result[0] && Array.isArray(result[0])) {
        const [tokensStr, lastRefillStr] = result[0] as [string | null, string | null];
        
        // If Redis has state, use it
        if (tokensStr !== null && lastRefillStr !== null) {
          const redisTokens = parseFloat(tokensStr);
          const redisLastRefill = parseInt(lastRefillStr);
          
          // Use the most restrictive state (minimum tokens)
          // First refill both according to their respective times
          const localTokensAfterRefill = this.calculateRefill();
          const redisTokensAfterRefill = this.calculateRefill(redisTokens, redisLastRefill * 1000);
          
          // Take the minimum tokens value
          this.tokens = Math.min(localTokensAfterRefill, redisTokensAfterRefill);
          
          // Update last refill time to now
          this.lastRefill = Date.now();
          
          // Update Redis with our current state
          await this.redis.pipelineExec([
            ["HMSET", this.redisKey, "tokens", String(this.tokens), "last_refill", String(Math.floor(this.lastRefill / 1000))]
          ]);
        } else {
          // Initialize Redis with our current state
          await this.redis.pipelineExec([
            ["HMSET", this.redisKey, "tokens", String(this.tokens), "last_refill", String(Math.floor(this.lastRefill / 1000))]
          ]);
        }
      }
      
      this.lastSync = Date.now();
    } catch (error) {
      console.error("Error syncing token bucket with Redis:", error);
      // On error, continue using local bucket
    }
  }
  
  /**
   * Refill tokens based on elapsed time
   */
  private refill(): void {
    const now = Date.now();
    const elapsedMs = now - this.lastRefill;
    
    if (elapsedMs > 0) {
      const newTokens = Math.floor((elapsedMs / 1000) / this.interval * this.tokensPerInterval);
      
      if (newTokens > 0) {
        this.tokens = Math.min(this.tokens + newTokens, this.tokensPerInterval);
        this.lastRefill = now;
      }
    }
  }
  
  /**
   * Calculate refill for arbitrary token count and time
   */
  private calculateRefill(tokenCount = this.tokens, lastRefillTime = this.lastRefill): number {
    const now = Date.now();
    const elapsedMs = now - lastRefillTime;
    
    if (elapsedMs <= 0) return tokenCount;
    
    const newTokens = Math.floor((elapsedMs / 1000) / this.interval * this.tokensPerInterval);
    return Math.min(tokenCount + newTokens, this.tokensPerInterval);
  }
  
  /**
   * Get current token count (for testing/debugging)
   */
  getTokens(): number {
    this.refill();
    return this.tokens;
  }
}
