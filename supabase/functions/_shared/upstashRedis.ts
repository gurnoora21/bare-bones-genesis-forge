
/**
 * Upstash Redis HTTP client for Deno/Edge Functions
 * Implements key Redis operations needed for rate limiting and caching
 * Optimized with pipelining and reduced commands
 */

import { MemoryCache } from "./memoryCache.ts";

export class UpstashRedis {
  private url: string;
  private token: string;
  private memoryCache: MemoryCache<any>;
  private statsBuffer: Map<string, Map<string, number>>;
  private lastStatsFlush: number;
  private statsFlushInterval: number;
  
  constructor() {
    this.url = Deno.env.get("UPSTASH_REDIS_REST_URL") || "";
    this.token = Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "";
    
    if (!this.url || !this.token) {
      throw new Error("Upstash Redis credentials must be provided as environment variables");
    }
    
    // Initialize memory cache (up to 5000 items, sync every 2 minutes)
    this.memoryCache = new MemoryCache(5000, 120000);
    
    // Initialize stats buffer for batched writes
    this.statsBuffer = new Map();
    this.lastStatsFlush = Date.now();
    this.statsFlushInterval = 60000; // Flush stats to Redis every 60 seconds
  }
  
  /**
   * Make an authenticated request to the Upstash Redis REST API
   * Fixed to ensure commands are properly formatted for Upstash REST API
   */
  private async request(commands: string[][]): Promise<any> {
    try {
      // Ensure each command element is a string (Upstash REST API requirement)
      const formattedCommands = commands.map(cmd => 
        cmd.map(item => typeof item === 'number' ? String(item) : item)
      );
      
      const response = await fetch(this.url, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${this.token}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify(formattedCommands)
      });
      
      if (!response.ok) {
        throw new Error(`Upstash Redis error: ${response.status} ${await response.text()}`);
      }
      
      const data = await response.json();
      
      // Handle single command or pipeline response
      if (Array.isArray(data)) {
        // Pipeline response
        for (const result of data) {
          if (result.error) {
            console.warn(`Redis pipeline error: ${result.error}`);
          }
        }
        return data.map(item => item.result);
      } else {
        // Single command response
        if (data.error) {
          throw new Error(`Redis error: ${data.error}`);
        }
        return data.result;
      }
    } catch (error) {
      console.error("Upstash Redis request failed:", error);
      throw error;
    }
  }
  
  /**
   * Execute a pipeline of commands in a single request
   */
  async pipelineExec(commands: string[][]): Promise<any[]> {
    return await this.request(commands);
  }
  
  // Basic Redis operations - all optimized to use memory cache when appropriate
  
  async get(key: string): Promise<any> {
    // Try memory cache first
    const cachedValue = this.memoryCache.get(key);
    if (cachedValue !== null && cachedValue !== undefined) {
      return cachedValue;
    }
    
    // Cache miss, get from Redis
    const result = await this.request([["GET", key]]);
    
    if (result) {
      try {
        const parsedValue = JSON.parse(result);
        // Store in memory cache with default TTL of 5 minutes
        this.memoryCache.set(key, parsedValue, 300);
        return parsedValue;
      } catch {
        // If not valid JSON, return as is
        this.memoryCache.set(key, result, 300);
        return result;
      }
    }
    
    return null;
  }
  
  async set(key: string, value: any, expireSeconds?: number): Promise<string> {
    // Update memory cache
    this.memoryCache.set(key, value, expireSeconds);
    
    // Format the command based on expiration
    const command = expireSeconds 
      ? [["SET", key, JSON.stringify(value), "EX", String(expireSeconds)]] 
      : [["SET", key, JSON.stringify(value)]];
    
    return await this.request(command);
  }
  
  async del(key: string): Promise<number> {
    // Remove from memory cache
    this.memoryCache.delete(key);
    
    return await this.request([["DEL", key]]);
  }
  
  async incr(key: string): Promise<number> {
    // Can't effectively cache this operation
    return await this.request([["INCR", key]]);
  }
  
  async decr(key: string): Promise<number> {
    // Can't effectively cache this operation
    return await this.request([["DECR", key]]);
  }
  
  async expire(key: string, seconds: number): Promise<number> {
    return await this.request([["EXPIRE", key, String(seconds)]]);
  }
  
  // Token bucket operations - simplified to use pipelining
  
  async evalTokenBucket(
    key: string, 
    tokensPerInterval: number, 
    interval: number, 
    tokensToConsume: number
  ): Promise<boolean> {
    const now = Math.floor(Date.now() / 1000);
    
    // Check if we already have the bucket state in memory cache
    const cacheKey = `token_bucket:${key}`;
    const cachedBucket = this.memoryCache.get(cacheKey);
    
    if (cachedBucket) {
      const { tokens, lastRefill } = cachedBucket;
      
      // Calculate elapsed time and refill tokens
      const elapsed = now - lastRefill;
      let currentTokens = tokens;
      
      if (elapsed > 0) {
        const newTokens = Math.floor((elapsed / interval) * tokensPerInterval);
        if (newTokens > 0) {
          currentTokens = Math.min(tokens + newTokens, tokensPerInterval);
        }
      }
      
      // Check if we can consume tokens
      if (currentTokens >= tokensToConsume) {
        // Update memory cache with new state
        this.memoryCache.set(cacheKey, {
          tokens: currentTokens - tokensToConsume,
          lastRefill: now
        }, interval * 2);
        
        // If it's time to sync with Redis, do so in the background
        if (this.memoryCache.shouldSync()) {
          // Don't await this to avoid blocking
          this.updateTokenBucketInRedis(key, currentTokens - tokensToConsume, now, interval * 2);
        }
        
        return true;
      }
      
      return false;
    }
    
    // Cache miss, get from Redis using pipeline
    try {
      const pipeline = [
        ["HMGET", key, "tokens", "last_refill"],
        ["EXPIRE", key, String(interval * 2)]
      ];
      
      const [bucketData] = await this.pipelineExec(pipeline);
      
      let tokens = bucketData[0] ? parseInt(bucketData[0]) : tokensPerInterval;
      let lastRefill = bucketData[1] ? parseInt(bucketData[1]) : now;
      
      // Refill tokens based on time elapsed
      const elapsed = now - lastRefill;
      if (elapsed > 0) {
        const newTokens = Math.floor((elapsed / interval) * tokensPerInterval);
        if (newTokens > 0) {
          tokens = Math.min(tokens + newTokens, tokensPerInterval);
          lastRefill = now;
        }
      }
      
      // Store in memory cache
      this.memoryCache.set(cacheKey, { tokens, lastRefill }, interval * 2);
      
      // Check if we can consume tokens
      if (tokens >= tokensToConsume) {
        // Update in Redis and memory cache
        const newTokens = tokens - tokensToConsume;
        this.memoryCache.set(cacheKey, { tokens: newTokens, lastRefill }, interval * 2);
        
        // Update Redis in background to avoid blocking
        this.updateTokenBucketInRedis(key, newTokens, lastRefill, interval * 2);
        
        return true;
      }
      
      return false;
    } catch (error) {
      console.error("Error in token bucket evaluation:", error);
      return false;
    }
  }
  
  // Helper method to update token bucket in Redis asynchronously
  private async updateTokenBucketInRedis(
    key: string, 
    tokens: number, 
    lastRefill: number, 
    expireSeconds: number
  ): Promise<void> {
    try {
      await this.pipelineExec([
        ["HMSET", key, "tokens", tokens.toString(), "last_refill", lastRefill.toString()],
        ["EXPIRE", key, String(expireSeconds)]
      ]);
    } catch (error) {
      console.error("Failed to update token bucket in Redis:", error);
    }
  }
  
  // API usage tracking with buffering
  
  async trackApiCall(api: string, endpoint: string, success: boolean): Promise<void> {
    const now = new Date();
    const day = now.toISOString().split('T')[0];
    const hour = now.getHours();
    
    // Buffer the stats in memory
    if (!this.statsBuffer.has(api)) {
      this.statsBuffer.set(api, new Map());
    }
    
    const apiMap = this.statsBuffer.get(api)!;
    
    // Track daily stats
    const dailyKey = `${day}:${endpoint}`;
    apiMap.set(dailyKey, (apiMap.get(dailyKey) || 0) + 1);
    
    // Track hourly stats
    const hourlyKey = `${day}:${hour}:${endpoint}`;
    apiMap.set(hourlyKey, (apiMap.get(hourlyKey) || 0) + 1);
    
    // Track error stats if needed
    if (!success) {
      const errorKey = `error:${day}:${endpoint}`;
      apiMap.set(errorKey, (apiMap.get(errorKey) || 0) + 1);
    }
    
    // Flush stats if interval has passed
    if (Date.now() - this.lastStatsFlush > this.statsFlushInterval) {
      this.flushStats();
    }
  }
  
  // Flush buffered stats to Redis
  private async flushStats(): Promise<void> {
    if (this.statsBuffer.size === 0) {
      return;
    }
    
    try {
      const pipeline: string[][] = [];
      
      // Process each API's stats
      for (const [api, endpoints] of this.statsBuffer.entries()) {
        for (const [key, count] of endpoints.entries()) {
          const [type, day, ...rest] = key.split(':');
          
          if (type === 'error') {
            pipeline.push(["HINCRBY", `errors:${api}:${day}`, rest.join(':'), String(count)]);
            pipeline.push(["EXPIRE", `errors:${api}:${day}`, String(60 * 60 * 24 * 30)]);
          } else if (key.split(':').length === 2) {
            // Daily stats
            pipeline.push(["HINCRBY", `stats:${api}:${day}`, rest[0], String(count)]);
            pipeline.push(["EXPIRE", `stats:${api}:${day}`, String(60 * 60 * 24 * 30)]);
          } else {
            // Hourly stats
            pipeline.push(["HINCRBY", `stats:${api}:${day}:${rest[0]}`, rest[1], String(count)]);
            pipeline.push(["EXPIRE", `stats:${api}:${day}:${rest[0]}`, String(60 * 60 * 24 * 30)]);
          }
        }
      }
      
      // Execute pipeline if we have commands
      if (pipeline.length > 0) {
        await this.pipelineExec(pipeline);
      }
      
      // Clear buffer and update last flush time
      this.statsBuffer.clear();
      this.lastStatsFlush = Date.now();
    } catch (error) {
      console.error("Failed to flush stats to Redis:", error);
    }
  }
  
  // Result caching - optimized with memory cache
  
  async cacheSet(key: string, value: any, ttlSeconds: number): Promise<string> {
    const cacheKey = `cache:${key}`;
    
    // Set in memory cache
    this.memoryCache.set(cacheKey, value, ttlSeconds);
    
    // Set in Redis
    return this.set(cacheKey, value, ttlSeconds);
  }
  
  async cacheGet(key: string): Promise<any> {
    const cacheKey = `cache:${key}`;
    
    // Try memory cache first
    const cachedValue = this.memoryCache.get(cacheKey);
    if (cachedValue !== null && cachedValue !== undefined) {
      return cachedValue;
    }
    
    // Cache miss, get from Redis
    return this.get(cacheKey);
  }
  
  // Force flush stats to Redis (useful before shutdown)
  async forceFlushStats(): Promise<void> {
    return this.flushStats();
  }
}

// Export a singleton instance
let redisInstance: UpstashRedis | null = null;

export function getRedis(): UpstashRedis {
  if (!redisInstance) {
    redisInstance = new UpstashRedis();
  }
  return redisInstance;
}
