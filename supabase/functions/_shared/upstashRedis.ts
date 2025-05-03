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
   * FIXED: Ensure ALL command elements are properly formatted for Upstash REST API
   */
  private async request(commands: string[][]): Promise<any> {
    try {
      if (!commands || !Array.isArray(commands) || commands.length === 0) {
        throw new Error("Invalid Redis commands: commands must be a non-empty array");
      }
      
      // Format commands for Upstash REST API compatibility
      const formattedCommands = commands.map(cmd => {
        if (!Array.isArray(cmd) || cmd.length === 0) {
          throw new Error("Invalid Redis command format: each command must be a non-empty array");
        }
        
        // Ensure all command elements are strings
        return cmd.map(item => {
          if (item === null || item === undefined) {
            return "";
          } else if (typeof item === 'object') {
            try {
              return JSON.stringify(item);
            } catch (e) {
              console.error("Failed to stringify object:", e);
              return "";
            }
          } else {
            return String(item);
          }
        });
      });
      
      // Log commands for debugging (but keep them short)
      const debugCommands = JSON.stringify(formattedCommands).substring(0, 500) + "...";
      console.debug("Sending Redis commands:", debugCommands);
      
      const response = await fetch(this.url, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${this.token}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify(formattedCommands)
      });
      
      if (!response.ok) {
        const errorText = await response.text();
        console.error("Redis error response:", errorText);
        throw new Error(`Upstash Redis error: ${response.status} ${errorText}`);
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
   * Execute a safe version of a command with fallbacks for error conditions
   */
  private async safeCommand(command: string, ...args: any[]): Promise<any> {
    try {
      const stringArgs = args.map(arg => {
        if (arg === null || arg === undefined) {
          return "";
        } else if (typeof arg === 'object') {
          try {
            return JSON.stringify(arg);
          } catch (e) {
            return String(arg);
          }
        } else {
          return String(arg);
        }
      });
      
      return await this.request([[command, ...stringArgs]]);
    } catch (error) {
      console.warn(`Redis ${command} command failed:`, error);
      return null;
    }
  }
  
  /**
   * Basic Redis operations - all using new safe command approach
   */
  
  async get(key: string): Promise<any> {
    try {
      // Try memory cache first
      const cachedValue = this.memoryCache.get(key);
      if (cachedValue !== null && cachedValue !== undefined) {
        return cachedValue;
      }
      
      // Cache miss, get from Redis
      const result = await this.safeCommand("GET", key);
      
      if (result) {
        try {
          // Try to parse as JSON first
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
    } catch (error) {
      console.error(`Error getting key ${key} from Redis:`, error);
      // Return null on error to avoid breaking the application
      return null;
    }
  }
  
  async set(key: string, value: any, expireSeconds?: number): Promise<string> {
    try {
      // Update memory cache
      this.memoryCache.set(key, value, expireSeconds);
      
      // Format the value properly
      let stringValue: string;
      try {
        stringValue = typeof value === 'object' ? JSON.stringify(value) : String(value);
      } catch (e) {
        console.error(`Error stringifying value for key ${key}:`, e);
        stringValue = String(value);
      }
      
      // Use simplified command with proper string formatting
      if (expireSeconds !== undefined) {
        return await this.safeCommand("SET", key, stringValue, "EX", String(expireSeconds));
      } else {
        return await this.safeCommand("SET", key, stringValue);
      }
    } catch (error) {
      console.error(`Error setting key ${key} in Redis:`, error);
      return "ERROR";
    }
  }
  
  async del(key: string): Promise<number> {
    try {
      // Remove from memory cache
      this.memoryCache.delete(key);
      
      return await this.safeCommand("DEL", key);
    } catch (error) {
      console.error(`Error deleting key ${key} from Redis:`, error);
      return 0;
    }
  }
  
  async incr(key: string): Promise<number> {
    try {
      // Can't effectively cache this operation
      return await this.safeCommand("INCR", key);
    } catch (error) {
      console.error(`Error incrementing key ${key} in Redis:`, error);
      return 0;
    }
  }
  
  async decr(key: string): Promise<number> {
    try {
      // Can't effectively cache this operation
      return await this.safeCommand("DECR", key);
    } catch (error) {
      console.error(`Error decrementing key ${key} in Redis:`, error);
      return 0;
    }
  }
  
  async expire(key: string, seconds: number): Promise<number> {
    try {
      return await this.safeCommand("EXPIRE", key, seconds);
    } catch (error) {
      console.error(`Error setting expiry for key ${key} in Redis:`, error);
      return 0;
    }
  }
  
  // Token bucket operations - simplified to use pipelining
  
  async evalTokenBucket(
    key: string, 
    tokensPerInterval: number, 
    interval: number, 
    tokensToConsume: number
  ): Promise<boolean> {
    const now = Math.floor(Date.now() / 1000);
    
    try {
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
      const results = await this.pipelineExec([
        ["HMGET", key, "tokens", "last_refill"],
        ["EXPIRE", key, String(interval * 2)]
      ]);
      
      const bucketData = results[0];
      let tokens = bucketData && bucketData[0] ? parseInt(bucketData[0]) : tokensPerInterval;
      let lastRefill = bucketData && bucketData[1] ? parseInt(bucketData[1]) : now;
      
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
        
        await this.pipelineExec([
          ["HMSET", key, "tokens", String(newTokens), "last_refill", String(lastRefill)],
          ["EXPIRE", key, String(interval * 2)]
        ]);
        
        return true;
      }
      
      return false;
    } catch (error) {
      console.error("Error in token bucket evaluation:", error);
      // Default to allowing the request in case of Redis errors
      // This prevents the application from stopping entirely
      return true;
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
        ["HMSET", key, "tokens", String(tokens), "last_refill", String(lastRefill)],
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
          const parts = key.split(':');
          const redisKey = parts[0] === 'error' 
            ? `errors:${api}:${parts[1]}`
            : parts.length === 2
              ? `stats:${api}:${parts[0]}`
              : `stats:${api}:${parts[0]}:${parts[1]}`;
              
          const field = parts[0] === 'error'
            ? parts.slice(2).join(':')
            : parts.length === 2
              ? parts[1]
              : parts[2];
          
          pipeline.push(["HINCRBY", redisKey, field, String(count)]);
          pipeline.push(["EXPIRE", redisKey, String(60 * 60 * 24 * 30)]);
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
    try {
      const cacheKey = `cache:${key}`;
      
      // Set in memory cache
      this.memoryCache.set(cacheKey, value, ttlSeconds);
      
      // Set in Redis, but use a safer approach for large values
      let stringValue: string;
      try {
        stringValue = typeof value === 'object' ? JSON.stringify(value) : String(value);
      } catch (e) {
        console.error(`Error stringifying cache value for ${key}:`, e);
        // Store a simplified version if full serialization fails
        stringValue = JSON.stringify({
          error: "Failed to serialize full value",
          type: typeof value,
          timestamp: Date.now()
        });
      }
      
      // For very large values, store a truncated version to avoid Redis errors
      if (stringValue.length > 5000000) { // 5MB limit
        console.warn(`Cache value for ${key} exceeds 5MB, truncating`);
        stringValue = JSON.stringify({
          error: "Value exceeded 5MB limit",
          originalType: typeof value,
          timestamp: Date.now()
        });
      }
      
      return this.set(cacheKey, stringValue, ttlSeconds);
    } catch (error) {
      console.error(`Error setting cache for key ${key}:`, error);
      return "";
    }
  }
  
  async cacheGet(key: string): Promise<any> {
    try {
      const cacheKey = `cache:${key}`;
      
      // Try memory cache first
      const cachedValue = this.memoryCache.get(cacheKey);
      if (cachedValue !== null && cachedValue !== undefined) {
        return cachedValue;
      }
      
      // Cache miss, get from Redis
      const redisValue = await this.get(cacheKey);
      
      // Update memory cache if we got a value
      if (redisValue !== null && redisValue !== undefined) {
        this.memoryCache.set(cacheKey, redisValue, 300); // 5 minute default TTL
      }
      
      return redisValue;
    } catch (error) {
      console.error(`Error getting cache for key ${key}:`, error);
      return null;
    }
  }
  
  // Force flush stats to Redis (useful before shutdown)
  async forceFlushStats(): Promise<void> {
    return this.flushStats();
  }

  // Add a new method to check if Redis is available
  async ping(): Promise<boolean> {
    try {
      const result = await this.safeCommand("PING");
      return result === "PONG";
    } catch (error) {
      console.error("Redis PING failed:", error);
      return false;
    }
  }
  
  // Add pipeline execution helper that was missing
  async pipelineExec(pipeline: string[][]): Promise<any[]> {
    try {
      // Format the pipeline commands to ensure all values are strings
      const formattedPipeline = pipeline.map(cmd => {
        return cmd.map(arg => {
          if (arg === null || arg === undefined) {
            return "";
          } else if (typeof arg === 'object') {
            try {
              return JSON.stringify(arg);
            } catch (e) {
              return String(arg);
            }
          } else {
            return String(arg);
          }
        });
      });
      
      return await this.request(formattedPipeline);
    } catch (error) {
      console.error("Redis pipeline execution failed:", error);
      // Return empty results on error to avoid breaking the application
      return Array(pipeline.length).fill(null);
    }
  }
  
  // Helper for compressing long strings in logs and debugging
  private compressString(str: string): string {
    return str.length > 1000 ? str.substring(0, 1000) + "..." : str;
  }
}

// Export a singleton instance with error handling
let redisInstance: UpstashRedis | null = null;

export function getRedis(): UpstashRedis | null {
  if (!redisInstance) {
    try {
      redisInstance = new UpstashRedis();
    } catch (error) {
      console.error("Failed to initialize Redis client:", error);
      return null;
    }
  }
  return redisInstance;
}

// Export a memory-only fallback for when Redis is unavailable
export class MemoryOnlyCache {
  private cache: Map<string, any>;
  
  constructor() {
    this.cache = new Map();
  }
  
  async get(key: string): Promise<any> {
    return this.cache.get(key) || null;
  }
  
  async set(key: string, value: any): Promise<string> {
    this.cache.set(key, value);
    return "OK";
  }
  
  async delete(key: string): Promise<number> {
    return this.cache.delete(key) ? 1 : 0;
  }
}
