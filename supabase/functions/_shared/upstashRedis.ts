/**
 * Upstash Redis HTTP client for Deno/Edge Functions
 * Implements key Redis operations needed for rate limiting and caching
 * Optimized with pipelining and reduced commands
 */

import { MemoryCache } from "./memoryCache.ts";

// Size limits for Redis storage
const MAX_REDIS_KEY_LENGTH = 500;
const MAX_REDIS_VALUE_SIZE = 5 * 1024 * 1024; // 5MB limit
const CHUNKING_THRESHOLD = 1 * 1024 * 1024; // 1MB

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
   * with proper command formatting for the REST API
   */
  private async request(commands: string[][]): Promise<any> {
    try {
      if (!commands || !Array.isArray(commands) || commands.length === 0) {
        throw new Error("Invalid Redis commands: commands must be a non-empty array");
      }
      
      // IMPORTANT: Properly format commands for Upstash REST API compatibility
      const formattedCommands = commands.map(cmd => {
        if (!Array.isArray(cmd) || cmd.length === 0) {
          throw new Error("Invalid Redis command format: each command must be a non-empty array");
        }
        
        // Ensure all command elements are properly formatted strings
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
      const debugCommands = this.compressString(JSON.stringify(formattedCommands));
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
  
  // Enhanced result caching with large value support
  
  async cacheSet(key: string, value: any, ttlSeconds: number): Promise<string> {
    try {
      const cacheKey = `cache:${key}`;
      
      // Set in memory cache first (always works)
      this.memoryCache.set(cacheKey, value, ttlSeconds);
      
      // Serialize the value
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
      
      // For very large values, store a truncated version or use chunking
      if (stringValue.length > MAX_REDIS_VALUE_SIZE) {
        console.warn(`Cache value for ${key} exceeds ${MAX_REDIS_VALUE_SIZE} bytes, truncating`);
        
        // Create a simplified summary to store instead
        const summary = {
          error: "Value exceeded size limit",
          originalType: typeof value,
          sizeBytes: stringValue.length,
          timestamp: Date.now(),
          memoryOnlyCached: true
        };
        
        // Store the summary in Redis
        const shortenedKey = this.shortenKeyIfNeeded(cacheKey);
        await this.safeCommand("SET", shortenedKey, JSON.stringify(summary), "EX", String(ttlSeconds));
        
        return "OK"; // We still have the full value in memory cache
      }
      
      // For large but manageable values, use chunking
      if (stringValue.length > CHUNKING_THRESHOLD) {
        const shortenedKey = this.shortenKeyIfNeeded(cacheKey);
        const success = await this.setLargeValue(shortenedKey, stringValue, ttlSeconds);
        return success ? "OK" : "CHUNKING_ERROR";
      }
      
      // For normal sized values, use standard SET
      const shortenedKey = this.shortenKeyIfNeeded(cacheKey);
      try {
        return await this.safeCommand("SET", shortenedKey, stringValue, "EX", String(ttlSeconds));
      } catch (error) {
        console.error(`Redis SET failed for key ${shortenedKey}:`, error);
        return "OK"; // Return OK despite Redis failure - we still have memory cache
      }
    } catch (error) {
      console.error(`Error setting cache for key ${key}:`, error);
      return "OK"; // Return success to prevent pipeline failures
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
      
      // Cache miss, try retrieving from Redis
      const shortenedKey = this.shortenKeyIfNeeded(cacheKey);
      
      try {
        // First try normal GET
        const normalValue = await this.safeCommand("GET", shortenedKey);
        
        if (normalValue !== null) {
          // Check if this is just a size limit message
          try {
            const parsed = JSON.parse(normalValue);
            if (parsed && parsed.error === "Value exceeded size limit") {
              console.log(`Found size-limited placeholder for ${key}, returning cache miss`);
              return null;
            }
          } catch (e) {
            // Not JSON or not our size limit message, continue
          }
          
          // Try to parse as JSON
          try {
            const parsedValue = JSON.parse(normalValue);
            this.memoryCache.set(cacheKey, parsedValue, 300); // 5 min default TTL
            return parsedValue;
          } catch (e) {
            // Not valid JSON, return as string
            this.memoryCache.set(cacheKey, normalValue, 300);
            return normalValue;
          }
        }
        
        // If nothing found, try chunked retrieval
        const chunkedValue = await this.getLargeValue(shortenedKey);
        if (chunkedValue) {
          try {
            const parsedValue = JSON.parse(chunkedValue);
            this.memoryCache.set(cacheKey, parsedValue, 300);
            return parsedValue;
          } catch (e) {
            this.memoryCache.set(cacheKey, chunkedValue, 300);
            return chunkedValue;
          }
        }
        
        // Nothing found in either normal or chunked storage
        return null;
      } catch (error) {
        console.warn(`Redis GET failed for key ${shortenedKey}:`, error);
        return null; // Return null (cache miss) when Redis fails
      }
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

  // Helper to handle very long keys
  private shortenKeyIfNeeded(key: string): string {
    // Upstash Redis has a key length limit (typically 512 bytes)
    if (key.length > MAX_REDIS_KEY_LENGTH) {
      // Use a hash of the key as the actual Redis key
      const hash = this.hashString(key);
      const shortened = `hash:${hash}`;
      console.warn(`Key ${key} shortened to ${shortened}`);
      return shortened;
    }
    return key;
  }

  // Simple string hashing function
  private hashString(str: string): string {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32bit integer
    }
    return Math.abs(hash).toString(16); // Convert to hex
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

// Helper method to store a large value by chunking if necessary
private async setLargeValue(key: string, value: string, expireSeconds?: number): Promise<boolean> {
  try {
    // For small values, use a simple SET
    if (value.length < CHUNKING_THRESHOLD) {
      await this.safeCommand("SET", key, value, expireSeconds ? "EX" : "", expireSeconds ? String(expireSeconds) : "");
      return true;
    }
    
    // For large values, use chunking
    console.log(`Chunking large value for key ${key} (size: ${value.length} bytes)`);
    
    // Split into chunks of ~512KB
    const chunkSize = 512 * 1024;
    const chunks = [];
    for (let i = 0; i < value.length; i += chunkSize) {
      chunks.push(value.substring(i, i + chunkSize));
    }
    
    // Store metadata about chunks
    const metaKey = `${key}:meta`;
    const metadata = {
      chunks: chunks.length,
      totalSize: value.length,
      timestamp: Date.now()
    };
    
    // Create a pipeline to store all chunks
    const pipeline = [];
    
    // Delete any existing keys first
    pipeline.push(["DEL", key]);
    pipeline.push(["DEL", metaKey]);
    
    // Store metadata
    pipeline.push(["SET", metaKey, JSON.stringify(metadata)]);
    if (expireSeconds) {
      pipeline.push(["EXPIRE", metaKey, String(expireSeconds)]);
    }
    
    // Store each chunk
    for (let i = 0; i < chunks.length; i++) {
      const chunkKey = `${key}:chunk:${i}`;
      pipeline.push(["SET", chunkKey, chunks[i]]);
      if (expireSeconds) {
        pipeline.push(["EXPIRE", chunkKey, String(expireSeconds)]);
      }
    }
    
    // Execute pipeline
    await this.pipelineExec(pipeline);
    return true;
  } catch (error) {
    console.error(`Failed to store large value for key ${key}:`, error);
    return false;
  }
}

// Helper method to retrieve a value that was stored with chunking
private async getLargeValue(key: string): Promise<string | null> {
  try {
    // First try to get the value directly (might not be chunked)
    const directValue = await this.safeCommand("GET", key);
    if (directValue) {
      return directValue;
    }
    
    // Check for chunked metadata
    const metaKey = `${key}:meta`;
    const metaValue = await this.safeCommand("GET", metaKey);
    
    if (!metaValue) {
      return null; // No metadata, key doesn't exist
    }
    
    // Parse metadata
    const metadata = JSON.parse(metaValue);
    if (!metadata || !metadata.chunks) {
      return null; // Invalid metadata
    }
    
    // Fetch all chunks in parallel
    const chunkPromises = [];
    for (let i = 0; i < metadata.chunks; i++) {
      const chunkKey = `${key}:chunk:${i}`;
      chunkPromises.push(this.safeCommand("GET", chunkKey));
    }
    
    const chunks = await Promise.all(chunkPromises);
    
    // Validate that we got all chunks
    if (chunks.some(chunk => chunk === null)) {
      console.error(`Missing chunks for key ${key}`);
      return null;
    }
    
    // Combine all chunks
    return chunks.join("");
  } catch (error) {
    console.error(`Failed to retrieve large value for key ${key}:`, error);
    return null;
  }
}
