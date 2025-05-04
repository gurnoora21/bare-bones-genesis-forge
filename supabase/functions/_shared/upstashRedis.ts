
/**
 * Reliable Redis client for Supabase Edge Functions
 * With fallback mechanisms for production reliability
 */

import { MemoryCache } from "./memoryCache.ts";

// Size limits for Redis storage
const MAX_REDIS_KEY_LENGTH = 500;
const MAX_CHUNK_SIZE = 750 * 1024; // 750KB to safely stay under 1MB limit

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
    
    // Initialize with empty values - pipeline will still work without Redis
    if (!this.url || !this.token) {
      console.warn("Upstash Redis credentials missing - using memory-only mode");
    }
    
    // Initialize memory cache (up to 1000 items, sync every 2 minutes)
    this.memoryCache = new MemoryCache(1000, 120000);
    
    // Initialize stats buffer for batched writes
    this.statsBuffer = new Map();
    this.lastStatsFlush = Date.now();
    this.statsFlushInterval = 60000; // Flush stats to Redis every 60 seconds
  }
  
  /**
   * Make a fetch request to the Upstash Redis REST API
   * with proper formatting and error handling
   */
  private async makeRequest(commands: string[][]): Promise<any> {
    try {
      if (!this.url || !this.token) {
        throw new Error("Redis credentials not configured");
      }

      if (!commands || !Array.isArray(commands) || commands.length === 0) {
        throw new Error("Invalid Redis commands: commands must be a non-empty array");
      }
      
      // Format commands for Upstash REST API
      const formattedCommands = commands.map(cmd => {
        if (!Array.isArray(cmd) || cmd.length === 0) {
          throw new Error("Invalid Redis command format: each command must be a non-empty array");
        }
        
        // Ensure all elements are properly formatted strings
        return cmd.map(item => {
          if (item === null || item === undefined) {
            return "";
          } else if (typeof item === 'object') {
            try {
              return JSON.stringify(item);
            } catch (e) {
              return String(item);
            }
          } else {
            return String(item);
          }
        });
      });
      
      // Make the actual request
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
        throw new Error(`Upstash Redis error: ${response.status} ${errorText}`);
      }
      
      const data = await response.json();
      
      // Handle response format
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
   * Execute a Redis command with error handling
   */
  private async execCommand(command: string, ...args: any[]): Promise<any> {
    try {
      // Format all args as strings
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
      
      return await this.makeRequest([[command, ...stringArgs]]);
    } catch (error) {
      console.warn(`Redis ${command} command failed:`, error);
      return null;
    }
  }
  
  /**
   * Get a value from Redis
   */
  async get(key: string): Promise<any> {
    try {
      // Try memory cache first
      const cachedValue = this.memoryCache.get(key);
      if (cachedValue !== null && cachedValue !== undefined) {
        return cachedValue;
      }
      
      // Get from Redis
      const result = await this.execCommand("GET", key);
      
      if (result) {
        try {
          // Try to parse as JSON
          const parsedValue = JSON.parse(result);
          this.memoryCache.set(key, parsedValue, 300);
          return parsedValue;
        } catch {
          // Not valid JSON, return as is
          this.memoryCache.set(key, result, 300);
          return result;
        }
      }
      
      return null;
    } catch (error) {
      console.error(`Error getting key ${key} from Redis:`, error);
      return null;
    }
  }
  
  /**
   * Set a value in Redis
   */
  async set(key: string, value: any, expireSeconds?: number): Promise<string> {
    try {
      // Update memory cache
      this.memoryCache.set(key, value, expireSeconds);
      
      // Format value as string
      let stringValue: string;
      try {
        stringValue = typeof value === 'object' ? JSON.stringify(value) : String(value);
      } catch (e) {
        console.error(`Error stringifying value for key ${key}:`, e);
        stringValue = String(value);
      }
      
      // Set in Redis
      if (expireSeconds !== undefined) {
        return await this.execCommand("SET", key, stringValue, "EX", String(expireSeconds)) || "OK";
      } else {
        return await this.execCommand("SET", key, stringValue) || "OK";
      }
    } catch (error) {
      console.error(`Error setting key ${key} in Redis:`, error);
      return "OK"; // Return OK to avoid breaking the pipeline
    }
  }
  
  /**
   * Delete a key from Redis
   */
  async del(key: string): Promise<number> {
    try {
      // Remove from memory cache
      this.memoryCache.delete(key);
      
      // Delete from Redis
      return await this.execCommand("DEL", key) || 0;
    } catch (error) {
      console.error(`Error deleting key ${key} from Redis:`, error);
      return 0;
    }
  }
  
  /**
   * Enhanced cache get with chunking support
   */
  async cacheGet(key: string): Promise<any> {
    try {
      const cacheKey = `cache:${key}`;
      
      // Try memory cache first
      const cachedValue = this.memoryCache.get(cacheKey);
      if (cachedValue !== null && cachedValue !== undefined) {
        return cachedValue;
      }
      
      // Check if this is a chunked value
      const metaKey = `${cacheKey}:meta`;
      const metaValue = await this.execCommand("GET", metaKey);
      
      if (metaValue) {
        try {
          const metadata = JSON.parse(metaValue);
          if (metadata.chunked && metadata.chunks > 0) {
            // Retrieve all chunks
            let fullValue = '';
            for (let i = 0; i < metadata.chunks; i++) {
              const chunkKey = `${cacheKey}:chunk:${i}`;
              const chunk = await this.execCommand("GET", chunkKey);
              
              if (!chunk) {
                console.warn(`Missing chunk ${i} for key ${key}, cache is incomplete`);
                return null;
              }
              
              fullValue += chunk;
            }
            
            // Parse if it's JSON
            try {
              const parsed = JSON.parse(fullValue);
              this.memoryCache.set(cacheKey, parsed);
              return parsed;
            } catch {
              // Not JSON, return as string
              this.memoryCache.set(cacheKey, fullValue);
              return fullValue;
            }
          }
        } catch (e) {
          console.warn(`Error parsing chunked metadata for ${key}:`, e);
        }
      }
      
      // Try regular get as fallback
      const value = await this.execCommand("GET", cacheKey);
      
      if (value) {
        try {
          const parsed = JSON.parse(value);
          this.memoryCache.set(cacheKey, parsed);
          return parsed;
        } catch {
          // Not JSON, return as string
          this.memoryCache.set(cacheKey, value);
          return value;
        }
      }
      
      return null; // Cache miss
    } catch (error) {
      console.error(`Error in cacheGet for key ${key}:`, error);
      return null;
    }
  }
  
  /**
   * Enhanced cache set with chunking for large values
   */
  async cacheSet(key: string, value: any, ttlSeconds: number): Promise<string> {
    try {
      const cacheKey = `cache:${key}`;
      
      // Set in memory cache first (reliable fallback)
      this.memoryCache.set(cacheKey, value, ttlSeconds);
      
      // Serialize value
      let stringValue: string;
      try {
        stringValue = typeof value === 'object' ? JSON.stringify(value) : String(value);
      } catch (e) {
        console.error(`Error stringifying cache value for ${key}:`, e);
        return "OK"; // Still return success since we have memory cache
      }
      
      // For large values, use chunking
      if (stringValue.length > MAX_CHUNK_SIZE) {
        console.log(`Large value detected for key ${key}, using chunking (${Math.ceil(stringValue.length / 1024)}KB)`);
        
        try {
          // Store metadata about the chunked value
          const chunkCount = Math.ceil(stringValue.length / MAX_CHUNK_SIZE);
          const metadata = {
            chunked: true,
            totalSize: stringValue.length,
            chunks: chunkCount,
            timestamp: Date.now()
          };
          
          // Store metadata
          const metaKey = `${cacheKey}:meta`;
          await this.execCommand("SET", metaKey, JSON.stringify(metadata), "EX", String(ttlSeconds));
          
          // Store each chunk
          for (let i = 0; i < chunkCount; i++) {
            const chunkKey = `${cacheKey}:chunk:${i}`;
            const start = i * MAX_CHUNK_SIZE;
            const end = Math.min(start + MAX_CHUNK_SIZE, stringValue.length);
            const chunk = stringValue.substring(start, end);
            
            await this.execCommand("SET", chunkKey, chunk, "EX", String(ttlSeconds));
          }
          
          return "OK";
        } catch (chunkError) {
          console.warn(`Chunking failed for key ${key}, using memory-only cache:`, chunkError);
          return "OK"; // We still have memory cache
        }
      }
      
      // For normal sized values, use standard SET
      try {
        return await this.execCommand("SET", cacheKey, stringValue, "EX", String(ttlSeconds)) || "OK";
      } catch (error) {
        console.warn(`Redis SET failed for key ${cacheKey}:`, error);
        return "OK"; // We still have memory cache
      }
    } catch (error) {
      console.error(`Error in cacheSet for key ${key}:`, error);
      return "OK"; // Return success to prevent pipeline failures
    }
  }
  
  // Pipeline execution 
  async pipelineExec(pipeline: string[][]): Promise<any[]> {
    try {
      return await this.makeRequest(pipeline);
    } catch (error) {
      console.error("Redis pipeline execution failed:", error);
      return Array(pipeline.length).fill(null);
    }
  }
  
  // Track API usage - simplified to reduce Redis operations
  async trackApiCall(api: string, endpoint: string, success: boolean): Promise<void> {
    try {
      // Buffer stats in memory
      if (!this.statsBuffer.has(api)) {
        this.statsBuffer.set(api, new Map());
      }
      
      const apiMap = this.statsBuffer.get(api)!;
      
      // Track endpoint call
      const key = `${endpoint}:${success ? 'success' : 'error'}`;
      apiMap.set(key, (apiMap.get(key) || 0) + 1);
      
      // Flush stats if interval has passed
      if (Date.now() - this.lastStatsFlush > this.statsFlushInterval) {
        this.flushStats().catch(e => console.error("Error flushing stats:", e));
      }
    } catch (error) {
      console.error(`Error tracking API call for ${api}/${endpoint}:`, error);
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
          const endpoint = parts[0];
          const success = parts[1] === 'success';
          
          const day = new Date().toISOString().split('T')[0];
          const redisKey = success ? `stats:${api}:${day}` : `errors:${api}:${day}`;
          
          pipeline.push(["HINCRBY", redisKey, endpoint, String(count)]);
          pipeline.push(["EXPIRE", redisKey, String(60 * 60 * 24 * 30)]); // 30 days
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
  
  // Force flush stats
  async forceFlushStats(): Promise<void> {
    return this.flushStats();
  }
  
  // Ping to check connectivity
  async ping(): Promise<boolean> {
    try {
      const result = await this.execCommand("PING");
      return result === "PONG";
    } catch {
      return false;
    }
  }
}

// Export a singleton instance
let redisInstance: UpstashRedis | null = null;

export function getRedis(): UpstashRedis {
  if (!redisInstance) {
    try {
      redisInstance = new UpstashRedis();
    } catch (error) {
      console.error("Failed to initialize Redis client:", error);
      // Create a minimal instance that will use memory cache only
      redisInstance = new UpstashRedis();
    }
  }
  return redisInstance;
}
