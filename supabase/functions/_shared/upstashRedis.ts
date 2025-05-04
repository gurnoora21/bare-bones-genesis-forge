
/**
 * Enhanced Redis client using the official Upstash Redis SDK for Deno
 * with fallbacks for resilience
 */

import { Redis } from "https://deno.land/x/upstash_redis@v1.20.2/mod.ts";
import { MemoryCache } from "./memoryCache.ts";

export class EnhancedRedisClient {
  private redis: Redis;
  private memoryCache: MemoryCache<any>;
  
  constructor() {
    // Initialize the official Redis client
    this.redis = new Redis({
      url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
      token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
    });
    
    // Initialize memory cache as fallback
    this.memoryCache = new MemoryCache(1000, 120000);
  }
  
  /**
   * Get a value with memory cache fallback
   */
  async get(key: string): Promise<any> {
    try {
      // Try memory cache first
      const cachedValue = this.memoryCache.get(key);
      if (cachedValue !== null && cachedValue !== undefined) {
        return cachedValue;
      }
      
      // Get from Redis
      const value = await this.redis.get(key);
      
      // Update memory cache
      if (value !== null) {
        this.memoryCache.set(key, value, 300);
      }
      
      return value;
    } catch (error) {
      console.warn(`Redis get failed for key ${key}:`, error);
      return null;
    }
  }
  
  /**
   * Set a value with memory cache backup
   */
  async set(key: string, value: any, expireSeconds?: number): Promise<any> {
    try {
      // Set in memory cache
      this.memoryCache.set(key, value, expireSeconds);
      
      // Set in Redis
      if (expireSeconds) {
        return await this.redis.set(key, value, { ex: expireSeconds });
      } else {
        return await this.redis.set(key, value);
      }
    } catch (error) {
      console.warn(`Redis set failed for key ${key}:`, error);
      return "OK"; // Return success for pipeline continuity
    }
  }
  
  /**
   * Delete a key
   */
  async del(key: string): Promise<number> {
    try {
      // Remove from memory cache
      this.memoryCache.delete(key);
      
      // Delete from Redis
      return await this.redis.del(key);
    } catch (error) {
      console.warn(`Redis del failed for key ${key}:`, error);
      return 0;
    }
  }
  
  /**
   * Get a cached value with prefix
   */
  async cacheGet(key: string): Promise<any> {
    const cacheKey = `cache:${key}`;
    return this.get(cacheKey);
  }
  
  /**
   * Set a cached value with prefix
   */
  async cacheSet(key: string, value: any, ttlSeconds: number): Promise<any> {
    const cacheKey = `cache:${key}`;
    return this.set(cacheKey, value, ttlSeconds);
  }
  
  /**
   * Convenient method to track API calls
   */
  async trackApiCall(api: string, endpoint: string, success: boolean): Promise<void> {
    try {
      const day = new Date().toISOString().split('T')[0];
      const redisKey = success ? `stats:${api}:${day}` : `errors:${api}:${day}`;
      
      // Increment counter
      await this.redis.hincrby(redisKey, endpoint, 1);
      
      // Set expiry
      await this.redis.expire(redisKey, 60 * 60 * 24 * 30); // 30 days
    } catch (error) {
      console.warn(`Failed to track API call: ${api}/${endpoint}`, error);
    }
  }
  
  /**
   * Pipeline execution 
   */
  async pipelineExec(commands: string[][]): Promise<any[]> {
    try {
      const pipeline = this.redis.pipeline();
      
      for (const cmd of commands) {
        const [command, ...args] = cmd;
        // @ts-ignore: Runtime method invocation
        pipeline[command.toLowerCase()](...args);
      }
      
      return await pipeline.exec();
    } catch (error) {
      console.error("Redis pipeline execution failed:", error);
      return [];
    }
  }
  
  /**
   * Ping Redis to check connectivity
   */
  async ping(): Promise<boolean> {
    try {
      const result = await this.redis.ping();
      return result === "PONG";
    } catch {
      return false;
    }
  }
}

// Export a singleton instance and compatibility function
let clientInstance: EnhancedRedisClient | null = null;

export function getRedis(): EnhancedRedisClient {
  if (!clientInstance) {
    try {
      clientInstance = new EnhancedRedisClient();
    } catch (error) {
      console.error("Failed to initialize Redis client:", error);
      clientInstance = new EnhancedRedisClient();
    }
  }
  return clientInstance;
}
