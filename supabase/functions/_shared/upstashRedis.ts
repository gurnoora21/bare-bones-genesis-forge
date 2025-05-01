
/**
 * Upstash Redis HTTP client for Deno/Edge Functions
 * Implements key Redis operations needed for rate limiting and caching
 */

export class UpstashRedis {
  private url: string;
  private token: string;
  
  constructor() {
    this.url = Deno.env.get("UPSTASH_REDIS_REST_URL") || "";
    this.token = Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "";
    
    if (!this.url || !this.token) {
      throw new Error("Upstash Redis credentials must be provided as environment variables");
    }
  }
  
  /**
   * Make an authenticated request to the Upstash Redis REST API
   */
  private async request(command: string[]): Promise<any> {
    try {
      const response = await fetch(this.url, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${this.token}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify(command)
      });
      
      if (!response.ok) {
        throw new Error(`Upstash Redis error: ${response.status} ${await response.text()}`);
      }
      
      const data = await response.json();
      if (data.error) {
        throw new Error(`Redis error: ${data.error}`);
      }
      
      return data.result;
    } catch (error) {
      console.error("Upstash Redis request failed:", error);
      throw error;
    }
  }
  
  // Basic Redis operations
  
  async get(key: string): Promise<any> {
    const result = await this.request(["GET", key]);
    return result ? JSON.parse(result) : null;
  }
  
  async set(key: string, value: any, expireSeconds?: number): Promise<string> {
    const command = ["SET", key, JSON.stringify(value)];
    if (expireSeconds) {
      command.push("EX", String(expireSeconds));
    }
    return await this.request(command);
  }
  
  async del(key: string): Promise<number> {
    return await this.request(["DEL", key]);
  }
  
  async incr(key: string): Promise<number> {
    return await this.request(["INCR", key]);
  }
  
  async decr(key: string): Promise<number> {
    return await this.request(["DECR", key]);
  }
  
  async expire(key: string, seconds: number): Promise<number> {
    return await this.request(["EXPIRE", key, String(seconds)]);
  }
  
  // Token bucket operations
  
  async evalTokenBucket(
    key: string, 
    tokensPerInterval: number, 
    interval: number, 
    tokensToConsume: number
  ): Promise<boolean> {
    const script = `
      local key = KEYS[1]
      local tokens_per_interval = tonumber(ARGV[1])
      local interval = tonumber(ARGV[2])
      local tokens_to_consume = tonumber(ARGV[3])
      local now = tonumber(ARGV[4])

      -- Initialize bucket if it doesn't exist
      local bucket = redis.call("HMGET", key, "tokens", "last_refill")
      local tokens = tonumber(bucket[1])
      local last_refill = tonumber(bucket[2])
      
      if tokens == nil then
        tokens = tokens_per_interval
        last_refill = now
        redis.call("HMSET", key, "tokens", tokens, "last_refill", last_refill)
        redis.call("EXPIRE", key, interval * 2)
      end

      -- Refill tokens based on time elapsed
      local elapsed = now - last_refill
      local new_tokens = 0
      
      if elapsed > 0 then
        -- Calculate tokens to add
        new_tokens = math.floor(elapsed / interval * tokens_per_interval)
        
        if new_tokens > 0 then
          tokens = math.min(tokens + new_tokens, tokens_per_interval)
          last_refill = now
          redis.call("HMSET", key, "tokens", tokens, "last_refill", last_refill)
        end
      end
      
      -- Check if we can consume tokens
      if tokens >= tokens_to_consume then
        redis.call("HSET", key, "tokens", tokens - tokens_to_consume)
        return 1
      end
      
      return 0
    `;
    
    // Use a simplified approach since we don't have EVAL in REST API
    // Get current bucket state
    const now = Math.floor(Date.now() / 1000);
    const bucket = await this.request(["HMGET", key, "tokens", "last_refill"]);
    
    let tokens = bucket[0] ? parseInt(bucket[0]) : tokensPerInterval;
    let lastRefill = bucket[1] ? parseInt(bucket[1]) : now;
    
    // Refill tokens based on time elapsed
    const elapsed = now - lastRefill;
    if (elapsed > 0) {
      const newTokens = Math.floor((elapsed / interval) * tokensPerInterval);
      if (newTokens > 0) {
        tokens = Math.min(tokens + newTokens, tokensPerInterval);
        lastRefill = now;
      }
    }
    
    // Check if we can consume tokens and update
    if (tokens >= tokensToConsume) {
      await this.request(["HMSET", key, "tokens", tokens - tokensToConsume, "last_refill", lastRefill]);
      await this.request(["EXPIRE", key, interval * 2]);
      return true;
    }
    
    // Can't consume tokens, but update last_refill time
    await this.request(["HMSET", key, "tokens", tokens, "last_refill", lastRefill]);
    await this.request(["EXPIRE", key, interval * 2]);
    return false;
  }
  
  // API usage tracking
  
  async trackApiCall(api: string, endpoint: string, success: boolean): Promise<void> {
    const now = new Date();
    const day = now.toISOString().split('T')[0];
    const hour = now.getHours();
    
    // Increment daily counter
    await this.request(["HINCRBY", `stats:${api}:${day}`, endpoint, 1]);
    
    // Increment hourly counter
    await this.request(["HINCRBY", `stats:${api}:${day}:${hour}`, endpoint, 1]);
    
    // Track success/failure
    if (!success) {
      await this.request(["HINCRBY", `errors:${api}:${day}`, endpoint, 1]);
    }
    
    // Set expiry for stats keys (30 days)
    await this.request(["EXPIRE", `stats:${api}:${day}`, 60 * 60 * 24 * 30]);
    await this.request(["EXPIRE", `stats:${api}:${day}:${hour}`, 60 * 60 * 24 * 30]);
    await this.request(["EXPIRE", `errors:${api}:${day}`, 60 * 60 * 24 * 30]);
  }
  
  // Result caching
  
  async cacheSet(key: string, value: any, ttlSeconds: number): Promise<string> {
    return this.set(`cache:${key}`, value, ttlSeconds);
  }
  
  async cacheGet(key: string): Promise<any> {
    return this.get(`cache:${key}`);
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
