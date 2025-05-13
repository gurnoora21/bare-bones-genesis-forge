
/**
 * Rate Limiter Demonstration Edge Function
 * Shows the enhanced rate limiting in action with efficiency metrics
 */

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { getRedis } from "../_shared/upstashRedis.ts";
import { getEnhancedRateLimiter } from "../_shared/enhancedRateLimiter.ts";
import { RATE_LIMITERS } from "../_shared/rateLimiter.ts";

serve(async (req) => {
  try {
    const url = new URL(req.url);
    const test = url.searchParams.get('test') || 'default';
    
    const redis = getRedis();
    const rateLimiter = getEnhancedRateLimiter(redis.redis);
    
    // Ensure Redis is healthy
    const isHealthy = await redis.isHealthy();
    const redisHealthInfo = await redis.getHealth();
    
    // Simulated API usage to demonstrate rate limiting behavior
    const results = [];
    const start = Date.now();
    
    switch (test) {
      case 'burst': {
        // Test burst handling
        for (let i = 0; i < 20; i++) {
          const startReq = Date.now();
          const limiterResponse = await rateLimiter.limit({
            ...RATE_LIMITERS.GENIUS.DEFAULT,
            identifier: "demo:burst"
          });
          
          results.push({
            attempt: i + 1,
            allowed: limiterResponse.success,
            remaining: limiterResponse.remaining,
            latency: Date.now() - startReq,
            resetIn: limiterResponse.reset - Date.now()
          });
        }
        break;
      }
      
      case 'sliding': {
        // Test sliding window behavior with slight delays
        for (let i = 0; i < 10; i++) {
          const startReq = Date.now();
          const limiterResponse = await rateLimiter.limit({
            identifier: "demo:sliding",
            limit: 5,
            windowSeconds: 10,
            algorithm: "sliding"
          });
          
          results.push({
            attempt: i + 1,
            allowed: limiterResponse.success,
            remaining: limiterResponse.remaining,
            latency: Date.now() - startReq,
            resetIn: limiterResponse.reset - Date.now()
          });
          
          // Small delay between requests
          await new Promise(resolve => setTimeout(resolve, 200));
        }
        break;
      }
      
      case 'token': {
        // Test token bucket behavior
        for (let i = 0; i < 15; i++) {
          const startReq = Date.now();
          const limiterResponse = await rateLimiter.limit({
            identifier: "demo:token",
            limit: 10,
            windowSeconds: 5,
            algorithm: "token"
          });
          
          results.push({
            attempt: i + 1,
            allowed: limiterResponse.success,
            remaining: limiterResponse.remaining,
            latency: Date.now() - startReq,
            resetIn: limiterResponse.reset - Date.now()
          });
          
          // Add a small delay between requests
          if (i % 3 === 0) {
            await new Promise(resolve => setTimeout(resolve, 500));
          }
        }
        break;
      }
      
      default: {
        // Basic test of rate limiter
        const limiterResponse = await rateLimiter.limit({
          identifier: "demo:default",
          limit: 10,
          windowSeconds: 10
        });
        
        results.push({
          allowed: limiterResponse.success,
          remaining: limiterResponse.remaining,
          resetIn: limiterResponse.reset - Date.now()
        });
      }
    }
    
    const totalDuration = Date.now() - start;
    const avgLatency = results.reduce((sum, r) => sum + (r.latency || 0), 0) / results.length;
    
    return new Response(JSON.stringify({
      test,
      redis: {
        healthy: isHealthy,
        health: redisHealthInfo
      },
      metrics: {
        totalRequests: results.length,
        totalDuration: `${totalDuration}ms`,
        averageLatency: `${avgLatency.toFixed(2)}ms`,
        requestsPerSecond: (results.length / (totalDuration / 1000)).toFixed(2)
      },
      results
    }), {
      headers: { "Content-Type": "application/json" }
    });
    
  } catch (error) {
    return new Response(JSON.stringify({
      error: error.message,
      stack: error.stack
    }), {
      status: 500,
      headers: { "Content-Type": "application/json" }
    });
  }
});
