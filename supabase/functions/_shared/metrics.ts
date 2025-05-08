
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getEnvironmentTTL } from "./stateManager.ts";

// Constants for Redis key prefixes
const DEDUPLICATION_METRICS_PREFIX = "metrics:deduplication";

// Interface for deduplication metrics
export interface DeduplicationMetrics {
  recordDeduplicated(queueName: string, source: string): Promise<void>;
  recordProcessed(queueName: string, source: string): Promise<void>;
  getMetricsForQueue(queueName: string): Promise<DeduplicationMetricsData>;
  resetMetricsForQueue(queueName: string): Promise<boolean>;
}

// Data structure for deduplication metrics
export interface DeduplicationMetricsData {
  totalProcessed: number;
  totalDeduplicated: number;
  deduplicationRate: number;
  queueName: string;
  sources: Record<string, {
    processed: number;
    deduplicated: number;
    rate: number;
  }>;
}

/**
 * Deduplication metrics service implementation
 */
class DeduplicationMetricsImpl implements DeduplicationMetrics {
  private redis: Redis;
  private ttl: number;

  constructor(redis: Redis) {
    this.redis = redis;
    this.ttl = getEnvironmentTTL(); // Use environment-specific TTL
  }

  /**
   * Record a deduplicated message
   */
  async recordDeduplicated(queueName: string, source: string): Promise<void> {
    try {
      const queueKey = `${DEDUPLICATION_METRICS_PREFIX}:${queueName}:deduplicated`;
      const sourceKey = `${DEDUPLICATION_METRICS_PREFIX}:${queueName}:source:${source}:deduplicated`;
      
      await Promise.all([
        this.redis.incr(queueKey),
        this.redis.incr(sourceKey),
        this.redis.expire(queueKey, this.ttl),
        this.redis.expire(sourceKey, this.ttl)
      ]);
    } catch (error) {
      console.warn(`Failed to record deduplicated metric: ${error}`);
    }
  }

  /**
   * Record a processed message
   */
  async recordProcessed(queueName: string, source: string): Promise<void> {
    try {
      const queueKey = `${DEDUPLICATION_METRICS_PREFIX}:${queueName}:processed`;
      const sourceKey = `${DEDUPLICATION_METRICS_PREFIX}:${queueName}:source:${source}:processed`;
      
      await Promise.all([
        this.redis.incr(queueKey),
        this.redis.incr(sourceKey),
        this.redis.expire(queueKey, this.ttl),
        this.redis.expire(sourceKey, this.ttl)
      ]);
    } catch (error) {
      console.warn(`Failed to record processed metric: ${error}`);
    }
  }

  /**
   * Get metrics for a specific queue
   */
  async getMetricsForQueue(queueName: string): Promise<DeduplicationMetricsData> {
    try {
      // Get the total counts
      const [totalProcessed, totalDeduplicated] = await Promise.all([
        this.redis.get(`${DEDUPLICATION_METRICS_PREFIX}:${queueName}:processed`),
        this.redis.get(`${DEDUPLICATION_METRICS_PREFIX}:${queueName}:deduplicated`)
      ]);
      
      // Calculate deduplication rate
      const processed = parseInt(totalProcessed as string) || 0;
      const deduplicated = parseInt(totalDeduplicated as string) || 0;
      const rate = processed > 0 ? deduplicated / processed : 0;
      
      // Get the pattern for source keys
      const sourceProcessedPattern = `${DEDUPLICATION_METRICS_PREFIX}:${queueName}:source:*:processed`;
      const sourceDeduplicatedPattern = `${DEDUPLICATION_METRICS_PREFIX}:${queueName}:source:*:deduplicated`;
      
      // Get all source keys
      const [processedKeys, deduplicatedKeys] = await Promise.all([
        this.redis.keys(sourceProcessedPattern),
        this.redis.keys(sourceDeduplicatedPattern)
      ]);
      
      // Build a unique set of sources
      const sourcesSet = new Set<string>();
      processedKeys.forEach(key => {
        const source = key.split(':')[4]; // Extract source from key pattern
        sourcesSet.add(source);
      });
      deduplicatedKeys.forEach(key => {
        const source = key.split(':')[4]; // Extract source from key pattern
        sourcesSet.add(source);
      });
      
      // For each source, get the metrics
      const sources: Record<string, { processed: number; deduplicated: number; rate: number }> = {};
      
      await Promise.all(Array.from(sourcesSet).map(async source => {
        const [sourceProcessed, sourceDeduplicated] = await Promise.all([
          this.redis.get(`${DEDUPLICATION_METRICS_PREFIX}:${queueName}:source:${source}:processed`),
          this.redis.get(`${DEDUPLICATION_METRICS_PREFIX}:${queueName}:source:${source}:deduplicated`)
        ]);
        
        const p = parseInt(sourceProcessed as string) || 0;
        const d = parseInt(sourceDeduplicated as string) || 0;
        const r = p > 0 ? d / p : 0;
        
        sources[source] = {
          processed: p,
          deduplicated: d,
          rate: r
        };
      }));
      
      return {
        totalProcessed: processed,
        totalDeduplicated: deduplicated,
        deduplicationRate: rate,
        queueName,
        sources
      };
    } catch (error) {
      console.error(`Failed to get metrics for queue ${queueName}:`, error);
      
      // Return empty metrics on error
      return {
        totalProcessed: 0,
        totalDeduplicated: 0,
        deduplicationRate: 0,
        queueName,
        sources: {}
      };
    }
  }

  /**
   * Reset metrics for a specific queue
   */
  async resetMetricsForQueue(queueName: string): Promise<boolean> {
    try {
      // Get all keys for this queue
      const keys = await this.redis.keys(`${DEDUPLICATION_METRICS_PREFIX}:${queueName}:*`);
      
      // Delete all keys if there are any
      if (keys.length > 0) {
        await this.redis.del(keys);
      }
      
      return true;
    } catch (error) {
      console.error(`Failed to reset metrics for queue ${queueName}:`, error);
      return false;
    }
  }
}

// Singleton instance
let deduplicationMetricsInstance: DeduplicationMetrics | null = null;

/**
 * Get or create deduplication metrics instance
 */
export function getDeduplicationMetrics(redis: Redis): DeduplicationMetrics {
  if (!deduplicationMetricsInstance && redis) {
    deduplicationMetricsInstance = new DeduplicationMetricsImpl(redis);
  }
  return deduplicationMetricsInstance || new DeduplicationMetricsImpl(redis);
}
