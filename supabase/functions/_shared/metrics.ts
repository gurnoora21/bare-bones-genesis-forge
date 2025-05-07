import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

export class DeduplicationMetrics {
  private redis: Redis;
  
  constructor(redis: Redis) {
    this.redis = redis;
  }
  
  /**
   * Record a deduplication event
   */
  async recordDeduplicated(queueName: string, type: 'producer' | 'consumer'): Promise<void> {
    try {
      // Get the current date for daily stats
      const today = new Date().toISOString().split('T')[0];
      
      // Increment counters
      await this.redis.hincrby(`metrics:dedup:${today}`, `${queueName}:${type}`, 1);
      await this.redis.hincrby(`metrics:dedup:${today}`, `total:${type}`, 1);
      
      // Keep deduplication metrics for 30 days
      await this.redis.expire(`metrics:dedup:${today}`, 60 * 60 * 24 * 30);
    } catch (error) {
      console.warn(`Error recording deduplication metric: ${error}`);
    }
  }
  
  /**
   * Get deduplication stats for a specific day
   */
  async getDailyStats(date: string = new Date().toISOString().split('T')[0]): Promise<Record<string, number>> {
    try {
      const stats = await this.redis.hgetall(`metrics:dedup:${date}`);
      return stats || {};
    } catch (error) {
      console.warn(`Error getting deduplication stats: ${error}`);
      return {};
    }
  }
  
  /**
   * Get deduplication total for a queue
   */
  async getQueueTotal(queueName: string, days: number = 7): Promise<{producer: number, consumer: number}> {
    try {
      let producerTotal = 0;
      let consumerTotal = 0;
      
      // Get stats for last N days
      for (let i = 0; i < days; i++) {
        const date = new Date();
        date.setDate(date.getDate() - i);
        const dateStr = date.toISOString().split('T')[0];
        
        const stats = await this.getDailyStats(dateStr);
        
        producerTotal += parseInt(stats[`${queueName}:producer`] || '0');
        consumerTotal += parseInt(stats[`${queueName}:consumer`] || '0');
      }
      
      return {
        producer: producerTotal,
        consumer: consumerTotal
      };
    } catch (error) {
      console.warn(`Error calculating queue totals: ${error}`);
      return { producer: 0, consumer: 0 };
    }
  }
}

// Singleton instance
let metricsInstance: DeduplicationMetrics | null = null;

/**
 * Get or create the metrics instance
 */
export function getDeduplicationMetrics(redis: Redis): DeduplicationMetrics {
  if (!metricsInstance && redis) {
    metricsInstance = new DeduplicationMetrics(redis);
  }
  return metricsInstance || new DeduplicationMetrics(redis);
}
