
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

interface MetricsService {
  recordDeduplicated: (queueName: string, role: string) => Promise<void>;
  recordProcessed: (queueName: string) => Promise<void>;
  getDeduplicationRate: (queueName: string) => Promise<{
    duplicateCount: number;
    processedCount: number;
    rate: number;
  }>;
}

export function getDeduplicationMetrics(redis: Redis): MetricsService {
  return {
    async recordDeduplicated(queueName: string, role: string = "consumer"): Promise<void> {
      try {
        const key = `metrics:${queueName}:duplicates`;
        const countKey = `metrics:${queueName}:${role}:duplicates`;
        await redis.incr(key);
        await redis.incr(countKey);
        
        // Set expiry if not set
        await redis.expire(key, 86400 * 30); // 30 days
        await redis.expire(countKey, 86400 * 30); // 30 days
      } catch (error) {
        console.warn(`Failed to record deduplicated count for ${queueName}:`, error);
      }
    },
    
    async recordProcessed(queueName: string): Promise<void> {
      try {
        const key = `metrics:${queueName}:processed`;
        await redis.incr(key);
        
        // Set expiry if not set
        await redis.expire(key, 86400 * 30); // 30 days
      } catch (error) {
        console.warn(`Failed to record processed count for ${queueName}:`, error);
      }
    },
    
    async getDeduplicationRate(queueName: string): Promise<{
      duplicateCount: number;
      processedCount: number;
      rate: number;
    }> {
      try {
        const duplicateKey = `metrics:${queueName}:duplicates`;
        const processedKey = `metrics:${queueName}:processed`;
        
        const duplicateCount = await redis.get(duplicateKey) as string;
        const processedCount = await redis.get(processedKey) as string;
        
        const dCount = parseInt(duplicateCount || '0', 10);
        const pCount = parseInt(processedCount || '0', 10);
        
        const rate = pCount > 0 ? dCount / pCount : 0;
        
        return {
          duplicateCount: dCount,
          processedCount: pCount,
          rate
        };
      } catch (error) {
        console.warn(`Failed to get deduplication rate for ${queueName}:`, error);
        return {
          duplicateCount: 0,
          processedCount: 0,
          rate: 0
        };
      }
    }
  };
}

export async function recordQueueMetrics(
  supabase: any, 
  queueName: string, 
  operation: string,
  processedCount: number,
  successCount: number,
  errorCount: number,
  details: any = {}
): Promise<void> {
  try {
    const startedAt = new Date();
    const finishedAt = new Date();
    
    await supabase.from('queue_metrics').insert({
      queue_name: queueName,
      operation: operation,
      started_at: startedAt.toISOString(),
      finished_at: finishedAt.toISOString(),
      processed_count: processedCount,
      success_count: successCount,
      error_count: errorCount,
      details: details
    });
  } catch (error) {
    console.error(`Failed to record queue metrics for ${queueName}:`, error);
  }
}
