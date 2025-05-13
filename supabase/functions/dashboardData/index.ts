
/**
 * Dashboard Data Edge Function
 * 
 * Provides consolidated monitoring data for the system dashboard
 */
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { StructuredLogger } from "../_shared/structuredLogger.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }
  
  const logger = new StructuredLogger({ service: 'dashboard-data' });
  
  const supabase = createClient(
    Deno.env.get('SUPABASE_URL') || '',
    Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') || ''
  );
  
  // Initialize Redis if credentials are available
  let redis: Redis | null = null;
  try {
    if (Deno.env.get('UPSTASH_REDIS_REST_URL') && Deno.env.get('UPSTASH_REDIS_REST_TOKEN')) {
      redis = new Redis({
        url: Deno.env.get('UPSTASH_REDIS_REST_URL') || '',
        token: Deno.env.get('UPSTASH_REDIS_REST_TOKEN') || '',
      });
    }
  } catch (error) {
    logger.error("Failed to initialize Redis", error);
  }

  try {
    // Fetch all necessary data in parallel
    const [
      queueStats,
      processingStats,
      redisKeyStats,
      workerIssues,
      deduplicationMetrics
    ] = await Promise.all([
      getQueueStats(supabase, logger),
      getProcessingStats(supabase, logger),
      getRedisKeyStats(redis, logger),
      getWorkerIssues(supabase, logger),
      getDeduplicationMetrics(supabase, logger)
    ]);
    
    // Return combined dashboard data
    return new Response(
      JSON.stringify({
        success: true,
        queueStats,
        processingStats,
        redisKeyStats,
        workerIssues,
        deduplicationMetrics,
        timestamp: new Date().toISOString()
      }),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 200
      }
    );
  } catch (error) {
    logger.error("Error retrieving dashboard data", error);
    
    return new Response(
      JSON.stringify({ 
        success: false, 
        error: error.message 
      }),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 500
      }
    );
  }
});

/**
 * Get queue statistics
 */
async function getQueueStats(supabase: any, logger: StructuredLogger) {
  try {
    // Get queue tables
    const { data: queueTables, error: queueError } = await supabase.rpc('get_all_queue_tables');
    
    if (queueError) {
      throw new Error(`Failed to get queue tables: ${queueError.message}`);
    }
    
    // Get statistics for each queue
    const queueStats = [];
    
    for (const queue of queueTables) {
      try {
        // Get queue status
        const { data: status } = await supabase.rpc(
          'pg_queue_status',
          { queue_name: queue.queue_name }
        );
        
        // Get queue metrics
        const { data: metrics } = await supabase
          .from('monitoring.queue_metrics')
          .select('*')
          .eq('queue_name', queue.queue_name)
          .order('hour', { ascending: false })
          .limit(24);
        
        // Calculate metrics
        let processingRate = null;
        let avgProcessingTime = null;
        let errorRate = null;
        
        if (metrics && metrics.length > 0) {
          // Calculate processing rate from most recent hour
          const recentMetric = metrics[0];
          processingRate = (recentMetric.success_count + recentMetric.error_count) / 60;
          
          // Calculate average processing time
          avgProcessingTime = recentMetric.avg_processing_time_ms / 1000; // convert to seconds
          
          // Calculate error rate
          const totalProcessed = recentMetric.success_count + recentMetric.error_count;
          errorRate = totalProcessed > 0 ? recentMetric.error_count / totalProcessed : 0;
        }
        
        queueStats.push({
          name: queue.queue_name,
          messageCount: status?.count || 0,
          oldestMessage: status?.oldest_message,
          processingRate,
          avgProcessingTime,
          errorRate
        });
      } catch (error) {
        logger.error(`Error getting stats for queue ${queue.queue_name}`, error);
        queueStats.push({
          name: queue.queue_name,
          messageCount: 0,
          oldestMessage: null,
          processingRate: null,
          avgProcessingTime: null,
          errorRate: null
        });
      }
    }
    
    return queueStats;
  } catch (error) {
    logger.error("Failed to get queue stats", error);
    return [];
  }
}

/**
 * Get processing status statistics
 */
async function getProcessingStats(supabase: any, logger: StructuredLogger) {
  try {
    // Get all entity types
    const { data: entityTypes } = await supabase
      .from('processing_status')
      .select('entity_type')
      .limit(1000);
    
    const uniqueTypes = [...new Set(entityTypes.map((et: any) => et.entity_type))];
    const stats = [];
    
    // Get stats for each entity type
    for (const entityType of uniqueTypes) {
      // Get counts by state
      const { data: stateCounts } = await supabase
        .from('processing_status')
        .select('state')
        .eq('entity_type', entityType)
        .limit(10000);
      
      // Count states
      const counts: Record<string, number> = {
        pending: 0,
        inProgress: 0,
        completed: 0,
        failed: 0
      };
      
      for (const record of stateCounts) {
        counts[record.state.toLowerCase()] = (counts[record.state.toLowerCase()] || 0) + 1;
      }
      
      // Get count of stuck entities
      const { data: stuckEntities } = await supabase
        .rpc('find_inconsistent_states', {
          p_entity_type: entityType,
          p_older_than_minutes: 30
        });
      
      stats.push({
        entityType,
        total: stateCounts.length,
        pending: counts.pending || 0,
        inProgress: counts.inprogress || 0,
        completed: counts.completed || 0,
        failed: counts.failed || 0,
        stuckCount: stuckEntities ? stuckEntities.length : 0
      });
    }
    
    return stats;
  } catch (error) {
    logger.error("Failed to get processing stats", error);
    return [];
  }
}

/**
 * Get Redis key statistics
 */
async function getRedisKeyStats(redis: Redis | null, logger: StructuredLogger) {
  if (!redis) {
    return {};
  }
  
  try {
    // Get all keys
    const keys = await redis.keys("*");
    
    // Group keys by prefix
    const prefixCounts: Record<string, number> = {};
    
    for (const key of keys) {
      const prefix = key.split(':')[0] || 'other';
      prefixCounts[prefix] = (prefixCounts[prefix] || 0) + 1;
    }
    
    return prefixCounts;
  } catch (error) {
    logger.error("Failed to get Redis key stats", error);
    return {};
  }
}

/**
 * Get worker issues
 */
async function getWorkerIssues(supabase: any, logger: StructuredLogger) {
  try {
    // Get recent issues
    const { data: issues } = await supabase
      .from('monitoring.worker_issues')
      .select('*')
      .order('created_at', { ascending: false })
      .eq('resolved', false)
      .limit(50);
    
    // Count by worker
    const byWorker: Record<string, number> = {};
    const byType: Record<string, number> = {};
    
    for (const issue of issues || []) {
      byWorker[issue.worker_name] = (byWorker[issue.worker_name] || 0) + 1;
      byType[issue.issue_type] = (byType[issue.issue_type] || 0) + 1;
    }
    
    return {
      total: issues ? issues.length : 0,
      byWorker,
      byType,
      recent: issues ? issues.slice(0, 10) : []
    };
  } catch (error) {
    logger.error("Failed to get worker issues", error);
    return {
      total: 0,
      byWorker: {},
      byType: {},
      recent: []
    };
  }
}

/**
 * Get API deduplication metrics
 */
async function getDeduplicationMetrics(supabase: any, logger: StructuredLogger) {
  try {
    // Get today's API metrics
    const { data: todayMetrics } = await supabase
      .from('monitoring.api_metrics')
      .select('*')
      .gte('hour', new Date(new Date().setHours(0, 0, 0, 0)).toISOString())
      .order('hour', { ascending: false });
    
    // Group by API
    const apiMetrics: Record<string, any> = {};
    
    // Process daily totals
    for (const metric of todayMetrics || []) {
      const api = metric.api_name;
      
      if (!apiMetrics[api]) {
        apiMetrics[api] = {
          daily: {
            total: 0,
            success: 0,
            error: 0,
            deduplicated: 0
          },
          hourly: {}
        };
      }
      
      // Add to daily totals
      apiMetrics[api].daily.total += metric.total_calls;
      apiMetrics[api].daily.success += metric.success_count;
      apiMetrics[api].daily.error += metric.error_count;
      
      // Calculate deduplicated calls
      const deduplicatedCalls = Math.round(metric.total_calls * 0.3); // Example calculation
      apiMetrics[api].daily.deduplicated += deduplicatedCalls;
      
      // Record hourly data
      const hour = new Date(metric.hour).getHours();
      apiMetrics[api].hourly[hour] = {
        total: metric.total_calls,
        success: metric.success_count,
        error: metric.error_count,
        deduplicated: deduplicatedCalls,
        p95: metric.p95_time_ms
      };
    }
    
    return apiMetrics;
  } catch (error) {
    logger.error("Failed to get deduplication metrics", error);
    return {};
  }
}
