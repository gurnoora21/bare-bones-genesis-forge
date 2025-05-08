
// Dashboard data edge function
// Provides monitoring and system health data

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getDeduplicationMetrics } from "../_shared/metrics.ts";

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
};

interface QueueStats {
  name: string;
  messageCount: number;
  oldestMessage: string | null;
  processingRate: number | null;
  avgProcessingTime: number | null; // in seconds
  errorRate: number | null;
}

interface ProcessingStats {
  entityType: string;
  total: number;
  pending: number;
  inProgress: number;
  completed: number;
  failed: number;
  stuckCount: number; // in_progress but timed out
}

interface SystemHealth {
  queueStats: QueueStats[];
  processingStats: ProcessingStats[];
  deduplicationMetrics: Record<string, any>;
  redisKeyStats: Record<string, number>;
  workerIssues: {
    total: number;
    byWorker: Record<string, number>;
    byType: Record<string, number>;
    recent: any[];
  };
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  // Only allow GET requests for dashboard data
  if (req.method !== 'GET') {
    return new Response(
      JSON.stringify({ error: "Method not allowed" }),
      { status: 405, headers: corsHeaders }
    );
  }

  try {
    // Initialize Supabase client
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    // Initialize Redis client
    const redis = new Redis({
      url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
      token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
    });

    // Get queue metrics
    const queueStats: QueueStats[] = await getQueueStats(supabase);
    
    // Get processing stats
    const processingStats: ProcessingStats[] = await getProcessingStats(supabase);
    
    // Get Redis key stats
    const redisKeyStats = await getRedisKeyStats(redis);
    
    // Get worker issues
    const workerIssues = await getWorkerIssues(supabase);
    
    // Get deduplication metrics
    const deduplicationMetrics = await getDeduplicationData(redis);

    // Return combined dashboard data
    const dashboardData: SystemHealth = {
      queueStats,
      processingStats,
      redisKeyStats,
      workerIssues,
      deduplicationMetrics
    };

    return new Response(
      JSON.stringify(dashboardData),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 200
      }
    );
  } catch (error) {
    console.error("Error fetching dashboard data:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      }
    );
  }
});

async function getQueueStats(supabase: any): Promise<QueueStats[]> {
  // Get list of queues
  const { data: queueData, error: queueError } = await supabase.rpc('list_queues');
  
  if (queueError) {
    console.error("Error listing queues:", queueError);
    return [];
  }
  
  const queueNames = queueData || ["artist_discovery", "album_discovery", "track_discovery", "producer_identification", "social_enrichment"];
  
  // Get stats for each queue
  const queueStatsPromises = queueNames.map(async (queueName: string): Promise<QueueStats> => {
    try {
      // Get message count and oldest message
      const { data: queueStats } = await supabase.rpc('queue_stats', { queue_name: queueName });
      
      // Get processing metrics
      const { data: metrics } = await supabase
        .from('queue_metrics')
        .select('*')
        .eq('queue_name', queueName)
        .order('created_at', { ascending: false })
        .limit(20);
      
      // Calculate processing rate and error rate
      let processingRate = null;
      let errorRate = null;
      let avgProcessingTime = null;
      
      if (metrics && metrics.length > 0) {
        // Calculate average processing time
        const completedMetrics = metrics.filter(m => m.finished_at);
        if (completedMetrics.length > 0) {
          const processingTimes = completedMetrics.map(m => {
            const start = new Date(m.started_at).getTime();
            const end = new Date(m.finished_at).getTime();
            return (end - start) / 1000; // in seconds
          });
          
          avgProcessingTime = processingTimes.reduce((a, b) => a + b, 0) / processingTimes.length;
        }
        
        // Calculate processing rate (messages per minute)
        const totalProcessed = metrics.reduce((sum, m) => sum + (m.processed_count || 0), 0);
        const totalTimeMinutes = metrics.reduce((sum, m) => {
          if (m.started_at && m.finished_at) {
            const start = new Date(m.started_at).getTime();
            const end = new Date(m.finished_at).getTime();
            return sum + ((end - start) / 1000 / 60); // in minutes
          }
          return sum;
        }, 0);
        
        if (totalTimeMinutes > 0) {
          processingRate = totalProcessed / totalTimeMinutes;
        }
        
        // Calculate error rate
        const totalErrors = metrics.reduce((sum, m) => sum + (m.error_count || 0), 0);
        if (totalProcessed > 0) {
          errorRate = totalErrors / totalProcessed;
        }
      }
      
      return {
        name: queueName,
        messageCount: queueStats?.count || 0,
        oldestMessage: queueStats?.oldest_message,
        processingRate,
        avgProcessingTime,
        errorRate
      };
    } catch (error) {
      console.error(`Error getting queue stats for ${queueName}:`, error);
      return {
        name: queueName,
        messageCount: 0,
        oldestMessage: null,
        processingRate: null,
        avgProcessingTime: null,
        errorRate: null
      };
    }
  });
  
  return Promise.all(queueStatsPromises);
}

async function getProcessingStats(supabase: any): Promise<ProcessingStats[]> {
  try {
    // Get stats grouped by entity_type
    const { data, error } = await supabase.rpc('get_processing_stats');
    
    if (error) {
      console.error("Error getting processing stats:", error);
      return [];
    }
    
    return data || [];
  } catch (error) {
    console.error("Error fetching processing stats:", error);
    return [];
  }
}

async function getRedisKeyStats(redis: Redis): Promise<Record<string, number>> {
  try {
    // Get all keys
    let cursor = 0;
    const allKeys: string[] = [];
    
    do {
      const [nextCursor, keys] = await redis.scan(cursor, {
        count: 1000
      });
      
      cursor = nextCursor;
      allKeys.push(...keys);
    } while (cursor !== 0);
    
    // Group keys by prefix
    const prefixCounts: Record<string, number> = {};
    
    for (const key of allKeys) {
      const prefix = key.split(':')[0];
      prefixCounts[prefix] = (prefixCounts[prefix] || 0) + 1;
    }
    
    // Add special count for deduplication keys by queue
    const dedupCounts: Record<string, number> = {};
    
    for (const key of allKeys) {
      if (key.startsWith('dedup:')) {
        const parts = key.split(':');
        if (parts.length >= 2) {
          const queueName = parts[1];
          const dedupKey = `dedup:${queueName}`;
          dedupCounts[dedupKey] = (dedupCounts[dedupKey] || 0) + 1;
        }
      }
    }
    
    return { ...prefixCounts, ...dedupCounts };
  } catch (error) {
    console.error("Error fetching Redis key stats:", error);
    return {};
  }
}

async function getWorkerIssues(supabase: any): Promise<any> {
  try {
    // Get recent worker issues
    const { data, error } = await supabase
      .from('worker_issues')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(10);
    
    if (error) {
      console.error("Error fetching worker issues:", error);
      return { total: 0, byWorker: {}, byType: {}, recent: [] };
    }
    
    // Get issue counts by worker and type
    const { data: stats, error: statsError } = await supabase.rpc('get_worker_issue_stats');
    
    if (statsError) {
      console.error("Error fetching worker issue stats:", statsError);
      return { 
        total: data.length || 0, 
        byWorker: {}, 
        byType: {}, 
        recent: data || [] 
      };
    }
    
    // Process stats
    const byWorker: Record<string, number> = {};
    const byType: Record<string, number> = {};
    let total = 0;
    
    if (stats) {
      for (const stat of stats) {
        if (stat.worker_name) {
          byWorker[stat.worker_name] = stat.count || 0;
        }
        
        if (stat.issue_type) {
          byType[stat.issue_type] = stat.count || 0;
        }
        
        total += stat.count || 0;
      }
    }
    
    return {
      total,
      byWorker,
      byType,
      recent: data || []
    };
  } catch (error) {
    console.error("Error processing worker issues:", error);
    return { total: 0, byWorker: {}, byType: {}, recent: [] };
  }
}

async function getDeduplicationData(redis: Redis): Promise<Record<string, any>> {
  try {
    const metrics = getDeduplicationMetrics(redis);
    
    // Get deduplication metrics for common queues
    const queues = ["artist_discovery", "album_discovery", "track_discovery", "producer_identification", "social_enrichment"];
    
    const queueMetricsPromises = queues.map(async (queueName) => {
      const queueMetrics = await metrics.getMetricsForQueue(queueName);
      return { queueName, metrics: queueMetrics };
    });
    
    const queueMetrics = await Promise.all(queueMetricsPromises);
    
    // Convert to Record
    const metricsRecord: Record<string, any> = {};
    for (const queueMetric of queueMetrics) {
      metricsRecord[queueMetric.queueName] = queueMetric.metrics;
    }
    
    return metricsRecord;
  } catch (error) {
    console.error("Error fetching deduplication metrics:", error);
    return {};
  }
}
