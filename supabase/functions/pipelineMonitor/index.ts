
/**
 * Pipeline Monitor Edge Function
 * 
 * Provides comprehensive monitoring for the music producer discovery pipeline:
 * - Queue depths and processing rates
 * - Worker health and error rates
 * - System scaling recommendations
 * - Pipeline status by artist
 */
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { PgQueueManager, getQueueManager } from "../_shared/pgQueueManager.ts";

// Initialize Supabase client
const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const supabase = createClient(supabaseUrl, supabaseKey);

// Initialize queue manager
const queueManager = getQueueManager(supabase);

// CORS headers for browser access
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Standard queue list for the pipeline
const standardQueues = [
  'artist_discovery',
  'album_discovery',
  'track_discovery',
  'producer_identification',
  'social_enrichment'
];

// Monitor specific artist's pipeline progress
async function monitorArtistProgress(artistId: string): Promise<any> {
  try {
    // Call the SQL function to check pipeline progress
    const { data, error } = await supabase.rpc('check_artist_pipeline_progress', {
      artist_id: artistId
    });
    
    if (error) {
      throw new Error(`Error checking artist progress: ${error.message}`);
    }
    
    return data;
  } catch (error) {
    console.error(`Error monitoring artist progress: ${error.message}`);
    throw error;
  }
}

// Get queue depths for all pipeline queues
async function getQueueDepths(): Promise<Record<string, any>> {
  const queueStatuses: Record<string, any> = {};
  
  try {
    // Try to get queues from registry for dynamically registered queues
    const { data: registeredQueues, error } = await supabase
      .from('queue_registry')
      .select('queue_name, active')
      .eq('active', true);
    
    // Default to standard list if registry query fails
    const queueList = error || !registeredQueues 
      ? standardQueues 
      : registeredQueues.map(q => q.queue_name);
      
    // Get status for each queue
    for (const queueName of queueList) {
      try {
        const status = await queueManager.getQueueStatus(queueName);
        queueStatuses[queueName] = status;
      } catch (queueError) {
        console.error(`Error getting status for queue ${queueName}: ${queueError.message}`);
        queueStatuses[queueName] = { error: queueError.message };
      }
    }
    
    return queueStatuses;
  } catch (error) {
    console.error(`Error getting queue depths: ${error.message}`);
    return { error: error.message };
  }
}

// Get recent metrics from queue_metrics table
async function getRecentMetrics(hoursBack: number = 24): Promise<any> {
  try {
    const { data, error } = await supabase
      .from('queue_metrics')
      .select('*')
      .gte('created_at', new Date(Date.now() - hoursBack * 60 * 60 * 1000).toISOString())
      .order('created_at', { ascending: false });
    
    if (error) {
      throw new Error(`Error fetching recent metrics: ${error.message}`);
    }
    
    // Group by queue_name for analysis
    const metricsByQueue: Record<string, any[]> = {};
    
    for (const metric of (data || [])) {
      if (!metricsByQueue[metric.queue_name]) {
        metricsByQueue[metric.queue_name] = [];
      }
      metricsByQueue[metric.queue_name].push(metric);
    }
    
    // Calculate summary stats per queue
    const summaryStats: Record<string, any> = {};
    
    for (const [queueName, metrics] of Object.entries(metricsByQueue)) {
      const totalProcessed = metrics.reduce((sum, m) => sum + (m.processed_count || 0), 0);
      const totalErrors = metrics.reduce((sum, m) => sum + (m.error_count || 0), 0);
      const avgProcessingTime = metrics.reduce((sum, m) => {
        const processingTime = m.details?.processing_time_ms || 0;
        return processingTime > 0 ? sum + processingTime : sum;
      }, 0) / (metrics.length || 1);
      
      summaryStats[queueName] = {
        metrics_count: metrics.length,
        total_processed: totalProcessed,
        total_errors: totalErrors,
        error_rate: totalProcessed > 0 ? (totalErrors / totalProcessed) : 0,
        avg_processing_time_ms: Math.round(avgProcessingTime),
        last_run: metrics[0]?.created_at
      };
    }
    
    return {
      summary: summaryStats,
      recent_metrics: metricsByQueue
    };
  } catch (error) {
    console.error(`Error fetching metrics: ${error.message}`);
    return { error: error.message };
  }
}

// Get recent worker issues
async function getWorkerIssues(hoursBack: number = 24, limit: number = 100): Promise<any> {
  try {
    const { data, error } = await supabase
      .from('worker_issues')
      .select('*')
      .gte('created_at', new Date(Date.now() - hoursBack * 60 * 60 * 1000).toISOString())
      .order('created_at', { ascending: false })
      .limit(limit);
    
    if (error) {
      throw new Error(`Error fetching worker issues: ${error.message}`);
    }
    
    // Group by worker_name and issue_type
    const issuesByWorker: Record<string, any> = {};
    const issuesByType: Record<string, number> = {};
    
    for (const issue of (data || [])) {
      // Count by worker
      if (!issuesByWorker[issue.worker_name]) {
        issuesByWorker[issue.worker_name] = {
          count: 0,
          issues: []
        };
      }
      issuesByWorker[issue.worker_name].count++;
      issuesByWorker[issue.worker_name].issues.push(issue);
      
      // Count by type
      if (!issuesByType[issue.issue_type]) {
        issuesByType[issue.issue_type] = 0;
      }
      issuesByType[issue.issue_type]++;
    }
    
    return {
      total_issues: data?.length || 0,
      by_worker: issuesByWorker,
      by_type: issuesByType,
      recent_issues: data?.slice(0, 10) // Just return most recent 10 for display
    };
  } catch (error) {
    console.error(`Error fetching worker issues: ${error.message}`);
    return { error: error.message };
  }
}

// Generate scaling recommendations based on queue depths and processing rates
async function getScalingRecommendations(): Promise<any> {
  try {
    const queueDepths = await getQueueDepths();
    const recentMetrics = await getRecentMetrics(6); // Last 6 hours
    
    const recommendations: Record<string, any> = {};
    
    // For each standard queue, analyze and make recommendations
    for (const queue of standardQueues) {
      const depth = queueDepths[queue]?.count || 0;
      const metrics = recentMetrics.summary[queue];
      
      // Skip if no metrics data available
      if (!metrics) {
        recommendations[queue] = {
          status: "unknown",
          message: "Insufficient metrics data"
        };
        continue;
      }
      
      const processingRate = metrics.total_processed / 6; // Messages per hour
      const hoursToEmpty = processingRate > 0 ? depth / processingRate : 0;
      
      // Generate recommendations based on backlog
      if (depth === 0) {
        recommendations[queue] = {
          status: "optimal",
          message: "Queue empty, no scaling needed"
        };
      } else if (hoursToEmpty < 1) {
        recommendations[queue] = {
          status: "optimal",
          message: "Queue will be processed within an hour, no scaling needed",
          estimated_time_to_empty: `${(hoursToEmpty * 60).toFixed(0)} minutes`
        };
      } else if (hoursToEmpty < 4) {
        recommendations[queue] = {
          status: "acceptable",
          message: "Queue backlog is moderate",
          estimated_time_to_empty: `${hoursToEmpty.toFixed(1)} hours`
        };
      } else if (hoursToEmpty < 24) {
        recommendations[queue] = {
          status: "attention",
          message: "Consider increasing worker frequency or batch size",
          estimated_time_to_empty: `${hoursToEmpty.toFixed(1)} hours`,
          suggestion: "Increase worker batch size or reduce worker interval"
        };
      } else {
        recommendations[queue] = {
          status: "critical",
          message: "Significant backlog detected",
          estimated_time_to_empty: `${(hoursToEmpty / 24).toFixed(1)} days`,
          suggestion: "Deploy additional worker instances or increase batch size significantly"
        };
      }
      
      // Add context metrics
      recommendations[queue].metrics = {
        current_depth: depth,
        processing_rate: `${processingRate.toFixed(1)} msgs/hour`,
        error_rate: `${(metrics.error_rate * 100).toFixed(1)}%`
      };
    }
    
    return recommendations;
  } catch (error) {
    console.error(`Error generating scaling recommendations: ${error.message}`);
    return { error: error.message };
  }
}

// Main handler function
serve(async (req) => {
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }
  
  try {
    // Parse URL to get operation and parameters
    const url = new URL(req.url);
    const operation = url.searchParams.get('operation') || 'status';
    
    let responseData: any = {};
    
    // Handle different operations
    switch (operation) {
      case 'status':
        // Basic status - queue depths and recent metrics
        const [queueDepths, recentMetrics, workerIssues] = await Promise.all([
          getQueueDepths(),
          getRecentMetrics(6),  // Last 6 hours
          getWorkerIssues(24)   // Last 24 hours
        ]);
        
        responseData = {
          timestamp: new Date().toISOString(),
          queue_depths: queueDepths,
          metrics_summary: recentMetrics.summary,
          issues_summary: {
            total: workerIssues.total_issues,
            by_type: workerIssues.by_type
          }
        };
        break;
        
      case 'artist':
        // Check specific artist's pipeline progress
        const artistId = url.searchParams.get('artistId');
        if (!artistId) {
          return new Response(
            JSON.stringify({ error: "artistId parameter is required" }),
            { 
              status: 400,
              headers: { ...corsHeaders, 'Content-Type': 'application/json' }
            }
          );
        }
        responseData = await monitorArtistProgress(artistId);
        break;
        
      case 'queues':
        // Detailed queue information
        responseData = await getQueueDepths();
        break;
        
      case 'metrics':
        // Detailed metrics
        const hoursBack = parseInt(url.searchParams.get('hours') || '24', 10);
        responseData = await getRecentMetrics(hoursBack);
        break;
        
      case 'issues':
        // Worker issues
        const issueHours = parseInt(url.searchParams.get('hours') || '24', 10);
        const issueLimit = parseInt(url.searchParams.get('limit') || '100', 10);
        responseData = await getWorkerIssues(issueHours, issueLimit);
        break;
        
      case 'recommendations':
        // Scaling recommendations
        responseData = await getScalingRecommendations();
        break;
        
      default:
        responseData = { 
          error: "Unknown operation", 
          available_operations: ["status", "artist", "queues", "metrics", "issues", "recommendations"]
        };
    }
    
    return new Response(
      JSON.stringify(responseData),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      }
    );
  } catch (error) {
    console.error("Error in pipeline monitor:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      }
    );
  }
});
