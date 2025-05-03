
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// List of all worker queues to monitor
const QUEUE_NAMES = [
  'artist_discovery',
  'album_discovery',
  'track_discovery',
  'producer_identification',
  'social_enrichment'
];

// Threshold in minutes for detecting stuck messages
const STUCK_THRESHOLD_MINUTES = 10;

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    console.log("Running queue health check");
    
    const results = {};
    let allStuckMessages = [];
    let hasStuckMessages = false;
    
    // Check each queue
    for (const queueName of QUEUE_NAMES) {
      try {
        // Get queue stats
        const { data: statsData, error: statsError } = await supabase.rpc(
          'raw_sql_query',
          {
            sql_query: `
              SELECT COUNT(*) as message_count,
                     COUNT(*) FILTER (WHERE vt IS NOT NULL) as in_progress_count,
                     MAX(EXTRACT(EPOCH FROM (NOW() - vt))/60) FILTER (WHERE vt IS NOT NULL) as max_minutes_locked
              FROM pgmq_${queueName}
            `,
            params: []
          }
        );
        
        if (statsError) throw statsError;
        
        // Look for stuck messages
        const { data: stuckData, error: stuckError } = await supabase.rpc(
          'list_stuck_messages',
          {
            queue_name: queueName,
            min_minutes_locked: STUCK_THRESHOLD_MINUTES
          }
        );
        
        if (stuckError) throw stuckError;
        
        const stuckCount = stuckData?.length || 0;
        
        // Store results for this queue
        results[queueName] = {
          total_messages: statsData?.[0]?.message_count || 0,
          in_progress: statsData?.[0]?.in_progress_count || 0,
          stuck_messages: stuckCount,
          max_minutes_locked: statsData?.[0]?.max_minutes_locked || 0,
          status: stuckCount > 0 ? 'warning' : 'healthy'
        };
        
        if (stuckCount > 0) {
          hasStuckMessages = true;
          allStuckMessages = [...allStuckMessages, ...stuckData.map(msg => ({
            queue: queueName,
            ...msg
          }))];
        }
      } catch (queueError) {
        console.error(`Error checking queue ${queueName}:`, queueError);
        results[queueName] = { status: 'error', error: queueError.message };
      }
    }
    
    // Check for worker issues
    const { data: workerIssues, error: issuesError } = await supabase
      .from('worker_issues')
      .select('*')
      .eq('resolved', false)
      .order('created_at', { ascending: false })
      .limit(20);
    
    if (issuesError) {
      console.error("Error fetching worker issues:", issuesError);
    }
    
    // Auto-fix stuck messages if requested
    const url = new URL(req.url);
    const autoFix = url.searchParams.get('auto_fix') === 'true';
    
    let fixResults = {};
    if (autoFix && hasStuckMessages) {
      console.log("Auto-fixing stuck messages");
      
      for (const queueName of QUEUE_NAMES) {
        if (results[queueName]?.stuck_messages > 0) {
          try {
            const { data: fixData, error: fixError } = await supabase.rpc(
              'reset_stuck_messages',
              {
                queue_name: queueName,
                min_minutes_locked: STUCK_THRESHOLD_MINUTES
              }
            );
            
            if (fixError) throw fixError;
            
            fixResults[queueName] = { messages_fixed: fixData };
          } catch (fixError) {
            console.error(`Error fixing stuck messages in ${queueName}:`, fixError);
            fixResults[queueName] = { error: fixError.message };
          }
        }
      }
    }
    
    // Record monitoring run
    try {
      await supabase
        .from('queue_metrics')
        .insert({
          queue_name: 'all',
          operation: 'health_check',
          started_at: new Date().toISOString(),
          finished_at: new Date().toISOString(),
          details: {
            queue_status: results,
            stuck_messages_found: hasStuckMessages,
            stuck_message_count: allStuckMessages.length,
            auto_fix_applied: autoFix,
            fix_results: Object.keys(fixResults).length > 0 ? fixResults : null
          }
        });
    } catch (metricsError) {
      console.error("Error recording monitoring metrics:", metricsError);
    }
    
    return new Response(
      JSON.stringify({
        timestamp: new Date().toISOString(),
        queues: results,
        system_status: hasStuckMessages ? 'warning' : 'healthy',
        stuck_messages: allStuckMessages.length > 0 ? allStuckMessages : null,
        worker_issues: workerIssues && workerIssues.length > 0 ? workerIssues : null,
        auto_fix_applied: autoFix,
        fix_results: Object.keys(fixResults).length > 0 ? fixResults : null
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Error monitoring queues:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
