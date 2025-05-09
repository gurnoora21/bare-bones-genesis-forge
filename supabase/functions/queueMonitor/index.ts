
// Queue Monitoring Service
// Detects stuck messages, inconsistencies, and provides queue health metrics

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getEnvironmentTTL } from "../_shared/stateManager.ts";
import { logWorkerIssue } from "../_shared/queueHelper.ts";

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );
  
  // Initialize Redis for locking
  const redis = new Redis({
    url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
    token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
  });
  
  try {
    console.log("Running queue monitor...");
    
    // Parse request body for parameters
    const body = await req.json().catch(() => ({}));
    const thresholdMinutes = body.threshold_minutes || 10;
    const autoFix = body.auto_fix === true;
    const correlationId = body.correlation_id || 
      `monitor_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
    
    // Use a lock to ensure only one queue monitor runs at a time
    const lockKey = "lock:queue_monitor";
    const lockResult = await redis.set(lockKey, correlationId, { 
      ex: 300,  // 5 minute expiry
      nx: true  // Only set if not exists
    });
    
    if (lockResult !== "OK") {
      console.log(`[${correlationId}] Another queue monitor is already running, exiting`);
      return new Response(JSON.stringify({ 
        status: "skipped",
        reason: "already_running",
        correlationId
      }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    // Define queues to monitor - get from the database
    const { data: queueTables, error: tablesError } = await supabase.rpc(
      'get_all_queue_tables'
    );
    
    if (tablesError) {
      console.error(`[${correlationId}] Error getting queue tables: ${tablesError.message}`);
      return new Response(JSON.stringify({ 
        status: "error",
        error: tablesError.message,
        correlationId 
      }), { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      });
    }
    
    const queueNames = queueTables.map((q: any) => q.queue_name);
    console.log(`[${correlationId}] Monitoring queues:`, queueNames);
    
    const results: Record<string, any> = {};
    let totalStuckMessages = 0;
    let totalFixedMessages = 0;
    
    // Check each queue for stuck messages
    for (const queueName of queueNames) {
      try {
        console.log(`[${correlationId}] Checking queue ${queueName} for stuck messages...`);
        
        // List stuck messages (those with visibility timeout > threshold minutes)
        const { data: stuckMessages, error: listError } = await supabase.rpc(
          'list_stuck_messages',
          { 
            queue_name: queueName, 
            min_minutes_locked: thresholdMinutes
          }
        );
        
        if (listError) {
          console.error(`[${correlationId}] Error listing stuck messages for ${queueName}:`, listError);
          results[queueName] = {
            error: listError.message,
            checked: true,
            fixed: false
          };
          continue;
        }
        
        if (!stuckMessages || stuckMessages.length === 0) {
          console.log(`[${correlationId}] No stuck messages found in ${queueName}`);
          results[queueName] = {
            status: "ok",
            stuck_messages: 0
          };
          continue;
        }
        
        console.log(`[${correlationId}] Found ${stuckMessages.length} stuck messages in ${queueName}`);
        totalStuckMessages += stuckMessages.length;
        
        // Fix stuck messages if autofix is enabled
        if (autoFix) {
          console.log(`[${correlationId}] Auto-fixing stuck messages in ${queueName}...`);
          
          // Reset visibility timeout for these messages
          const { data: resetResult, error: resetError } = await supabase.rpc(
            'reset_stuck_messages',
            { 
              queue_name: queueName, 
              min_minutes_locked: thresholdMinutes
            }
          );
          
          if (resetError) {
            console.error(`[${correlationId}] Error resetting stuck messages for ${queueName}:`, resetError);
            results[queueName] = {
              error: resetError.message,
              checked: true,
              fixed: false,
              stuck_messages: stuckMessages.length
            };
          } else {
            const fixedCount = resetResult || 0;
            totalFixedMessages += fixedCount;
            
            console.log(`[${correlationId}] Fixed ${fixedCount} of ${stuckMessages.length} stuck messages in ${queueName}`);
            results[queueName] = {
              status: "fixed",
              stuck_messages: stuckMessages.length,
              fixed_messages: fixedCount
            };
          }
        } else {
          // Just report the stuck messages without fixing
          results[queueName] = {
            status: "reported",
            stuck_messages: stuckMessages.length
          };
        }
        
        // Record the issue if we found stuck messages
        await logWorkerIssue(
          supabase,
          "queueMonitor",
          "stuck_messages",
          `Found ${stuckMessages.length} stuck messages in ${queueName}${autoFix ? `, fixed ${results[queueName].fixed_messages || 0}` : ''}`,
          { 
            queue_name: queueName,
            stuck_count: stuckMessages.length,
            fixed_count: autoFix ? (results[queueName].fixed_messages || 0) : 0,
            correlation_id: correlationId,
            threshold_minutes: thresholdMinutes
          }
        );
      } catch (queueError) {
        console.error(`[${correlationId}] Error monitoring queue ${queueName}:`, queueError);
        results[queueName] = {
          error: queueError.message,
          checked: true,
          fixed: false
        };
      }
    }
    
    // Check for inconsistencies if auto-fix is enabled
    let inconsistencyResults = {
      checked: false,
      found: 0,
      fixed: 0
    };
    
    if (autoFix) {
      try {
        // Call manageRedis function to check inconsistencies
        const { data: inconsistencyData, error: inconsistencyError } = await supabase.functions.invoke('manageRedis', {
          body: {
            operation: 'check-inconsistencies',
            entityType: null, // All entity types
            minutes: thresholdMinutes,
            fix: true // Fix inconsistencies
          }
        });
        
        if (inconsistencyError) {
          console.error(`[${correlationId}] Error checking inconsistencies:`, inconsistencyError);
        } else if (inconsistencyData) {
          inconsistencyResults = {
            checked: true,
            found: inconsistencyData.inconsistenciesFound || 0,
            fixed: inconsistencyData.inconsistenciesFixed || 0
          };
          
          console.log(`[${correlationId}] Found ${inconsistencyResults.found} inconsistencies, fixed ${inconsistencyResults.fixed}`);
        }
      } catch (inconsistencyError) {
        console.error(`[${correlationId}] Exception checking inconsistencies:`, inconsistencyError);
      }
    }
    
    // Release the lock when done
    await redis.del(lockKey);
    
    // Save monitoring results to metrics
    try {
      await supabase.from('queue_metrics').insert({
        queue_name: 'all',
        operation: 'queue_monitor',
        started_at: new Date().toISOString(),
        finished_at: new Date().toISOString(),
        processed_count: totalStuckMessages,
        success_count: totalFixedMessages,
        details: {
          threshold_minutes: thresholdMinutes,
          auto_fix: autoFix,
          correlation_id: correlationId,
          results,
          inconsistency_results: inconsistencyResults
        }
      });
    } catch (metricsError) {
      console.warn(`[${correlationId}] Failed to save metrics:`, metricsError);
    }
    
    return new Response(JSON.stringify({
      status: "completed",
      total_stuck_messages: totalStuckMessages,
      total_fixed_messages: totalFixedMessages,
      inconsistencies: inconsistencyResults,
      autofix_applied: autoFix,
      threshold_minutes: thresholdMinutes,
      results,
      correlation_id: correlationId,
      timestamp: new Date().toISOString()
    }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  } catch (error) {
    console.error("Error in queue monitor:", error);
    
    // Try to release the lock even on error
    try {
      await redis.del("lock:queue_monitor");
    } catch (e) {
      console.error("Error releasing lock:", e);
    }
    
    await logWorkerIssue(
      supabase,
      "queueMonitor", 
      "fatal_error", 
      `Fatal error in queue monitor: ${error.message}`, 
      { error: error.stack }
    );
    
    return new Response(JSON.stringify({ 
      status: "error", 
      error: error.message 
    }), { 
      status: 500, 
      headers: corsHeaders 
    });
  }
});
