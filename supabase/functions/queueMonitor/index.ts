
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { generateCorrelationId } from "../_shared/stateManager.ts";
import { CoordinatedStateManager, getStateManager } from "../_shared/coordinatedStateManager.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
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
  
  // Parse request body
  let params = { threshold_minutes: 10, auto_fix: false };
  try {
    const requestBody = await req.json();
    params = { ...params, ...requestBody };
  } catch (e) {
    // Use default values if parsing fails
  }
  
  // Generate a unique correlation ID for this monitor run
  const monitorId = generateCorrelationId('monitor');
  
  try {
    // Check if another monitor is already running
    const redis = new Redis({
      url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
      token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
    });
    
    const monitorLockKey = "queue_monitor:running";
    const lockResult = await redis.set(monitorLockKey, monitorId, { nx: true, ex: 120 });
    
    if (lockResult !== "OK") {
      console.log(`[${monitorId}] Another queue monitor is already running, exiting`);
      return new Response(JSON.stringify({ 
        success: true, 
        message: "Another monitor is already running" 
      }), { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      });
    }
    
    console.log(`Running queue monitor...`);
    
    // Get all queue names first
    const { data: queueTables, error: queueError } = await supabase.rpc('get_all_queue_tables');
    
    if (queueError) {
      console.error(`[${monitorId}] Error getting queue tables:`, queueError.message);
      return new Response(JSON.stringify({ error: queueError.message }), { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      });
    }
    
    if (!queueTables || queueTables.length === 0) {
      console.log(`[${monitorId}] No queue tables found`);
      return new Response(JSON.stringify({ 
        success: true, 
        message: "No queue tables found" 
      }), { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      });
    }
    
    // Process each queue table
    const results = [];
    for (const queueTable of queueTables) {
      try {
        const queueName = queueTable.queue_name;
        console.log(`[${monitorId}] Checking queue: ${queueName}`);
        
        // Get stuck messages
        const { data: stuckMessages, error: stuckError } = await supabase.rpc(
          'list_stuck_messages',
          { 
            queue_name: queueName, 
            min_minutes_locked: params.threshold_minutes 
          }
        );
        
        if (stuckError) {
          console.error(`[${monitorId}] Error listing stuck messages for ${queueName}:`, stuckError.message);
          results.push({
            queue: queueName,
            success: false,
            error: stuckError.message
          });
          continue;
        }
        
        if (!stuckMessages || stuckMessages.length === 0) {
          console.log(`[${monitorId}] No stuck messages in queue: ${queueName}`);
          results.push({
            queue: queueName,
            success: true,
            stuckMessages: 0
          });
          continue;
        }
        
        console.log(`[${monitorId}] Found ${stuckMessages.length} stuck messages in queue: ${queueName}`);
        
        // Automatically fix stuck messages if requested
        if (params.auto_fix) {
          let fixedCount = 0;
          
          try {
            // Use reset_stuck_messages function
            const { data: resetResult, error: resetError } = await supabase.rpc(
              'reset_stuck_messages',
              { 
                queue_name: queueName, 
                min_minutes_locked: params.threshold_minutes 
              }
            );
            
            if (resetError) {
              console.error(`[${monitorId}] Error resetting stuck messages for ${queueName}:`, resetError.message);
            } else {
              fixedCount = resetResult || 0;
              console.log(`[${monitorId}] Reset ${fixedCount} stuck messages in queue: ${queueName}`);
            }
          } catch (resetError) {
            console.error(`[${monitorId}] Exception resetting stuck messages:`, resetError);
          }
          
          results.push({
            queue: queueName,
            success: true,
            stuckMessages: stuckMessages.length,
            fixed: fixedCount,
            messages: stuckMessages.slice(0, 10).map(m => ({
              id: m.id,
              minutes_locked: m.minutes_locked
            }))
          });
        } else {
          // Just report the stuck messages
          results.push({
            queue: queueName,
            success: true,
            stuckMessages: stuckMessages.length,
            messages: stuckMessages.slice(0, 10).map(m => ({
              id: m.id,
              minutes_locked: m.minutes_locked
            }))
          });
        }
      } catch (queueError) {
        console.error(`[${monitorId}] Error processing queue ${queueTable.queue_name}:`, queueError);
        results.push({
          queue: queueTable.queue_name,
          success: false,
          error: queueError.message
        });
      }
    }
    
    // Also check for inconsistent states between Redis and DB
    try {
      console.log(`[${monitorId}] Checking for inconsistent states...`);
      
      const stateManager = getStateManager(supabase, redis) as CoordinatedStateManager;
      const fixedCount = await stateManager.reconcileInconsistentStates(undefined, params.threshold_minutes);
      
      console.log(`[${monitorId}] Fixed ${fixedCount} inconsistent states`);
      
      results.push({
        type: 'state_reconciliation',
        success: true,
        inconsistentStatesFixed: fixedCount
      });
    } catch (stateError) {
      console.error(`[${monitorId}] Error reconciling states:`, stateError);
      results.push({
        type: 'state_reconciliation',
        success: false,
        error: stateError.message
      });
    }
    
    // Clean up stuck processing states
    try {
      console.log(`[${monitorId}] Cleaning up stuck processing states...`);
      
      const { data: cleanupResult, error: cleanupError } = await supabase.rpc('cleanup_stuck_processing_states');
      
      if (cleanupError) {
        console.error(`[${monitorId}] Error cleaning up stuck processing states:`, cleanupError.message);
        results.push({
          type: 'processing_states_cleanup',
          success: false,
          error: cleanupError.message
        });
      } else {
        console.log(`[${monitorId}] Cleaned up ${cleanupResult} stuck processing states`);
        results.push({
          type: 'processing_states_cleanup',
          success: true,
          cleanedStates: cleanupResult
        });
      }
    } catch (cleanupError) {
      console.error(`[${monitorId}] Exception cleaning up stuck processing states:`, cleanupError);
      results.push({
        type: 'processing_states_cleanup',
        success: false,
        error: cleanupError.message
      });
    }
    
    // Release the lock when done
    try {
      const currentLock = await redis.get(monitorLockKey);
      if (currentLock === monitorId) {
        await redis.del(monitorLockKey);
      }
    } catch (e) {
      console.warn(`[${monitorId}] Error releasing monitor lock:`, e);
    }
    
    return new Response(JSON.stringify({ 
      success: true, 
      monitorId,
      results
    }), { 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
    });
    
  } catch (error) {
    console.error(`[${monitorId}] Unexpected error in queue monitor:`, error);
    return new Response(JSON.stringify({ 
      error: error.message
    }), { 
      status: 500, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
    });
  }
});
