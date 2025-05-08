
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { logWorkerIssue } from "../_shared/queueHelper.ts";

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
  
  // Initialize Redis for locking
  const redis = new Redis({
    url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
    token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
  });
  
  try {
    console.log("Running queue monitor...");
    
    // Use a lock to ensure only one queue monitor runs at a time
    const lockKey = "lock:queue_monitor";
    const lockResult = await redis.set(lockKey, "running", { 
      ex: 300,  // 5 minute expiry
      nx: true  // Only set if not exists
    });
    
    if (lockResult !== "OK") {
      console.log("Another queue monitor is already running, exiting");
      return new Response(JSON.stringify({ 
        status: "skipped",
        reason: "already_running"
      }), { headers: corsHeaders });
    }
    
    // Define queues to monitor
    const queues = [
      "artist_discovery",
      "album_discovery",
      "track_discovery",
      "producer_identification",
      "social_enrichment"
    ];
    
    const results = {};
    let totalStuckMessages = 0;
    let totalFixedMessages = 0;
    
    // Check each queue for stuck messages
    for (const queueName of queues) {
      try {
        console.log(`Checking queue ${queueName} for stuck messages...`);
        
        // List stuck messages (those with visibility timeout > 15 minutes)
        const { data: stuckMessages, error: listError } = await supabase.rpc(
          'list_stuck_messages',
          { 
            queue_name: queueName, 
            min_minutes_locked: 15
          }
        );
        
        if (listError) {
          console.error(`Error listing stuck messages for ${queueName}:`, listError);
          results[queueName] = {
            error: listError.message,
            checked: true,
            fixed: false
          };
          continue;
        }
        
        if (!stuckMessages || stuckMessages.length === 0) {
          console.log(`No stuck messages found in ${queueName}`);
          results[queueName] = {
            status: "ok",
            stuck_messages: 0
          };
          continue;
        }
        
        console.log(`Found ${stuckMessages.length} stuck messages in ${queueName}`);
        totalStuckMessages += stuckMessages.length;
        
        // Fix stuck messages that have been read too many times (likely processing failures)
        const messagesToFix = stuckMessages.filter(msg => msg.read_count >= 2);
        
        if (messagesToFix.length === 0) {
          console.log(`No messages need fixing in ${queueName} (read counts too low)`);
          results[queueName] = {
            status: "monitored",
            stuck_messages: stuckMessages.length,
            fixed_messages: 0
          };
          continue;
        }
        
        console.log(`Fixing ${messagesToFix.length} stuck messages in ${queueName}...`);
        
        // Reset visibility timeout for these messages
        let fixedCount = 0;
        for (const msg of messagesToFix) {
          const { data: resetResult, error: resetError } = await supabase.rpc(
            'reset_stuck_message',
            { 
              queue_name: queueName, 
              message_id: msg.id
            }
          );
          
          if (resetError) {
            console.error(`Error resetting message ${msg.id}:`, resetError);
          } else if (resetResult) {
            console.log(`Successfully reset message ${msg.id}`);
            fixedCount++;
          }
        }
        
        totalFixedMessages += fixedCount;
        results[queueName] = {
          status: "fixed",
          stuck_messages: stuckMessages.length,
          fixed_messages: fixedCount
        };
        
        // Record the issue if we found stuck messages
        await logWorkerIssue(
          supabase,
          "queueMonitor",
          "stuck_messages",
          `Found ${stuckMessages.length} stuck messages in ${queueName}, fixed ${fixedCount}`,
          { 
            queue_name: queueName,
            stuck_count: stuckMessages.length,
            fixed_count: fixedCount,
            stuck_messages: stuckMessages.map(m => ({ id: m.id, read_count: m.read_count, minutes_locked: m.minutes_locked }))
          }
        );
      } catch (queueError) {
        console.error(`Error monitoring queue ${queueName}:`, queueError);
        results[queueName] = {
          error: queueError.message,
          checked: true,
          fixed: false
        };
      }
    }
    
    // Release the lock when done
    await redis.del(lockKey);
    
    return new Response(JSON.stringify({
      status: "completed",
      total_stuck_messages: totalStuckMessages,
      total_fixed_messages: totalFixedMessages,
      results,
      timestamp: new Date().toISOString()
    }), { headers: corsHeaders });
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
