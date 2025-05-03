import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

const QUEUE_NAMES = [
  'artist_discovery',
  'album_discovery',
  'track_discovery',
  'producer_identification',
  'social_enrichment'
];

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const { threshold_minutes = 10, auto_fix = false } = await req.json();
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    const results: any = {};
    let totalStuck = 0;
    let fixedCount = 0;
    
    // Check each queue for stuck messages
    for (const queueName of QUEUE_NAMES) {
      const { data: stuckMessages, error } = await supabase.rpc(
        'list_stuck_messages',
        { 
          queue_name: queueName, 
          min_minutes_locked: threshold_minutes 
        }
      );
      
      if (error) {
        console.error(`Error checking queue ${queueName}:`, error);
        results[queueName] = { error: error.message };
        continue;
      }
      
      if (!stuckMessages || stuckMessages.length === 0) {
        results[queueName] = { stuck: 0, message: "No stuck messages" };
        continue;
      }
      
      totalStuck += stuckMessages.length;
      
      results[queueName] = {
        stuck: stuckMessages.length,
        messages: stuckMessages.map((m: any) => ({
          id: m.id || m.msg_id,
          minutes_locked: m.minutes_locked,
          read_count: m.read_count
        }))
      };
      
      // Auto fix stuck messages if requested
      if (auto_fix) {
        for (const message of stuckMessages) {
          const messageId = message.id || message.msg_id;
          
          try {
            // Try force-delete first for messages that have been stuck for too long
            // or have been read too many times
            if (message.minutes_locked > 30 || message.read_count > 5) {
              // Use the forceDeleteMessage function
              const forceResponse = await fetch(
                `${Deno.env.get("SUPABASE_URL")}/functions/v1/forceDeleteMessage`,
                {
                  method: "POST",
                  headers: {
                    "Content-Type": "application/json",
                    "Authorization": `Bearer ${Deno.env.get("SUPABASE_ANON_KEY")}`
                  },
                  body: JSON.stringify({
                    queue_name: queueName,
                    message_id: messageId,
                    bypass_checks: message.minutes_locked > 60 // Use aggressive deletion for very old messages
                  })
                }
              );
              
              const forceResult = await forceResponse.json();
              
              if (forceResponse.ok && (forceResult.success || forceResult.reset)) {
                console.log(`Successfully force-handled message ${messageId} in ${queueName}`);
                fixedCount++;
                continue;
              }
            }
            
            // Otherwise try the standard reset function
            const { data: resetResult, error: resetError } = await supabase.rpc(
              'reset_stuck_messages',
              { 
                queue_name: queueName, 
                min_minutes_locked: threshold_minutes 
              }
            );
            
            if (!resetError && resetResult > 0) {
              console.log(`Reset ${resetResult} stuck messages in ${queueName}`);
              fixedCount += resetResult;
            } else if (resetError) {
              console.error(`Error resetting stuck messages in ${queueName}:`, resetError);
            }
          } catch (fixError) {
            console.error(`Error fixing stuck message ${messageId} in ${queueName}:`, fixError);
          }
        }
      }
    }
    
    // Summary results
    const summary = {
      total_queues_checked: QUEUE_NAMES.length,
      total_stuck_messages: totalStuck,
      threshold_minutes: threshold_minutes,
      auto_fix_enabled: auto_fix,
      messages_fixed: fixedCount,
      details: results,
      timestamp: new Date().toISOString()
    };
    
    // Log the results as well
    if (totalStuck > 0) {
      console.log(`Found ${totalStuck} stuck messages across ${QUEUE_NAMES.length} queues`);
      if (auto_fix) {
        console.log(`Fixed ${fixedCount} stuck messages`);
      }
    } else {
      console.log(`No stuck messages found in any queue`);
    }
    
    return new Response(
      JSON.stringify(summary),
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
