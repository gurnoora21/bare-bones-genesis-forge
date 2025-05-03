
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

interface QueueMonitorOptions {
  threshold_minutes?: number;
  auto_fix?: boolean;
  queues?: string[];
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const options: QueueMonitorOptions = await req.json().catch(() => ({}));
    
    const thresholdMinutes = options.threshold_minutes || 10;
    const autoFix = options.auto_fix || false;
    const queues = options.queues || [
      'artist_discovery',
      'album_discovery',
      'track_discovery',
      'producer_identification',
      'social_enrichment'
    ];
    
    console.log(`Queue monitor started with threshold: ${thresholdMinutes}m, auto-fix: ${autoFix}`);
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    const results = [];
    
    for (const queueName of queues) {
      try {
        console.log(`Checking queue: ${queueName}`);
        
        // Get stuck messages
        const { data: stuckMessages, error: stuckError } = await supabase.rpc(
          'list_stuck_messages',
          { 
            queue_name: queueName,
            min_minutes_locked: thresholdMinutes
          }
        );
        
        if (stuckError) {
          console.error(`Error listing stuck messages in ${queueName}:`, stuckError);
          results.push({
            queue: queueName,
            status: 'error',
            message: stuckError.message
          });
          continue;
        }
        
        if (!stuckMessages || stuckMessages.length === 0) {
          console.log(`No stuck messages found in queue: ${queueName}`);
          results.push({
            queue: queueName,
            status: 'ok',
            stuck_count: 0
          });
          continue;
        }
        
        console.log(`Found ${stuckMessages.length} stuck messages in queue ${queueName}`);
        
        if (autoFix) {
          try {
            const { data: fixData, error: fixError } = await supabase.rpc(
              'reset_stuck_messages',
              { 
                queue_name: queueName,
                min_minutes_locked: thresholdMinutes
              }
            );
            
            if (fixError) {
              console.error(`Error fixing stuck messages in ${queueName}:`, fixError);
              results.push({
                queue: queueName,
                status: 'fix_error',
                stuck_count: stuckMessages.length,
                stuck_messages: stuckMessages,
                error: fixError.message
              });
            } else {
              console.log(`Successfully reset ${fixData} stuck messages in queue ${queueName}`);
              results.push({
                queue: queueName,
                status: 'fixed',
                stuck_count: stuckMessages.length,
                reset_count: fixData
              });
              
              // Log to worker_issues table for tracking
              await supabase.from('worker_issues').insert({
                worker_name: queueName,
                issue_type: 'stuck_messages',
                message: `Reset ${fixData} stuck messages in queue ${queueName}`,
                details: { 
                  stuck_count: stuckMessages.length,
                  reset_count: fixData,
                  messages: stuckMessages.map(m => ({
                    id: m.id,
                    msg_id: m.msg_id,
                    minutes_locked: m.minutes_locked
                  }))
                },
                resolved: true
              });
            }
          } catch (fixError) {
            console.error(`Error in auto-fix for ${queueName}:`, fixError);
            results.push({
              queue: queueName,
              status: 'fix_exception',
              stuck_count: stuckMessages.length,
              stuck_messages: stuckMessages,
              error: fixError.message
            });
          }
        } else {
          // Just report stuck messages without fixing
          results.push({
            queue: queueName,
            status: 'stuck',
            stuck_count: stuckMessages.length,
            stuck_messages: stuckMessages
          });
          
          // Log to worker_issues table
          await supabase.from('worker_issues').insert({
            worker_name: queueName,
            issue_type: 'stuck_messages',
            message: `Found ${stuckMessages.length} stuck messages in queue ${queueName}`,
            details: { 
              stuck_count: stuckMessages.length, 
              messages: stuckMessages.map(m => ({
                id: m.id,
                msg_id: m.msg_id,
                minutes_locked: m.minutes_locked
              }))
            },
            resolved: false
          });
        }
      } catch (queueError) {
        console.error(`Error processing queue ${queueName}:`, queueError);
        results.push({
          queue: queueName,
          status: 'exception',
          error: queueError.message
        });
      }
    }
    
    console.log(`Queue monitor completed with results:`, results);
    
    return new Response(
      JSON.stringify({ results }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Error in queue monitor:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
