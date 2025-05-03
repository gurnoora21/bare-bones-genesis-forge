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
    
    // Get diagnostic info about all queue tables
    const { data: diagData, error: diagError } = await supabase.rpc(
      'diagnose_queue_tables'
    );
    
    if (diagError) {
      console.error("Error getting queue table diagnostics:", diagError);
    } else {
      console.log("Queue table diagnostics:", diagData);
    }
    
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
            // Try our new cross-schema operation for messages that have been stuck for too long
            if (message.minutes_locked > 30 || message.read_count > 5) {
              const { data: crossSchemaResult, error: crossSchemaError } = await supabase.rpc(
                'cross_schema_queue_op',
                { 
                  p_operation: 'delete',
                  p_queue_name: queueName,
                  p_message_id: messageId
                }
              );
              
              if (!crossSchemaError && crossSchemaResult && crossSchemaResult.success === true) {
                console.log(`Successfully deleted stuck message ${messageId} from ${queueName} using cross-schema op`);
                fixedCount++;
                continue;
              }
              
              // If direct delete failed but message is very old, try force delete
              if (message.minutes_locked > 60) {
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
                      bypass_checks: true // Enable aggressive deletion for very old messages
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
            }
            
            // Otherwise try the standard reset function
            const { data: resetResult, error: resetError } = await supabase.rpc(
              'cross_schema_queue_op',
              { 
                p_operation: 'reset',
                p_queue_name: queueName,
                p_message_id: messageId
              }
            );
            
            if (!resetError && resetResult && resetResult.success === true) {
              console.log(`Reset stuck message ${messageId} in ${queueName}`);
              fixedCount++;
            } else if (resetError) {
              console.error(`Error resetting stuck message in ${queueName}:`, resetError);
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
      queue_table_diagnostics: diagData,
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
