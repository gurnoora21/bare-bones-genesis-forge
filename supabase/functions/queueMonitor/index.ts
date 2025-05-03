
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
      try {
        // Get diagnostic info for this queue
        const { data: queueDiag } = await supabase.rpc(
          'diagnose_queue_tables',
          { queue_name: queueName }
        );
        
        if (!queueDiag || !queueDiag.queue_tables || queueDiag.queue_tables.length === 0) {
          results[queueName] = { 
            stuck: 0, 
            message: "No queue tables found for this queue name",
            diagnostics: queueDiag
          };
          continue;
        }
        
        // Get the table name to use
        const fullTableName = queueDiag.queue_tables[0].full_name;
        console.log(`Checking for stuck messages in ${fullTableName}`);
        
        // Run direct SQL query to find stuck messages
        const { data: stuckMessages } = await supabase.rpc(
          'raw_sql_query',
          {
            sql_query: `
              SELECT 
                id::TEXT, 
                msg_id::TEXT, 
                message::TEXT AS message_text,
                vt AS locked_since, 
                read_ct AS read_count,
                EXTRACT(EPOCH FROM (NOW() - vt))/60 AS minutes_locked
              FROM ${fullTableName}
              WHERE 
                vt IS NOT NULL 
                AND vt < NOW() - INTERVAL '${threshold_minutes} minutes'
              ORDER BY vt ASC
            `
          }
        );
        
        if (!stuckMessages || stuckMessages.length === 0) {
          results[queueName] = { stuck: 0, message: "No stuck messages" };
          continue;
        }
        
        totalStuck += stuckMessages.length;
        
        results[queueName] = {
          stuck: stuckMessages.length,
          table_name: fullTableName,
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
              // Use forceDeleteMessage which handles both schemas
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
                    bypass_checks: true // Enable aggressive deletion for problematic messages
                  })
                }
              );
              
              const forceResult = await forceResponse.json();
              
              if (forceResponse.ok && (forceResult.success || forceResult.reset)) {
                console.log(`Successfully force-handled message ${messageId} in ${queueName}`);
                fixedCount++;
              } else {
                console.error(`Failed to handle message ${messageId}:`, forceResult);
              }
            } catch (fixError) {
              console.error(`Error fixing stuck message ${messageId} in ${queueName}:`, fixError);
            }
          }
        }
      } catch (queueError) {
        console.error(`Error processing queue ${queueName}:`, queueError);
        results[queueName] = { error: queueError.message };
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
