
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// How long to consider a message "stuck" (in minutes)
const DEFAULT_STUCK_THRESHOLD = 10;

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Parse options if provided
    const options = await req.json().catch(() => ({}));
    const {
      threshold_minutes = DEFAULT_STUCK_THRESHOLD,
      queue_names = ["artist_discovery", "album_discovery", "track_discovery", "producer_identification", "social_enrichment"],
      auto_fix = true
    } = options;

    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    console.log(`Starting queue monitor with threshold: ${threshold_minutes} minutes`);
    
    const results: Record<string, any> = {};
    let totalFound = 0;
    let totalFixed = 0;
    
    // Process each queue
    for (const queueName of queue_names) {
      console.log(`Checking queue: ${queueName}`);
      
      try {
        // Find stuck messages
        const { data: stuckMessages, error: stuckError } = await supabase.rpc(
          'list_stuck_messages',
          {
            queue_name: queueName,
            min_minutes_locked: threshold_minutes
          }
        );
        
        if (stuckError) {
          console.error(`Error listing stuck messages for ${queueName}:`, stuckError);
          results[queueName] = { error: stuckError.message };
          continue;
        }
        
        const messagesFound = stuckMessages?.length || 0;
        totalFound += messagesFound;
        
        console.log(`Found ${messagesFound} stuck messages in ${queueName}`);
        
        results[queueName] = { 
          found: messagesFound,
          fixed: 0,
          messages: []
        };
        
        if (messagesFound === 0 || !auto_fix) {
          continue;
        }
        
        // Fix stuck messages if auto_fix is true
        for (const message of stuckMessages) {
          console.log(`Attempting to fix stuck message: ${message.id} (read count: ${message.read_count}, locked for ${message.minutes_locked} minutes)`);
          
          try {
            // For messages with high read count, try deleting
            if (message.read_count > 5) {
              // Try using enhanced_delete_message
              const { data: deleteData, error: deleteError } = await supabase.rpc(
                'enhanced_delete_message',
                { 
                  p_queue_name: queueName,
                  p_message_id: message.id.toString(),
                  p_is_numeric: !isNaN(Number(message.id))
                }
              );
              
              if (!deleteError && deleteData === true) {
                console.log(`Successfully deleted stuck message ${message.id}`);
                results[queueName].messages.push({ id: message.id, action: "deleted" });
                results[queueName].fixed += 1;
                totalFixed += 1;
                continue;
              }
            }
            
            // For all other messages, or if deletion failed, reset visibility timeout
            const { data: resetData, error: resetError } = await supabase.rpc(
              'emergency_reset_message',
              { 
                p_queue_name: queueName,
                p_message_id: message.id.toString(),
                p_is_numeric: !isNaN(Number(message.id))
              }
            );
            
            if (!resetError && resetData === true) {
              console.log(`Successfully reset visibility timeout for message ${message.id}`);
              results[queueName].messages.push({ id: message.id, action: "reset" });
              results[queueName].fixed += 1;
              totalFixed += 1;
            } else {
              console.error(`Failed to reset visibility for message ${message.id}:`, resetError);
              results[queueName].messages.push({ id: message.id, action: "failed", error: resetError?.message });
            }
          } catch (msgError) {
            console.error(`Error handling stuck message ${message.id}:`, msgError);
            results[queueName].messages.push({ id: message.id, action: "failed", error: msgError.message });
          }
        }
      } catch (queueError) {
        console.error(`Error processing queue ${queueName}:`, queueError);
        results[queueName] = { error: queueError.message };
      }
    }
    
    // Log overall summary
    console.log(`Queue monitor summary: found ${totalFound} stuck messages, fixed ${totalFixed}`);
    
    return new Response(
      JSON.stringify({
        summary: {
          found: totalFound,
          fixed: totalFixed
        },
        results
      }),
      { 
        status: 200,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      }
    );
    
  } catch (error) {
    console.error("Error in queue monitor:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
