
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Parse request parameters with defaults
    const { threshold_minutes = 15, auto_fix = true, queue_name = null } = await req.json();
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    // Get queue status and diagnostics
    const diagnostics = await getQueueDiagnostics(supabase, queue_name);
    
    let stuckMessages = [];
    let resetResults = [];
    
    // Check for stuck messages
    if (queue_name) {
      stuckMessages = await getStuckMessages(supabase, queue_name, threshold_minutes);
    } else {
      // Get stuck messages from all queues
      const { data: queueTables, error } = await supabase.rpc('get_all_queue_tables');
      
      if (error) {
        console.error("Error getting queue tables:", error);
        throw error;
      }
      
      // For each queue, get stuck messages
      if (queueTables && queueTables.length > 0) {
        for (const qt of queueTables) {
          // Extract queue name from table name
          const queueName = qt.table_name.startsWith('q_') 
            ? qt.table_name.substring(2) 
            : qt.table_name.startsWith('pgmq_')
              ? qt.table_name.substring(5)
              : qt.table_name;
              
          const queueStuckMessages = await getStuckMessages(supabase, queueName, threshold_minutes);
          stuckMessages.push(...queueStuckMessages);
        }
      }
    }
    
    // If auto_fix is enabled, try to reset stuck messages
    if (auto_fix && stuckMessages.length > 0) {
      if (queue_name) {
        // Reset stuck messages for specific queue
        const result = await resetStuckMessages(supabase, queue_name, threshold_minutes);
        resetResults.push(result);
      } else {
        // Reset stuck messages for all queues
        const { data, error } = await supabase.rpc('reset_all_stuck_messages', {
          threshold_minutes
        });
        
        if (error) {
          console.error("Error resetting stuck messages:", error);
        } else if (data) {
          resetResults = data;
        }
      }
    }
    
    return new Response(
      JSON.stringify({ 
        diagnostics,
        stuck_messages: stuckMessages,
        reset_results: resetResults,
        auto_fix_enabled: auto_fix,
        threshold_minutes
      }),
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

async function getQueueDiagnostics(supabase, queueName = null) {
  const { data, error } = await supabase.rpc(
    'diagnose_queue_tables',
    queueName ? { queue_name: queueName } : {}
  );
  
  if (error) {
    console.error("Error getting queue diagnostics:", error);
    throw error;
  }
  
  return data;
}

async function getStuckMessages(supabase, queueName, thresholdMinutes) {
  try {
    const { data, error } = await supabase.rpc('list_stuck_messages', { 
      queue_name: queueName,
      min_minutes_locked: thresholdMinutes
    });
    
    if (error) {
      console.error(`Error inspecting queue ${queueName}:`, error);
      return [];
    }
    
    return data || [];
  } catch (error) {
    console.error(`Error getting stuck messages for queue ${queueName}:`, error);
    return [];
  }
}

async function resetStuckMessages(supabase, queueName, thresholdMinutes) {
  try {
    const { data, error } = await supabase.rpc(
      'reset_stuck_messages',
      { 
        queue_name: queueName,
        min_minutes_locked: thresholdMinutes
      }
    );
    
    if (error) {
      console.error(`Error resetting stuck messages for queue ${queueName}:`, error);
      return {
        queue_name: queueName,
        success: false,
        error: error.message
      };
    }
    
    return {
      queue_name: queueName,
      success: true,
      messages_reset: data
    };
  } catch (error) {
    console.error(`Error resetting stuck messages for queue ${queueName}:`, error);
    return {
      queue_name: queueName,
      success: false,
      error: error.message
    };
  }
}
