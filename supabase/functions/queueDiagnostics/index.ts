
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
    const { action, queue_name, message_id, limit = 10, include_content = false } = await req.json();
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    // Initialize results
    let result: any;
    
    if (action === "list_queues") {
      // List all PGMQ queues
      const { data, error } = await supabase.rpc('diagnose_queue_tables');
      
      if (error) throw error;
      result = { queues: data };
    } 
    else if (action === "list_messages") {
      if (!queue_name) throw new Error("queue_name is required for list_messages action");
      
      // List messages in a queue with pagination
      const { data: messages, error: messagesError } = await supabase
        .rpc('list_stuck_messages', { 
          queue_name, 
          min_minutes_locked: 0 // Include all messages
        });
      
      if (messagesError) throw messagesError;
      
      // Get summary of queue
      const { data: queueStats, error: statsError } = await supabase
        .rpc('pg_queue_status', { queue_name });
      
      if (statsError) throw statsError;
      
      result = { 
        queue_name,
        stats: queueStats,
        messages: messages || []
      };
    }
    else if (action === "inspect_message") {
      if (!queue_name || !message_id) {
        throw new Error("queue_name and message_id are required for inspect_message action");
      }
      
      // Get detailed info about a single message
      const sql = `
        SELECT * FROM pgmq.q_${queue_name}
        WHERE id::TEXT = '${message_id}' OR msg_id::TEXT = '${message_id}'
      `;
      
      const { data, error } = await supabase.rpc('raw_sql_query', { 
        sql_query: sql
      });
      
      if (error) throw error;
      result = { message: data };
    }
    else if (action === "fix_stuck_messages") {
      if (!queue_name) throw new Error("queue_name is required for fix_stuck_messages action");
      
      // Reset visibility timeout for stuck messages
      const { data, error } = await supabase.rpc('reset_stuck_messages', { 
        queue_name,
        min_minutes_locked: 10 // Only reset messages locked for at least 10 min
      });
      
      if (error) throw error;
      result = { 
        queue_name,
        reset_count: data || 0,
        success: true
      };
    }
    else if (action === "problematic_messages") {
      // List problematic messages recorded in the queue_mgmt schema
      const sql = `
        SELECT * FROM queue_mgmt.problematic_messages
        ${queue_name ? `WHERE queue_name = '${queue_name}'` : ''}
        ORDER BY created_at DESC
        LIMIT ${limit}
      `;
      
      const { data, error } = await supabase.rpc('raw_sql_query', { 
        sql_query: sql
      });
      
      if (error) throw error;
      result = { problematic_messages: data };
    }
    else {
      throw new Error(`Unknown action: ${action}`);
    }
    
    return new Response(
      JSON.stringify(result),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Error in queueDiagnostics:", error);
    
    return new Response(
      JSON.stringify({ 
        error: error.message,
        stack: error.stack
      }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
