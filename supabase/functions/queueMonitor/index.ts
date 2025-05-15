
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
    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const supabaseServiceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    
    if (!supabaseUrl || !supabaseServiceRoleKey) {
      console.error("Missing environment variables");
      return new Response(
        JSON.stringify({ error: "Server configuration error" }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const supabase = createClient(supabaseUrl, supabaseServiceRoleKey);
    
    // Parse request body for threshold settings
    const { threshold_minutes = 10, auto_fix = true } = 
      req.method === 'POST' ? await req.json() : {};
    
    // Reset stuck messages if requested
    const { data: results, error } = await supabase.rpc(
      'reset_all_stuck_messages', 
      { threshold_minutes }
    );
    
    if (error) {
      console.error("Error resetting stuck messages:", error);
      return new Response(
        JSON.stringify({ error: error.message }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    console.log(`Queue monitor executed: reset stuck messages older than ${threshold_minutes} minutes`);
    
    // Report on results
    const fixedMessages = results || [];
    const queueCounts = {};
    
    // Group by queue
    fixedMessages.forEach(msg => {
      const queue = msg.queue_name;
      queueCounts[queue] = (queueCounts[queue] || 0) + 1;
    });
    
    return new Response(
      JSON.stringify({ 
        success: true, 
        fixed_messages: fixedMessages,
        counts_by_queue: queueCounts,
        total_fixed: fixedMessages.length,
        timestamp: new Date().toISOString() 
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error("Error in queueMonitor:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
