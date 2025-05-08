
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
    // Parse request body
    const { queue_name, message_id } = await req.json();
    
    if (!queue_name || !message_id) {
      return new Response(
        JSON.stringify({ error: "queue_name and message_id are required" }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    // Initialize Supabase client
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL") || "",
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || ""
    );
    
    console.log(`Checking if message ${message_id} exists in queue ${queue_name}`);
    
    // Try to get the message from the queue using the raw_sql_query function we created
    const { data, error } = await supabase.rpc('raw_sql_query', {
      sql_query: `
        SELECT EXISTS(
          SELECT 1 
          FROM pgmq.q_${queue_name}
          WHERE msg_id = $1::TEXT OR id::TEXT = $1::TEXT OR id = $1::UUID
        ) AS exists
      `,
      params: [message_id.toString()]
    });
    
    if (error) {
      console.error("Error checking queue:", error);
      
      // Try an alternative approach
      const { data: fallbackResult, error: fallbackError } = await supabase.rpc('raw_sql_query', {
        sql_query: `
          SELECT EXISTS(
            SELECT 1 
            FROM pgmq.q_${queue_name}
            WHERE msg_id::TEXT = $1 OR id::TEXT = $1
          ) AS exists
        `,
        params: [message_id.toString()]
      });
      
      if (fallbackError) {
        throw new Error(`Both primary and fallback verification methods failed: ${error.message}, ${fallbackError.message}`);
      }
      
      console.log(`Fallback exists check result:`, fallbackResult);
      
      return new Response(
        JSON.stringify({ 
          exists: fallbackResult?.exists || false,
          queue: queue_name,
          message_id: message_id,
          method: "fallback"
        }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    console.log(`Message exists check result:`, data);
    
    return new Response(
      JSON.stringify({ 
        exists: data?.exists || false,
        queue: queue_name,
        message_id: message_id,
        method: "primary"
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error("Error checking queue message:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
