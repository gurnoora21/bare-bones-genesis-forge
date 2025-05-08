
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
    
    // Try to get the message from the queue using a direct SQL query
    const { data: messageExists, error } = await supabase.rpc('raw_sql_query', {
      sql_query: `
        SELECT EXISTS(
          SELECT 1 
          FROM pgmq.q_${queue_name} 
          WHERE msg_id = $1::TEXT OR id::TEXT = $1::TEXT
        ) as exists
      `,
      params: [message_id.toString()]
    });
    
    if (error) {
      console.error("Error checking queue:", error);
      throw error;
    }
    
    console.log(`Message exists check result:`, messageExists);
    
    return new Response(
      JSON.stringify({ 
        exists: messageExists?.exists || false,
        queue: queue_name,
        message_id: message_id
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
