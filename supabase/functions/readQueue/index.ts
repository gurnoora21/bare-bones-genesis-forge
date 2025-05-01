
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "@supabase/supabase-js";

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
    const { queue_name, batch_size, visibility_timeout } = await req.json();
    
    if (!queue_name) {
      throw new Error("queue_name is required");
    }
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    const { data, error } = await supabase.rpc('pg_dequeue', {
      queue_name,
      batch_size: batch_size || 5,
      visibility_timeout: visibility_timeout || 60
    });
    
    if (error) throw error;
    
    // Parse message bodies if they're strings
    const messages = data.messages.map((msg: any) => ({
      ...msg,
      message: typeof msg.message === 'string' ? JSON.parse(msg.message) : msg.message
    }));
    
    return new Response(JSON.stringify(messages), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
    
  } catch (error) {
    console.error("Error reading from queue:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
