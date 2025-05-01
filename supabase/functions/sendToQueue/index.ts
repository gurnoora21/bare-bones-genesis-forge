
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
    const { queue_name, message } = await req.json();
    
    if (!queue_name || !message) {
      throw new Error("queue_name and message are required");
    }
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    console.log(`Sending message to queue ${queue_name}: ${JSON.stringify(message)}`);
    
    const { data: messageId, error } = await supabase.rpc('pg_enqueue', {
      queue_name,
      message_body: JSON.stringify(message)
    });
    
    if (error) {
      console.error(`Error sending to queue ${queue_name}:`, error);
      throw error;
    }
    
    console.log(`Successfully sent message to queue ${queue_name} with ID: ${messageId}`);
    
    return new Response(
      JSON.stringify({ success: true, messageId }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Error sending to queue:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
