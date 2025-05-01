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
    // Expect { artistName: "Artist Name" } in the request body
    const { artistName } = await req.json();
    
    if (!artistName) {
      return new Response(
        JSON.stringify({ error: "artistName is required" }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    // Enqueue artist discovery task
    const { data: messageId, error } = await supabase.rpc('pg_enqueue', {
      queue_name: 'artist_discovery',
      message_body: JSON.stringify({ artistName })
    });
    
    if (error) throw error;
    
    return new Response(
      JSON.stringify({ 
        success: true, 
        message: `Discovery process started for artist: ${artistName}`,
        messageId
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Error starting discovery:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
