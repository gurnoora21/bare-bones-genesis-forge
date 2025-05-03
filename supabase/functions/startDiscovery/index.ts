
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
    
    console.log(`Attempting to enqueue artist discovery for: ${artistName}`);
    
    // Use pg_enqueue RPC function for message insertion
    const { data: messageId, error } = await supabase.rpc('pg_enqueue', {
      queue_name: 'artist_discovery',
      message_body: { artistName } // This will be automatically converted to JSONB
    });
    
    if (error) {
      console.error("Queue error:", error);
      throw error;
    }
    
    console.log(`Successfully enqueued artist discovery, message ID: ${messageId}`);
    
    // Manually trigger the artist discovery worker to process immediately
    try {
      await fetch(
        `${Deno.env.get("SUPABASE_URL")}/functions/v1/artistDiscovery`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${Deno.env.get("SUPABASE_ANON_KEY")}`
          }
        }
      );
      console.log("Artist discovery worker triggered successfully");
    } catch (triggerError) {
      console.warn("Could not trigger worker directly, queue will be processed on schedule:", triggerError);
    }
    
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
