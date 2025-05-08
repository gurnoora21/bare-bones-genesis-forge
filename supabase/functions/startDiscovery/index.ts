
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
    let artistName;
    try {
      const body = await req.json();
      artistName = body.artistName;
      
      if (!artistName) {
        return new Response(
          JSON.stringify({ error: "artistName is required" }),
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    } catch (parseError) {
      console.error("Error parsing request body:", parseError);
      return new Response(
        JSON.stringify({ error: "Invalid request body - must be valid JSON with artistName" }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    // Initialize Supabase client
    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    
    if (!supabaseUrl || !supabaseServiceKey) {
      throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables");
    }
    
    const supabase = createClient(supabaseUrl, supabaseServiceKey);
    
    console.log(`Starting artist discovery for: ${artistName}`);

    // Create a unique idempotency key for this request
    const idempotencyKey = `artist:name:${artistName.toLowerCase()}`;

    // Use direct pg_enqueue RPC function for message insertion
    const { data: messageId, error: enqueueError } = await supabase.rpc('pg_enqueue', {
      queue_name: 'artist_discovery',
      message_body: { artistName, _idempotencyKey: idempotencyKey }
    });
    
    if (enqueueError) {
      console.error("Failed to enqueue artist discovery:", enqueueError);
      throw new Error(`Failed to enqueue artist discovery: ${enqueueError.message}`);
    }
    
    if (!messageId) {
      throw new Error("Failed to get message ID after enqueueing");
    }
    
    console.log(`Successfully enqueued artist discovery, message ID: ${messageId}`);
    
    // Manually trigger the artist discovery worker to process immediately
    console.log("Triggering artist discovery worker");
    
    try {
      const workerResponse = await supabase.functions.invoke('artistDiscovery', {
        body: { triggeredManually: true }
      });
      
      if (workerResponse.error) {
        console.warn("Could not trigger worker via functions.invoke:", workerResponse.error);
      } else {
        console.log("Artist discovery worker triggered successfully");
      }
    } catch (triggerError) {
      console.warn("Error triggering worker:", triggerError);
      console.log("Worker will be triggered by scheduled cron job instead");
    }
    
    // Return success response
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
