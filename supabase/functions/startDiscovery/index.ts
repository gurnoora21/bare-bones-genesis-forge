
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

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
    
    console.log(`Attempting to enqueue artist discovery for: ${artistName}`);

    // Create a unique idempotency key for this request
    const idempotencyKey = `artist:name:${artistName.toLowerCase()}`;

    // Method 1: Use the direct pg_enqueue RPC function for message insertion
    const { data: messageId, error: enqueueError } = await supabase.rpc('pg_enqueue', {
      queue_name: 'artist_discovery',
      message_body: { artistName, _idempotencyKey: idempotencyKey }
    });
    
    if (enqueueError) {
      console.error("Queue error from pg_enqueue:", enqueueError);
      
      // Fall back to Method 2: Try sendToQueue function 
      try {
        console.log("Falling back to sendToQueue function");
        
        const response = await supabase.functions.invoke('sendToQueue', { 
          body: { 
            queue_name: 'artist_discovery',
            message: { artistName, _idempotencyKey: idempotencyKey },
            deduplication_options: {
              enabled: true,
              ttlSeconds: 3600 // 1 hour
            }
          }
        });
        
        if (response.error) {
          throw new Error(`sendToQueue failed: ${response.error}`);
        } else {
          console.log("Successfully used sendToQueue function:", response.data);
          messageId = response.data.messageId;
        }
      } catch (sendToQueueError) {
        console.error("Both enqueueing methods failed:", sendToQueueError);
        throw enqueueError; // Throw the original error since that's the primary method
      }
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
        
        // Fall back to direct HTTP call
        try {
          const directResponse = await fetch(
            `${supabaseUrl}/functions/v1/artistDiscovery`,
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${Deno.env.get("SUPABASE_ANON_KEY")}`
              },
              body: JSON.stringify({ triggeredManually: true })
            }
          );
          
          if (!directResponse.ok) {
            throw new Error(`HTTP error: ${directResponse.status} ${directResponse.statusText}`);
          }
          
          console.log("Artist discovery worker triggered successfully via direct HTTP");
        } catch (httpError) {
          console.warn("Could not trigger worker via direct HTTP:", httpError);
          console.log("Worker will be triggered by scheduled cron job instead");
        }
      } else {
        console.log("Artist discovery worker triggered successfully via functions.invoke");
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
