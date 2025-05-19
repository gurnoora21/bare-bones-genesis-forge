
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  const executionId = `exec_${Date.now()}_${Math.floor(Math.random() * 1000)}`;
  console.log(`[${executionId}] Request received:`, req.method);
  
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    console.log(`[${executionId}] Handling CORS preflight request`);
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Parse request body
    console.log(`[${executionId}] Parsing request body...`);
    let requestBody, artistName;
    try {
      requestBody = await req.json();
      console.log(`[${executionId}] Request payload:`, JSON.stringify(requestBody));
      artistName = requestBody.artistName;
      
      if (!artistName) {
        console.error(`[${executionId}] Error: Missing artistName in request`);
        return new Response(
          JSON.stringify({ error: "artistName is required" }),
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      console.log(`[${executionId}] Extracted artistName: "${artistName}"`);
    } catch (parseError) {
      console.error(`[${executionId}] Error parsing request body:`, parseError);
      return new Response(
        JSON.stringify({ error: "Invalid request body - must be valid JSON with artistName" }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    // Initialize Supabase client
    console.log(`[${executionId}] Initializing Supabase client...`);
    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    
    if (!supabaseUrl || !supabaseServiceKey) {
      console.error(`[${executionId}] Error: Missing Supabase environment variables`);
      throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables");
    }
    
    console.log(`[${executionId}] Supabase URL: ${supabaseUrl.substring(0, 20)}...`);
    const supabase = createClient(supabaseUrl, supabaseServiceKey);
    
    console.log(`[${executionId}] Starting artist discovery for: "${artistName}"`);

    // Create a unique idempotency key for this request
    const idempotencyKey = `artist:name:${artistName.toLowerCase()}`;
    console.log(`[${executionId}] Generated idempotency key: ${idempotencyKey}`);

    // Use the database function directly for reliable enqueuing
    console.log(`[${executionId}] Using direct database function start_artist_discovery...`);
    const { data: messageId, error: dbError } = await supabase.rpc('start_artist_discovery', {
      artist_name: artistName
    });
    
    if (dbError) {
      console.error(`[${executionId}] Failed to start artist discovery:`, dbError);
      throw new Error(`Failed to start artist discovery: ${dbError.message}`);
    }
    
    console.log(`[${executionId}] Successfully started discovery, message ID: ${messageId}`);
    
    // Trigger the worker to process immediately (optional)
    console.log(`[${executionId}] Triggering artist discovery worker...`);
    try {
      await supabase.functions.invoke('artistDiscovery', {
        body: { triggeredManually: true, artistName: artistName, executionId: executionId }
      });
      console.log(`[${executionId}] Artist discovery worker triggered successfully`);
    } catch (triggerError) {
      // This is non-critical - the worker will be triggered by the cron job if this fails
      console.log(`[${executionId}] Worker will be triggered by scheduled cron job instead`);
    }
    
    // Return success response
    return new Response(
      JSON.stringify({ 
        success: true, 
        message: `Discovery process started for artist: ${artistName}`,
        messageId: messageId,
        executionId: executionId
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error(`[${executionId}] Error starting discovery:`, error);
    
    return new Response(
      JSON.stringify({ 
        error: error.message,
        executionId: executionId,
        timestamp: new Date().toISOString()
      }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
