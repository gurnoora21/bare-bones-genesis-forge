
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

    // Step 1: Enqueue the message using pg_enqueue RPC
    console.log(`[${executionId}] Calling pg_enqueue RPC function...`);
    const { data: messageId, error: enqueueError } = await supabase.rpc('pg_enqueue', {
      queue_name: 'artist_discovery',
      message_body: { artistName, _idempotencyKey: idempotencyKey }
    });
    
    if (enqueueError) {
      console.error(`[${executionId}] Failed to enqueue artist discovery:`, enqueueError);
      
      // Try a fallback method with direct SQL query
      console.log(`[${executionId}] Attempting fallback using SQL query...`);
      try {
        const { data: fallbackResult, error: fallbackError } = await supabase.rpc('raw_sql_query', {
          sql_query: `SELECT pgmq.send('artist_discovery', $1::jsonb) as message_id`,
          params: [JSON.stringify({ artistName, _idempotencyKey: idempotencyKey })]
        });
        
        if (fallbackError) {
          console.error(`[${executionId}] Fallback method also failed:`, fallbackError);
          throw new Error(`Failed to enqueue using both primary and fallback methods: ${enqueueError.message}`);
        }
        
        if (fallbackResult && fallbackResult.message_id) {
          console.log(`[${executionId}] Successfully enqueued using fallback method, ID:`, fallbackResult.message_id);
          return new Response(
            JSON.stringify({ 
              success: true, 
              message: `Discovery process started for artist: ${artistName} (via fallback)`,
              messageId: fallbackResult.message_id,
              executionId: executionId
            }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
      } catch (fallbackCatchError) {
        console.error(`[${executionId}] Error in fallback method:`, fallbackCatchError);
      }
      
      throw new Error(`Failed to enqueue artist discovery: ${enqueueError.message}`);
    }
    
    if (!messageId) {
      console.error(`[${executionId}] Failed to get message ID after enqueueing`);
      throw new Error("Failed to get message ID after enqueueing");
    }
    
    console.log(`[${executionId}] Successfully enqueued artist discovery, message ID: ${messageId}`);
    
    // Step 2: Verify the message was actually added to the queue
    console.log(`[${executionId}] Verifying message was added to queue...`);
    try {
      const { data: verificationData, error: verificationError } = await supabase.rpc('raw_sql_query', {
        sql_query: `SELECT EXISTS(
          SELECT 1 FROM pgmq.q_artist_discovery 
          WHERE msg_id = $1::TEXT OR id::TEXT = $1::TEXT
        ) as exists`,
        params: [messageId.toString()]
      });
      
      if (verificationError) {
        console.warn(`[${executionId}] Could not verify message in queue:`, verificationError);
      } else if (verificationData && verificationData.exists === true) {
        console.log(`[${executionId}] Verified message ${messageId} exists in queue`);
      } else {
        console.warn(`[${executionId}] Message verification failed - message may not be in queue`);
      }
    } catch (verifyError) {
      console.warn(`[${executionId}] Error verifying message:`, verifyError);
    }
    
    // Step 3: Trigger the artist discovery worker to process immediately
    console.log(`[${executionId}] Triggering artist discovery worker...`);
    
    let workerTriggered = false;
    
    // Method 1: Using functions.invoke
    try {
      console.log(`[${executionId}] Triggering worker via functions.invoke...`);
      const workerResponse = await supabase.functions.invoke('artistDiscovery', {
        body: { triggeredManually: true, messageId: messageId, executionId: executionId }
      });
      
      if (workerResponse.error) {
        console.warn(`[${executionId}] Could not trigger worker via functions.invoke:`, workerResponse.error);
      } else {
        console.log(`[${executionId}] Artist discovery worker triggered successfully`);
        workerTriggered = true;
      }
    } catch (triggerError) {
      console.warn(`[${executionId}] Error triggering worker:`, triggerError);
    }
    
    // Method 2: Using manual_trigger_worker RPC function
    if (!workerTriggered) {
      try {
        console.log(`[${executionId}] Attempting to trigger worker via manual_trigger_worker RPC...`);
        const { data: rpcTriggerResult, error: rpcTriggerError } = await supabase.rpc('manual_trigger_worker', {
          worker_name: 'artistDiscovery'
        });
        
        if (rpcTriggerError) {
          console.warn(`[${executionId}] Could not trigger worker via RPC:`, rpcTriggerError);
          console.log(`[${executionId}] Worker will be triggered by scheduled cron job instead`);
        } else {
          console.log(`[${executionId}] Worker triggered via RPC:`, rpcTriggerResult);
          workerTriggered = true;
        }
      } catch (rpcError) {
        console.warn(`[${executionId}] Error in RPC worker trigger:`, rpcError);
        console.log(`[${executionId}] Worker will be triggered by scheduled cron job instead`);
      }
    }
    
    // Return success response with detailed information
    return new Response(
      JSON.stringify({ 
        success: true, 
        message: `Discovery process started for artist: ${artistName}`,
        messageId: messageId,
        executionId: executionId,
        workerTriggered: workerTriggered,
        idempotencyKey: idempotencyKey
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
