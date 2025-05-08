
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

    // DIRECT DATABASE APPROACH - Use the database function directly for reliable enqueuing
    console.log(`[${executionId}] Using direct database function start_artist_discovery...`);
    const { data: directDbResult, error: directDbError } = await supabase.rpc('start_artist_discovery', {
      artist_name: artistName
    });
    
    if (directDbError) {
      console.error(`[${executionId}] Failed to start artist discovery via direct DB call:`, directDbError);
      
      // Try the fallback approach with pg_enqueue
      console.log(`[${executionId}] Attempting fallback with pg_enqueue RPC...`);
      const { data: messageId, error: enqueueError } = await supabase.rpc('pg_enqueue', {
        queue_name: 'artist_discovery',
        message_body: { artistName, _idempotencyKey: idempotencyKey }
      });
      
      if (enqueueError) {
        console.error(`[${executionId}] Fallback also failed:`, enqueueError);
        
        // Last resort - try raw SQL command
        console.log(`[${executionId}] Last resort: attempting raw SQL insertion...`);
        try {
          const { data: rawSqlResult, error: rawSqlError } = await supabase.rpc('raw_sql_query', {
            sql_query: `SELECT pgmq.send('artist_discovery', $1::jsonb) as message_id`,
            params: [JSON.stringify({ artistName, _idempotencyKey: idempotencyKey })]
          });
          
          if (rawSqlError) {
            console.error(`[${executionId}] Raw SQL method also failed:`, rawSqlError);
            throw new Error(`All enqueue methods failed. Last error: ${rawSqlError.message}`);
          }
          
          console.log(`[${executionId}] Raw SQL insertion result:`, rawSqlResult);
          
          if (rawSqlResult && rawSqlResult.message_id) {
            console.log(`[${executionId}] Successfully enqueued via raw SQL, message ID:`, rawSqlResult.message_id);
            
            // Immediately trigger worker without verification (since verification seems problematic)
            console.log(`[${executionId}] Immediately triggering worker without verification...`);
            await supabase.functions.invoke('artistDiscovery', {
              body: { triggeredManually: true, messageId: rawSqlResult.message_id, executionId: executionId }
            });
            
            return new Response(
              JSON.stringify({ 
                success: true, 
                message: `Discovery process started for artist: ${artistName} (via raw SQL)`,
                messageId: rawSqlResult.message_id,
                executionId: executionId
              }),
              { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
            );
          }
        } catch (finalError) {
          console.error(`[${executionId}] Final attempt failed:`, finalError);
          throw new Error(`All enqueue methods failed. Unable to start discovery.`);
        }
      }
      
      if (messageId) {
        console.log(`[${executionId}] Successfully enqueued via pg_enqueue fallback, message ID: ${messageId}`);
        
        // Immediately trigger worker to ensure processing (skip verification)
        console.log(`[${executionId}] Triggering worker immediately...`);
        await supabase.functions.invoke('artistDiscovery', {
          body: { triggeredManually: true, messageId: messageId, executionId: executionId }
        });
        
        return new Response(
          JSON.stringify({ 
            success: true, 
            message: `Discovery process started for artist: ${artistName} (via fallback)`,
            messageId: messageId,
            executionId: executionId
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      } else {
        throw new Error(`Failed to enqueue artist discovery after all attempts`);
      }
    }
    
    // If we get here, the direct DB call succeeded
    console.log(`[${executionId}] Successfully started discovery via direct DB call, ID: ${directDbResult}`);
    
    // Immediately trigger the worker to process
    console.log(`[${executionId}] Triggering artist discovery worker...`);
    try {
      const workerResponse = await supabase.functions.invoke('artistDiscovery', {
        body: { triggeredManually: true, artistName: artistName, executionId: executionId }
      });
      
      if (workerResponse.error) {
        console.warn(`[${executionId}] Could not trigger worker directly: ${workerResponse.error.message}`);
        console.log(`[${executionId}] Worker will be triggered by scheduled cron job instead`);
      } else {
        console.log(`[${executionId}] Artist discovery worker triggered successfully`);
      }
    } catch (triggerError) {
      console.warn(`[${executionId}] Error triggering worker:`, triggerError);
      console.log(`[${executionId}] Worker will be triggered by scheduled cron job instead`);
    }
    
    // Return success response with detailed information
    return new Response(
      JSON.stringify({ 
        success: true, 
        message: `Discovery process started for artist: ${artistName}`,
        messageId: directDbResult,
        executionId: executionId,
        method: "direct_db_function"
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
