
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

// Initialize Supabase client
const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const supabase = createClient(supabaseUrl, supabaseKey);

// Common CORS headers
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
    // Parse the request body
    const { queue_name, dlq_name, message_id, message, failure_reason, metadata } = await req.json();
    
    // Validate required parameters
    if (!queue_name || !dlq_name) {
      return new Response(
        JSON.stringify({ error: "Missing required parameters: queue_name and dlq_name" }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    // Ensure we have a valid message_id or message body
    if (!message_id && !message) {
      return new Response(
        JSON.stringify({ error: "Either message_id or message must be provided" }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    console.log(`Moving message ${message_id || 'with no ID'} from ${queue_name} to DLQ ${dlq_name}`);
    
    // First, ensure DLQ exists
    try {
      await supabase.functions.invoke('sendToQueue', {
        body: {
          queue_name: dlq_name,
          create_only: true
        }
      });
    } catch (createError) {
      console.log(`Note: DLQ ${dlq_name} might already exist`);
    }
    
    // Generate a unique ID if none provided
    const dlqMessageId = message_id || `generated_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
    
    // Prepare message with DLQ metadata
    const dlqMessage = {
      ...(message || {}),
      _dlq_metadata: {
        source_queue: queue_name,
        original_message_id: message_id || 'unknown',
        moved_at: new Date().toISOString(),
        failure_reason: failure_reason || 'Unknown error',
        custom_metadata: metadata || {}
      }
    };
    
    // Send to DLQ
    const { data: sendResult, error: sendError } = await supabase.functions.invoke('sendToQueue', {
      body: {
        queue_name: dlq_name,
        message: dlqMessage,
        idempotency_key: `dlq:${queue_name}:${dlqMessageId}`
      }
    });
    
    if (sendError) {
      console.error(`Failed to send to DLQ: ${sendError.message}`, sendError);
      return new Response(
        JSON.stringify({ error: `Failed to send to DLQ: ${sendError.message}` }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    // Record the move in monitoring if possible
    try {
      await supabase
        .from('monitoring_events')
        .insert({
          event_type: 'message_moved_to_dlq',
          details: {
            source_queue: queue_name,
            dlq_name: dlq_name,
            source_message_id: message_id || 'unknown',
            dlq_message_id: sendResult.message_id,
            failure_reason: failure_reason,
            timestamp: new Date().toISOString()
          }
        });
    } catch (monitoringError) {
      console.log(`Note: Could not record monitoring event: ${monitoringError.message}`);
    }
    
    return new Response(
      JSON.stringify({ success: true, message_id: sendResult.message_id }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Error in sendToDLQ handler:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      }
    );
  }
});
