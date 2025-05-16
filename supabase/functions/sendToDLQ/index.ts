
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
  
  const executionId = `dlq_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
  console.log(`[${executionId}] DLQ request received`);
  
  try {
    // Parse the request body
    const { queue_name, dlq_name, message_id, message, failure_reason, metadata } = await req.json();
    
    console.log(`[${executionId}] DLQ parameters:`, { 
      queue_name, 
      dlq_name,
      message_id: message_id || 'none',
      has_message: !!message,
      failure_reason: failure_reason || 'none'
    });
    
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
    
    console.log(`[${executionId}] Moving message ${message_id || 'with no ID'} from ${queue_name} to DLQ ${dlq_name}`);
    
    // First, ensure DLQ exists
    try {
      await supabase.functions.invoke('sendToQueue', {
        body: {
          queue_name: dlq_name,
          create_only: true
        }
      });
    } catch (createError) {
      console.log(`[${executionId}] Note: DLQ ${dlq_name} might already exist`);
    }
    
    // Prepare the message for the DLQ
    let dlqMessage = message;
    let messageContent;
    
    if (!dlqMessage && message_id) {
      try {
        // Try to fetch the original message using the message_id
        const { data } = await supabase.rpc('raw_sql_query', {
          sql_query: `
            SELECT message FROM pgmq.q_${queue_name} 
            WHERE id::TEXT = $1 OR msg_id::TEXT = $1 LIMIT 1
          `,
          params: [message_id.toString()]
        });
        
        if (data && data.message) {
          messageContent = data.message;
          console.log(`[${executionId}] Retrieved original message from queue`);
        } else {
          console.log(`[${executionId}] Could not find original message, will use empty object`);
          messageContent = {};
        }
      } catch (fetchError) {
        console.error(`[${executionId}] Error fetching original message:`, fetchError);
        messageContent = {};
      }
    } else {
      // Use the provided message
      messageContent = message || {};
    }
    
    // Generate a unique ID if none provided
    const dlqMessageId = message_id || `generated_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
    
    // Ensure messageContent is an object so we can add metadata
    if (typeof messageContent !== 'object' || messageContent === null) {
      messageContent = { original_content: messageContent };
    }
    
    // Prepare message with DLQ metadata
    dlqMessage = {
      ...messageContent,
      _dlq_metadata: {
        source_queue: queue_name,
        original_message_id: message_id || 'unknown',
        moved_at: new Date().toISOString(),
        failure_reason: failure_reason || 'Unknown error',
        custom_metadata: metadata || {}
      }
    };
    
    console.log(`[${executionId}] Sending message to DLQ with metadata`);
    
    // Send to DLQ
    const { data: sendResult, error: sendError } = await supabase.functions.invoke('sendToQueue', {
      body: {
        queue_name: dlq_name,
        message: dlqMessage,
        idempotency_key: `dlq:${queue_name}:${dlqMessageId}`
      }
    });
    
    if (sendError) {
      console.error(`[${executionId}] Failed to send to DLQ: ${sendError.message}`, sendError);
      return new Response(
        JSON.stringify({ error: `Failed to send to DLQ: ${sendError.message}` }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    console.log(`[${executionId}] Successfully sent to DLQ, now removing from source queue if needed`);
    
    // If we have a message_id, try to delete the original message
    if (message_id) {
      try {
        await supabase.functions.invoke('deleteFromQueue', {
          body: {
            queue_name,
            message_id: dlqMessageId
          }
        });
        console.log(`[${executionId}] Successfully deleted source message`);
      } catch (deleteError) {
        console.warn(`[${executionId}] Could not delete source message: ${deleteError.message}`);
        // Continue anyway since the message is in DLQ now
      }
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
      console.log(`[${executionId}] Recorded monitoring event`);
    } catch (monitoringError) {
      console.log(`[${executionId}] Note: Could not record monitoring event: ${monitoringError.message}`);
    }
    
    console.log(`[${executionId}] DLQ operation completed successfully`);
    
    return new Response(
      JSON.stringify({ success: true, message_id: sendResult.message_id }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error(`[${executionId}] Error in sendToDLQ handler:`, error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      }
    );
  }
});
