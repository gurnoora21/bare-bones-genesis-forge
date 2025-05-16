
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
    
    if (!queue_name || !dlq_name || !message_id) {
      return new Response(
        JSON.stringify({ error: "Missing required parameters" }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    console.log(`Moving message ${message_id} from ${queue_name} to DLQ ${dlq_name}`);
    
    // Call the database function to move the message
    const { data, error } = await supabase.rpc('move_to_dead_letter_queue', {
      source_queue: queue_name,
      dlq_name: dlq_name,
      message_id: message_id.toString(),
      failure_reason: failure_reason || 'Unknown error',
      metadata: metadata || {}
    });
    
    if (error) {
      console.error(`Error moving message to DLQ:`, error);
      
      // Try manual fallback approach if the function call fails
      try {
        // First, ensure DLQ exists
        const { data: dlqCreate } = await supabase.functions.invoke('sendToQueue', {
          body: {
            queue_name: dlq_name,
            message: {
              _dlq_test: true
            },
            create_only: true
          }
        });
        
        // Prepare the message with DLQ metadata
        const dlqMessage = {
          ...message,
          _dlq_metadata: {
            source_queue: queue_name,
            original_message_id: message_id,
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
            idempotency_key: `dlq:${queue_name}:${message_id}`
          }
        });
        
        if (sendError) {
          throw new Error(`Failed to send to DLQ: ${sendError.message}`);
        }
        
        // Delete original message
        await supabase.functions.invoke('deleteMessage', {
          body: {
            queue_name: queue_name,
            msg_id: message_id
          }
        });
        
        return new Response(
          JSON.stringify({ success: true, method: 'fallback', message_id }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
        
      } catch (fallbackError) {
        return new Response(
          JSON.stringify({ error: `Both primary and fallback DLQ methods failed: ${fallbackError.message}` }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    }
    
    return new Response(
      JSON.stringify({ success: true, message_id }),
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
