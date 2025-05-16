
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
    const { queue_name, message, idempotency_key, create_only } = await req.json();
    
    if (!queue_name) {
      return new Response(
        JSON.stringify({ error: "Missing queue_name parameter" }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    // First, ensure the queue exists
    try {
      // Register the queue using a database function
      await supabase.rpc('register_queue', {
        p_queue_name: queue_name,
        p_display_name: queue_name,
        p_description: `Queue for ${queue_name} messages`,
        p_active: true
      });
    } catch (createError) {
      // Queue might already exist, which is fine
      console.log(`Note: Queue ${queue_name} might already exist: ${createError.message}`);
    }
    
    // If create_only flag is set, we only want to create the queue
    if (create_only) {
      return new Response(
        JSON.stringify({ success: true, queue_created: queue_name }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    if (!message) {
      return new Response(
        JSON.stringify({ error: "Missing message parameter" }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    console.log(`Sending message to queue ${queue_name}`);
    
    // Send to queue using pg_enqueue
    const { data: messageId, error } = await supabase.rpc('pg_enqueue', {
      queue_name: queue_name,
      message_body: {
        ...message,
        _idempotencyKey: idempotency_key || `msg_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`
      }
    });
    
    if (error) {
      // Try alternative method
      try {
        const { data: altSendResult } = await supabase.rpc('pg_send_text', {
          queue_name: queue_name,
          msg_text: JSON.stringify({
            ...message,
            _idempotencyKey: idempotency_key || `msg_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`
          })
        });
        
        return new Response(
          JSON.stringify({ success: true, message_id: altSendResult, method: 'pg_send_text' }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
        
      } catch (altError) {
        return new Response(
          JSON.stringify({ error: `All queue sending methods failed: ${error.message} / ${altError.message}` }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    }
    
    return new Response(
      JSON.stringify({ success: true, message_id: messageId }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Error in sendToQueue handler:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      }
    );
  }
});
