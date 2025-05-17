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

// Add this function at the top of the file, after imports
function normalizeQueueName(queueName: string): string {
  // Remove any existing prefixes
  const baseName = queueName.replace(/^(pgmq\.|q_)/, '');
  // Return the normalized name without any prefix
  return baseName;
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }
  
  const executionId = `dlq_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
  console.log(`[${executionId}] DLQ request received`);
  
  try {
    const { queue_name, dlq_name, message_id, message, failure_reason, metadata } = await req.json();
    
    if (!queue_name || !dlq_name || !message_id || !message) {
      return new Response(
        JSON.stringify({ error: 'Missing required parameters' }),
        { status: 400 }
      );
    }

    // Normalize the queue names
    const normalizedQueueName = normalizeQueueName(queue_name);
    const normalizedDLQName = normalizeQueueName(dlq_name);
    console.log(`DEBUG: Normalized queue names - source: ${normalizedQueueName}, DLQ: ${normalizedDLQName}`);

    // Ensure DLQ exists
    const { error: createError } = await supabase.rpc('pgmq_create_queue', {
      queue_name: normalizedDLQName
    });

    if (createError) {
      console.error('Error creating DLQ:', createError);
      return new Response(
        JSON.stringify({ error: 'Failed to create DLQ' }),
        { status: 500 }
      );
    }

    // Prepare the DLQ message
    const dlqMessage = {
      original_message: message,
      original_queue: normalizedQueueName,
      original_message_id: message_id,
      failure_reason: failure_reason || 'Unknown failure',
      failure_timestamp: new Date().toISOString(),
      ...metadata
    };

    // Send message to DLQ
    const { data, error } = await supabase.rpc('pgmq_send', {
      queue_name: normalizedDLQName,
      message: dlqMessage,
      priority: 0 // DLQ messages are always high priority
    });

    if (error) {
      console.error('Error sending to DLQ:', error);
      return new Response(
        JSON.stringify({ error: 'Failed to send to DLQ' }),
        { status: 500 }
      );
    }

    return new Response(
      JSON.stringify({ 
        success: true, 
        message_id: data.message_id,
        dlq_name: normalizedDLQName
      }),
      { status: 200 }
    );
  } catch (error) {
    console.error('Unexpected error:', error);
    return new Response(
      JSON.stringify({ error: 'Internal server error' }),
      { status: 500 }
    );
  }
});
