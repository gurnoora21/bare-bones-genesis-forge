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
  
  const executionId = `queue_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
  console.log(`[${executionId}] Queue request received`);
  
  try {
    const { queue_name, message, priority } = await req.json();
    console.log(`[${executionId}] Request parameters:`, { 
      queue_name, 
      has_message: !!message,
      priority: priority || 0
    });
    
    if (!queue_name || !message) {
      console.log(`[${executionId}] Missing required parameters`);
      return new Response(
        JSON.stringify({ error: 'Missing required parameters' }),
        { status: 400, headers: corsHeaders }
      );
    }

    // Normalize the queue name
    const normalizedQueueName = normalizeQueueName(queue_name);
    console.log(`[${executionId}] Normalized queue name: ${normalizedQueueName}`);

    // Ensure queue exists
    console.log(`[${executionId}] Creating queue if not exists...`);
    const { error: createError } = await supabase.rpc('pgmq_create_queue', {
      queue_name: normalizedQueueName
    });

    if (createError) {
      console.error(`[${executionId}] Error creating queue:`, createError);
      return new Response(
        JSON.stringify({ 
          error: 'Failed to create queue',
          details: createError.message
        }),
        { status: 500, headers: corsHeaders }
      );
    }
    console.log(`[${executionId}] Queue created or already exists`);

    // Send message to queue
    console.log(`[${executionId}] Sending message to queue...`);
    const { data, error } = await supabase.rpc('pgmq_send', {
      queue_name: normalizedQueueName,
      message: message,
      priority: priority || 0
    });

    if (error) {
      console.error(`[${executionId}] Error sending message:`, error);
      return new Response(
        JSON.stringify({ 
          error: 'Failed to send message',
          details: error.message
        }),
        { status: 500, headers: corsHeaders }
      );
    }

    console.log(`[${executionId}] Message sent successfully, ID: ${data.message_id}`);
    return new Response(
      JSON.stringify({ 
        success: true, 
        message_id: data.message_id,
        queue_name: normalizedQueueName
      }),
      { status: 200, headers: corsHeaders }
    );
  } catch (error) {
    console.error(`[${executionId}] Unexpected error:`, error);
    return new Response(
      JSON.stringify({ 
        error: 'Internal server error',
        details: error.message,
        stack: error.stack
      }),
      { status: 500, headers: corsHeaders }
    );
  }
});
