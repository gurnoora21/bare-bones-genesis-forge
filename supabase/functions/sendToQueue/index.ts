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
  
  try {
    const { queue_name, message, priority } = await req.json();
    
    if (!queue_name || !message) {
      return new Response(
        JSON.stringify({ error: 'Missing required parameters' }),
        { status: 400 }
      );
    }

    // Normalize the queue name
    const normalizedQueueName = normalizeQueueName(queue_name);
    console.log(`DEBUG: Normalized queue name: ${normalizedQueueName}`);

    // Ensure queue exists
    const { error: createError } = await supabase.rpc('pgmq_create_queue', {
      queue_name: normalizedQueueName
    });

    if (createError) {
      console.error('Error creating queue:', createError);
      return new Response(
        JSON.stringify({ error: 'Failed to create queue' }),
        { status: 500 }
      );
    }

    // Send message to queue
    const { data, error } = await supabase.rpc('pgmq_send', {
      queue_name: normalizedQueueName,
      message: message,
      priority: priority || 0
    });

    if (error) {
      console.error('Error sending message:', error);
      return new Response(
        JSON.stringify({ error: 'Failed to send message' }),
        { status: 500 }
      );
    }

    return new Response(
      JSON.stringify({ 
        success: true, 
        message_id: data.message_id,
        queue_name: normalizedQueueName
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
