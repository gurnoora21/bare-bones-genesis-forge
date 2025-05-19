import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { logDebug, logError } from "../_shared/debugHelper.ts";
import { enqueue } from "../_shared/queueHelper.ts";

// Initialize Supabase client
const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const supabase = createClient(supabaseUrl, supabaseKey);

// Common CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Normalize queue name to remove any existing prefixes and ensure consistent naming
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
  logDebug("SendToQueue", `Queue request received`, { executionId });
  
  try {
    const { queue_name, message, priority } = await req.json();
    logDebug("SendToQueue", `Request parameters:`, { 
      executionId,
      queue_name, 
      has_message: !!message,
      priority: priority || 0
    });
    
    if (!queue_name || !message) {
      logDebug("SendToQueue", `Missing required parameters`, { executionId });
      return new Response(
        JSON.stringify({ error: 'Missing required parameters' }),
        { status: 400, headers: corsHeaders }
      );
    }

    // Normalize the queue name
    const normalizedQueueName = normalizeQueueName(queue_name);
    logDebug("SendToQueue", `Normalized queue name: ${normalizedQueueName}`, { executionId });

    // Ensure queue exists
    logDebug("SendToQueue", `Creating queue if not exists...`, { executionId });
    
    try {
      const { error: createError } = await supabase.rpc('raw_sql_query', {
        sql_query: `SELECT pgmq.create($1)`,
        params: JSON.stringify([normalizedQueueName])
      });
      
      if (createError) {
        logDebug("SendToQueue", `Error creating queue with pgmq.create: ${createError.message}`);
        // Continue anyway - queue might exist already
      }
    } catch (createErr) {
      logDebug("SendToQueue", `Exception creating queue: ${createErr.message}`);
      // Continue anyway - might exist already
    }
    
    logDebug("SendToQueue", `Queue created or already exists`, { executionId });

    // Format message to ensure it's properly handled
    const messageToSend = typeof message === 'string' ? message : JSON.stringify(message);
    
    // Send message to queue using our simplified enqueue function
    logDebug("SendToQueue", `Sending message to queue using simplified approach...`, { executionId });
    
    try {
      // Use the enqueue function from queueHelper.ts
      const messageId = await enqueue(supabase, normalizedQueueName, message);
      
      if (!messageId) {
        logError("SendToQueue", `Failed to enqueue message to ${normalizedQueueName}`);
        return new Response(
          JSON.stringify({ 
            error: 'Failed to send message',
            details: 'Message could not be enqueued'
          }),
          { status: 500, headers: corsHeaders }
        );
      }
      
      logDebug("SendToQueue", `Message sent successfully, ID: ${messageId}`, { executionId });
      return new Response(
        JSON.stringify({ 
          success: true, 
          message_id: messageId,
          queue_name: normalizedQueueName
        }),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      logError("SendToQueue", `Exception sending message: ${error.message}`);
      
      return new Response(
        JSON.stringify({ 
          error: 'Failed to send message',
          details: error.message
        }),
        { status: 500, headers: corsHeaders }
      );
    }
  } catch (error) {
    logError("SendToQueue", `Unexpected error: ${error.message}`);
    
    return new Response(
      JSON.stringify({ 
        error: 'Internal server error',
        details: error.message
      }),
      { status: 500, headers: corsHeaders }
    );
  }
});
