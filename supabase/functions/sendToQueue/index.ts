
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { safeStringify, logDebug } from "../_shared/debugHelper.ts";

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
      // First try with pgmq.create directly
      const { error: createError } = await supabase.rpc('raw_sql_query', {
        sql_query: `SELECT pgmq.create($1)`,
        params: JSON.stringify([normalizedQueueName])
      });
      
      if (createError) {
        logDebug("SendToQueue", `Error creating queue with pgmq.create:`, { 
          executionId,
          error: safeStringify(createError)
        });
        // Continue anyway - queue might exist already
      }
    } catch (createErr) {
      logDebug("SendToQueue", `Exception creating queue:`, { 
        executionId, 
        error: safeStringify(createErr)
      });
      // Continue anyway - might exist already
    }
    
    logDebug("SendToQueue", `Queue created or already exists`, { executionId });

    // Format message to ensure it's properly handled
    const messageToSend = typeof message === 'string' ? message : JSON.stringify(message);
    
    // Send message to queue - try multiple approaches
    logDebug("SendToQueue", `Sending message to queue...`, { executionId });
    
    // Approach 1: Use pg_send_text function
    try {
      const { data, error } = await supabase.rpc('pg_send_text', {
        queue_name: normalizedQueueName,
        msg_text: messageToSend
      });

      if (error) {
        logDebug("SendToQueue", `Error with pg_send_text:`, { 
          executionId,
          error: safeStringify(error)
        });
      } else {
        logDebug("SendToQueue", `Message sent successfully using pg_send_text, ID: ${data}`, { executionId });
        return new Response(
          JSON.stringify({ 
            success: true, 
            message_id: data,
            queue_name: normalizedQueueName
          }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    } catch (error) {
      logDebug("SendToQueue", `Exception using pg_send_text:`, { 
        executionId,
        error: safeStringify(error)
      });
    }
    
    // Approach 2: Use pg_enqueue function
    try {
      const { data, error } = await supabase.rpc('pg_enqueue', {
        queue_name: normalizedQueueName,
        message_body: JSON.parse(messageToSend)
      });
      
      if (error) {
        logDebug("SendToQueue", `Error with pg_enqueue:`, { 
          executionId,
          error: safeStringify(error)
        });
      } else {
        logDebug("SendToQueue", `Message sent successfully using pg_enqueue, ID: ${data}`, { executionId });
        return new Response(
          JSON.stringify({ 
            success: true, 
            message_id: data,
            queue_name: normalizedQueueName
          }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    } catch (error) {
      logDebug("SendToQueue", `Exception using pg_enqueue:`, { 
        executionId,
        error: safeStringify(error)
      });
    }
    
    // Approach 3: Fall back to raw SQL to call pgmq.send directly
    try {
      const { data: rawData, error: rawError } = await supabase.rpc('raw_sql_query', {
        sql_query: `SELECT send FROM pgmq.send($1, $2::jsonb) AS send`,
        params: JSON.stringify([
          normalizedQueueName, 
          messageToSend
        ])
      });
      
      if (rawError) {
        logDebug("SendToQueue", `Error sending message with raw SQL:`, { 
          executionId,
          error: safeStringify(rawError)
        });
        return new Response(
          JSON.stringify({ 
            error: 'Failed to send message after multiple attempts',
            details: rawError.message
          }),
          { status: 500, headers: corsHeaders }
        );
      }
      
      logDebug("SendToQueue", `Message sent successfully via raw SQL, ID:`, { 
        executionId,
        result: safeStringify(rawData)
      });
      
      return new Response(
        JSON.stringify({ 
          success: true, 
          message_id: rawData?.result,
          queue_name: normalizedQueueName
        }),
        { status: 200, headers: corsHeaders }
      );
    } catch (sendError) {
      logDebug("SendToQueue", `Exception sending message with raw SQL:`, { 
        executionId,
        error: safeStringify(sendError)
      });
      
      return new Response(
        JSON.stringify({ 
          error: 'Failed to send message',
          details: sendError.message
        }),
        { status: 500, headers: corsHeaders }
      );
    }
  } catch (error) {
    logDebug("SendToQueue", `Unexpected error:`, { 
      executionId,
      error: safeStringify(error)
    });
    
    return new Response(
      JSON.stringify({ 
        error: 'Internal server error',
        details: error.message
      }),
      { status: 500, headers: corsHeaders }
    );
  }
});
