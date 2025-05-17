
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
        { status: 400, headers: corsHeaders }
      );
    }

    // Normalize the queue names
    const normalizedQueueName = normalizeQueueName(queue_name);
    const normalizedDLQName = normalizeQueueName(dlq_name);
    console.log(`[${executionId}] Normalized queue names - source: ${normalizedQueueName}, DLQ: ${normalizedDLQName}`);

    // Ensure DLQ exists
    try {
      // Try to create the queue directly with proper pgmq schema
      const { error: createError } = await supabase.rpc('raw_sql_query', {
        sql_query: `SELECT pgmq.create($1)`,
        params: JSON.stringify([normalizedDLQName])
      });
      
      if (createError) {
        console.error(`[${executionId}] Error creating DLQ with raw SQL:`, createError);
        // Continue anyway - might exist already
      }
    } catch (createErr) {
      console.error(`[${executionId}] Exception creating DLQ:`, createErr);
      // Continue anyway - might exist already
    }
    
    console.log(`[${executionId}] DLQ created or already exists`);

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
    try {
      // Try using pg_send_text directly with the proper schema
      const { data, error } = await supabase.rpc('pg_send_text', {
        queue_name: normalizedDLQName,
        msg_text: JSON.stringify(dlqMessage)
      });

      if (error) {
        console.error(`[${executionId}] Error with pg_send_text:`, error);
        
        // Fall back to raw SQL to call pgmq.send directly
        const { data: rawData, error: rawError } = await supabase.rpc('raw_sql_query', {
          sql_query: `SELECT send FROM pgmq.send($1, $2::jsonb) AS send`,
          params: JSON.stringify([
            normalizedDLQName, 
            JSON.stringify(dlqMessage)
          ])
        });
        
        if (rawError) {
          console.error(`[${executionId}] Error sending message with raw SQL:`, rawError);
          return new Response(
            JSON.stringify({ error: 'Failed to send to DLQ', details: rawError }),
            { status: 500, headers: corsHeaders }
          );
        }
        
        console.log(`[${executionId}] Message sent to DLQ successfully via raw SQL`);
        return new Response(
          JSON.stringify({ 
            success: true, 
            message_id: rawData?.result,
            dlq_name: normalizedDLQName
          }),
          { status: 200, headers: corsHeaders }
        );
      }

      console.log(`[${executionId}] Message sent to DLQ successfully, ID: ${data}`);
      return new Response(
        JSON.stringify({ 
          success: true, 
          message_id: data,
          dlq_name: normalizedDLQName
        }),
        { status: 200, headers: corsHeaders }
      );
    } catch (sendError) {
      console.error(`[${executionId}] Exception sending message to DLQ:`, sendError);
      return new Response(
        JSON.stringify({ 
          error: 'Failed to send to DLQ',
          details: sendError.message
        }),
        { status: 500, headers: corsHeaders }
      );
    }
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
