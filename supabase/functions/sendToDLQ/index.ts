
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

// Normalize queue name to ensure consistent naming across all queue operations
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
  logDebug("SendToDLQ", `DLQ request received`, { executionId });
  
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
    logDebug("SendToDLQ", `Normalized queue names`, { 
      executionId,
      source: normalizedQueueName, 
      DLQ: normalizedDLQName
    });

    // Ensure DLQ exists using multiple approaches
    try {
      // Try to create the queue directly with proper pgmq schema
      const { error: createError } = await supabase.rpc('raw_sql_query', {
        sql_query: `SELECT pgmq.create($1)`,
        params: JSON.stringify([normalizedDLQName])
      });
      
      if (createError) {
        logDebug("SendToDLQ", `Error creating DLQ with pgmq.create:`, {
          executionId,
          error: safeStringify(createError)
        });
        // Continue anyway - might exist already
      }
    } catch (createErr) {
      logDebug("SendToDLQ", `Exception creating DLQ:`, {
        executionId,
        error: safeStringify(createErr)
      });
      // Continue anyway - might exist already
    }
    
    logDebug("SendToDLQ", `DLQ created or already exists`, { executionId });

    // Prepare the DLQ message
    const dlqMessage = {
      original_message: message,
      original_queue: normalizedQueueName,
      original_message_id: message_id,
      failure_reason: failure_reason || 'Unknown failure',
      failure_timestamp: new Date().toISOString(),
      ...metadata
    };

    // Send message to DLQ using multiple approaches
    try {
      // Approach 1: Try using the move_to_dead_letter_queue function
      const { data: moveData, error: moveError } = await supabase.rpc('move_to_dead_letter_queue', {
        source_queue: normalizedQueueName,
        dlq_name: normalizedDLQName,
        message_id: message_id,
        failure_reason: failure_reason || 'Unknown failure',
        metadata: metadata || {}
      });
      
      if (!moveError && moveData === true) {
        logDebug("SendToDLQ", `Message sent to DLQ successfully via move_to_dead_letter_queue`, { executionId });
        return new Response(
          JSON.stringify({ 
            success: true,
            dlq_name: normalizedDLQName
          }),
          { status: 200, headers: corsHeaders }
        );
      }
      
      // Approach 2: Try using pg_enqueue
      const { data: enqueueData, error: enqueueError } = await supabase.rpc('pg_enqueue', {
        queue_name: normalizedDLQName,
        message_body: dlqMessage
      });
      
      if (!enqueueError && enqueueData) {
        logDebug("SendToDLQ", `Message sent to DLQ successfully via pg_enqueue, ID: ${enqueueData}`, { executionId });
        
        // Now try to delete the original message
        await supabase.rpc('ensure_message_deleted', {
          queue_name: normalizedQueueName,
          message_id: message_id,
          max_attempts: 3
        });
        
        return new Response(
          JSON.stringify({ 
            success: true, 
            message_id: enqueueData,
            dlq_name: normalizedDLQName
          }),
          { status: 200, headers: corsHeaders }
        );
      }
      
      // Approach 3: Try using raw SQL to call pgmq.send directly
      const { data: rawData, error: rawError } = await supabase.rpc('raw_sql_query', {
        sql_query: `SELECT send FROM pgmq.send($1, $2::jsonb) AS send`,
        params: JSON.stringify([
          normalizedDLQName, 
          JSON.stringify(dlqMessage)
        ])
      });
      
      if (rawError) {
        logDebug("SendToDLQ", `Error sending message with raw SQL:`, {
          executionId,
          error: safeStringify(rawError)
        });
        
        // Try one more approach - direct SQL 
        const { data: directData, error: directError } = await supabase.rpc('raw_sql_query', {
          sql_query: `
            INSERT INTO pgmq.q_${normalizedDLQName} (message, read_ct) 
            VALUES ($1::jsonb, 0) 
            RETURNING id`,
          params: JSON.stringify([JSON.stringify(dlqMessage)])
        });
        
        if (directError) {
          logDebug("SendToDLQ", `Direct SQL insert failed:`, {
            executionId, 
            error: safeStringify(directError)
          });
          
          return new Response(
            JSON.stringify({ error: 'Failed to send to DLQ after all attempts', details: directError }),
            { status: 500, headers: corsHeaders }
          );
        }
        
        logDebug("SendToDLQ", `Message sent to DLQ via direct SQL insert`, { executionId });
        
        // Try to delete the original message
        await supabase.rpc('ensure_message_deleted', {
          queue_name: normalizedQueueName,
          message_id: message_id,
          max_attempts: 3
        });
        
        return new Response(
          JSON.stringify({ 
            success: true, 
            message_id: directData?.result || 'unknown',
            dlq_name: normalizedDLQName
          }),
          { status: 200, headers: corsHeaders }
        );
      }
      
      logDebug("SendToDLQ", `Message sent to DLQ successfully via raw SQL`, { executionId });
      
      // Try to delete the original message
      await supabase.rpc('ensure_message_deleted', {
        queue_name: normalizedQueueName,
        message_id: message_id,
        max_attempts: 3
      });
      
      return new Response(
        JSON.stringify({ 
          success: true, 
          message_id: rawData?.result,
          dlq_name: normalizedDLQName
        }),
        { status: 200, headers: corsHeaders }
      );
    } catch (sendError) {
      logDebug("SendToDLQ", `Exception sending message to DLQ:`, {
        executionId,
        error: safeStringify(sendError)
      });
      
      return new Response(
        JSON.stringify({ 
          error: 'Failed to send to DLQ',
          details: sendError.message
        }),
        { status: 500, headers: corsHeaders }
      );
    }
  } catch (error) {
    logDebug("SendToDLQ", `Unexpected error:`, {
      executionId,
      error: safeStringify(error)
    });
    
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
