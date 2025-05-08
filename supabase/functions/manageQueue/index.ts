
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

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
    const requestBody = await req.json();
    const { 
      queue_name,
      operation, // One of: 'purge', 'reset_visibility', 'list_stuck', 'delete_by_id', 'fix_stuck'
      message_id,
      max_age_minutes,
      visibility_timeout
    } = requestBody;
    
    if (!queue_name || !operation) {
      throw new Error("queue_name and operation are required");
    }
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    console.log(`Queue operation: ${operation} on queue ${queue_name}`);
    
    // Handle different operations
    switch (operation) {
      case 'list_stuck': {
        // List messages with visibility timeouts that haven't been processed
        const min_minutes = max_age_minutes || 10;
        const { data, error } = await supabase.rpc(
          'list_stuck_messages',
          { queue_name, min_minutes_locked: min_minutes }
        );
        
        if (error) {
          console.error("Error listing stuck messages:", error);
          return new Response(JSON.stringify({ error: error.message }), 
            { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
        
        return new Response(JSON.stringify({ 
          stuck_messages: data,
          count: data?.length || 0
        }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      case 'reset_visibility': {
        // Reset visibility timeout for stuck messages
        const min_minutes = max_age_minutes || 10;
        const { data, error } = await supabase.rpc(
          'reset_stuck_messages',
          { queue_name, min_minutes_locked: min_minutes }
        );
        
        if (error) {
          console.error("Error resetting stuck messages:", error);
          return new Response(JSON.stringify({ error: error.message }), 
            { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
        
        return new Response(JSON.stringify({ 
          reset_count: data,
          success: true
        }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      case 'delete_by_id': {
        if (!message_id) {
          throw new Error("message_id is required for delete_by_id operation");
        }
        
        // Use enhanced deletion to handle different message ID formats
        const { data, error } = await supabase.rpc(
          'ensure_message_deleted',
          { 
            queue_name, 
            message_id,
            max_attempts: 3
          }
        );
        
        if (error) {
          console.error(`Error deleting message ${message_id}:`, error);
          return new Response(JSON.stringify({ error: error.message }), 
            { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
        
        return new Response(JSON.stringify({ 
          deleted: data,
          message_id
        }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      case 'fix_stuck': {
        // This is a stronger reset that forces cleanup on all stuck messages
        // It first lists all stuck messages, then deletes and re-enqueues them if needed
        const min_minutes = max_age_minutes || 20; // Higher threshold for intervention
        
        // Get list of stuck messages
        const { data: stuckMessages, error: listError } = await supabase.rpc(
          'list_stuck_messages',
          { queue_name, min_minutes_locked: min_minutes }
        );
        
        if (listError) {
          console.error("Error listing stuck messages:", listError);
          return new Response(JSON.stringify({ error: listError.message }), 
            { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
        
        if (!stuckMessages || stuckMessages.length === 0) {
          return new Response(JSON.stringify({ 
            message: "No stuck messages found",
            count: 0
          }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
        
        // Process each stuck message
        const results = [];
        let fixCount = 0;
        
        for (const stuck of stuckMessages) {
          // Try to delete the message first
          const { data: deleted, error: deleteError } = await supabase.rpc(
            'ensure_message_deleted',
            { 
              queue_name, 
              message_id: stuck.id,
              max_attempts: 2
            }
          );
          
          if (deleteError) {
            console.error(`Error deleting stuck message ${stuck.id}:`, deleteError);
            results.push({
              message_id: stuck.id,
              success: false,
              error: deleteError.message
            });
            continue;
          }
          
          // Re-enqueue the message if requested
          if (deleted) {
            try {
              const { data: reEnqueued, error: enqueueError } = await supabase.rpc(
                'pg_enqueue',
                { 
                  queue_name,
                  message_body: stuck.message
                }
              );
              
              if (enqueueError) {
                console.error(`Error re-enqueueing message:`, enqueueError);
                results.push({
                  message_id: stuck.id,
                  deleted: true,
                  re_enqueued: false,
                  error: enqueueError.message
                });
              } else {
                results.push({
                  message_id: stuck.id,
                  deleted: true,
                  re_enqueued: true,
                  new_message_id: reEnqueued
                });
                fixCount++;
              }
            } catch (e) {
              console.error(`Error in re-enqueue process:`, e);
              results.push({
                message_id: stuck.id,
                deleted: true,
                re_enqueued: false,
                error: e.message
              });
            }
          } else {
            results.push({
              message_id: stuck.id,
              deleted: false,
              re_enqueued: false,
              error: "Could not delete message"
            });
          }
        }
        
        return new Response(JSON.stringify({ 
          fixed_count: fixCount,
          total_count: stuckMessages.length,
          results
        }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      case 'purge': {
        // This is a dangerous operation that clears all messages from a queue
        throw new Error("Purge operation not implemented yet for safety. Contact support.");
      }
      
      default:
        throw new Error(`Unknown operation: ${operation}`);
    }
  } catch (error) {
    console.error("Error in queue management:", error);
    return new Response(JSON.stringify({ 
      error: error.message 
    }), { 
      status: 400, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
    });
  }
});
