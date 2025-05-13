
/**
 * Queue Processor Demo - Demonstrates the enhanced queue processing architecture
 *
 * This edge function shows proper queue handling with Postgres-backed queues,
 * atomic operations, idempotency, and robust error handling
 */
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { EnhancedIdempotentWorker } from "../_shared/enhancedIdempotentWorker.ts";
import { getQueueManager } from "../_shared/pgQueueManager.ts";

// CORS headers for browser requests
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Handle CORS preflight requests
const handleOptionsRequest = () => {
  return new Response(null, {
    headers: {
      ...corsHeaders,
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
    },
    status: 204,
  });
}

// Implementation of a worker that processes demo messages
class DemoQueueWorker extends EnhancedIdempotentWorker<{ jobType: string, payload: any }> {
  async processMessage(message: { jobType: string, payload: any }): Promise<any> {
    console.log(`Processing message of type: ${message.jobType}`);
    
    // Simulate different processing based on job type
    switch (message.jobType) {
      case 'fast':
        // Quick job simulation
        return { status: 'completed', result: 'Fast job processed' };
        
      case 'slow':
        // Slower job simulation with artificial delay
        await new Promise(resolve => setTimeout(resolve, 2000));
        return { status: 'completed', result: 'Slow job processed' };
        
      case 'error':
        // Simulate a job that fails with an error
        throw new Error('This job is designed to fail');
        
      case 'random':
        // Sometimes succeeds, sometimes fails
        if (Math.random() > 0.5) {
          return { status: 'completed', result: 'Random job succeeded' };
        } else {
          throw new Error('Random job failed this time');
        }
        
      default:
        return { status: 'completed', result: `Processed ${message.jobType} job` };
    }
  }
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return handleOptionsRequest();
  }

  // Only process POST requests for queue operations
  if (req.method !== 'POST') {
    return new Response(JSON.stringify({ error: 'Method not allowed' }), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      status: 405,
    });
  }

  try {
    const supabase = createClient(
      Deno.env.get('SUPABASE_URL') ?? '',
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
    );

    // Parse the request body
    const requestData = await req.json();
    const { 
      action = 'process',
      queue = 'demo_queue',
      message,
      batchSize = 5
    } = requestData;

    // Invoke different actions based on request
    switch (action) {
      case 'enqueue': {
        // Add a message to the queue
        if (!message) {
          throw new Error('Message is required for enqueue action');
        }
        
        const queueManager = getQueueManager(supabase);
        const messageId = await queueManager.sendMessage(queue, message);
        
        return new Response(
          JSON.stringify({ success: true, messageId, queue }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      case 'process': {
        // Process messages in the queue
        const worker = new DemoQueueWorker(queue, supabase);
        
        const result = await worker.processBatch({
          batchSize,
          processorName: 'demo-processor',
          timeoutSeconds: 25,
          visibilityTimeoutSeconds: 60
        });
        
        return new Response(
          JSON.stringify({ 
            success: true, 
            processed: result.processed,
            errors: result.errors,
            results: result.messages
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      case 'status': {
        // Get queue status information
        const queueManager = getQueueManager(supabase);
        const status = await queueManager.getQueueStatus(queue);
        
        return new Response(
          JSON.stringify({ 
            success: true, 
            queue,
            count: status.count,
            oldestMessage: status.oldestMessage
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      case 'reset_stuck': {
        // Reset stuck messages
        const queueManager = getQueueManager(supabase);
        const minMinutesLocked = requestData.minMinutesLocked || 10;
        const count = await queueManager.resetAllStuckMessages(queue, minMinutesLocked);
        
        return new Response(
          JSON.stringify({ 
            success: true, 
            queue,
            messagesReset: count
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      default:
        throw new Error(`Unknown action: ${action}`);
    }
  } catch (error) {
    console.error('Error processing request:', error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 400
      }
    );
  }
});
