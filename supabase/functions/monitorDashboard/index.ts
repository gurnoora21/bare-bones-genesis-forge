
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getStateManager } from "../_shared/coordinatedStateManager.ts";
import { ProcessingState } from "../_shared/stateManager.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );

  // Initialize Redis client
  const redis = new Redis({
    url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
    token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
  });

  try {
    // Parse request params
    const url = new URL(req.url);
    const action = url.searchParams.get('action') || 'summary';
    const entityType = url.searchParams.get('entityType') || null;
    const entityId = url.searchParams.get('entityId') || null;
    const queueName = url.searchParams.get('queue') || null;
    const messageId = url.searchParams.get('messageId') || null;
    
    switch (action) {
      case 'summary':
        return await handleSummary(supabase, redis);
        
      case 'queues':
        return await handleQueues(supabase);
        
      case 'states':
        return await handleStates(supabase, entityType);
        
      case 'reset':
        if (!entityType || !entityId) {
          return new Response(JSON.stringify({
            success: false,
            error: "Missing entityType or entityId"
          }), { 
            status: 400, 
            headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }
        return await handleReset(supabase, redis, entityType, entityId);
        
      case 'resetMessage':
        if (!queueName || !messageId) {
          return new Response(JSON.stringify({
            success: false,
            error: "Missing queueName or messageId"
          }), { 
            status: 400, 
            headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }
        return await handleResetMessage(supabase, queueName, messageId);
        
      case 'requeueDLQ':
        if (!queueName || !messageId) {
          return new Response(JSON.stringify({
            success: false,
            error: "Missing queueName or messageId"
          }), { 
            status: 400, 
            headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }
        return await handleRequeueDLQ(supabase, queueName, messageId);
        
      case 'activeHeartbeats':
        return await handleActiveHeartbeats(supabase, redis);
        
      case 'runQueueMonitor':
        return await handleRunQueueMonitor(supabase);
        
      default:
        return new Response(JSON.stringify({
          success: false,
          error: "Unknown action"
        }), { 
          status: 400, 
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
    }
  } catch (error) {
    console.error("Error in monitorDashboard:", error);
    return new Response(JSON.stringify({ 
      success: false,
      error: error.message || String(error)
    }), { 
      status: 500, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
});

async function handleSummary(supabase: any, redis: Redis) {
  // Get summary information about queues, states, etc.
  const summary = {
    timestamp: new Date().toISOString(),
    queues: [],
    states: {},
    heartbeats: [],
    workers: {},
    metrics: {}
  };
  
  // Get queue information
  try {
    const { data: queueTables, error: queueError } = await supabase.rpc('get_all_queue_tables');
    if (!queueError && queueTables) {
      summary.queues = queueTables;
    }
  } catch (error) {
    console.error("Error getting queue info:", error);
  }
  
  // Get processing state counts
  try {
    const { data: stateCounts, error: stateError } = await supabase
      .from('processing_status')
      .select('state', { count: 'exact', head: false })
      .gt('updated_at', new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString())
      .group('state');
      
    if (!stateError && stateCounts) {
      summary.states = stateCounts.reduce((acc: any, item: any) => {
        acc[item.state] = item.count;
        return acc;
      }, {});
    }
  } catch (error) {
    console.error("Error getting state counts:", error);
  }
  
  // Get active heartbeats
  try {
    const stateManager = getStateManager(supabase, redis);
    summary.heartbeats = await stateManager.getActiveHeartbeats() || [];
  } catch (error) {
    console.error("Error getting heartbeats:", error);
  }
  
  // Get worker metrics
  try {
    const { data: metrics, error: metricsError } = await supabase
      .from('queue_metrics')
      .select('queue_name, operation, success_count, error_count')
      .gt('created_at', new Date(Date.now() - 60 * 60 * 1000).toISOString())
      .order('created_at', { ascending: false })
      .limit(50);
      
    if (!metricsError && metrics) {
      const workerStats = metrics.reduce((acc: any, item: any) => {
        const key = item.queue_name;
        if (!acc[key]) {
          acc[key] = {
            success: 0,
            error: 0,
            total: 0
          };
        }
        acc[key].success += item.success_count || 0;
        acc[key].error += item.error_count || 0;
        acc[key].total += (item.success_count || 0) + (item.error_count || 0);
        return acc;
      }, {});
      
      summary.metrics = workerStats;
    }
  } catch (error) {
    console.error("Error getting worker metrics:", error);
  }
  
  return new Response(JSON.stringify({ 
    success: true,
    summary
  }), { 
    headers: { ...corsHeaders, 'Content-Type': 'application/json' }
  });
}

async function handleQueues(supabase: any) {
  // Get detailed information about all queues
  const { data: queueTables, error: queueError } = await supabase.rpc('get_all_queue_tables');
  if (queueError) {
    return new Response(JSON.stringify({
      success: false,
      error: queueError.message
    }), { 
      status: 500, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
  
  // For each queue, get stuck messages
  const queueDetails = [];
  for (const queue of queueTables) {
    try {
      const { data: stuckMessages, error: stuckError } = await supabase.rpc(
        'list_stuck_messages', 
        {
          queue_name: queue.queue_name,
          min_minutes_locked: 10
        }
      );
      
      queueDetails.push({
        ...queue,
        stuck_messages: stuckError ? null : (stuckMessages?.length || 0),
        stuck_message_details: stuckError ? null : stuckMessages
      });
    } catch (error) {
      console.error(`Error getting stuck messages for ${queue.queue_name}:`, error);
      queueDetails.push({
        ...queue,
        stuck_messages: null,
        error: error.message
      });
    }
  }
  
  return new Response(JSON.stringify({ 
    success: true,
    queues: queueDetails
  }), { 
    headers: { ...corsHeaders, 'Content-Type': 'application/json' }
  });
}

async function handleStates(supabase: any, entityType: string | null) {
  const query = supabase
    .from('processing_status')
    .select('*')
    .order('updated_at', { ascending: false })
    .limit(100);
    
  if (entityType) {
    query.eq('entity_type', entityType);
  }
  
  const { data, error } = await query;
  
  if (error) {
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), { 
      status: 500, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
  
  return new Response(JSON.stringify({ 
    success: true,
    states: data
  }), { 
    headers: { ...corsHeaders, 'Content-Type': 'application/json' }
  });
}

async function handleReset(supabase: any, redis: Redis, entityType: string, entityId: string) {
  // Reset the entity state to PENDING to allow reprocessing
  try {
    const stateManager = getStateManager(supabase, redis);
    
    const result = await stateManager.updateEntityState(
      entityType,
      entityId,
      ProcessingState.PENDING,
      null,
      {
        resetAt: new Date().toISOString(),
        resetBy: 'monitorDashboard'
      }
    );
    
    if (!result.success) {
      return new Response(JSON.stringify({
        success: false,
        error: result.error || "Failed to reset entity state"
      }), { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }
    
    // Also release any lock
    await stateManager.releaseLock(entityType, entityId);
    
    return new Response(JSON.stringify({ 
      success: true,
      previousState: result.previousState,
      newState: ProcessingState.PENDING
    }), { 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  } catch (error) {
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), { 
      status: 500, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
}

async function handleResetMessage(supabase: any, queueName: string, messageId: string) {
  // Reset the visibility timeout for a stuck message
  try {
    const { data, error } = await supabase.rpc(
      'reset_stuck_message',
      {
        p_queue_name: queueName,
        p_message_id: messageId
      }
    );
    
    if (error) {
      return new Response(JSON.stringify({
        success: false,
        error: error.message
      }), { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }
    
    return new Response(JSON.stringify({ 
      success: true,
      reset: data
    }), { 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  } catch (error) {
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), { 
      status: 500, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
}

async function handleRequeueDLQ(supabase: any, queueName: string, messageId: string) {
  // Requeue a message from the DLQ back to the original queue
  try {
    // Check if this is actually a DLQ
    if (!queueName.endsWith('_dlq')) {
      return new Response(JSON.stringify({
        success: false,
        error: "Not a dead-letter queue"
      }), { 
        status: 400, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }
    
    // Read the message from DLQ
    const { data: messages, error } = await supabase.functions.invoke("readQueue", {
      body: { 
        queue_name: queueName,
        batch_size: 1,
        visibility_timeout: 60,
        message_id: messageId
      }
    });
    
    if (error || !messages || messages.length === 0) {
      return new Response(JSON.stringify({
        success: false,
        error: error?.message || "Message not found"
      }), { 
        status: 404, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }
    
    const dlqMessage = messages[0];
    const originalMessage = dlqMessage.message.originalMessage;
    const originalQueue = dlqMessage.message.metadata?.originalQueue;
    
    if (!originalMessage || !originalQueue) {
      return new Response(JSON.stringify({
        success: false,
        error: "Invalid DLQ message format"
      }), { 
        status: 400, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }
    
    // Send message back to original queue
    const { error: sendError } = await supabase.functions.invoke("sendToQueue", {
      body: {
        queue_name: originalQueue,
        message: originalMessage,
        metadata: {
          requeuedAt: new Date().toISOString(),
          requeuedFrom: queueName,
          originalDlqMessageId: messageId,
          retryAttempt: (dlqMessage.message.metadata?.retryAttempt || 0) + 1
        }
      }
    });
    
    if (sendError) {
      return new Response(JSON.stringify({
        success: false,
        error: sendError.message
      }), { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }
    
    // Delete message from DLQ
    const { error: deleteError } = await supabase.functions.invoke("deleteFromQueue", {
      body: {
        queue_name: queueName,
        message_id: messageId
      }
    });
    
    if (deleteError) {
      return new Response(JSON.stringify({
        success: true,
        warning: `Message sent to ${originalQueue} but failed to delete from ${queueName}: ${deleteError.message}`
      }), { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }
    
    return new Response(JSON.stringify({ 
      success: true,
      requeuedTo: originalQueue
    }), { 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  } catch (error) {
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), { 
      status: 500, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
}

async function handleActiveHeartbeats(supabase: any, redis: Redis) {
  // Get active heartbeats
  try {
    const stateManager = getStateManager(supabase, redis);
    const heartbeats = await stateManager.getActiveHeartbeats();
    
    return new Response(JSON.stringify({ 
      success: true,
      heartbeats
    }), { 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  } catch (error) {
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), { 
      status: 500, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
}

async function handleRunQueueMonitor(supabase: any) {
  // Manually trigger the queue monitor
  try {
    const { data, error } = await supabase.functions.invoke("queueMonitor", {
      body: { 
        threshold_minutes: 10,
        auto_fix: true
      }
    });
    
    if (error) {
      return new Response(JSON.stringify({
        success: false,
        error: error.message
      }), { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }
    
    return new Response(JSON.stringify({ 
      success: true,
      monitorResults: data
    }), { 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  } catch (error) {
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), { 
      status: 500, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
}
