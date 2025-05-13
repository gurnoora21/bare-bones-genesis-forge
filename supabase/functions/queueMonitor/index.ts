
/**
 * Queue Monitor Edge Function
 * 
 * Monitors all message queues for stuck messages and other issues
 * Optionally auto-fixes common problems
 */
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { StructuredLogger } from "../_shared/structuredLogger.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

interface QueueStats {
  queue_name: string;
  message_count: number;
  oldest_message?: string;
  stuck_count: number;
}

interface StuckMessage {
  id: string;
  msg_id: string;
  locked_since: string;
  minutes_locked: number;
}

serve(async (req) => {
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  const logger = new StructuredLogger({ 
    service: 'queue-monitor',
    functionId: crypto.randomUUID()
  });

  const supabase = createClient(
    Deno.env.get('SUPABASE_URL') || '',
    Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') || ''
  );

  try {
    // Parse request parameters
    const params = req.method === 'POST' 
      ? await req.json() 
      : { threshold_minutes: 10, auto_fix: true };
    
    const threshold_minutes = params.threshold_minutes || 10;
    const auto_fix = params.auto_fix === false ? false : true;

    logger.info("Starting queue monitor", { 
      threshold_minutes,
      auto_fix
    });
    
    // Get all queue tables
    const { data: queueTables, error: queueError } = await supabase.rpc('get_all_queue_tables');
    
    if (queueError) {
      throw new Error(`Failed to get queue tables: ${queueError.message}`);
    }

    const queueStats: QueueStats[] = [];
    const fixResults: any[] = [];

    // Process each queue
    for (const queue of queueTables) {
      try {
        logger.info(`Checking queue: ${queue.queue_name}`);
        
        // Check queue size and oldest message
        const { data: queueStatus, error: statusError } = await supabase.rpc(
          'pg_queue_status',
          { queue_name: queue.queue_name }
        );
        
        if (statusError) {
          logger.error(`Error getting status for queue ${queue.queue_name}`, statusError);
          continue;
        }

        // Get stuck messages
        const { data: stuckMessages, error: stuckError } = await supabase.rpc(
          'list_stuck_messages',
          {
            queue_name: queue.queue_name,
            min_minutes_locked: threshold_minutes
          }
        );
        
        if (stuckError) {
          logger.error(`Error listing stuck messages for queue ${queue.queue_name}`, stuckError);
          continue;
        }

        const queueStat: QueueStats = {
          queue_name: queue.queue_name,
          message_count: queueStatus ? queueStatus.count : 0,
          oldest_message: queueStatus ? queueStatus.oldest_message : null,
          stuck_count: stuckMessages ? stuckMessages.length : 0
        };
        
        queueStats.push(queueStat);
        
        // Auto-fix stuck messages if enabled
        if (auto_fix && stuckMessages && stuckMessages.length > 0) {
          logger.warn(`Found ${stuckMessages.length} stuck messages in ${queue.queue_name}`, {
            queue: queue.queue_name,
            stuckCount: stuckMessages.length
          });
          
          for (const message of stuckMessages) {
            try {
              // Reset the message's visibility timeout
              const resetResult = await resetStuckMessage(supabase, queue.queue_name, message.id, message);
              fixResults.push(resetResult);
              
              logger.info(`Reset stuck message ${message.id} in queue ${queue.queue_name}`, {
                queue: queue.queue_name,
                messageId: message.id, 
                minutesLocked: message.minutes_locked,
                resetResult
              });
            } catch (resetError) {
              logger.error(`Failed to reset message ${message.id}`, resetError, {
                queue: queue.queue_name,
                messageId: message.id
              });
              
              fixResults.push({
                queue: queue.queue_name,
                messageId: message.id,
                success: false,
                error: resetError.message
              });
            }
          }
        }
      } catch (queueError) {
        logger.error(`Error processing queue ${queue.queue_name}`, queueError);
      }
    }

    // Record monitoring event
    await recordMonitoringEvent(supabase, {
      queuesChecked: queueTables.length,
      totalMessagesInQueues: queueStats.reduce((sum, q) => sum + q.message_count, 0),
      totalStuckMessages: queueStats.reduce((sum, q) => sum + q.stuck_count, 0),
      messagesFixed: fixResults.filter(r => r.success).length,
      threshold_minutes
    });

    return new Response(
      JSON.stringify({
        success: true,
        queueStats,
        fixResults,
        timestamp: new Date().toISOString()
      }),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 200
      }
    );
    
  } catch (error) {
    logger.error("Queue monitor failed", error);
    
    return new Response(
      JSON.stringify({ 
        success: false, 
        error: error.message 
      }),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 500
      }
    );
  }
});

/**
 * Reset a stuck message by clearing its visibility timeout
 */
async function resetStuckMessage(
  supabase: any,
  queueName: string,
  messageId: string,
  messageDetails: StuckMessage
): Promise<any> {
  // Try to use the reset_stuck_message RPC first
  const { data, error } = await supabase.rpc(
    'reset_stuck_message',
    {
      queue_name: queueName,
      message_id: messageId
    }
  );
  
  if (!error && data) {
    return {
      queue: queueName,
      messageId,
      success: true,
      method: 'reset_stuck_message',
      messageDetails
    };
  }
  
  // If that fails, try the emergency reset method
  const { data: emergencyData, error: emergencyError } = await supabase.rpc(
    'emergency_reset_message',
    {
      p_queue_name: queueName,
      p_message_id: messageId
    }
  );
  
  if (!emergencyError && emergencyData) {
    return {
      queue: queueName,
      messageId,
      success: true,
      method: 'emergency_reset',
      messageDetails
    };
  }
  
  // Both methods failed
  throw new Error(`Failed to reset message: ${error?.message || emergencyError?.message}`);
}

/**
 * Record monitoring event for historical tracking
 */
async function recordMonitoringEvent(supabase: any, stats: any): Promise<void> {
  await supabase.from('monitoring_events')
    .insert([{
      event_type: 'queue_monitor',
      details: stats,
      created_at: new Date().toISOString()
    }])
    .select();
}
