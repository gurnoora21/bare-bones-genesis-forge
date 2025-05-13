
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
    
    // Get queue stats directly from the view we just fixed
    const { data: queueStats, error: viewError } = await supabase
      .from('queue_monitoring_view')
      .select('*');
    
    if (viewError) {
      throw new Error(`Failed to get queue stats: ${viewError.message}`);
    }
    
    const fixResults: any[] = [];

    // Process each queue that needs fixing
    for (const queueStat of queueStats) {
      try {
        if (queueStat.stuck_messages > 0 && auto_fix) {
          logger.warn(`Found ${queueStat.stuck_messages} stuck messages in ${queueStat.queue_name}`, {
            queue: queueStat.queue_name,
            stuckCount: queueStat.stuck_messages
          });
          
          // Reset stuck messages using our new function
          const { data: resetCount, error: resetError } = await supabase.rpc(
            'reset_stuck_messages',
            {
              queue_name: queueStat.queue_name,
              min_minutes_locked: threshold_minutes
            }
          );
          
          if (resetError) {
            logger.error(`Failed to reset stuck messages in ${queueStat.queue_name}`, resetError, {
              queue: queueStat.queue_name
            });
            
            fixResults.push({
              queue: queueStat.queue_name,
              success: false,
              error: resetError.message
            });
          } else {
            logger.info(`Reset ${resetCount} stuck messages in queue ${queueStat.queue_name}`, {
              queue: queueStat.queue_name,
              resetCount
            });
            
            fixResults.push({
              queue: queueStat.queue_name,
              success: true,
              messagesReset: resetCount
            });
          }
        }
      } catch (queueError) {
        logger.error(`Error processing queue ${queueStat.queue_name}`, queueError);
      }
    }

    // Record monitoring event
    await recordMonitoringEvent(supabase, {
      queuesChecked: queueStats.length,
      totalMessagesInQueues: queueStats.reduce((sum, q) => sum + q.total_messages, 0),
      totalStuckMessages: queueStats.reduce((sum, q) => sum + q.stuck_messages, 0),
      messagesFixed: fixResults.filter(r => r.success).reduce((sum, r) => sum + (r.messagesReset || 0), 0),
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
  // Use the reset_stuck_message RPC function
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
  
  throw new Error(`Failed to reset message: ${error?.message}`);
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
