
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { logDebug, validateEnvironment, validateQueueMessage } from "../_shared/debugHelper.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  // CORS handling
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Validate environment
    if (!validateEnvironment(['SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY'])) {
      throw new Error("Missing required environment variables");
    }

    // Initialize Supabase client
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    // Extract parameters from request
    const { queueName, batchSize = 5, force = false } = await req.json();

    if (!queueName) {
      return new Response(
        JSON.stringify({ error: "queueName is required" }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    logDebug("forceProcessQueue", `Processing queue ${queueName} with batch size ${batchSize}`);

    // 1. Read messages from queue
    const { data: queueData, error: queueError } = await supabase.rpc('pg_dequeue', {
      queue_name: queueName,
      batch_size: batchSize,
      visibility_timeout: 300 // 5 minutes
    });

    if (queueError) {
      throw new Error(`Error reading from queue: ${queueError.message}`);
    }

    logDebug("forceProcessQueue", "Queue data received", queueData);

    const messages = queueData || [];
    const messageCount = Array.isArray(messages) ? messages.length : 0;

    if (messageCount === 0) {
      return new Response(
        JSON.stringify({ success: true, message: `No messages in queue ${queueName}` }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // 2. Trigger the appropriate worker based on queue name
    let workerName;
    switch (queueName) {
      case 'artist_discovery':
        workerName = 'artistDiscovery';
        break;
      case 'album_discovery':
        workerName = 'albumDiscovery';
        break;
      case 'track_discovery':
        workerName = 'trackDiscovery';
        break;
      case 'producer_identification':
        workerName = 'producerIdentification';
        break;
      case 'social_enrichment':
        workerName = 'socialEnrichment';
        break;
      default:
        throw new Error(`Unknown queue name: ${queueName}`);
    }

    // 3. Directly call the worker function
    logDebug("forceProcessQueue", `Triggering worker ${workerName} for ${messageCount} messages`);

    try {
      const response = await supabase.functions.invoke(workerName, {
        body: {
          forceProcessing: true,  // Signal the worker this is a forced run
          skipDeduplication: force // Option to skip deduplication
        }
      });

      if (response.error) {
        throw new Error(`Error triggering worker: ${response.error}`);
      }

      logDebug("forceProcessQueue", `Successfully triggered worker ${workerName}`, response.data);

      // Wait briefly to allow worker to process
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Check if the messages are still in the queue
      const checkResults = await Promise.all(messages.map(async (msg: any) => {
        const msgId = msg.id || msg.msg_id;
        if (!msgId) return { id: 'unknown', status: 'unknown' };

        // Check if message was processed by looking it up
        const { data: checkData } = await supabase.rpc('pg_dequeue', {
          queue_name: queueName,
          batch_size: 1,
          visibility_timeout: 1
        });

        const stillExists = Array.isArray(checkData) && checkData.length > 0 && 
          checkData.some((m: any) => (m.id || m.msg_id) === msgId);

        return {
          id: msgId,
          status: stillExists ? 'still_queued' : 'processed'
        };
      }));

      return new Response(
        JSON.stringify({
          success: true,
          message: `Triggered worker ${workerName} to process ${messageCount} messages`,
          results: checkResults
        }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );

    } catch (error) {
      logDebug("forceProcessQueue", "Error during force processing", error);
      throw error;
    }

  } catch (error) {
    console.error("Error in forceProcessQueue:", error);
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
