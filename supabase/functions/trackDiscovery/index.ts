import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.7.1";
import { corsHeaders } from "../_shared/cors.ts";
import { validateMessage, TrackDiscoveryMessageSchema, type TrackDiscoveryMessage } from "../_shared/types/queueMessages.ts";

const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
const supabase = createClient(supabaseUrl, supabaseServiceKey);

const QUEUE_NAME = "track_discovery";
const BATCH_SIZE = 10;
const VISIBILITY_TIMEOUT = 30; // seconds

async function processMessage(message: TrackDiscoveryMessage) {
  try {
    // Your existing message processing logic here
    console.log("Processing track:", message.trackName);
    
    // Example processing steps:
    // 1. Fetch track details from Spotify
    // 2. Store track in database
    // 3. Enqueue for producer identification
    
    return { success: true };
  } catch (error) {
    console.error("Error processing message:", error);
    return { success: false, error: error.message };
  }
}

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    // Read messages from the queue
    const { data: messages, error: readError } = await supabase
      .rpc("pgmq_read", {
        queue_name: QUEUE_NAME,
        max_messages: BATCH_SIZE,
        visibility_timeout: VISIBILITY_TIMEOUT,
      });

    if (readError) {
      throw readError;
    }

    if (!messages || messages.length === 0) {
      return new Response(
        JSON.stringify({ message: "No messages to process" }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    const results = [];
    const errors = [];

    for (const message of messages) {
      try {
        // Validate the message format
        const validatedMessage = validateMessage(TrackDiscoveryMessageSchema, message);
        
        // Process the validated message
        const result = await processMessage(validatedMessage);
        
        if (result.success) {
          // Delete the message from the queue after successful processing
          await supabase.rpc("pgmq_delete", {
            queue_name: QUEUE_NAME,
            message_id: message.id,
          });
          results.push({ id: message.id, status: "success" });
        } else {
          errors.push({ id: message.id, error: result.error });
        }
      } catch (error) {
        console.error("Error processing message:", error);
        errors.push({ id: message.id, error: error.message });
        
        // If it's a validation error, send to DLQ
        if (error.message.includes("Invalid message format")) {
          await supabase.rpc("pgmq_send", {
            queue_name: `${QUEUE_NAME}_dlq`,
            message: {
              originalMessage: message,
              error: error.message,
              timestamp: new Date().toISOString(),
            },
          });
        }
      }
    }

    return new Response(
      JSON.stringify({
        processed: results.length,
        errors: errors.length,
        results,
        errors,
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  } catch (error) {
    console.error("Error:", error);
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        status: 500,
        headers: { ...corsHeaders, "Content-Type": "application/json" }
      }
    );
  }
});
