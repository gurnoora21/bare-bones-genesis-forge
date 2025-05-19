import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.7.1";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { corsHeaders } from "../_shared/cors.ts";
import { validateMessage, TrackDiscoveryMessageSchema, type TrackDiscoveryMessage } from "../_shared/types/queueMessages.ts";
import { readQueueMessages, deleteQueueMessage } from "../_shared/pgmqBridge.ts";
import { QueueHelper, getQueueHelper } from "../_shared/queueHelper.ts";
import { logDebug, logError } from "../_shared/debugHelper.ts";

// Initialize Redis client
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
const supabase = createClient(supabaseUrl, supabaseServiceKey);

// Initialize QueueHelper
const queueHelper = getQueueHelper(supabase, redis);

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
    // Use our bridge function to read messages from the queue
    console.log(`Reading messages from ${QUEUE_NAME} queue using bridge function`);
    const messages = await readQueueMessages(
      supabase,
      QUEUE_NAME,
      BATCH_SIZE,
      VISIBILITY_TIMEOUT
    );

    if (!messages || messages.length === 0) {
      console.log("No messages to process");
      return new Response(
        JSON.stringify({ message: "No messages to process" }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    console.log(`Retrieved ${messages.length} messages to process`);

    const results = [];
    const errors = [];

    for (const message of messages) {
      try {
        // Validate the message format
        console.log(`Validating message format for message ID: ${message.id}`);
        const messageBody = typeof message.message === 'string' 
          ? JSON.parse(message.message) 
          : message.message;
        
        if (!messageBody) {
          throw new Error(`Invalid message format: message body is empty or null`);
        }
        
        const validatedMessage = validateMessage(TrackDiscoveryMessageSchema, messageBody);
        
        // Process the validated message
        console.log(`Processing track: ${validatedMessage.trackName}`);
        const result = await processMessage(validatedMessage);
        
        if (result.success) {
          // Debug message structure to identify the correct ID field
          console.log(`Message structure:`, {
            id: message.id,
            msgId: message.msgId,
            msg_id: message.msg_id,
            messageId: message.messageId,
            fullMessage: JSON.stringify(message).substring(0, 200) // Log first 200 chars to avoid huge logs
          });
          
          // Try to find the message ID from various possible properties
          const messageId = message.id || message.msgId || message.msg_id || message.messageId;
          
          // Delete the message from the queue after successful processing using our bridge function
          console.log(`Processing successful, deleting message ID: ${messageId}`);
          await deleteQueueMessage(supabase, QUEUE_NAME, messageId);
          
          results.push({ id: messageId, status: "success" });
        } else {
          errors.push({ id: message.id || message.msgId || message.msg_id || "unknown", error: result.error });
        }
      } catch (error) {
        console.error(`Error processing message ${message.id}:`, error);
        errors.push({ id: message.id, error: error.message });
        
        // If it's a validation error, send to DLQ
        if (error.message.includes("Invalid message format") || error.message.includes("Required")) {
          console.log(`Validation error, sending message ${message.id} to DLQ`);
          try {
            // Use QueueHelper to send to DLQ
            await queueHelper.sendToDLQ(
              QUEUE_NAME,
              message.id,
              message,
              error.message,
              { timestamp: new Date().toISOString() }
            );
          } catch (dlqError) {
            console.error(`Failed to send to DLQ: ${dlqError.message}`);
          }
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
    console.error("Fatal Error:", error);
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        status: 500,
        headers: { ...corsHeaders, "Content-Type": "application/json" }
      }
    );
  }
});
