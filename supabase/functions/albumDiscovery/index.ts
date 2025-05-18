
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.7.1";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { SpotifyClient } from "../_shared/spotifyClient.ts";
import { DeduplicationService, getDeduplicationService } from "../_shared/deduplication.ts";
import { QueueHelper, getQueueHelper } from "../_shared/queueHelper.ts";
import { getDeduplicationMetrics } from "../_shared/metrics.ts";
import { corsHeaders } from "../_shared/cors.ts";
import { validateMessage, AlbumDiscoveryMessageSchema, type AlbumDiscoveryMessage } from "../_shared/types/queueMessages.ts";
import { readQueueMessages, deleteQueueMessage } from "../_shared/pgmqBridge.ts";

// Initialize Redis client
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

interface AlbumDiscoveryMsg {
  spotifyId: string;
  artistName: string;
  offset?: number;
}

const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
const supabase = createClient(supabaseUrl, supabaseServiceKey);

const QUEUE_NAME = "album_discovery";
const BATCH_SIZE = 10;
const VISIBILITY_TIMEOUT = 30; // seconds

/**
 * Format a release date from Spotify to a valid Postgres date format
 * Handles different formats like "2002", "2002-01", or "2002-01-01"
 */
function formatReleaseDate(releaseDate: string | null | undefined): string | null {
  if (!releaseDate) return null;
  
  // If it's already a full date (YYYY-MM-DD), use as is
  if (/^\d{4}-\d{2}-\d{2}$/.test(releaseDate)) {
    return releaseDate;
  }
  
  // If it's just a year (YYYY), append -01-01
  if (/^\d{4}$/.test(releaseDate)) {
    return `${releaseDate}-01-01`;
  }
  
  // If it's a year and month (YYYY-MM), append -01
  if (/^\d{4}-\d{2}$/.test(releaseDate)) {
    return `${releaseDate}-01`;
  }
  
  // If unknown format, return null instead of crashing
  console.warn(`Unknown release date format: ${releaseDate}`);
  return null;
}

async function processMessage(message: AlbumDiscoveryMessage) {
  try {
    // Your existing message processing logic here
    console.log("Processing album:", message.albumName);
    
    // Example processing steps:
    // 1. Fetch album details from Spotify
    // 2. Store album in database
    // 3. Enqueue for track discovery
    
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
    // Use our new bridge function to read messages from the queue
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
        
        const validatedMessage = validateMessage(AlbumDiscoveryMessageSchema, messageBody);
        
        // Process the validated message
        const result = await processMessage(validatedMessage);
        
        if (result.success) {
          // Delete the message from the queue after successful processing using our bridge function
          console.log(`Processing successful, deleting message ID: ${message.id}`);
          await deleteQueueMessage(supabase, QUEUE_NAME, message.id);
          
          results.push({ id: message.id, status: "success" });
        } else {
          errors.push({ id: message.id, error: result.error });
        }
      } catch (error) {
        console.error(`Error processing message ${message.id}:`, error);
        errors.push({ id: message.id, error: error.message });
        
        // If it's a validation error, send to DLQ
        if (error.message.includes("Invalid message format")) {
          console.log(`Validation error, sending message ${message.id} to DLQ`);
          try {
            await supabase.rpc("move_to_dead_letter_queue", {
              source_queue: QUEUE_NAME,
              dlq_name: `${QUEUE_NAME}_dlq`,
              message_id: message.id,
              failure_reason: error.message
            });
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
