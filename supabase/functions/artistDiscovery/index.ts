
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.7.1";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { getSpotifyClient } from "../_shared/spotifyClient.ts";
import { EnhancedWorkerBase } from "../_shared/enhancedWorkerBase.ts";
import { StructuredLogger } from "../_shared/structuredLogger.ts";
import { QueueHelper, getQueueHelper } from "../_shared/queueHelper.ts";
import { safeStringify, logDebug, logError, logWarning } from "../_shared/debugHelper.ts";
import { corsHeaders } from "../_shared/cors.ts";
import { validateMessage, ArtistDiscoveryMessageSchema, type ArtistDiscoveryMessage } from "../_shared/types/queueMessages.ts";
import { readQueueMessages, deleteQueueMessage } from "../_shared/pgmqBridge.ts";

// Initialize Redis client
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

// Initialize Supabase client
const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
const supabase = createClient(supabaseUrl, supabaseServiceKey);

// Initialize QueueHelper
const queueHelper = getQueueHelper(supabase, redis);

const QUEUE_NAME = "artist_discovery";
const BATCH_SIZE = 10;
const VISIBILITY_TIMEOUT = 30; // seconds

/**
 * Process an artist discovery message
 * Looks up the artist on Spotify, inserts it into the database, and enqueues an album discovery job
 */
async function processMessage(message: ArtistDiscoveryMessage): Promise<{ success: boolean, error?: string }> {
  try {
    // Handle different message formats more robustly
    let artistMessage = message;
    
    // Extract the message content from different possible formats
    if (message.message) {
      if (typeof message.message === 'object') {
        artistMessage = message.message;
      } else if (typeof message.message === 'string') {
        try {
          artistMessage = JSON.parse(message.message);
        } catch (e) {
          logWarning("ArtistDiscovery", "Failed to parse message string as JSON", e);
          // Keep original message
        }
      }
    }
    
    // Ensure we have the artistName
    if (!artistMessage?.artistName) {
      logError("ArtistDiscovery", "Missing artistName in message", { message: artistMessage });
      throw new Error("Missing artistName in message");
    }
    
    const artistName = artistMessage.artistName;
    logDebug("ArtistDiscovery", "Processing artist discovery message", { artistName });
    
    // Get Spotify client
    const spotifyClient = getSpotifyClient();
    
    // Lookup artist on Spotify
    const searchResponse = await spotifyClient.getArtistByName(artistName);
    
    if (!searchResponse || !searchResponse.artists || !searchResponse.artists.items || searchResponse.artists.items.length === 0) {
      logDebug("ArtistDiscovery", `No results found for artist "${artistName}"`);
      return { success: true, result: 'no_results' };
    }
    
    // Get the first result
    const artist = searchResponse.artists.items[0];
    
    // Check if artist already exists in database
    const { data: existingArtist } = await supabase
      .from('artists')
      .select('id')
      .eq('spotify_id', artist.id)
      .maybeSingle();
    
    let artistId;
    
    if (existingArtist) {
      logDebug("ArtistDiscovery", `Artist ${artist.name} already exists, updating`);
      artistId = existingArtist.id;
      
      // Update the artist
      await supabase
        .from('artists')
        .update({
          name: artist.name,
          followers: artist.followers?.total || 0,
          popularity: artist.popularity || 0,
          image_url: artist.images?.[0]?.url,
          metadata: {
            genres: artist.genres,
            updated_at: new Date().toISOString()
          }
        })
        .eq('id', artistId);
    } else {
      logDebug("ArtistDiscovery", `Creating new artist: ${artist.name}`);
      
      // Insert the artist
      const { data: newArtist, error: insertError } = await supabase
        .from('artists')
        .insert({
          name: artist.name,
          spotify_id: artist.id,
          followers: artist.followers?.total || 0,
          popularity: artist.popularity || 0,
          image_url: artist.images?.[0]?.url,
          metadata: {
            genres: artist.genres,
            created_at: new Date().toISOString()
          }
        })
        .select('id')
        .single();
        
      if (insertError) {
        logError("ArtistDiscovery", `Error inserting artist ${artist.name}:`, insertError);
        throw insertError;
      }
      
      artistId = newArtist.id;
    }
    
    // Enqueue album discovery for this artist
    
    // Create a consistent deduplication key for album discovery
    const albumDedupKey = `album_discovery:artist:${artist.id}:offset:0`;
    logDebug("ArtistDiscovery", `Enqueueing album discovery for artist ${artist.name} (${artist.id})`);
    
    // Prepare message with necessary details
    const albumMessage = {
      spotifyId: artist.id,
      artistName: artist.name,
      artistId: artistId,
      enqueueTime: new Date().toISOString()
    };
    
    // Use QueueHelper to enqueue with deduplication
    const enqueueResult = await queueHelper.enqueue(
      'album_discovery',
      albumMessage,
      albumDedupKey,
      { ttl: 86400 * 7 } // 7 day TTL to avoid duplicate processing
    );
    
    if (enqueueResult) {
      logDebug("ArtistDiscovery", `Album discovery enqueued successfully`, { 
        artistName: artist.name,
        artistId: artist.id,
        messageId: enqueueResult
      });
    } else {
      logError("ArtistDiscovery", `Failed to enqueue album discovery`, {
        artistName: artist.name,
        artistId: artist.id
      });
    }
    
    // Return successful completion even if album enqueue failed
    // We don't want to redo artist discovery just because album discovery couldn't be enqueued
    return { 
      success: true,
      status: 'completed',
      result: 'success',
      artistId,
      spotifyId: artist.id,
      albumEnqueued: !!enqueueResult
    };
  } catch (error) {
    logError("ArtistDiscovery", `Error processing artist message: ${error.message}`, error);
    return { 
      success: false, 
      error: error.message 
    };
  }
}

// Handle HTTP requests
serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response("ok", { headers: corsHeaders });
  }
  
  try {
    console.log("Artist Discovery worker starting");
    
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
          
        const validatedMessage = validateMessage(ArtistDiscoveryMessageSchema, messageBody);
        
        // Process the validated message
        console.log(`Processing artist: ${validatedMessage.artistName}`);
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
          
          // Use msg_id directly when available, otherwise fall back to other ID fields
          const messageId = message.msg_id || message.id || message.msgId || message.messageId;
          
          // Delete the message from the queue after successful processing using our bridge function
          console.log(`Processing successful, deleting message ID: ${messageId}`);
          await deleteQueueMessage(supabase, QUEUE_NAME, messageId, message);
          
          results.push({ id: messageId, status: "success", ...result });
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
