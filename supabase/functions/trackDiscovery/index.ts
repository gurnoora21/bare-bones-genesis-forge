import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.7.1";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { corsHeaders } from "../_shared/cors.ts";
import { validateMessage, TrackDiscoveryMessageSchema, type TrackDiscoveryMessage } from "../_shared/types/queueMessages.ts";
import { readQueueMessages, deleteQueueMessage } from "../_shared/pgmqBridge.ts";
import { QueueHelper, getQueueHelper } from "../_shared/queueHelper.ts";
import { logDebug, logError } from "../_shared/debugHelper.ts";
import { SpotifyClient } from "../_shared/spotifyClient.ts";
import { DeduplicationService, getDeduplicationService } from "../_shared/deduplication.ts";

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
// Initialize Deduplication Service
const deduplication = getDeduplicationService(redis);

const QUEUE_NAME = "track_discovery";
const BATCH_SIZE = 10;
const VISIBILITY_TIMEOUT = 30; // seconds

/**
 * Process a track discovery message
 * For each album, fetch its tracks from Spotify and store them in the database
 * Then enqueue producer identification jobs for each track
 */
async function processMessage(message: TrackDiscoveryMessage): Promise<{ success: boolean, error?: string }> {
  try {
    const spotifyClient = new SpotifyClient();
    
    // Extract required fields with fallbacks
    const albumId = message.albumId;
    const albumSpotifyId = message.spotifyId || message.albumSpotifyId;
    const artistId = message.artistId;
    const albumName = message.albumName || "Unknown Album";
    const offset = message.offset || 0;
    const limit = 50; // Spotify's default limit for tracks
    
    if (!albumSpotifyId) {
      throw new Error("Missing required field: albumSpotifyId must be provided");
    }
    
    if (!albumId) {
      throw new Error("Missing required field: albumId must be provided");
    }
    
    console.log(`Processing tracks for album ${albumName} (${albumSpotifyId}), offset: ${offset}`);
    
    // Fetch tracks from Spotify
    const tracks = await spotifyClient.getAlbumTracks(albumSpotifyId, offset);
    
    if (!tracks || !tracks.items || tracks.items.length === 0) {
      console.log(`No tracks found for album ${albumName}`);
      return { success: true, message: "No tracks found" };
    }
    
    console.log(`Found ${tracks.items.length} tracks for album ${albumName}`);
    
    // Process each track
    const results = [];
    
    for (const track of tracks.items) {
      // Generate a consistent deduplication key
      const dedupKey = `track_discovery:track:${track.id}`;
      
      // Check if we've already processed this track
      const isDuplicate = await deduplication.isDuplicate(QUEUE_NAME, dedupKey);
      if (isDuplicate) {
        console.log(`Track ${track.name} (${track.id}) already processed, skipping`);
        continue;
      }
      
      // Mark as processed to prevent duplicates
      await deduplication.markAsProcessed(QUEUE_NAME, dedupKey, 86400 * 30); // 30 days TTL
      
      // Insert track into database
      try {
        // Check if track already exists
        const { data: existingTrack } = await supabase
          .from('tracks')
          .select('id')
          .eq('spotify_id', track.id)
          .maybeSingle();
          
        let trackId;
        
        if (existingTrack) {
          trackId = existingTrack.id;
          console.log(`Track ${track.name} already exists, updating`);
          
          // Update track details
          await supabase
            .from('tracks')
            .update({
              name: track.name,
              album_id: albumId,
              duration_ms: track.duration_ms,
              track_number: track.track_number,
              metadata: {
                preview_url: track.preview_url,
                explicit: track.explicit,
                updated_at: new Date().toISOString()
              }
            })
            .eq('id', trackId);
        } else {
          console.log(`Creating new track: ${track.name}`);
          
          // Insert new track
          const { data: newTrack, error: insertError } = await supabase
            .from('tracks')
            .insert({
              album_id: albumId,
              name: track.name,
              spotify_id: track.id,
              duration_ms: track.duration_ms,
              track_number: track.track_number,
              metadata: {
                preview_url: track.preview_url,
                explicit: track.explicit,
                created_at: new Date().toISOString()
              }
            })
            .select('id')
            .single();
            
          if (insertError) {
            console.error(`Error inserting track ${track.name}:`, insertError);
            continue;
          }
          
          trackId = newTrack.id;
        }
        
        // Enqueue producer identification for this track
        const producerDedupKey = `producer_identification:track:${track.id}`;
        const producerMessage = {
          trackId,
          trackName: track.name,
          trackSpotifyId: track.id,
          artistId,
          albumId
        };
        
        const producerEnqueueResult = await queueHelper.enqueue(
          'producer_identification',
          producerMessage,
          producerDedupKey,
          { ttl: 86400 * 7 } // 7 day TTL
        );
        
        console.log(`Enqueued producer identification for track ${track.name}, message ID: ${producerEnqueueResult}`);
        results.push({ trackId, trackName: track.name, producerEnqueued: true });
      } catch (error) {
        console.error(`Error processing track ${track.name}:`, error);
      }
    }
    
    // If there are more tracks, enqueue the next batch
    if (tracks.next) {
      const nextOffset = offset + tracks.limit;
      const nextDedupKey = `track_discovery:album:${albumSpotifyId}:offset:${nextOffset}`;
      
      await queueHelper.enqueue(
        QUEUE_NAME,
        { ...message, offset: nextOffset },
        nextDedupKey,
        { ttl: 86400 * 7 } // 7 day TTL
      );
      
      console.log(`Enqueued next batch of tracks for ${albumName}, offset: ${nextOffset}`);
    }
    
    return { 
      success: true,
      tracks: results.length,
      hasMore: !!tracks.next
    };
  } catch (error) {
    console.error("Error processing track discovery message:", error);
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
        console.log(`Processing tracks for album: ${validatedMessage.albumName || validatedMessage.spotifyId}`);
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
