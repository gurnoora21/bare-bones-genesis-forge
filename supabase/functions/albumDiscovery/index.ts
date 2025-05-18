
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
import { logDebug, logError, logWarning } from "../_shared/debugHelper.ts";

// Initialize Redis client
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

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

/**
 * Process an album discovery message
 */
async function processMessage(message: AlbumDiscoveryMessage): Promise<{ success: boolean, error?: string }> {
  try {
    const spotifyClient = new SpotifyClient();
    const queueHelper = getQueueHelper(supabase, redis);
    const deduplication = getDeduplicationService(redis);
    
    // Ensure we have the required fields
    if (!message.spotifyId || !message.artistName) {
      throw new Error("Missing required fields: spotifyId and artistName must be provided");
    }
    
    console.log(`Processing albums for artist ${message.artistName} (${message.spotifyId})`);
    
    // Fetch albums from Spotify
    const offset = message.offset || 0;
    const albums = await spotifyClient.getArtistAlbums(message.spotifyId, offset);
    
    if (!albums || !albums.items || albums.items.length === 0) {
      console.log(`No albums found for artist ${message.artistName}`);
      return { success: true, message: "No albums found" };
    }
    
    console.log(`Found ${albums.items.length} albums for artist ${message.artistName}`);
    
    // Process each album
    const artistId = message.artistId;
    const results = [];
    
    for (const album of albums.items) {
      // Generate a consistent deduplication key
      const dedupKey = `album_discovery:album:${album.id}`;
      
      // Check if we've already processed this album
      const isDuplicate = await deduplication.checkAndSet(dedupKey, { ttl: 86400 * 30 });
      if (isDuplicate) {
        console.log(`Album ${album.name} (${album.id}) already processed, skipping`);
        continue;
      }
      
      // Insert album into database
      try {
        const { data: existingAlbum } = await supabase
          .from('albums')
          .select('id')
          .eq('spotify_id', album.id)
          .maybeSingle();
          
        let albumId;
        
        if (existingAlbum) {
          albumId = existingAlbum.id;
          console.log(`Album ${album.name} already exists, updating`);
          
          await supabase
            .from('albums')
            .update({
              name: album.name,
              release_date: formatReleaseDate(album.release_date),
              cover_url: album.images?.[0]?.url,
              metadata: {
                album_type: album.album_type,
                total_tracks: album.total_tracks,
                updated_at: new Date().toISOString()
              }
            })
            .eq('id', albumId);
        } else {
          console.log(`Creating new album: ${album.name}`);
          
          const { data: newAlbum, error: insertError } = await supabase
            .from('albums')
            .insert({
              artist_id: artistId,
              name: album.name,
              spotify_id: album.id,
              release_date: formatReleaseDate(album.release_date),
              cover_url: album.images?.[0]?.url,
              metadata: {
                album_type: album.album_type,
                total_tracks: album.total_tracks,
                created_at: new Date().toISOString()
              }
            })
            .select('id')
            .single();
            
          if (insertError) {
            console.error(`Error inserting album ${album.name}:`, insertError);
            continue;
          }
          
          albumId = newAlbum.id;
        }
        
        // Enqueue track discovery for this album
        const trackDedupKey = `track_discovery:album:${album.id}:offset:0`;
        const trackMessage = {
          albumId,
          albumName: album.name,
          spotifyId: album.id,
          artistId,
          artistName: message.artistName,
          offset: 0,
          totalTracks: album.total_tracks || 50
        };
        
        const trackEnqueueResult = await queueHelper.enqueue(
          'track_discovery',
          trackMessage,
          trackDedupKey,
          { ttl: 86400 * 7 }
        );
        
        console.log(`Enqueued track discovery for album ${album.name}, message ID: ${trackEnqueueResult}`);
        results.push({ albumId, albumName: album.name, tracksEnqueued: true });
      } catch (error) {
        console.error(`Error processing album ${album.name}:`, error);
      }
    }
    
    // If there are more albums, enqueue the next batch
    if (albums.next) {
      const nextOffset = offset + albums.limit;
      const nextDedupKey = `album_discovery:artist:${message.spotifyId}:offset:${nextOffset}`;
      
      await queueHelper.enqueue(
        QUEUE_NAME,
        { ...message, offset: nextOffset },
        nextDedupKey,
        { ttl: 86400 * 7 }
      );
      
      console.log(`Enqueued next batch of albums for ${message.artistName}, offset: ${nextOffset}`);
    }
    
    return { 
      success: true,
      albums: results.length,
      hasMore: !!albums.next
    };
  } catch (error) {
    console.error("Error processing album discovery message:", error);
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
        
        const validatedMessage = validateMessage(AlbumDiscoveryMessageSchema, messageBody);
        
        // Process the validated message
        console.log(`Processing album discovery for artist: ${validatedMessage.artistName}`);
        const result = await processMessage(validatedMessage);
        
        if (result.success) {
          // Delete the message from the queue after successful processing using our bridge function
          console.log(`Processing successful, deleting message ID: ${message.id}`);
          await deleteQueueMessage(supabase, QUEUE_NAME, message.id);
          
          results.push({ id: message.id, status: "success", ...result });
        } else {
          errors.push({ id: message.id, error: result.error });
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
