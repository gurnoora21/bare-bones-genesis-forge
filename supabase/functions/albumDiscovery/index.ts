import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { SpotifyClient } from "../_shared/spotifyClient.ts";
import { DeduplicationService, getDeduplicationService } from "../_shared/deduplication.ts";
import { QueueHelper, getQueueHelper } from "../_shared/queueHelper.ts";
import { getDeduplicationMetrics } from "../_shared/metrics.ts";

// Initialize Redis client
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

interface AlbumDiscoveryMsg {
  artistId: string;
  artistName: string;
  offset?: number;
}

// Common CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Maximum retries for a message before sending to DLQ
const MAX_RETRIES = 3;

serve(async (req) => {
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  // Initialize Supabase client
  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );
  
  // Initialize services
  const spotifyClient = new SpotifyClient();
  const deduplicationService = getDeduplicationService(redis);
  const queueHelper = getQueueHelper(supabase, redis);
  const metrics = getDeduplicationMetrics(redis);

  try {
    console.log("Starting album discovery process");
    
    // Direct RPC call to pg_dequeue for better performance
    const { data: queueData, error } = await supabase.rpc('pg_dequeue', { 
      queue_name: "album_discovery",
      batch_size: 5,
      visibility_timeout: 900 // 15 minutes
    });

    if (error) {
      console.error("Error reading from queue:", error);
      return new Response(JSON.stringify({ error: error.message }), { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    // Parse messages
    let messages;
    if (typeof queueData === 'string') {
      try {
        messages = JSON.parse(queueData);
      } catch (parseError) {
        console.error("Error parsing queue data:", parseError);
        return new Response(JSON.stringify({ error: "Invalid queue data format" }), { 
          status: 500, 
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    } else {
      messages = queueData;
    }
    
    if (!messages || messages.length === 0) {
      console.log("No messages to process in album_discovery queue");
      return new Response(JSON.stringify({ processed: 0, message: "No messages to process" }), { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      });
    }

    console.log(`Found ${messages.length} messages to process in album_discovery queue`);
    
    // Process in background to avoid edge function timeout
    const response = new Response(JSON.stringify({ 
      processing: true, 
      message_count: messages.length 
    }), { 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
    });

    // Process messages in background to avoid CPU timeout
    EdgeRuntime.waitUntil((async () => {
      // Track success/error counts for metrics
      let successCount = 0;
      let errorCount = 0;
      let dedupedCount = 0;
      let sentToDlqCount = 0;
      
      // Process each message independently
      for (const message of messages) {
        try {
          let msg: AlbumDiscoveryMsg;
          if (typeof message.message === 'string') {
            msg = JSON.parse(message.message) as AlbumDiscoveryMsg;
          } else {
            msg = message.message as AlbumDiscoveryMsg;
          }
          
          const messageId = message.id || message.msg_id;
          if (!messageId) {
            console.error("Message ID is undefined or null, cannot process this message safely");
            errorCount++;
            continue;
          }
          
          // Process the message with isolated error handling
          const result = await processAlbumMessage(
            supabase, spotifyClient, deduplicationService, queueHelper, 
            msg, messageId, message.attempts || 0
          );
          
          // Track metrics based on result
          if (result.success) {
            successCount++;
            
            // Delete the message after successful processing
            await queueHelper.deleteMessage("album_discovery", messageId);
          } else if (result.deduped) {
            dedupedCount++;
            
            // Delete the message if it was a duplicate
            await queueHelper.deleteMessage("album_discovery", messageId);
          } else if (result.sentToDlq) {
            sentToDlqCount++;
            
            // Message already deleted by sendToDLQ
          } else {
            errorCount++;
            
            // Only send to DLQ if max retries exceeded
            const attempts = message.attempts || 0;
            if (attempts >= MAX_RETRIES) {
              await queueHelper.sendToDLQ(
                "album_discovery", 
                messageId, 
                msg, 
                result.error || "Max retries exceeded",
                { attempts }
              );
            }
          }
        } catch (messageError) {
          console.error(`Error processing message:`, messageError);
          errorCount++;
        }
      }

      // Record final metrics
      console.log(`Completed background processing: ${successCount} successful, ${dedupedCount} deduplicated, ${errorCount} failed, ${sentToDlqCount} sent to DLQ`);
      
      try {
        await supabase.from('queue_metrics').insert({
          queue_name: "album_discovery",
          operation: "batch_processing",
          processed_count: messages.length,
          success_count: successCount,
          error_count: errorCount,
          details: { 
            timestamp: new Date().toISOString(),
            deduplicated_count: dedupedCount,
            sent_to_dlq_count: sentToDlqCount
          }
        });
      } catch (metricsError) {
        console.error("Failed to record metrics:", metricsError);
      }
    })());
    
    return response;
  } catch (error) {
    console.error("Unexpected error in album discovery worker:", error);
    return new Response(JSON.stringify({ error: error.message }), 
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }
});

/**
 * Process a single album discovery message with isolated error handling
 */
async function processAlbumMessage(
  supabase: any,
  spotifyClient: SpotifyClient,
  deduplicationService: DeduplicationService,
  queueHelper: QueueHelper,
  msg: AlbumDiscoveryMsg,
  messageId: string,
  attempts: number
): Promise<{
  success: boolean;
  deduped?: boolean;
  sentToDlq?: boolean;
  error?: string;
}> {
  const { artistId, artistName, offset = 0 } = msg;
  console.log(`Processing albums for artist ${artistName} (ID: ${artistId}) at offset ${offset}`);
  
  try {
    // Generate deduplication key
    const dedupKey = `artist:${artistId}:offset:${offset}`;
    
    // Check for deduplication
    const isDuplicate = await deduplicationService.isDuplicate(
      'album_discovery',
      dedupKey,
      { logDetails: true },
      { entityId: artistId }
    );
    
    if (isDuplicate) {
      console.log(`Albums for artist ${artistId} at offset ${offset} already processed, skipping`);
      return { success: true, deduped: true };
    }
    
    // Check if artist exists in DB
    const { data: artist, error: artistError } = await supabase
      .from('artists')
      .select('id, name')
      .eq('id', artistId)
      .single();
    
    if (artistError || !artist) {
      console.error(`Artist not found with ID: ${artistId}`);
      return { success: false, error: `Artist not found: ${artistId}` };
    }
    
    // Fetch albums from Spotify in an isolated transaction
    console.log(`Fetching albums from Spotify for artist ${artistName} (ID: ${artistId})`);
    const albumsData = await spotifyClient.getArtistAlbums(artistId, offset);
    console.log(`Found ${albumsData.items.length} albums for artist ${artistName} (total: ${albumsData.total})`);
    
    // Process inside a transaction
    await supabase.transaction(async (tx: any) => {
      // Process each album
      for (const album of albumsData.items) {
        try {
          // Check if album already exists
          const { data: existingAlbum } = await tx
            .from('albums')
            .select('id')
            .eq('spotify_id', album.id)
            .maybeSingle();
          
          if (existingAlbum) {
            console.log(`Album ${album.name} (${album.id}) already exists, skipping`);
            continue;
          }
          
          // Insert album
          const { data: newAlbum, error: insertError } = await tx
            .from('albums')
            .insert({
              name: album.name,
              spotify_id: album.id,
              release_date: album.release_date,
              album_type: album.album_type,
              total_tracks: album.total_tracks,
              metadata: {
                images: album.images,
                uri: album.uri,
                markets: album.available_markets,
                updated_at: new Date().toISOString()
              }
            })
            .select('id')
            .single();
          
          if (insertError) {
            console.error(`Error inserting album ${album.name}:`, insertError);
            continue;
          }
          
          // Create artist-album relationship
          await tx.from('artist_albums').insert({
            artist_id: artistId,
            album_id: newAlbum.id,
            is_primary_artist: true
          });
          
          // Enqueue track discovery for this album
          await queueHelper.enqueue(
            'track_discovery',
            {
              albumId: newAlbum.id,
              albumName: album.name,
              artistId: artistId
            },
            `album:${newAlbum.id}`
          );
        } catch (albumError) {
          // Log error but continue with next album
          console.error(`Error processing album ${album.name}:`, albumError);
        }
      }
    });
    
    // Handle pagination - enqueue next page if needed
    if (albumsData.items.length > 0 && offset + albumsData.items.length < albumsData.total) {
      const newOffset = offset + albumsData.items.length;
      console.log(`Enqueueing next page of albums for artist ${artistName} with offset ${newOffset}`);
      
      await queueHelper.enqueue(
        'album_discovery',
        { 
          artistId, 
          artistName, 
          offset: newOffset 
        },
        `artist:${artistId}:offset:${newOffset}`
      );
    }
    
    // Mark this batch as processed
    await deduplicationService.markAsProcessed(
      'album_discovery',
      dedupKey,
      86400, // 24 hour TTL
      { entityId: artistId }
    );
    
    return { success: true };
  } catch (error) {
    console.error(`Error processing albums for artist ${artistName}:`, error);
    return { 
      success: false, 
      error: error instanceof Error ? error.message : String(error)
    };
  }
}
