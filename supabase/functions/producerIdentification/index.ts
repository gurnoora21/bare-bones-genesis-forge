
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { GeniusClient } from "../_shared/geniusClient.ts";

interface ProducerIdentificationMsg {
  trackId: string;
  trackName: string;
  albumId: string;
  artistId: string;
}

interface ProducerCandidate {
  name: string;
  confidence: number;
  source: string;
}

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );

  // Process queue batch
  const { data: messages, error } = await supabase.functions.invoke("readQueue", {
    body: { 
      queue_name: "producer_identification",
      batch_size: 5,
      visibility_timeout: 300 // 5 minutes
    }
  });

  if (error) {
    console.error("Error reading from queue:", error);
    return new Response(JSON.stringify({ error }), { status: 500, headers: corsHeaders });
  }

  if (!messages || messages.length === 0) {
    return new Response(JSON.stringify({ processed: 0, message: "No messages to process" }), { headers: corsHeaders });
  }

  // Initialize the Genius client
  const geniusClient = new GeniusClient();

  // Process messages with background tasks
  const promises = messages.map(async (message) => {
    // Ensure the message is properly typed
    const msg = typeof message.message === 'string' 
      ? JSON.parse(message.message) as ProducerIdentificationMsg 
      : message.message as ProducerIdentificationMsg;
      
    const messageId = message.id;
    
    try {
      await identifyProducers(supabase, geniusClient, msg);
      // Archive processed message
      await supabase.functions.invoke("deleteFromQueue", {
        body: { queue_name: "producer_identification", message_id: messageId }
      });
      console.log(`Successfully processed producer identification message ${messageId}`);
    } catch (error) {
      console.error(`Error processing producer identification message ${messageId}:`, error);
      // Message will return to queue after visibility timeout
    }
  });

  // Wait for all background tasks in a background process
  EdgeRuntime.waitUntil(Promise.all(promises));
  
  return new Response(JSON.stringify({ 
    processed: messages.length,
    success: true
  }), { headers: corsHeaders });
});

async function identifyProducers(
  supabase: any, 
  geniusClient: any,
  msg: ProducerIdentificationMsg
) {
  const { trackId, trackName, albumId, artistId } = msg;
  
  // Get track details from database
  const { data: trackData, error: trackError } = await supabase
    .from('tracks')
    .select('id, name, metadata')
    .eq('id', trackId)
    .single();

  if (trackError || !trackData) {
    throw new Error(`Track not found with ID: ${trackId}`);
  }

  // Get artist details for Genius search
  const { data: artistData, error: artistError } = await supabase
    .from('artists')
    .select('id, name')
    .eq('id', artistId)
    .single();

  if (artistError || !artistData) {
    throw new Error(`Artist not found with ID: ${artistId}`);
  }

  // Extract producers from multiple sources
  const producers: ProducerCandidate[] = [];
  
  // 1. Extract from Spotify metadata if available
  if (trackData.metadata) {
    // Look for producer credits in metadata
    try {
      if (trackData.metadata.producers) {
        trackData.metadata.producers.forEach((producer: string) => {
          producers.push({
            name: producer,
            confidence: 0.9,
            source: 'spotify_metadata'
          });
        });
      }
      
      // Extract from artists list (collaborators)
      if (trackData.metadata.artists) {
        const collaborators = trackData.metadata.artists.filter((artist: any) => 
          artist.id !== artistId // Skip the main artist
        );
        
        collaborators.forEach((artist: any) => {
          producers.push({
            name: artist.name,
            confidence: 0.7, // Lower confidence for mere collaborators
            source: 'spotify_collaboration'
          });
        });
      }
      
      // Look for producer information in track credits
      if (trackData.metadata.credits) {
        const producerCredits = trackData.metadata.credits.filter((credit: any) => 
          credit.role?.toLowerCase().includes('produc') || 
          credit.role?.toLowerCase().includes('beat') ||
          credit.role?.toLowerCase().includes('instrumental')
        );
        
        producerCredits.forEach((credit: any) => {
          producers.push({
            name: credit.name,
            confidence: 0.85,
            source: 'spotify_credits'
          });
        });
      }
    } catch (error) {
      console.warn('Error extracting Spotify producers:', error);
    }
  }
  
  // 2. Search Genius for producer information
  try {
    // First, search for the song on Genius
    const searchResult = await geniusClient.search(trackName, artistData.name);
    
    if (searchResult) {
      // Get song details
      const songDetails = await geniusClient.getSong(searchResult.id);
      
      if (songDetails) {
        // Extract producers from song details
        const geniusProducers = geniusClient.extractProducers(songDetails);
        
        // Add to our producers list
        geniusProducers.forEach((producer: any) => {
          producers.push({
            name: producer.name,
            confidence: producer.confidence,
            source: producer.source
          });
        });
      }
    }
  } catch (error) {
    console.warn(`Genius search failed for ${trackName}:`, error);
  }

  // Deduplicate producers
  const uniqueProducers = deduplicateProducers(producers);
  console.log(`Found ${uniqueProducers.length} producers for track "${trackName}"`);
  
  // Process each producer
  for (const producer of uniqueProducers) {
    // Normalize producer name for database
    const normalizedName = normalizeProducerName(producer.name);
    
    if (!normalizedName) continue; // Skip empty names
    
    // Upsert producer in database
    const { data: dbProducer, error } = await supabase
      .from('producers')
      .upsert({
        name: producer.name,
        normalized_name: normalizedName,
        metadata: {
          source: producer.source,
          updated_at: new Date().toISOString()
        }
      }, {
        onConflict: 'normalized_name'
      })
      .select('id, enriched_at, enrichment_failed')
      .single();

    if (error) {
      console.error(`Error upserting producer ${producer.name}:`, error);
      continue;
    }
    
    // Make sure producer exists and has an id
    if (!dbProducer || !dbProducer.id) {
      console.error(`Missing producer data for ${producer.name}`);
      continue;
    }

    // Associate producer with track
    await supabase
      .from('track_producers')
      .upsert({
        track_id: trackId,
        producer_id: dbProducer.id,
        confidence: producer.confidence,
        source: producer.source
      }, {
        onConflict: 'track_id,producer_id'
      });
    
    console.log(`Linked producer ${producer.name} to track "${trackName}"`);

    // If producer hasn't been enriched yet, enqueue social enrichment
    if (!dbProducer.enriched_at && !dbProducer.enrichment_failed) {
      await supabase.functions.invoke("sendToQueue", {
        body: {
          queue_name: "social_enrichment",
          message: { 
            producerId: dbProducer.id,
            producerName: producer.name
          }
        }
      });
      console.log(`Enqueued social enrichment for producer ${producer.name}`);
    }
  }
}

// Helper functions
function normalizeProducerName(name: string): string {
  if (!name) return '';
  
  // Remove extraneous information
  let normalized = name
    .replace(/\([^)]*\)/g, '') // Remove text in parentheses
    .replace(/\[[^\]]*\]/g, '') // Remove text in brackets
    
  // Remove special characters and trim
  normalized = normalized
    .replace(/[^\w\s]/g, ' ') // Replace special chars with space
    .replace(/\s+/g, ' ')     // Replace multiple spaces with single space
    .trim()
    .toLowerCase();
    
  return normalized;
}

function deduplicateProducers(producers: ProducerCandidate[]): ProducerCandidate[] {
  const producerMap = new Map<string, ProducerCandidate>();
  
  for (const producer of producers) {
    if (!producer.name) continue;
    
    const normalizedName = normalizeProducerName(producer.name);
    
    if (!normalizedName) continue;
    
    const existing = producerMap.get(normalizedName);
    
    if (!existing || producer.confidence > existing.confidence) {
      producerMap.set(normalizedName, producer);
    }
  }
  
  return Array.from(producerMap.values());
}
