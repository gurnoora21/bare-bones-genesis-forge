
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Initialize Supabase client
    const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
    const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
    const supabase = createClient(supabaseUrl, supabaseKey);
    
    // Get counts from all relevant tables
    const [
      artists,
      albums,
      tracks,
      producers
    ] = await Promise.all([
      supabase.from('artists').select('count', { count: 'exact', head: true }),
      supabase.from('albums').select('count', { count: 'exact', head: true }),
      supabase.from('tracks').select('count', { count: 'exact', head: true }),
      supabase.from('producers').select('count', { count: 'exact', head: true })
    ]);
    
    // Calculate pipeline health
    const artistCount = artists.count || 0;
    const albumCount = albums.count || 0;
    const trackCount = tracks.count || 0;
    const producerCount = producers.count || 0;
    
    // Simple pipeline status information
    const pipelineStatus = {
      table_counts: {
        artists: artistCount,
        albums: albumCount,
        tracks: trackCount,
        producers: producerCount
      },
      timestamp: new Date().toISOString(),
      health_check: {
        status: "ok",
        issues: []
      }
    };
    
    // Perform basic health checks
    const issues = [];
    
    // Check if we have artists but no albums (potential pipeline breakage)
    if (artistCount > 0 && albumCount === 0) {
      issues.push("Artists exist but no albums found - potential issue with album discovery");
      pipelineStatus.health_check.status = "warning";
    }
    
    // Check if we have albums but no tracks (potential pipeline breakage)
    if (albumCount > 0 && trackCount === 0) {
      issues.push("Albums exist but no tracks found - potential issue with track discovery");
      pipelineStatus.health_check.status = "warning";
    }
    
    // Check if we have tracks but no producers (potential pipeline breakage)
    if (trackCount > 0 && producerCount === 0) {
      issues.push("Tracks exist but no producers found - potential issue with producer identification");
      pipelineStatus.health_check.status = "warning";
    }
    
    pipelineStatus.health_check.issues = issues;
    
    return new Response(
      JSON.stringify(pipelineStatus),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error("Error checking pipeline status:", error);
    
    return new Response(
      JSON.stringify({ 
        error: error.message,
        timestamp: new Date().toISOString()
      }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
