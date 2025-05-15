
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
    
    // Get stats from all relevant tables
    const [
      artists,
      albums,
      tracks,
      producers,
      trackProducers,
      queueRegistry,
      queueMetrics
    ] = await Promise.all([
      supabase.from('artists').select('count', { count: 'exact', head: true }),
      supabase.from('albums').select('count', { count: 'exact', head: true }),
      supabase.from('tracks').select('count', { count: 'exact', head: true }),
      supabase.from('producers').select('count', { count: 'exact', head: true }),
      supabase.from('track_producers').select('count', { count: 'exact', head: true }),
      supabase.from('queue_registry').select('*'),
      supabase.from('queue_metrics')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(20)
    ]);
    
    // Get the most recent artists to check pipeline status
    const { data: recentArtists } = await supabase
      .from('artists')
      .select('id, name, created_at')
      .order('created_at', { ascending: false })
      .limit(5);
      
    // For each recent artist, check if they have albums and tracks
    const artistDetails = [];
    
    if (recentArtists) {
      for (const artist of recentArtists) {
        const [artistAlbums, trackCount] = await Promise.all([
          supabase.from('albums').select('id').eq('artist_id', artist.id),
          supabase.rpc('get_artist_track_count', { artist_id: artist.id })
        ]);
        
        artistDetails.push({
          id: artist.id,
          name: artist.name,
          created_at: artist.created_at,
          album_count: artistAlbums.data?.length || 0,
          track_count: trackCount.data || 0
        });
      }
    }
    
    // Check for any errors in the worker_issues table
    const { data: recentIssues } = await supabase
      .from('worker_issues')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(5);
    
    // Format and send the pipeline status data
    const pipelineStatus = {
      table_counts: {
        artists: artists.count || 0,
        albums: albums.count || 0,
        tracks: tracks.count || 0,
        producers: producers.count || 0,
        track_producers: trackProducers.count || 0
      },
      queue_registry: queueRegistry.data || [],
      recent_queue_metrics: queueMetrics.data || [],
      recent_artists: artistDetails || [],
      recent_issues: recentIssues || [],
      timestamp: new Date().toISOString(),
      status: "healthy" // Default to healthy
    };
    
    // Determine overall health
    if (recentIssues && recentIssues.length > 0) {
      pipelineStatus.status = "warning";
    }
    
    // Check if artists have albums - this would indicate pipeline issues
    if (artistDetails.some(artist => artist.album_count === 0 && new Date(artist.created_at) < new Date(Date.now() - 10 * 60 * 1000))) {
      pipelineStatus.status = "unhealthy";
    }
    
    return new Response(
      JSON.stringify(pipelineStatus),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error("Error checking pipeline status:", error);
    
    return new Response(
      JSON.stringify({ error: error.message, status: "error" }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
