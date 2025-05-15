
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
    
    // Get stats from all relevant tables - simplified version
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
    
    // Simplified response with just basic counts
    const pipelineStatus = {
      table_counts: {
        artists: artists.count || 0,
        albums: albums.count || 0,
        tracks: tracks.count || 0,
        producers: producers.count || 0
      },
      timestamp: new Date().toISOString()
    };
    
    return new Response(
      JSON.stringify(pipelineStatus),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error("Error checking pipeline status:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
