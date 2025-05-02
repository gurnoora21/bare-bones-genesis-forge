
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

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

  try {
    // Get stats from various tables
    const [
      artistsCount, 
      albumsCount, 
      tracksCount, 
      producersCount,
      queueStats,
      recentIssues
    ] = await Promise.all([
      supabase.from('artists').select('*', { count: 'exact', head: true }),
      supabase.from('albums').select('*', { count: 'exact', head: true }),
      supabase.from('tracks').select('*', { count: 'exact', head: true }),
      supabase.from('producers').select('*', { count: 'exact', head: true }),
      getQueueStatistics(supabase),
      supabase.from('worker_issues')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(10)
    ]);
    
    // Compile stats
    const stats = {
      counts: {
        artists: artistsCount.count,
        albums: albumsCount.count,
        tracks: tracksCount.count,
        producers: producersCount.count
      },
      queues: queueStats,
      recent_issues: recentIssues.data || []
    };

    return new Response(
      JSON.stringify(stats),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error("Error getting system metrics:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});

async function getQueueStatistics(supabase: any) {
  const queues = [
    'artist_discovery',
    'album_discovery',
    'track_discovery',
    'producer_identification',
    'social_enrichment'
  ];
  
  const stats: Record<string, any> = {};
  
  for (const queue of queues) {
    const result = await supabase.rpc('pg_queue_status', { queue_name: queue });
    stats[queue] = result.data || { count: 0, oldest_message: null };
  }
  
  return stats;
}
