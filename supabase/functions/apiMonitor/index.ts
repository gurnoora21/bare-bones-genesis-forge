
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { getRedis } from "../_shared/upstashRedis.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const redis = getRedis();
    const url = new URL(req.url);
    const api = url.searchParams.get('api') || '*';
    const day = url.searchParams.get('day') || new Date().toISOString().split('T')[0];
    
    // Get all keys matching the pattern
    let stats: Record<string, any> = {};
    
    // Get daily stats
    if (api === '*') {
      // Get all APIs
      const apiPatterns = ['spotify', 'genius'];
      
      for (const apiName of apiPatterns) {
        const dailyStats = await redis.get(`stats:${apiName}:${day}`);
        const errorStats = await redis.get(`errors:${apiName}:${day}`);
        
        if (dailyStats) {
          stats[apiName] = {
            daily: dailyStats,
            errors: errorStats || {}
          };
          
          // Get hourly breakdown
          const hourlyStats: Record<string, any> = {};
          for (let h = 0; h < 24; h++) {
            const hour = h.toString().padStart(2, '0');
            const hourData = await redis.get(`stats:${apiName}:${day}:${hour}`);
            if (hourData) {
              hourlyStats[hour] = hourData;
            }
          }
          
          stats[apiName].hourly = hourlyStats;
        }
      }
    } else {
      // Get specific API
      const dailyStats = await redis.get(`stats:${api}:${day}`);
      const errorStats = await redis.get(`errors:${api}:${day}`);
      
      if (dailyStats) {
        stats[api] = {
          daily: dailyStats,
          errors: errorStats || {}
        };
        
        // Get hourly breakdown
        const hourlyStats: Record<string, any> = {};
        for (let h = 0; h < 24; h++) {
          const hour = h.toString().padStart(2, '0');
          const hourData = await redis.get(`stats:${api}:${day}:${hour}`);
          if (hourData) {
            hourlyStats[hour] = hourData;
          }
        }
        
        stats[api].hourly = hourlyStats;
      }
    }
    
    return new Response(
      JSON.stringify({ 
        success: true,
        day,
        stats
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Error getting API stats:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
