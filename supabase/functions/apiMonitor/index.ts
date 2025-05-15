
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
    // Simplified API to just report basic stats
    const url = new URL(req.url);
    const api = url.searchParams.get('api') || '*';
    
    return new Response(
      JSON.stringify({ 
        success: true,
        message: "API monitoring has been simplified. For detailed metrics, check Supabase logs directly.",
        api_requested: api
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error("Error in apiMonitor:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
