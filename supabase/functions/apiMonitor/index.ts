
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

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
    // Extract API parameter if provided
    const url = new URL(req.url);
    const api = url.searchParams.get('api') || 'all';
    
    // Simplified response - we won't track API stats anymore
    return new Response(
      JSON.stringify({ 
        success: true,
        message: "API monitoring has been simplified to focus on core pipeline functionality",
        timestamp: new Date().toISOString(),
        requested_api: api
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error("Error in apiMonitor:", error);
    
    return new Response(
      JSON.stringify({ 
        error: error.message,
        timestamp: new Date().toISOString()
      }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
