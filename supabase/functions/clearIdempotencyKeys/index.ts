
// Enhanced edge function to clear idempotency keys
// Supports queue-specific clearing and age-based filtering

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
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
    
    // Extract parameters from URL or request body
    let queueName = '';
    let ageMinutes = 0;
    
    // Parse the URL for path parameters, e.g., /clearIdempotencyKeys/artist_discovery/60
    const url = new URL(req.url);
    const pathParts = url.pathname.split('/').filter(Boolean);
    
    if (pathParts.length > 1) {
      queueName = pathParts[1]; // e.g., "artist_discovery"
      
      if (pathParts.length > 2) {
        ageMinutes = parseInt(pathParts[2]) || 0; // e.g., 60 (for 60 minutes)
      }
    }
    
    // If not found in path, try query parameters
    if (!queueName) {
      queueName = url.searchParams.get('queue') || '';
    }
    if (!ageMinutes) {
      ageMinutes = parseInt(url.searchParams.get('age') || '0');
    }
    
    // If still not found, try request body
    if ((!queueName || !ageMinutes) && req.body) {
      try {
        const body = await req.json();
        if (!queueName) {
          queueName = body.queue || body.queueName || '';
        }
        if (!ageMinutes) {
          ageMinutes = body.age || body.ageMinutes || 0;
        }
      } catch (e) {
        // Ignore JSON parsing errors
      }
    }
    
    console.log(`Calling manageRedis to clear idempotency keys for queue: ${queueName || 'all'}, age: ${ageMinutes || 'all'}`);
    
    // Call the manageRedis function with the proper path and method
    const { data, error } = await supabase.functions.invoke('manageRedis', {
      method: 'DELETE',
      body: { 
        operation: 'clear-idempotency',
        queueName,
        age: ageMinutes
      }
    });
    
    if (error) {
      console.error("Error clearing idempotency keys:", error);
      throw new Error(`Failed to clear idempotency keys: ${error.message}`);
    }
    
    console.log("Successfully cleared idempotency keys:", data);
    
    return new Response(
      JSON.stringify({ 
        success: true,
        message: `Successfully cleared ${data.keysDeleted} idempotency keys`,
        details: data
      }),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 200 
      }
    );
  } catch (error) {
    console.error("Error clearing idempotency keys:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      }
    );
  }
});
