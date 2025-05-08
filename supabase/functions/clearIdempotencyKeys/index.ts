
// Simple edge function to clear idempotency keys
// This is a convenience wrapper around the manageRedis function

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
    
    console.log("Calling manageRedis to clear idempotency keys");
    
    // Call the manageRedis function to clear idempotency keys
    const { data, error } = await supabase.functions.invoke('manageRedis', {
      method: 'DELETE',
      path: 'clear-idempotency'
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
