import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { logDebug, logError } from "../_shared/debugHelper.ts";

// Initialize Supabase client
const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const supabase = createClient(supabaseUrl, supabaseKey);

// Common CORS headers
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
    // Parse the request body
    const { queue_name, batch_size = 10, visibility_timeout = 60 } = await req.json();
    
    if (!queue_name) {
      return new Response(
        JSON.stringify({ error: "Missing queue_name parameter" }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    logDebug("ReadQueue", `Reading up to ${batch_size} messages from queue ${queue_name} with visibility timeout ${visibility_timeout}s`);
    
    // Use pg_dequeue directly - the standard way to read messages
    const { data, error } = await supabase.rpc('pg_dequeue', {
      queue_name: queue_name,
      batch_size: batch_size,
      visibility_timeout: visibility_timeout
    });
    
    if (error) {
      logError("ReadQueue", `Error reading from queue: ${error.message}`);
      return new Response(
        JSON.stringify({ error: error.message }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    // Parse the JSON result if it's a string
    const messages = typeof data === 'string' ? JSON.parse(data) : data;
    logDebug("ReadQueue", `Successfully read ${messages?.length || 0} messages from queue ${queue_name}`);
    
    return new Response(
      JSON.stringify(messages || []),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    logError("ReadQueue", `Unexpected error: ${error.message}`);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      }
    );
  }
});
