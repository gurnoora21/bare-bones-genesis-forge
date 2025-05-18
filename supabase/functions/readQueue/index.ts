
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

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
    
    console.log(`Reading up to ${batch_size} messages from queue ${queue_name} with visibility timeout ${visibility_timeout}s`);
    
    // Try the updated pgmq_read_safe function first
    const { data: safeData, error: safeError } = await supabase.rpc('pgmq_read_safe', {
      queue_name: queue_name,
      max_messages: batch_size,
      visibility_timeout: visibility_timeout
    });
    
    if (!safeError && safeData) {
      console.log(`Successfully read messages using pgmq_read_safe`);
      const messages = typeof safeData === 'string' ? JSON.parse(safeData) : safeData;
      
      return new Response(
        JSON.stringify(messages || []),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    if (safeError) {
      console.warn(`pgmq_read_safe error: ${safeError.message}. Falling back to pg_dequeue.`);
    }
    
    // Fall back to pg_dequeue if pgmq_read_safe fails
    const { data, error } = await supabase.rpc('pg_dequeue', {
      queue_name: queue_name,
      batch_size: batch_size,
      visibility_timeout: visibility_timeout
    });
    
    if (error) {
      console.error(`Error reading from queue:`, error);
      return new Response(
        JSON.stringify({ error: error.message }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    // Parse the JSON result if it's a string
    const messages = typeof data === 'string' ? JSON.parse(data) : data;
    
    return new Response(
      JSON.stringify(messages || []),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Error in readQueue handler:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      }
    );
  }
});
