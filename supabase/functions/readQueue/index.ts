
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { getRateLimiter } from "../_shared/rateLimiter.ts";

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
    const { queue_name, batch_size = 5, visibility_timeout = 60 } = await req.json();
    
    if (!queue_name) {
      throw new Error("queue_name is required");
    }
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    console.log(`Reading messages from queue: ${queue_name}, batch size: ${batch_size}`);
    
    // Use rate limiter for internal database operations to avoid overwhelming the database
    const rateLimiter = getRateLimiter();
    
    const { data, error } = await rateLimiter.execute({
      api: "supabase",
      endpoint: "rpc_dequeue",
      tokensPerInterval: 10,
      interval: 5,
      maxRetries: 3
    }, async () => {
      return supabase.rpc('pg_dequeue', {
        queue_name,
        batch_size,
        visibility_timeout
      });
    });
    
    if (error) {
      console.error(`Error reading from queue ${queue_name}:`, error);
      throw error;
    }
    
    console.log(`Successfully read ${data ? JSON.stringify(data).length : 0} bytes from queue ${queue_name}`);
    
    return new Response(
      JSON.stringify(data),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Error reading from queue:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
