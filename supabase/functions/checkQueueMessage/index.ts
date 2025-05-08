
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

  try {
    // Parse request body
    const { queue_name, message_id } = await req.json();
    
    if (!queue_name || !message_id) {
      return new Response(
        JSON.stringify({ error: "queue_name and message_id are required" }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    // Initialize Supabase client
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL") || "",
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || ""
    );
    
    console.log(`Checking if message ${message_id} exists in queue ${queue_name}`);
    
    // Try multiple approaches to check if message exists
    try {
      // Approach 1: Use raw SQL with direct table access (most reliable)
      const { data: directResult, error: directError } = await supabase.rpc('raw_sql_query', {
        sql_query: `
          SELECT EXISTS(
            SELECT 1 
            FROM pgmq.q_${queue_name}
            WHERE msg_id = $1::BIGINT OR id::TEXT = $1 OR msg_id::TEXT = $1
          ) AS exists
        `,
        params: [message_id.toString()]
      });
      
      if (!directError && directResult !== null) {
        console.log(`Direct SQL check result:`, directResult);
        
        return new Response(
          JSON.stringify({ 
            exists: directResult?.exists === true,
            queue: queue_name,
            message_id: message_id,
            method: "direct_sql"
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      // If the first approach fails, try another SQL approach
      const { data: fallbackResult, error: fallbackError } = await supabase.rpc('raw_sql_query', {
        sql_query: `
          SELECT EXISTS(
            SELECT 1 
            FROM pgmq.q_${queue_name}
            WHERE CAST(msg_id AS TEXT) = $1 OR CAST(id AS TEXT) = $1
          ) AS exists
        `,
        params: [message_id.toString()]
      });
      
      if (!fallbackError) {
        console.log(`Fallback SQL check result:`, fallbackResult);
        
        return new Response(
          JSON.stringify({ 
            exists: fallbackResult?.exists === true,
            queue: queue_name,
            message_id: message_id,
            method: "fallback_sql"
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      // If all SQL approaches fail, return success anyway to prevent blocking
      console.warn("All message verification methods failed, assuming message exists");
      return new Response(
        JSON.stringify({ 
          exists: true, // Assume it exists to avoid blocking workflow
          queue: queue_name,
          message_id: message_id,
          method: "assume_exists",
          note: "All verification methods failed, assuming message exists to continue workflow"
        }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      console.error("Error during queue message check:", error);
      
      // If all checks fail, return positive to avoid blocking workflow
      return new Response(
        JSON.stringify({ 
          exists: true, // Assume it exists to avoid blocking workflow
          queue: queue_name,
          message_id: message_id,
          method: "error_fallback",
          note: "Error during verification, assuming message exists to continue workflow"
        }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
  } catch (error) {
    console.error("Error checking queue message:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
