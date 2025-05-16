
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
    const { queue_name, msg_id } = await req.json();
    
    if (!queue_name || !msg_id) {
      return new Response(
        JSON.stringify({ error: "Missing required parameters" }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    console.log(`Deleting message ${msg_id} from queue ${queue_name}`);
    
    // Call the database function to delete the message
    const { data, error } = await supabase.rpc('pg_delete_message', {
      queue_name: queue_name,
      message_id: msg_id.toString()
    });
    
    if (error) {
      console.error(`Error deleting message:`, error);
      
      // Try enhanced delete as a fallback
      try {
        const { data: enhancedDelete, error: enhancedError } = await supabase.rpc('enhanced_delete_message', {
          p_queue_name: queue_name,
          p_message_id: msg_id.toString(),
          p_is_numeric: typeof msg_id === 'number'
        });
        
        if (enhancedError) {
          return new Response(
            JSON.stringify({ error: `Enhanced delete also failed: ${enhancedError.message}` }),
            { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
        
        return new Response(
          JSON.stringify({ success: enhancedDelete, method: 'enhanced_delete' }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      } catch (enhancedFallbackError) {
        return new Response(
          JSON.stringify({ error: `All delete methods failed: ${enhancedFallbackError.message}` }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    }
    
    return new Response(
      JSON.stringify({ success: data }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Error in deleteMessage handler:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      }
    );
  }
});
