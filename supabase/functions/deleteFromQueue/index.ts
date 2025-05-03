
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
    const { queue_name, message_id } = await req.json();
    
    if (!queue_name || !message_id) {
      throw new Error("queue_name and message_id are required");
    }
    
    console.log(`Attempting to delete message ${message_id} from queue ${queue_name}`);
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    // First try using the improved pg_delete_message function
    const { data, error } = await supabase.rpc(
      'pg_delete_message',
      { 
        queue_name,
        message_id: message_id.toString() // Ensure message_id is a string
      }
    );
    
    if (error) {
      console.error("Error using pg_delete_message:", error);
      throw error;
    }
    
    if (data === true) {
      console.log(`Successfully deleted message ${message_id} from queue ${queue_name}`);
      return new Response(
        JSON.stringify({ success: true, message: `Deleted message ${message_id} from queue ${queue_name}` }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    // If direct deletion failed, try to reset visibility timeout as a fallback
    console.log(`Direct deletion failed, trying to reset visibility timeout for message ${message_id}`);
    const { data: resetData, error: resetError } = await supabase.rpc(
      'reset_stuck_message',
      { 
        queue_name,
        message_id: message_id.toString()
      }
    );
    
    if (resetError) {
      console.error("Error resetting message visibility:", resetError);
      throw resetError;
    }
    
    if (resetData === true) {
      console.log(`Reset visibility timeout for message ${message_id}`);
      return new Response(
        JSON.stringify({ 
          success: false, 
          reset: true,
          message: `Reset visibility timeout for message ${message_id} in queue ${queue_name}` 
        }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    // If all attempts failed
    throw new Error(`Failed to delete or reset message ${message_id} in queue ${queue_name}`);
    
  } catch (error) {
    console.error("Error deleting message from queue:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
