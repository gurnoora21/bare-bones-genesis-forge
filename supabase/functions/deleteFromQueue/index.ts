
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
    const requestBody = await req.json();
    const { queue_name, message_id } = requestBody;
    
    if (!queue_name || !message_id) {
      console.error("Missing required parameters:", { queue_name, message_id });
      throw new Error("queue_name and message_id are required");
    }
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    console.log(`Attempting to delete message ${message_id} from queue ${queue_name}`);
    
    // Try standard deletion first
    const { data: deleteResult, error: deleteError } = await supabase.rpc(
      'pg_delete_message',
      { 
        queue_name, 
        message_id: message_id.toString()  // Ensure it's a string to handle various formats
      }
    );
    
    if (deleteError) {
      console.error("Error using pg_delete_message:", deleteError);
      
      // If standard deletion fails, try resetting the visibility timeout as a fallback
      const { data: resetResult, error: resetError } = await supabase.rpc(
        'reset_stuck_message',
        { 
          queue_name, 
          message_id: message_id.toString()
        }
      );
      
      if (resetError) {
        console.error("Error resetting visibility timeout:", resetError);
        throw new Error(`Failed to delete or reset message ${message_id}: ${deleteError.message}, reset error: ${resetError.message}`);
      }
      
      if (resetResult) {
        return new Response(
          JSON.stringify({ 
            success: false, 
            reset: true,
            message: `Reset visibility timeout for message ${message_id} in queue ${queue_name}` 
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      } else {
        throw new Error(`Failed to delete or reset message ${message_id} in queue ${queue_name}`);
      }
    }
    
    if (deleteResult) {
      return new Response(
        JSON.stringify({ 
          success: true, 
          message: `Successfully deleted message ${message_id} from queue ${queue_name}` 
        }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } else {
      return new Response(
        JSON.stringify({ 
          success: false, 
          message: `Message ${message_id} not found in queue ${queue_name} or could not be deleted` 
        }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
  } catch (error) {
    console.error("Error deleting message from queue:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
