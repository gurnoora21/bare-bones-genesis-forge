
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
    
    console.log(`Deleting message ${message_id} from queue ${queue_name}`);
    
    // FIXED: Use a direct SQL query with proper queue table name and parameters
    // This is more reliable than the pgmq.delete function which had issues
    const tableName = `pgmq_${queue_name}`;
    const { data, error } = await supabase.rpc('pg_delete_message', {
      queue_name,
      message_id
    });
    
    if (error) {
      console.error(`Error deleting message ${message_id}:`, error);
      
      // Fallback method: Try direct SQL delete as a backup
      const { error: fallbackError } = await supabase.from(tableName)
        .delete()
        .eq('id', message_id);
      
      if (fallbackError) {
        console.error(`Fallback also failed for message ${message_id}:`, fallbackError);
        throw fallbackError;
      } else {
        console.log(`Message ${message_id} deleted using fallback method`);
        return new Response(
          JSON.stringify({ success: true, method: 'fallback' }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    }
    
    console.log(`Successfully deleted message ${message_id} from queue ${queue_name}`);
    
    return new Response(
      JSON.stringify({ success: true }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Error deleting message from queue:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
