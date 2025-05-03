
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
    
    // Method 1: Try cross_schema_queue_op which now looks in pgmq.q_* tables first
    try {
      const { data: crossSchemaResult, error: crossSchemaError } = await supabase.rpc(
        'cross_schema_queue_op',
        { 
          p_operation: 'delete',
          p_queue_name: queue_name,
          p_message_id: message_id
        }
      );
      
      console.log("Cross-schema deletion result:", crossSchemaResult);
      
      if (crossSchemaError) {
        console.error("Cross-schema operation error:", crossSchemaError);
      }
      
      if (crossSchemaResult && crossSchemaResult.success === true) {
        console.log(`SUCCESS: Message ${message_id} has been deleted from ${crossSchemaResult.table}`);
        
        return new Response(
          JSON.stringify({ 
            success: true, 
            message: `Successfully deleted message ${message_id} from queue ${queue_name}`,
            details: crossSchemaResult
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    } catch (crossSchemaError) {
      console.error("Error using cross-schema deletion:", crossSchemaError);
    }
    
    // Method 2: Try calling pgmq.delete directly via RPC
    try {
      const { data: pgmqResult, error: pgmqError } = await supabase.rpc(
        'direct_pgmq_delete',
        { 
          p_queue_name: queue_name,
          p_message_id: message_id
        }
      );
      
      if (!pgmqError && pgmqResult === true) {
        return new Response(
          JSON.stringify({ 
            success: true,
            message: `Successfully deleted message ${message_id} from queue ${queue_name} using pgmq.delete`
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      console.error("PGMQ delete failed:", pgmqError || "No success");
    } catch (pgmqError) {
      console.error(`PGMQ delete procedure failed:`, pgmqError);
    }
    
    // Method 3: Try using the forceDeleteMessage edge function
    try {
      const forceDeleteResponse = await fetch(
        `${Deno.env.get("SUPABASE_URL")}/functions/v1/forceDeleteMessage`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${Deno.env.get("SUPABASE_ANON_KEY")}`
          },
          body: JSON.stringify({
            queue_name: queue_name,
            message_id: message_id,
            bypass_checks: true // Enable aggressive deletion
          })
        }
      );
      
      if (forceDeleteResponse.ok) {
        const forceResult = await forceDeleteResponse.json();
        
        if (forceResult.success || forceResult.reset) {
          return new Response(
            JSON.stringify(forceResult),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
      }
    } catch (forceError) {
      console.error("Force delete fallback failed:", forceError);
    }
    
    // If we get here, all methods have failed
    // Get diagnostic info to help troubleshoot
    const { data: diagData } = await supabase.rpc(
      'diagnose_queue_tables',
      { queue_name }
    );
    
    throw new Error(`Failed to delete message ${message_id} from queue ${queue_name} after multiple attempts. Queue tables found: ${JSON.stringify(diagData)}`);
  } catch (error) {
    console.error("Error deleting message from queue:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
