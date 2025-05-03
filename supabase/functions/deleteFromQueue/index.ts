
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
    
    // Determine message ID type and format
    const isNumeric = !isNaN(Number(message_id)) && message_id.toString().trim() !== '';
    
    // Method 1: Try our new cross-schema approach first
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
    
    // Method 2: Try enhanced delete function with improved schema handling
    try {
      const { data: enhancedData, error: enhancedError } = await supabase.rpc(
        'enhanced_delete_message',
        { 
          p_queue_name: queue_name,
          p_message_id: message_id.toString(),
          p_is_numeric: isNumeric
        }
      );
      
      if (!enhancedError && enhancedData === true) {
        return new Response(
          JSON.stringify({ 
            success: true,
            message: `Successfully deleted message ${message_id} from queue ${queue_name} using enhanced method`
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      console.error("Enhanced delete failed:", enhancedError || "No success");
    } catch (enhancedError) {
      console.error(`Enhanced delete procedure failed:`, enhancedError);
    }
    
    // Method 3: Last resort - try to reset visibility timeout
    try {
      const { data: resetResult, error: resetError } = await supabase.rpc(
        'cross_schema_queue_op',
        { 
          p_operation: 'reset',
          p_queue_name: queue_name,
          p_message_id: message_id
        }
      );
      
      if (!resetError && resetResult && resetResult.success === true) {
        return new Response(
          JSON.stringify({ 
            success: false,
            reset: true,
            message: `Could not delete message ${message_id}, but successfully reset visibility timeout`,
            details: resetResult
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      console.error("Reset failed:", resetError || "No success", resetResult);
    } catch (resetError) {
      console.error(`Reset procedure failed:`, resetError);
    }
    
    // If everything else failed, try invoking the forceDeleteMessage edge function
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
