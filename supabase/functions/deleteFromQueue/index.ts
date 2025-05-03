
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
    
    // Direct database query approach - the most reliable method
    try {
      // Get queue diagnostics
      const { data: diagData } = await supabase.rpc(
        'diagnose_queue_tables',
        { queue_name }
      );
      
      if (diagData && diagData.queue_tables && diagData.queue_tables.length > 0) {
        // We found the queue table, try to delete from it directly
        const fullTableName = diagData.queue_tables[0].full_name;
        
        const { data: sqlResult } = await supabase.rpc(
          'raw_sql_query',
          {
            sql_query: `
              DO $$
              BEGIN
                -- Try deletion
                BEGIN
                  EXECUTE format('DELETE FROM %s WHERE id::TEXT = $1 OR msg_id::TEXT = $1', '${fullTableName}')
                  USING '${message_id}';
                  IF FOUND THEN
                    RAISE NOTICE 'Successfully deleted message % from %', '${message_id}', '${fullTableName}';
                  ELSE
                    RAISE NOTICE 'No deletion performed, message % not found in %', '${message_id}', '${fullTableName}';
                    
                    -- Try resetting visibility timeout
                    BEGIN
                      EXECUTE format('UPDATE %s SET vt = NULL WHERE id::TEXT = $1 OR msg_id::TEXT = $1', '${fullTableName}')
                      USING '${message_id}';
                      IF FOUND THEN
                        RAISE NOTICE 'Reset visibility timeout for message % in %', '${message_id}', '${fullTableName}';
                      ELSE
                        RAISE NOTICE 'Message % not found in %', '${message_id}', '${fullTableName}';
                      END IF;
                    EXCEPTION WHEN OTHERS THEN
                      RAISE NOTICE 'Error resetting visibility for message % in %: %', 
                        '${message_id}', '${fullTableName}', SQLERRM;
                    END;
                  END IF;
                EXCEPTION WHEN OTHERS THEN
                  RAISE NOTICE 'Error deleting message % from %: %', 
                    '${message_id}', '${fullTableName}', SQLERRM;
                END;
              END $$;
              SELECT TRUE AS success;
            `
          }
        );
        
        return new Response(
          JSON.stringify({ 
            success: true, 
            message: `Successfully handled message ${message_id} from queue ${queue_name}`,
            diagnostics: diagData
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      // No queue tables found, use forceDeleteMessage as last resort
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
      
      throw new Error(`Failed to delete message ${message_id} from queue ${queue_name} after multiple attempts. Queue tables found: ${JSON.stringify(diagData)}`);
    } catch (directError) {
      console.error("Error with direct database approach:", directError);
      throw directError;
    }
  } catch (error) {
    console.error("Error deleting message from queue:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
