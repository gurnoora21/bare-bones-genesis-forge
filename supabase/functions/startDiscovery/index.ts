
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
    // Expect { artistName: "Artist Name" } in the request body
    const { artistName } = await req.json();
    
    if (!artistName) {
      return new Response(
        JSON.stringify({ error: "artistName is required" }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    console.log(`Attempting to enqueue artist discovery for: ${artistName}`);
    
    // Use direct SQL query approach for more reliable queueing
    const { data: enqueueResult, error: enqueueError } = await supabase.rpc('raw_sql_query', {
      sql_query: `
        DO $$
        DECLARE
          msg_id BIGINT;
          queue_table TEXT;
        BEGIN
          -- First try to determine the correct queue table
          SELECT get_queue_table_name_safe('artist_discovery') INTO queue_table;
          
          -- Insert directly into queue table with proper handling
          EXECUTE format('
            INSERT INTO %s (msg_id, message) 
            VALUES (nextval(''pgmq.queue_seq''), $1::jsonb) 
            RETURNING msg_id', queue_table)
          USING jsonb_build_object('artistName', $1)
          INTO msg_id;
          
          RAISE NOTICE 'Successfully queued message with ID %', msg_id;
        END $$;
        SELECT TRUE as success;
      `,
      params: [artistName]
    });
    
    if (enqueueError) {
      console.error("Direct queue error:", enqueueError);
      
      // Try fallback to pg_enqueue function
      const { data: messageId, error } = await supabase.rpc('pg_enqueue', {
        queue_name: 'artist_discovery',
        message_body: JSON.stringify({ artistName }) // Convert to JSON string which Postgres will parse as JSONB
      });
      
      if (error) {
        console.error("Queue fallback error:", error);
        throw error;
      }
      
      console.log(`Successfully enqueued artist discovery with fallback method, ID: ${messageId}`);
      
      return new Response(
        JSON.stringify({ 
          success: true, 
          message: `Discovery process started for artist: ${artistName}`,
          messageId
        }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    // Manually trigger the artist discovery worker to process immediately
    try {
      await fetch(
        `${Deno.env.get("SUPABASE_URL")}/functions/v1/artistDiscovery`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${Deno.env.get("SUPABASE_ANON_KEY")}`
          }
        }
      );
      console.log("Artist discovery worker triggered successfully");
    } catch (triggerError) {
      console.warn("Could not trigger worker directly, queue will be processed on schedule:", triggerError);
    }
    
    return new Response(
      JSON.stringify({ 
        success: true, 
        message: `Discovery process started for artist: ${artistName}`,
        directSQL: true
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Error starting discovery:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
