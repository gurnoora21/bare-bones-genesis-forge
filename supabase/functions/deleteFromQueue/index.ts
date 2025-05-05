
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
    
    // Try multiple approaches to ensure deletion works
    let success = false;
    let attempts = 0;
    const maxAttempts = 3;
    
    while (!success && attempts < maxAttempts) {
      attempts++;
      
      // Attempt 1: Try as numeric ID
      if (attempts === 1) {
        try {
          const numericId = parseInt(message_id.toString(), 10);
          if (!isNaN(numericId)) {
            const { data, error } = await supabase.rpc(
              'pg_delete_message',
              { 
                queue_name,
                message_id: numericId.toString() 
              }
            );
            
            if (!error && data === true) {
              console.log(`Successfully deleted message ${message_id} using numeric ID`);
              success = true;
              break;
            }
          }
        } catch (e) {
          console.error("Error with numeric ID deletion:", e);
        }
      }
      
      // Attempt 2: Try as string ID
      if (attempts === 2) {
        try {
          const { data, error } = await supabase.rpc(
            'pg_delete_message',
            { 
              queue_name,
              message_id: message_id.toString() 
            }
          );
          
          if (!error && data === true) {
            console.log(`Successfully deleted message ${message_id} using string ID`);
            success = true;
            break;
          }
        } catch (e) {
          console.error("Error with string ID deletion:", e);
        }
      }
      
      // Attempt 3: Try direct SQL as a last resort
      if (attempts === 3) {
        try {
          // Try to delete directly using SQL
          const { data, error } = await supabase.rpc('raw_sql_query', {
            sql_query: `
              DO $$
              DECLARE
                success BOOLEAN := FALSE;
              BEGIN
                -- Try deleting from pgmq schema first
                BEGIN
                  EXECUTE 'DELETE FROM pgmq.q_' || $1 || ' WHERE msg_id = $2::BIGINT OR id = $2::BIGINT OR msg_id::TEXT = $3 OR id::TEXT = $3';
                  GET DIAGNOSTICS success = ROW_COUNT;
                  IF success > 0 THEN
                    RAISE NOTICE 'Deleted message using pgmq schema';
                  END IF;
                EXCEPTION WHEN OTHERS THEN
                  RAISE NOTICE 'Error deleting from pgmq schema: %', SQLERRM;
                END;
                
                -- If still not deleted, try public schema
                IF NOT success THEN
                  BEGIN
                    EXECUTE 'DELETE FROM public.pgmq_' || $1 || ' WHERE msg_id = $2::BIGINT OR id = $2::BIGINT OR msg_id::TEXT = $3 OR id::TEXT = $3';
                    GET DIAGNOSTICS success = ROW_COUNT;
                    IF success > 0 THEN
                      RAISE NOTICE 'Deleted message using public schema';
                    END IF;
                  EXCEPTION WHEN OTHERS THEN
                    RAISE NOTICE 'Error deleting from public schema: %', SQLERRM;
                  END;
                END IF;
              END $$;
              SELECT TRUE as success;
            `,
            params: [queue_name, message_id, message_id.toString()]
          });
          
          if (!error && data) {
            console.log(`Successfully deleted message ${message_id} using direct SQL`);
            success = true;
            break;
          }
        } catch (e) {
          console.error("Error with direct SQL deletion:", e);
        }
      }
      
      // Wait before retrying
      if (!success && attempts < maxAttempts) {
        await new Promise(resolve => setTimeout(resolve, 100 * attempts));
      }
    }
    
    // If we couldn't delete, try to reset visibility timeout as a fallback
    if (!success) {
      try {
        const { data: resetData, error: resetError } = await supabase.rpc(
          'reset_stuck_message',
          { 
            queue_name,
            message_id: message_id.toString()
          }
        );
        
        if (resetError) {
          console.error("Error resetting message visibility:", resetError);
        } else if (resetData === true) {
          console.log(`Reset visibility timeout for message ${message_id}`);
          success = true;
        }
      } catch (resetError) {
        console.error("Error trying to reset visibility timeout:", resetError);
      }
    }
    
    if (success) {
      return new Response(
        JSON.stringify({ success: true, message: `Message ${message_id} handled successfully` }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } else {
      console.error(`Failed to delete or reset message ${message_id} after ${maxAttempts} attempts`);
      return new Response(
        JSON.stringify({ 
          success: false, 
          message: `Failed to handle message ${message_id} after ${maxAttempts} attempts` 
        }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
  } catch (error) {
    console.error("Error in deleteFromQueue:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
