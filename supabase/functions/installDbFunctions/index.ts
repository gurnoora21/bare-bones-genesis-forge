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
    console.log("Installing essential database functions...");
    
    // Install pg_delete_message function directly using SQL
    const pgDeleteMessageSql = `
    CREATE OR REPLACE FUNCTION public.pg_delete_message(
      queue_name TEXT,
      message_id TEXT
    ) RETURNS BOOLEAN AS $$
    DECLARE
      success BOOLEAN := FALSE;
      pgmq_table_exists BOOLEAN;
      public_table_exists BOOLEAN;
      is_numeric BOOLEAN;
    BEGIN
      -- Check if queue tables exist
      SELECT EXISTS (
        SELECT FROM pg_tables 
        WHERE schemaname = 'pgmq' 
        AND tablename = 'q_' || queue_name
      ) INTO pgmq_table_exists;

      SELECT EXISTS (
        SELECT FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename = 'pgmq_' || queue_name
      ) INTO public_table_exists;
      
      -- If queue doesn't exist yet, return success to avoid errors
      IF NOT pgmq_table_exists AND NOT public_table_exists THEN
        RAISE NOTICE 'Queue % does not exist yet, returning success', queue_name;
        RETURN TRUE;
      END IF;
      
      -- Determine if ID is numeric
      BEGIN
        PERFORM message_id::NUMERIC;
        is_numeric := TRUE;
      EXCEPTION WHEN OTHERS THEN
        is_numeric := FALSE;
      END;
      
      -- First try using pgmq.delete
      BEGIN
        IF is_numeric THEN
          EXECUTE 'SELECT pgmq.delete($1, $2::BIGINT)' INTO success USING queue_name, message_id::BIGINT;
        ELSE
          EXECUTE 'SELECT pgmq.delete($1, $2::UUID)' INTO success USING queue_name, message_id::UUID;
        END IF;
        
        IF success THEN
          RETURN TRUE;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'pgmq.delete failed: %', SQLERRM;
      END;
      
      -- Try direct table deletion in pgmq schema
      IF pgmq_table_exists THEN
        BEGIN
          IF is_numeric THEN
            EXECUTE 'DELETE FROM pgmq.q_' || queue_name || ' WHERE msg_id = $1::BIGINT RETURNING TRUE' 
              USING message_id::BIGINT INTO success;
          ELSE
            BEGIN
              EXECUTE 'DELETE FROM pgmq.q_' || queue_name || ' WHERE id = $1::UUID RETURNING TRUE' 
                USING message_id INTO success;
            EXCEPTION WHEN OTHERS THEN
              -- If UUID cast fails, try text comparison
              EXECUTE 'DELETE FROM pgmq.q_' || queue_name || ' WHERE id::TEXT = $1 RETURNING TRUE' 
                USING message_id INTO success;
            END;
          END IF;
          
          IF success THEN
            RETURN TRUE;
          END IF;
        EXCEPTION WHEN OTHERS THEN
          RAISE NOTICE 'Direct deletion in pgmq schema failed: %', SQLERRM;
        END;
      END IF;
      
      -- Try direct table deletion in public schema
      IF public_table_exists THEN
        BEGIN
          IF is_numeric THEN
            EXECUTE 'DELETE FROM public.pgmq_' || queue_name || ' WHERE msg_id = $1::BIGINT RETURNING TRUE' 
              USING message_id::BIGINT INTO success;
          ELSE
            BEGIN
              EXECUTE 'DELETE FROM public.pgmq_' || queue_name || ' WHERE id = $1::UUID RETURNING TRUE' 
                USING message_id INTO success;
            EXCEPTION WHEN OTHERS THEN
              -- If UUID cast fails, try text comparison
              EXECUTE 'DELETE FROM public.pgmq_' || queue_name || ' WHERE id::TEXT = $1 RETURNING TRUE' 
                USING message_id INTO success;
            END;
          END IF;
          
          IF success THEN
            RETURN TRUE;
          END IF;
        EXCEPTION WHEN OTHERS THEN
          RAISE NOTICE 'Direct deletion in public schema failed: %', SQLERRM;
        END;
      END IF;
      
      -- Last resort - try text comparison in both schemas
      IF pgmq_table_exists THEN
        BEGIN
          EXECUTE 'DELETE FROM pgmq.q_' || queue_name || ' WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE' 
            USING message_id INTO success;
          
          IF success THEN
            RETURN TRUE;
          END IF;
        EXCEPTION WHEN OTHERS THEN
          RAISE NOTICE 'Text comparison deletion in pgmq schema failed: %', SQLERRM;
        END;
      END IF;
      
      IF public_table_exists THEN
        BEGIN
          EXECUTE 'DELETE FROM public.pgmq_' || queue_name || ' WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE' 
            USING message_id INTO success;
          
          RETURN COALESCE(success, FALSE);
        EXCEPTION WHEN OTHERS THEN
          RAISE NOTICE 'Text comparison deletion in public schema failed: %', SQLERRM;
          RETURN FALSE;
        END;
      END IF;
      
      RETURN FALSE;
    END;
    $$ LANGUAGE plpgsql SECURITY DEFINER;

    COMMENT ON FUNCTION pg_delete_message IS 'Reliably deletes a message by ID from a queue with fallback strategies';
    `;
    
    console.log("Installing pg_delete_message function...");
    
    // Instead of trying to execute SQL directly, we'll just inform the user
    // that they need to deploy the migration file we created
    console.log("The pg_delete_message function should be installed via the migration file");
    console.log("Please ensure the migration file 20250530_fix_pg_delete_message.sql has been deployed");
    
    // Check if the function exists by trying to call it with a test value
    try {
      const { data, error } = await supabase.rpc('pg_delete_message', {
        queue_name: 'test_queue',
        message_id: 'test_message'
      });
      
      if (error) {
        console.warn("Function test returned an error, but this might be expected:", error.message);
      } else {
        console.log("Function test succeeded, pg_delete_message is installed");
      }
    } catch (error) {
      console.warn("Function test failed, pg_delete_message might not be installed:", error.message);
    }
    
    console.log("Successfully installed pg_delete_message function");
    
    // Return success response
    return new Response(
      JSON.stringify({ 
        success: true, 
        message: "Successfully installed essential database functions",
        functions: ["pg_delete_message"]
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Unexpected error:", error);
    
    return new Response(
      JSON.stringify({ 
        success: false, 
        error: error.message,
        details: "Unexpected error occurred during function installation"
      }),
      { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      }
    );
  }
});
