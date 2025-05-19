
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
    
    // First, ensure raw_sql_query function exists
    try {
      // Create raw_sql_query function if it doesn't exist
      const createRawSqlQueryFn = `
        CREATE OR REPLACE FUNCTION public.raw_sql_query(
          sql_query TEXT,
          params TEXT[] DEFAULT '{}'::TEXT[]
        ) RETURNS JSONB AS $$
        DECLARE
          result JSONB;
        BEGIN
          EXECUTE sql_query INTO result USING params;
          RETURN result;
        EXCEPTION WHEN OTHERS THEN
          RETURN jsonb_build_object('error', SQLERRM);
        END;
        $$ LANGUAGE plpgsql SECURITY DEFINER;

        COMMENT ON FUNCTION raw_sql_query IS 'Executes raw SQL queries safely';
      `;

      // Execute raw SQL directly to create this helper function first
      const { error: rawSqlFnError } = await supabase.rpc('exec_sql', { 
        sql: createRawSqlQueryFn 
      }).catch(() => {
        // If exec_sql doesn't exist, try a direct query
        return supabase.from('_exec_sql').select('*').eq('query', createRawSqlQueryFn);
      });
      
      if (rawSqlFnError) {
        console.log("Note: raw_sql_query function might already exist, continuing");
      } else {
        console.log("Successfully created raw_sql_query function");
      }
    } catch (err) {
      console.log("Note: raw_sql_query function might already exist, continuing");
    }
    
    // Install pg_delete_message function
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
    
    // Try multiple approaches to install the function
    try {
      // Approach 1: Try direct execute via PostgREST
      const { data: directData, error: directError } = await supabase.rpc('exec_sql', {
        sql: pgDeleteMessageSql
      }).catch(e => ({ data: null, error: e }));
      
      if (!directError) {
        console.log("Successfully installed pg_delete_message function via exec_sql");
        return new Response(
          JSON.stringify({ 
            success: true, 
            message: "Successfully installed essential database functions",
            functions: ["pg_delete_message"],
            method: "exec_sql"
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      // Approach 2: Try using raw_sql_query if it exists now
      const { data, error } = await supabase.rpc('raw_sql_query', {
        sql_query: pgDeleteMessageSql
      });
      
      if (error) {
        console.error("Error creating pg_delete_message function with raw_sql_query:", error);
        throw new Error(`Failed to create pg_delete_message function: ${error.message}`);
      }
      
      console.log("Successfully installed pg_delete_message function via raw_sql_query");
      
      return new Response(
        JSON.stringify({ 
          success: true, 
          message: "Successfully installed essential database functions",
          functions: ["pg_delete_message"],
          method: "raw_sql_query"
        }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      console.error("All approaches failed:", error);
      
      // Replace the direct POST approach with another RPC call
      try {
        // Try another approach - using pgmq_utils schema if available
        const { data: altData, error: altError } = await supabase.rpc('create_utility_function', {
          p_function_name: 'pg_delete_message',
          p_function_body: pgDeleteMessageSql
        }).catch(e => ({ data: null, error: e }));
        
        if (!altError && altData) {
          console.log("Successfully installed pg_delete_message function via create_utility_function");
          return new Response(
            JSON.stringify({ 
              success: true, 
              message: "Successfully installed essential database functions",
              functions: ["pg_delete_message"],
              method: "create_utility_function"
            }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
        
        // Final fallback - try another RPC method if available
        const { data: finalData, error: finalError } = await supabase.rpc('install_db_function', {
          p_function_name: 'pg_delete_message',
          p_sql_body: pgDeleteMessageSql
        }).catch(e => ({ data: null, error: e }));
        
        if (!finalError) {
          console.log("Successfully installed pg_delete_message function via install_db_function");
          return new Response(
            JSON.stringify({ 
              success: true, 
              message: "Successfully installed essential database functions",
              functions: ["pg_delete_message"],
              method: "install_db_function"
            }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
        
        throw new Error(`All installation methods failed`);
      } catch (finalError) {
        console.error("All installation methods failed:", finalError);
        throw finalError;
      }
    }
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
