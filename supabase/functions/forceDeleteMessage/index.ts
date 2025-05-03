
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
    const { queue_name, message_id, bypass_checks = false } = await req.json();
    
    if (!queue_name || !message_id) {
      throw new Error("queue_name and message_id are required");
    }

    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    console.log(`FORCE DELETE: Attempting to delete message ${message_id} from queue ${queue_name}`);
    
    // Get queue table diagnostic info
    const { data: diagData } = await supabase.rpc(
      'diagnose_queue_tables',
      { queue_name: queue_name }
    );
    
    // Try to create direct_pgmq_delete function if it doesn't exist (might be a new deploy)
    try {
      await supabase.rpc(
        'raw_sql_query',
        {
          sql_query: `
            CREATE OR REPLACE FUNCTION public.direct_pgmq_delete(
              p_queue_name TEXT,
              p_message_id TEXT
            ) RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
            DECLARE
              queue_table TEXT;
              success BOOLEAN := FALSE;
            BEGIN
              -- Get the correct table
              SELECT get_queue_table_name_safe(p_queue_name) INTO queue_table;
              
              -- Execute direct delete on this table
              BEGIN
                EXECUTE format('DELETE FROM %s WHERE id::TEXT = $1 OR msg_id::TEXT = $1', queue_table)
                USING p_message_id;
                GET DIAGNOSTICS success = ROW_COUNT;
                RETURN success > 0;
              EXCEPTION WHEN OTHERS THEN
                RAISE WARNING 'Direct delete failed on table %: %', queue_table, SQLERRM;
                RETURN FALSE;
              END;
            END;
            $$;
          `
        }
      );
      console.log("Created direct_pgmq_delete function:", diagData);
    } catch (funcError) {
      console.error("Error creating function:", funcError);
    }
    
    // 1. Try raw SQL deletion first
    const rawSqlResult = await supabase.rpc(
      'raw_sql_query',
      {
        sql_query: `
          DO $$
          DECLARE
            tables TEXT[] := ARRAY[
              'pgmq.q_${queue_name}',
              'public.pgmq_${queue_name}'
            ];
            t TEXT;
            success BOOLEAN := FALSE;
          BEGIN
            FOREACH t IN ARRAY tables
            LOOP
              BEGIN
                EXECUTE format('DELETE FROM %s WHERE id::TEXT = $1 OR msg_id::TEXT = $1', t)
                USING '${message_id}';
                GET DIAGNOSTICS success = ROW_COUNT;
                
                IF success THEN
                  RAISE NOTICE 'Successfully deleted message % from table %', '${message_id}', t;
                  EXIT;
                END IF;
              EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Error deleting from %: %', t, SQLERRM;
              END;
            END LOOP;
          END $$;
          SELECT TRUE AS success;
        `
      }
    );
    console.log("Raw SQL deletion result:", rawSqlResult);
    
    // 2. Try cross-schema operations
    let crossSchemaResult;
    try {
      const { data, error } = await supabase.rpc(
        'diagnose_queue_tables',
        { queue_name }
      );
      console.log("Cross-schema deletion result:", data);
      crossSchemaResult = data;
      
      if (error) {
        console.error("Cross-schema operation error:", error);
      }
    } catch (crossSchemaError) {
      console.error("Cross-schema operation error:", crossSchemaError);
    }
    
    // 3. Try direct reset if deletion didn't work
    let crossSchemaReset;
    try {
      const { data, error } = await supabase.rpc(
        'raw_sql_query',
        {
          sql_query: `
            DO $$
            DECLARE
              tables TEXT[] := ARRAY[
                'pgmq.q_${queue_name}',
                'public.pgmq_${queue_name}'
              ];
              t TEXT;
              success BOOLEAN := FALSE;
            BEGIN
              FOREACH t IN ARRAY tables
              LOOP
                BEGIN
                  EXECUTE format('UPDATE %s SET vt = NULL WHERE id::TEXT = $1 OR msg_id::TEXT = $1', t)
                  USING '${message_id}';
                  GET DIAGNOSTICS success = ROW_COUNT;
                  
                  IF success THEN
                    RAISE NOTICE 'Successfully reset message % visibility in table %', '${message_id}', t;
                    EXIT;
                  END IF;
                EXCEPTION WHEN OTHERS THEN
                  RAISE NOTICE 'Error resetting visibility in %: %', t, SQLERRM;
                END;
              END LOOP;
            END $$;
            SELECT TRUE AS success;
          `
        }
      );
      console.log("Cross-schema reset result:", data);
      crossSchemaReset = data;
      
      if (error) {
        console.error("Cross-schema reset error:", error);
      }
    } catch (resetError) {
      console.error("Cross-schema reset error:", resetError);
    }
    
    // 4. If bypass_checks is true, try aggressive approach
    let aggressiveFix = null;
    if (bypass_checks) {
      try {
        const { data } = await supabase.rpc(
          'raw_sql_query',
          {
            sql_query: `
              DO $$
              DECLARE
                tables RECORD;
              BEGIN
                FOR tables IN 
                  SELECT schemaname, tablename 
                  FROM pg_tables 
                  WHERE 
                    (schemaname = 'pgmq' AND tablename LIKE 'q\\_%') OR
                    (schemaname = 'public' AND tablename LIKE 'pgmq\\_%')
                LOOP
                  BEGIN
                    EXECUTE format(
                      'DELETE FROM %I.%I WHERE id::TEXT = $1 OR msg_id::TEXT = $1 OR id = $2 OR msg_id = $2', 
                      tables.schemaname, 
                      tables.tablename
                    ) USING '${message_id}', '${message_id}'::BIGINT;
                  EXCEPTION WHEN OTHERS THEN
                    NULL; -- ignore errors, keep trying
                  END;
                  
                  BEGIN
                    EXECUTE format(
                      'UPDATE %I.%I SET vt = NULL WHERE id::TEXT = $1 OR msg_id::TEXT = $1', 
                      tables.schemaname, 
                      tables.tablename
                    ) USING '${message_id}';
                  EXCEPTION WHEN OTHERS THEN
                    NULL; -- ignore errors, keep trying
                  END;
                END LOOP;
              END $$;
              SELECT TRUE AS success;
            `
          }
        );
        console.log("Aggressive fix result:", data);
        aggressiveFix = data;
      } catch (aggError) {
        console.error("Aggressive fix error:", aggError);
      }
    }
    
    return new Response(
      JSON.stringify({
        success: true,
        message: `Attempted all possible deletion methods for message ${message_id} from queue ${queue_name}`,
        raw_sql: rawSqlResult,
        cross_schema: crossSchemaResult,
        reset: crossSchemaReset,
        aggressive: aggressiveFix,
        diagnostics: diagData
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error("Error with force delete:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
