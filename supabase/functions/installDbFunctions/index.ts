
// Install necessary database functions for monitoring and state management

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

// SQL to create necessary functions
const installFunctionsSQL = `
-- Function to list all queues
CREATE OR REPLACE FUNCTION public.list_queues()
RETURNS TABLE (queue_name text)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  SELECT table_name::text
  FROM information_schema.tables 
  WHERE table_schema = 'pgmq'
  AND table_name LIKE 'q_%'
  ORDER BY table_name;
END;
$$;

-- Function to get processing statistics
CREATE OR REPLACE FUNCTION public.get_processing_stats()
RETURNS TABLE (
  entity_type text,
  total bigint,
  pending bigint,
  in_progress bigint,
  completed bigint,
  failed bigint,
  stuck_count bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  SELECT 
    ps.entity_type,
    count(*)::bigint AS total,
    count(*) FILTER (WHERE ps.state = 'PENDING')::bigint AS pending,
    count(*) FILTER (WHERE ps.state = 'IN_PROGRESS')::bigint AS in_progress,
    count(*) FILTER (WHERE ps.state = 'COMPLETED')::bigint AS completed,
    count(*) FILTER (WHERE ps.state = 'FAILED')::bigint AS failed,
    count(*) FILTER (WHERE ps.state = 'IN_PROGRESS' AND ps.updated_at < now() - interval '30 minutes')::bigint AS stuck_count
  FROM 
    processing_status ps
  GROUP BY 
    ps.entity_type;
END;
$$;

-- Function to get worker issue statistics
CREATE OR REPLACE FUNCTION public.get_worker_issue_stats()
RETURNS TABLE (
  worker_name text,
  issue_type text,
  count bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  SELECT 
    wi.worker_name,
    wi.issue_type,
    count(*)::bigint AS count
  FROM 
    worker_issues wi
  WHERE 
    wi.created_at > now() - interval '7 days'
  GROUP BY 
    GROUPING SETS (
      (wi.worker_name),
      (wi.issue_type),
      ()
    );
END;
$$;

-- Function to get queue stats
CREATE OR REPLACE FUNCTION public.queue_stats(queue_name text)
RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  queue_table text;
  msg_count int;
  oldest_msg timestamp with time zone;
  result json;
BEGIN
  -- Get the queue table name
  BEGIN
    SELECT pgmq.get_queue_table_name(queue_name) INTO STRICT queue_table;
  EXCEPTION WHEN OTHERS THEN
    queue_table := 'pgmq.q_' || queue_name;
  END;
  
  -- Safe execution in case table doesn't exist
  BEGIN
    EXECUTE format('
      SELECT 
        COUNT(*) as count,
        MIN(created_at) as oldest_message
      FROM 
        %I', queue_table)
    INTO msg_count, oldest_msg;
  EXCEPTION WHEN OTHERS THEN
    msg_count := 0;
    oldest_msg := NULL;
  END;
  
  -- Build result JSON
  SELECT json_build_object(
    'count', COALESCE(msg_count, 0),
    'oldest_message', oldest_msg
  ) INTO result;
  
  RETURN result;
END;
$$;

-- Enhanced function to reset entity processing state
-- Supports targeted state resets and coordination with Redis
CREATE OR REPLACE FUNCTION public.reset_entity_processing_state(
  p_entity_type TEXT DEFAULT NULL,
  p_older_than_minutes INT DEFAULT 1440, -- Default 24 hours
  p_target_states TEXT[] DEFAULT ARRAY['COMPLETED', 'FAILED', 'IN_PROGRESS']::TEXT[]
)
RETURNS TABLE (
  entity_type TEXT,
  entity_id TEXT,
  old_state TEXT,
  new_state TEXT,
  last_processed_at TIMESTAMP WITH TIME ZONE
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  WITH updated_records AS (
    UPDATE processing_status
    SET 
      state = 'PENDING',
      last_processed_at = now(),
      last_error = CASE 
        WHEN last_error IS NULL THEN 'State reset by system'
        ELSE 'Previous error: ' || last_error || ' | Reset by system'
      END
    WHERE
      (p_entity_type IS NULL OR entity_type = p_entity_type)
      AND (
        -- Age filter only applies to specified states
        (state = ANY(p_target_states) AND updated_at < now() - (p_older_than_minutes * interval '1 minute'))
        -- For IN_PROGRESS states, use last_processed_at to detect stuck states
        OR (state = 'IN_PROGRESS' AND last_processed_at < now() - interval '30 minutes')
      )
    RETURNING 
      entity_type,
      entity_id,
      state AS new_state,
      updated_at,
      last_processed_at
  )
  SELECT 
    ur.entity_type,
    ur.entity_id,
    'RESET', -- Previous state isn't preserved, we just mark as reset
    ur.new_state,
    ur.last_processed_at
  FROM 
    updated_records ur;
END;
$$;

-- Function to find inconsistencies between database and Redis
-- Note: This function only identifies potential database candidates
-- Redis check must be done at application level
CREATE OR REPLACE FUNCTION public.find_inconsistent_states(
  p_entity_type TEXT DEFAULT NULL,
  p_older_than_minutes INT DEFAULT 60
)
RETURNS TABLE (
  entity_type TEXT,
  entity_id TEXT,
  db_state TEXT,
  last_processed_at TIMESTAMP WITH TIME ZONE,
  updated_at TIMESTAMP WITH TIME ZONE,
  minutes_since_update NUMERIC
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  SELECT 
    ps.entity_type,
    ps.entity_id,
    ps.state AS db_state,
    ps.last_processed_at,
    ps.updated_at,
    EXTRACT(EPOCH FROM (now() - ps.updated_at))/60 AS minutes_since_update
  FROM 
    processing_status ps
  WHERE 
    (p_entity_type IS NULL OR ps.entity_type = p_entity_type)
    AND ps.updated_at < now() - (p_older_than_minutes * interval '1 minute')
    AND ps.state IN ('IN_PROGRESS', 'PENDING')
  ORDER BY 
    ps.updated_at ASC;
END;
$$;
`;

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }
  
  // Only allow POST requests
  if (req.method !== 'POST') {
    return new Response(
      JSON.stringify({ error: "Method not allowed" }),
      { status: 405, headers: corsHeaders }
    );
  }

  try {
    // Initialize Supabase client
    const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
    const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
    const supabase = createClient(supabaseUrl, supabaseKey);
    
    // Install database functions using raw SQL
    const { data, error } = await supabase.rpc('raw_sql_query', {
      sql_query: installFunctionsSQL
    });
    
    if (error) {
      console.error("Error installing database functions:", error);
      return new Response(
        JSON.stringify({ error }),
        { status: 500, headers: corsHeaders }
      );
    }
    
    return new Response(
      JSON.stringify({
        success: true,
        message: "Database functions installed successfully",
      }),
      { headers: corsHeaders }
    );
  } catch (error) {
    console.error("Error installing database functions:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: corsHeaders }
    );
  }
});
