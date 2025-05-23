
-- This migration adds a bridge function for pgmq_read functionality
-- to resolve the "Could not find the function public.pgmq_read" error

-- Create a function that safely reads from a queue using proper queue table name detection
CREATE OR REPLACE FUNCTION public.pgmq_read_safe(
  queue_name TEXT,
  max_messages INTEGER DEFAULT 10,
  visibility_timeout INTEGER DEFAULT 30
)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  queue_table TEXT;
  result JSONB;
  dynamic_sql TEXT;
BEGIN
  -- Get the right queue table name
  SELECT public.get_queue_table_name_safe(queue_name) INTO queue_table;
  
  -- Build a dynamic SQL statement to read from the queue
  -- Fixed the query to use msg_id instead of id
  dynamic_sql := format(
    'WITH visible_msgs AS (
      SELECT *
      FROM %s
      WHERE vt IS NULL
      ORDER BY msg_id
      LIMIT %s
      FOR UPDATE SKIP LOCKED
    ),
    updated AS (
      UPDATE %s t
      SET vt = now() + interval ''%s seconds'',
          read_ct = COALESCE(read_ct, 0) + 1
      FROM visible_msgs
      WHERE t.msg_id = visible_msgs.msg_id
      RETURNING t.*
    )
    SELECT jsonb_agg(
      jsonb_build_object(
        ''id'', msg_id::TEXT,
        ''msg_id'', msg_id::TEXT,
        ''message'', message,
        ''created_at'', enqueued_at,
        ''vt'', vt,
        ''read_ct'', read_ct
      )
    ) FROM updated', 
    queue_table, 
    max_messages, 
    queue_table,
    visibility_timeout
  );
  
  -- Execute the SQL and get the results
  EXECUTE dynamic_sql INTO result;
  
  -- Return empty array if no results
  RETURN COALESCE(result, '[]'::JSONB);
EXCEPTION WHEN OTHERS THEN
  -- Log the error but don't expose it to the caller
  RAISE WARNING 'pgmq_read_safe error for queue %: %', queue_name, SQLERRM;
  RETURN '[]'::JSONB;
END;
$$;

-- Create a synonym for pgmq_read that calls pgmq_read_safe
-- This helps existing code that expects pgmq_read to work
CREATE OR REPLACE FUNCTION public.pgmq_read(
  max_messages INTEGER, 
  queue_name TEXT,
  visibility_timeout INTEGER DEFAULT 30
)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  RETURN public.pgmq_read_safe(queue_name, max_messages, visibility_timeout);
EXCEPTION WHEN OTHERS THEN
  RAISE WARNING 'pgmq_read wrapper error: %', SQLERRM;
  RETURN '[]'::JSONB;
END;
$$;
