
-- Create a helper function for direct message inspection
CREATE OR REPLACE FUNCTION public.inspect_queue_messages(
  p_queue_name TEXT,
  p_limit INTEGER DEFAULT 10,
  p_include_processed BOOLEAN DEFAULT FALSE
) RETURNS TABLE (
  queue_name TEXT,
  queue_table TEXT, 
  message_id TEXT,
  message JSONB,
  created_at TIMESTAMPTZ,
  visibility_timeout TIMESTAMPTZ,
  read_count INT
) AS $$
DECLARE
  v_queue_table TEXT;
BEGIN
  -- Get the actual queue table name
  BEGIN
    SELECT pgmq.get_queue_table_name(p_queue_name) INTO STRICT v_queue_table;
  EXCEPTION WHEN OTHERS THEN
    v_queue_table := 'pgmq_' || p_queue_name;
  END;
  
  IF p_include_processed THEN
    RETURN QUERY EXECUTE format('
      SELECT 
        $1 AS queue_name,
        $2 AS queue_table,
        COALESCE(msg_id::TEXT, id::TEXT) AS message_id,
        message,
        created_at,
        vt AS visibility_timeout,
        read_ct AS read_count
      FROM %I
      ORDER BY created_at DESC
      LIMIT $3
    ', v_queue_table) USING p_queue_name, v_queue_table, p_limit;
  ELSE
    -- Only return messages that are not being processed
    RETURN QUERY EXECUTE format('
      SELECT 
        $1 AS queue_name,
        $2 AS queue_table,
        COALESCE(msg_id::TEXT, id::TEXT) AS message_id,
        message,
        created_at,
        vt AS visibility_timeout,
        read_ct AS read_count
      FROM %I
      WHERE vt IS NULL
      ORDER BY created_at DESC
      LIMIT $3
    ', v_queue_table) USING p_queue_name, v_queue_table, p_limit;
  END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
