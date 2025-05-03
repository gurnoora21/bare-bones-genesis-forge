
-- Create a dedicated function to correctly detect and use the right queue table
CREATE OR REPLACE FUNCTION public.get_queue_table_name_safe(p_queue_name TEXT)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  table_name TEXT;
  schema_table TEXT;
  pgmq_exists BOOLEAN;
  q_exists BOOLEAN;
  pgmq_exists BOOLEAN;
BEGIN
  -- Check if the pgmq schema exists
  SELECT EXISTS (
    SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgmq'
  ) INTO pgmq_exists;
  
  IF pgmq_exists THEN
    -- Check if the table exists in pgmq schema
    SELECT EXISTS (
      SELECT 1 FROM information_schema.tables 
      WHERE table_schema = 'pgmq' AND table_name = 'q_' || p_queue_name
    ) INTO q_exists;
    
    IF q_exists THEN
      RETURN 'pgmq.q_' || p_queue_name;
    END IF;
  END IF;
  
  -- Fallback to public schema with pgmq_ prefix
  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name = 'pgmq_' || p_queue_name
  ) INTO pgmq_exists;
  
  IF pgmq_exists THEN
    RETURN 'public.pgmq_' || p_queue_name;
  END IF;
  
  -- If no table exists, return the preferred naming convention for creation
  IF pgmq_exists THEN
    RETURN 'pgmq.q_' || p_queue_name;
  ELSE
    RETURN 'public.pgmq_' || p_queue_name;
  END IF;
END;
$$;

-- Function to handle queue operations in both schemas
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
