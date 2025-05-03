
-- Emergency fix for message ID 5 in artist_discovery queue
DO $$
DECLARE
  queue_table TEXT;
  message_exists BOOLEAN;
BEGIN
  -- Try to get the actual queue table name
  BEGIN
    SELECT pgmq.get_queue_table_name('artist_discovery') INTO queue_table;
  EXCEPTION WHEN OTHERS THEN
    queue_table := 'pgmq_artist_discovery';
  END;

  -- Check if message exists
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE msg_id = 5 OR id = 5 OR msg_id::TEXT = ''5'' OR id::TEXT = ''5'')', queue_table)
  INTO message_exists;
  
  IF message_exists THEN
    -- Try direct deletion
    BEGIN
      EXECUTE format('DELETE FROM %I WHERE msg_id = 5 OR id = 5 OR msg_id::TEXT = ''5'' OR id::TEXT = ''5''', queue_table);
      RAISE NOTICE 'Deleted message 5 from %', queue_table;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Failed to delete message 5: %', SQLERRM;
      
      -- Try to reset visibility timeout as fallback
      BEGIN
        EXECUTE format('UPDATE %I SET vt = NULL WHERE msg_id = 5 OR id = 5 OR msg_id::TEXT = ''5'' OR id::TEXT = ''5''', queue_table);
        RAISE NOTICE 'Reset visibility timeout for message 5 in %', queue_table;
      EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Failed to reset visibility timeout for message 5: %', SQLERRM;
      END;
    END;
  ELSE
    RAISE NOTICE 'Message 5 not found in %', queue_table;
  END IF;
  
  -- Add a helper function for direct message inspection
  CREATE OR REPLACE FUNCTION public.inspect_queue_message(
    p_queue_name TEXT,
    p_message_id TEXT
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
      SELECT pgmq.get_queue_table_name(p_queue_name) INTO v_queue_table;
    EXCEPTION WHEN OTHERS THEN
      v_queue_table := 'pgmq_' || p_queue_name;
    END;
    
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
      WHERE msg_id::TEXT = $3 OR id::TEXT = $3
    ', v_queue_table) USING p_queue_name, v_queue_table, p_message_id;
  END;
  $$ LANGUAGE plpgsql SECURITY DEFINER;
END $$;

-- Verify if any issue remains
SELECT * FROM public.inspect_queue_message('artist_discovery', '5');
