
-- Emergency fix for message ID 6 in artist_discovery queue using direct SQL
DO $$
DECLARE
  schema_table_name TEXT;
  table_exists BOOLEAN;
  result JSONB;
  deleted_count INT := 0;
BEGIN
  -- First, check if we can find the correct queue table in pgmq schema
  SELECT EXISTS (
    SELECT 1 
    FROM information_schema.tables 
    WHERE table_schema = 'pgmq' AND table_name = 'q_artist_discovery'
  ) INTO table_exists;
  
  -- Try pgmq schema with correct table pattern first
  IF table_exists THEN
    schema_table_name := 'pgmq.q_artist_discovery';
    RAISE NOTICE 'Found correct PGMQ queue table: %', schema_table_name;
    
    -- First try direct deletion with multiple approaches
    BEGIN
      EXECUTE format('DELETE FROM %I WHERE msg_id = 6', schema_table_name);
      GET DIAGNOSTICS deleted_count = ROW_COUNT;
      
      IF deleted_count > 0 THEN
        RAISE NOTICE 'Successfully deleted message 6 from %', schema_table_name;
      ELSE
        -- Try with text comparison
        EXECUTE format('DELETE FROM %I WHERE msg_id::TEXT = ''6''', schema_table_name);
        GET DIAGNOSTICS deleted_count = ROW_COUNT;
        
        IF deleted_count > 0 THEN
          RAISE NOTICE 'Successfully deleted message 6 using text comparison from %', schema_table_name;
        ELSE
          -- Try id column
          EXECUTE format('DELETE FROM %I WHERE id = 6 OR id::TEXT = ''6''', schema_table_name);
          GET DIAGNOSTICS deleted_count = ROW_COUNT;
          
          IF deleted_count > 0 THEN
            RAISE NOTICE 'Successfully deleted message 6 using id column from %', schema_table_name;
          END IF;
        END IF;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Error handling message in %: %', schema_table_name, SQLERRM;
    END;
  END IF;
  
  -- Try alternative table format name as fallback
  SELECT EXISTS (
    SELECT 1 
    FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name = 'pgmq_artist_discovery'
  ) INTO table_exists;
  
  -- Try public schema as fallback
  IF table_exists THEN
    schema_table_name := 'public.pgmq_artist_discovery';
    RAISE NOTICE 'Found fallback queue table: %', schema_table_name;
    
    -- First try direct deletion with multiple approaches
    BEGIN
      EXECUTE format('DELETE FROM %I WHERE msg_id = 6', schema_table_name);
      GET DIAGNOSTICS deleted_count = ROW_COUNT;
      
      IF deleted_count > 0 THEN
        RAISE NOTICE 'Successfully deleted message 6 from %', schema_table_name;
      ELSE
        -- Try with text comparison
        EXECUTE format('DELETE FROM %I WHERE msg_id::TEXT = ''6''', schema_table_name);
        GET DIAGNOSTICS deleted_count = ROW_COUNT;
        
        IF deleted_count > 0 THEN
          RAISE NOTICE 'Successfully deleted message 6 using text comparison from %', schema_table_name;
        ELSE
          -- Try id column
          EXECUTE format('DELETE FROM %I WHERE id = 6 OR id::TEXT = ''6''', schema_table_name);
          GET DIAGNOSTICS deleted_count = ROW_COUNT;
          
          IF deleted_count > 0 THEN
            RAISE NOTICE 'Successfully deleted message 6 using id column from %', schema_table_name;
          END IF;
        END IF;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Error handling message in %: %', schema_table_name, SQLERRM;
    END;
  END IF;
  
  -- Try with super aggressive approach across all queue tables
  BEGIN
    EXECUTE '
      DO $$
      DECLARE
        queue_tables RECORD;
      BEGIN
        FOR queue_tables IN 
          SELECT schemaname, tablename 
          FROM pg_tables 
          WHERE (schemaname = ''pgmq'' AND tablename LIKE ''q\\_%'') OR
                (schemaname = ''public'' AND tablename LIKE ''pgmq\\_%'')
        LOOP
          BEGIN
            EXECUTE format(''DELETE FROM %I.%I WHERE msg_id = 6 OR 
                                                     msg_id::TEXT = ''''6'''' OR 
                                                     id = 6 OR 
                                                     id::TEXT = ''''6'''' 
                          RETURNING TRUE AS deleted'',
              queue_tables.schemaname, queue_tables.tablename);
          EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE ''Error cleaning message from table %: %'', 
              queue_tables.schemaname || ''.'' || queue_tables.tablename, SQLERRM;
          END;
        END LOOP;
      END $$';
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error with aggressive cleanup: %', SQLERRM;
  END;

  -- Create a more reliable function to handle queue operations across schemas
  CREATE OR REPLACE FUNCTION cross_schema_queue_op(
    p_queue_name TEXT,
    p_message_id TEXT,
    p_operation TEXT -- 'delete' or 'reset'
  ) RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
  DECLARE
    schema_table_name TEXT;
    table_exists BOOLEAN;
    success_count INT := 0;
  BEGIN
    -- First try pgmq schema
    SELECT EXISTS (
      SELECT 1 
      FROM information_schema.tables 
      WHERE table_schema = 'pgmq' AND table_name = 'q_' || p_queue_name
    ) INTO table_exists;
    
    IF table_exists THEN
      schema_table_name := 'pgmq.q_' || p_queue_name;
      
      IF p_operation = 'delete' THEN
        BEGIN
          EXECUTE format('DELETE FROM %I WHERE msg_id::TEXT = $1 OR id::TEXT = $1', schema_table_name)
          USING p_message_id;
          GET DIAGNOSTICS success_count = ROW_COUNT;
        EXCEPTION WHEN OTHERS THEN
          RAISE NOTICE 'Error deleting from %: %', schema_table_name, SQLERRM;
        END;
      ELSIF p_operation = 'reset' THEN
        BEGIN
          EXECUTE format('UPDATE %I SET vt = NULL WHERE msg_id::TEXT = $1 OR id::TEXT = $1', schema_table_name)
          USING p_message_id;
          GET DIAGNOSTICS success_count = ROW_COUNT;
        EXCEPTION WHEN OTHERS THEN
          RAISE NOTICE 'Error resetting from %: %', schema_table_name, SQLERRM;
        END;
      END IF;
      
      IF success_count > 0 THEN
        RETURN TRUE;
      END IF;
    END IF;
    
    -- Next try public schema as fallback
    SELECT EXISTS (
      SELECT 1 
      FROM information_schema.tables 
      WHERE table_schema = 'public' AND table_name = 'pgmq_' || p_queue_name
    ) INTO table_exists;
    
    IF table_exists THEN
      schema_table_name := 'public.pgmq_' || p_queue_name;
      
      IF p_operation = 'delete' THEN
        BEGIN
          EXECUTE format('DELETE FROM %I WHERE msg_id::TEXT = $1 OR id::TEXT = $1', schema_table_name)
          USING p_message_id;
          GET DIAGNOSTICS success_count = ROW_COUNT;
        EXCEPTION WHEN OTHERS THEN
          RAISE NOTICE 'Error deleting from %: %', schema_table_name, SQLERRM;
        END;
      ELSIF p_operation = 'reset' THEN
        BEGIN
          EXECUTE format('UPDATE %I SET vt = NULL WHERE msg_id::TEXT = $1 OR id::TEXT = $1', schema_table_name)
          USING p_message_id;
          GET DIAGNOSTICS success_count = ROW_COUNT;
        EXCEPTION WHEN OTHERS THEN
          RAISE NOTICE 'Error resetting from %: %', schema_table_name, SQLERRM;
        END;
      END IF;
      
      IF success_count > 0 THEN
        RETURN TRUE;
      END IF;
    END IF;
    
    RETURN FALSE;
  END;
  $$;
  
  -- Schedule more aggressive queue monitoring to find and fix stuck messages
  DO $$
  BEGIN
    PERFORM cron.unschedule('force-queue-cleanup');
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Cron job did not exist yet, will be created';
  END $$;

  -- Create a more aggressive cleanup job that runs every 5 minutes
  SELECT cron.schedule(
    'force-queue-cleanup',
    '*/5 * * * *',  -- Run every 5 minutes
    $$
    DO $$
    DECLARE
      queue_tables RECORD;
    BEGIN
      FOR queue_tables IN 
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE (schemaname = 'pgmq' AND tablename LIKE 'q\\_%') OR
              (schemaname = 'public' AND tablename LIKE 'pgmq\\_%')
      LOOP
        BEGIN
          -- Delete messages that have been stuck for more than 30 minutes
          EXECUTE format('
            DELETE FROM %I.%I 
            WHERE vt IS NOT NULL AND vt < NOW() - INTERVAL ''30 minutes''
            RETURNING msg_id',
            queue_tables.schemaname, queue_tables.tablename);
        EXCEPTION WHEN OTHERS THEN
          NULL; -- Ignore errors and continue to next table
        END;
      END LOOP;
    END $$;
    $$
  );
END $$;
