
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

-- Create a function to diagnose queue issues
CREATE OR REPLACE FUNCTION public.diagnose_queue_tables(
  queue_name TEXT DEFAULT NULL
) RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  result JSONB;
  matches JSONB;
BEGIN
  WITH queue_tables AS (
    SELECT 
      n.nspname AS schema_name,
      c.relname AS table_name,
      n.nspname || '.' || c.relname AS full_name,
      pg_table_size(n.nspname || '.' || c.relname) AS table_size,
      (SELECT COUNT(*) FROM pg_catalog.pg_attribute 
       WHERE attrelid = c.oid AND attnum > 0) AS column_count
    FROM 
      pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE 
      c.relkind = 'r' AND
      (
        (n.nspname = 'pgmq' AND c.relname LIKE 'q\_%') OR
        (n.nspname = 'public' AND c.relname LIKE 'pgmq\_%')
      ) AND
      (queue_name IS NULL OR 
       c.relname = 'q_' || queue_name OR 
       c.relname = 'pgmq_' || queue_name)
  )
  SELECT 
    jsonb_build_object(
      'queue_tables', jsonb_agg(
        jsonb_build_object(
          'schema_name', schema_name,
          'table_name', table_name,
          'full_name', full_name,
          'table_size', table_size,
          'column_count', column_count
        )
      ),
      'count', COUNT(*),
      'found_in_pgmq', COUNT(*) FILTER (WHERE schema_name = 'pgmq'),
      'found_in_public', COUNT(*) FILTER (WHERE schema_name = 'public')
    )
  INTO result
  FROM queue_tables;
  
  -- Return empty structure if no tables found
  IF result IS NULL THEN
    RETURN jsonb_build_object(
      'queue_tables', '[]',
      'count', 0,
      'found_in_pgmq', 0,
      'found_in_public', 0,
      'error', 'No queue tables found'
    );
  END IF;
  
  -- Find actual table for this queue name if specified
  IF queue_name IS NOT NULL THEN
    WITH specific_tables AS (
      SELECT 
        n.nspname || '.' || c.relname AS full_name
      FROM 
        pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE 
        c.relkind = 'r' AND
        ((n.nspname = 'pgmq' AND c.relname = 'q_' || queue_name) OR
         (n.nspname = 'public' AND c.relname = 'pgmq_' || queue_name))
    )
    SELECT jsonb_agg(full_name) INTO matches FROM specific_tables;
    
    result = result || jsonb_build_object('queue_matches', matches);
  END IF;
  
  RETURN result;
END;
$$;

-- Special direct function to fix message 5 in artist_discovery queue
DO $$
DECLARE
  schema_table_name TEXT;
  table_exists BOOLEAN;
  result JSONB;
  success BOOLEAN := FALSE;
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
    
    BEGIN
      -- First try direct deletion of message 6
      EXECUTE format('DELETE FROM %I WHERE msg_id = 6 OR id = 6 OR msg_id::TEXT = ''6'' OR id::TEXT = ''6''', schema_table_name);
      GET DIAGNOSTICS success = ROW_COUNT;
      
      IF success > 0 THEN
        RAISE NOTICE 'Successfully deleted message 6 from %', schema_table_name;
      ELSE
        -- Try visibility timeout reset
        EXECUTE format('UPDATE %I SET vt = NULL WHERE msg_id = 6 OR id = 6 OR msg_id::TEXT = ''6'' OR id::TEXT = ''6''', schema_table_name);
        GET DIAGNOSTICS success = ROW_COUNT;
        
        IF success > 0 THEN
          RAISE NOTICE 'Reset visibility timeout for message 6 in %', schema_table_name;
        ELSE
          RAISE NOTICE 'Message 6 not found in %', schema_table_name;
        END IF;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Error handling message in %: %', schema_table_name, SQLERRM;
    END;
  END IF;
  
  -- Next, check the public schema as fallback
  SELECT EXISTS (
    SELECT 1 
    FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name = 'pgmq_artist_discovery'
  ) INTO table_exists;
  
  -- Try public schema if not already fixed
  IF table_exists AND NOT success THEN
    schema_table_name := 'public.pgmq_artist_discovery';
    RAISE NOTICE 'Found fallback queue table: %', schema_table_name;
    
    BEGIN
      -- Try direct deletion
      EXECUTE format('DELETE FROM %I WHERE msg_id = 6 OR id = 6 OR msg_id::TEXT = ''6'' OR id::TEXT = ''6''', schema_table_name);
      GET DIAGNOSTICS success = ROW_COUNT;
      
      IF success > 0 THEN
        RAISE NOTICE 'Successfully deleted message 6 from %', schema_table_name;
      ELSE
        -- Try visibility timeout reset
        EXECUTE format('UPDATE %I SET vt = NULL WHERE msg_id = 6 OR id = 6 OR msg_id::TEXT = ''6'' OR id::TEXT = ''6''', schema_table_name);
        GET DIAGNOSTICS success = ROW_COUNT;
        
        IF success > 0 THEN
          RAISE NOTICE 'Reset visibility timeout for message 6 in %', schema_table_name;
        ELSE
          RAISE NOTICE 'Message 6 not found in %', schema_table_name;
        END IF;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Error handling message in %: %', schema_table_name, SQLERRM;
    END;
  END IF;
  
  -- Output diagnostic information about queue tables
  RAISE NOTICE 'Queue table information:';
  SELECT diagnose_queue_tables('artist_discovery') INTO result;
  RAISE NOTICE 'Diagnostic result: %', result;
END $$;

-- Set up a better cron job to automatically fix stuck messages
DO $$
BEGIN
  PERFORM cron.unschedule('auto-fix-stuck-messages-cron');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Cron job did not exist yet, will be created';
END $$;

SELECT cron.schedule(
  'auto-fix-stuck-messages-cron',
  '*/10 * * * *',  -- Run every 10 minutes
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/queueMonitor',
      headers:= json_build_object(
        'Content-type', 'application/json', 
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"threshold_minutes":10,"auto_fix":true}'::jsonb,
      timeout_milliseconds:= 30000
    ) as request_id;
  $$
);
