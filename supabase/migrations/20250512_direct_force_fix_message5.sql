
-- Emergency fix for message ID 5 in artist_discovery queue using direct SQL
DO $$
DECLARE
  schema_table_name TEXT;
  table_exists BOOLEAN;
  result JSONB;
  success BOOLEAN := FALSE;
BEGIN
  -- First, check if we can find the queue table in pgmq schema
  SELECT EXISTS (
    SELECT 1 
    FROM information_schema.tables 
    WHERE table_schema = 'pgmq' AND table_name = 'q_artist_discovery'
  ) INTO table_exists;
  
  -- Try pgmq schema first
  IF table_exists THEN
    schema_table_name := 'pgmq.q_artist_discovery';
    RAISE NOTICE 'Found pgmq queue table: %', schema_table_name;
    
    BEGIN
      -- First try direct deletion
      EXECUTE format('DELETE FROM %I WHERE msg_id = 5 OR id = 5 OR msg_id::TEXT = ''5'' OR id::TEXT = ''5''', schema_table_name);
      GET DIAGNOSTICS success = ROW_COUNT;
      
      IF success > 0 THEN
        RAISE NOTICE 'Successfully deleted message 5 from %', schema_table_name;
      ELSE
        -- Try visibility timeout reset
        EXECUTE format('UPDATE %I SET vt = NULL WHERE msg_id = 5 OR id = 5 OR msg_id::TEXT = ''5'' OR id::TEXT = ''5''', schema_table_name);
        GET DIAGNOSTICS success = ROW_COUNT;
        
        IF success > 0 THEN
          RAISE NOTICE 'Reset visibility timeout for message 5 in %', schema_table_name;
        ELSE
          RAISE NOTICE 'Message 5 not found in %', schema_table_name;
        END IF;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Error handling message in %: %', schema_table_name, SQLERRM;
    END;
  END IF;
  
  -- Next, check the public schema
  SELECT EXISTS (
    SELECT 1 
    FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name = 'pgmq_artist_discovery'
  ) INTO table_exists;
  
  -- Try public schema if not already fixed
  IF table_exists AND NOT success THEN
    schema_table_name := 'public.pgmq_artist_discovery';
    RAISE NOTICE 'Found public queue table: %', schema_table_name;
    
    BEGIN
      -- Try direct deletion
      EXECUTE format('DELETE FROM %I WHERE msg_id = 5 OR id = 5 OR msg_id::TEXT = ''5'' OR id::TEXT = ''5''', schema_table_name);
      GET DIAGNOSTICS success = ROW_COUNT;
      
      IF success > 0 THEN
        RAISE NOTICE 'Successfully deleted message 5 from %', schema_table_name;
      ELSE
        -- Try visibility timeout reset
        EXECUTE format('UPDATE %I SET vt = NULL WHERE msg_id = 5 OR id = 5 OR msg_id::TEXT = ''5'' OR id::TEXT = ''5''', schema_table_name);
        GET DIAGNOSTICS success = ROW_COUNT;
        
        IF success > 0 THEN
          RAISE NOTICE 'Reset visibility timeout for message 5 in %', schema_table_name;
        ELSE
          RAISE NOTICE 'Message 5 not found in %', schema_table_name;
        END IF;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Error handling message in %: %', schema_table_name, SQLERRM;
    END;
  END IF;
  
  -- Output diagnostic information about queue tables
  RAISE NOTICE 'Queue table information:';
  FOR result IN
    SELECT row_to_json(t) FROM (
      SELECT 
        n.nspname AS schema_name, 
        c.relname AS table_name,
        n.nspname || '.' || c.relname AS full_name,
        pg_table_size(n.nspname || '.' || c.relname) AS table_size
      FROM 
        pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE 
        c.relkind = 'r' AND
        (c.relname LIKE 'q\_%' OR c.relname LIKE 'pgmq\_%')
    ) t
  LOOP
    RAISE NOTICE '%', result;
  END LOOP;
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
