
-- Ensure we have the raw_sql_query function for the monitor to use
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'raw_sql_query') THEN
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
  END IF;
END $$;

-- Create a better function to list stuck messages
CREATE OR REPLACE FUNCTION public.list_stuck_messages(
  queue_name TEXT,
  min_minutes_locked INT DEFAULT 10
) RETURNS TABLE (
  id TEXT,
  msg_id TEXT,
  message JSONB,
  locked_since TIMESTAMP WITH TIME ZONE,
  read_count INT,
  minutes_locked NUMERIC
) AS $$
DECLARE
  queue_table TEXT;
BEGIN
  -- Get the actual queue table name with schema
  SELECT public.get_queue_table_name_safe(queue_name) INTO queue_table;
  
  -- Return stuck messages
  RETURN QUERY EXECUTE format('
    SELECT 
      id::TEXT, 
      msg_id::TEXT, 
      message::JSONB, 
      vt, 
      read_ct,
      EXTRACT(EPOCH FROM (NOW() - vt))/60 AS minutes_locked
    FROM %s
    WHERE 
      vt IS NOT NULL 
      AND vt < NOW() - INTERVAL ''%s minutes''
    ORDER BY vt ASC
  ', queue_table, min_minutes_locked);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to reset visibility timeout for stuck messages
CREATE OR REPLACE FUNCTION public.reset_stuck_messages(
  queue_name TEXT,
  min_minutes_locked INT DEFAULT 10
) RETURNS INT AS $$
DECLARE
  queue_table TEXT;
  reset_count INT;
BEGIN
  -- Get the actual queue table name with schema
  SELECT public.get_queue_table_name_safe(queue_name) INTO queue_table;
  
  -- Reset visibility timeout for stuck messages
  EXECUTE format('
    UPDATE %s
    SET vt = NULL
    WHERE 
      vt IS NOT NULL 
      AND vt < NOW() - INTERVAL ''%s minutes''
  ', queue_table, min_minutes_locked);
  
  GET DIAGNOSTICS reset_count = ROW_COUNT;
  RETURN reset_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to reset all stuck messages across all queues
CREATE OR REPLACE FUNCTION public.reset_all_stuck_messages(
  threshold_minutes INT DEFAULT 10
) RETURNS TABLE (
  queue_name TEXT,
  messages_reset INT
) AS $$
DECLARE
  q RECORD;
  reset_count INT;
BEGIN
  -- Find all queue tables using information_schema
  FOR q IN 
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE 
      (table_name LIKE 'q\_%' OR table_name LIKE 'pgmq\_%') 
      AND table_type = 'BASE TABLE'
  LOOP
    -- Extract queue name from table name
    queue_name := CASE 
      WHEN q.table_name LIKE 'q\_%' THEN substring(q.table_name from 3)
      WHEN q.table_name LIKE 'pgmq\_%' THEN substring(q.table_name from 6)
      ELSE q.table_name
    END;
    
    -- Reset stuck messages
    EXECUTE format('
      UPDATE %I.%I
      SET vt = NULL
      WHERE 
        vt IS NOT NULL 
        AND vt < NOW() - INTERVAL ''%s minutes''
      RETURNING count(*)', 
      q.table_schema, q.table_name, threshold_minutes
    ) INTO reset_count;
    
    IF reset_count > 0 THEN
      RETURN QUERY SELECT queue_name::TEXT, reset_count::INT;
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Set up improved auto queue monitoring
DO $$
BEGIN
  -- Remove old cron jobs to avoid duplication
  PERFORM cron.unschedule('queue-monitor-job');
  PERFORM cron.unschedule('auto-queue-monitor-job');
  
  -- Create a new cron job with better monitoring
  PERFORM cron.schedule(
    'queue-auto-fix-job',
    '*/3 * * * *',  -- Run every 3 minutes
    $$
    SELECT
      net.http_post(
        url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/queueMonitor',
        headers:= json_build_object(
          'Content-type', 'application/json', 
          'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
        )::jsonb,
        body:= '{"threshold_minutes":5,"auto_fix":true}'::jsonb,
        timeout_milliseconds:= 30000
      ) as request_id;
    $$
  );
END $$;
