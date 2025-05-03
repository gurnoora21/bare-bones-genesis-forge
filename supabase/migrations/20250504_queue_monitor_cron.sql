
-- Add the new queue management functions
CREATE OR REPLACE FUNCTION public.ensure_message_deleted(
  queue_name TEXT,
  message_id TEXT,
  max_attempts INT DEFAULT 3
) RETURNS BOOLEAN AS $$
DECLARE
  queue_table TEXT;
  is_numeric BOOLEAN;
  attempt_count INT := 0;
  msg_deleted BOOLEAN := FALSE;
BEGIN
  -- Determine if the ID is numeric
  BEGIN
    PERFORM message_id::NUMERIC;
    is_numeric := TRUE;
  EXCEPTION WHEN OTHERS THEN
    is_numeric := FALSE;
  END;
  
  -- Get the actual queue table name
  BEGIN
    SELECT pgmq.get_queue_table_name(queue_name) INTO STRICT queue_table;
  EXCEPTION WHEN OTHERS THEN
    queue_table := 'pgmq_' || queue_name;
  END;
  
  -- Try multiple deletion methods with retries
  WHILE attempt_count < max_attempts AND NOT msg_deleted LOOP
    attempt_count := attempt_count + 1;
    
    -- Method 1: Try standard pgmq.delete
    BEGIN
      SELECT pgmq.delete(queue_name, 
        CASE WHEN is_numeric 
          THEN message_id::NUMERIC 
          ELSE message_id::UUID 
        END
      ) INTO msg_deleted;
      
      IF msg_deleted THEN
        RETURN TRUE;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      -- Just continue to next method
    END;
    
    -- Method 2: Try direct table deletion with the appropriate type
    IF is_numeric THEN
      BEGIN
        EXECUTE format('DELETE FROM %I WHERE msg_id = $1::BIGINT RETURNING TRUE', queue_table)
          USING message_id::BIGINT INTO msg_deleted;
          
        IF msg_deleted THEN
          RETURN TRUE;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        -- Continue to next method
      END;
      
      BEGIN
        EXECUTE format('DELETE FROM %I WHERE id = $1::BIGINT RETURNING TRUE', queue_table)
          USING message_id::BIGINT INTO msg_deleted;
          
        IF msg_deleted THEN
          RETURN TRUE;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        -- Continue to next method
      END;
    ELSE -- UUID or string ID
      BEGIN
        EXECUTE format('DELETE FROM %I WHERE id = $1::UUID RETURNING TRUE', queue_table)
          USING message_id INTO msg_deleted;
          
        IF msg_deleted THEN
          RETURN TRUE;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        -- Continue to next method
      END;
    END IF;
    
    -- Method 3: Most flexible but slowest - text comparison
    BEGIN
      EXECUTE format('DELETE FROM %I WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE', queue_table)
        USING message_id INTO msg_deleted;
        
      IF msg_deleted THEN
        RETURN TRUE;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      -- Continue to next attempt
    END;
    
    -- Wait a bit before next attempt (exponential backoff)
    IF NOT msg_deleted AND attempt_count < max_attempts THEN
      PERFORM pg_sleep(0.1 * (2 ^ attempt_count));
    END IF;
  END LOOP;
  
  -- Final verification to see if the message still exists
  BEGIN
    EXECUTE format('SELECT NOT EXISTS(SELECT 1 FROM %I WHERE id::TEXT = $1 OR msg_id::TEXT = $1)', queue_table)
      USING message_id INTO msg_deleted;
      
    RETURN msg_deleted; -- TRUE if message is gone (deleted or never existed)
  EXCEPTION WHEN OTHERS THEN
    RETURN FALSE; -- Something went wrong with verification
  END;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to list potentially stuck messages
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
  -- Get the actual queue table name
  BEGIN
    SELECT pgmq.get_queue_table_name(queue_name) INTO STRICT queue_table;
  EXCEPTION WHEN OTHERS THEN
    queue_table := 'pgmq_' || queue_name;
  END;
  
  -- Return stuck messages
  RETURN QUERY EXECUTE format('
    SELECT 
      id::TEXT, 
      msg_id::TEXT, 
      message::JSONB, 
      vt, 
      read_ct,
      EXTRACT(EPOCH FROM (NOW() - vt))/60 AS minutes_locked
    FROM %I
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
  -- Get the actual queue table name
  BEGIN
    SELECT pgmq.get_queue_table_name(queue_name) INTO STRICT queue_table;
  EXCEPTION WHEN OTHERS THEN
    queue_table := 'pgmq_' || queue_name;
  END;
  
  -- Reset visibility timeout for stuck messages
  EXECUTE format('
    UPDATE %I
    SET vt = NULL
    WHERE 
      vt IS NOT NULL 
      AND vt < NOW() - INTERVAL ''%s minutes''
  ', queue_table, min_minutes_locked);
  
  GET DIAGNOSTICS reset_count = ROW_COUNT;
  RETURN reset_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Set up a cron job to run the queue monitor every 5 minutes
-- First make sure the cron job doesn't already exist
DO $$
BEGIN
  PERFORM cron.unschedule('queue-monitor-job');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Job did not exist yet, will be created';
END $$;

-- Now schedule the cron job
SELECT cron.schedule(
  'queue-monitor-job',
  '*/5 * * * *',  -- Run every 5 minutes
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/queueMonitor',
      headers:= json_build_object(
        'Content-type', 'application/json', 
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"auto_fix":"true"}'::jsonb,
      timeout_milliseconds:= 30000
    );
  $$
);

-- Add automatic stuck message recovery
DO $$
BEGIN
  PERFORM cron.unschedule('auto-fix-stuck-messages-job');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Job did not exist yet, will be created';
END $$;

-- Schedule automatic fix job to run every 15 minutes
SELECT cron.schedule(
  'auto-fix-stuck-messages-job',
  '*/15 * * * *',  -- Run every 15 minutes
  $$
  -- Reset stuck messages in each queue
  DO $$
  DECLARE
    queue_name TEXT;
    fixed_count INT;
  BEGIN
    -- Process each queue
    FOR queue_name IN 
      SELECT unnest(ARRAY['artist_discovery', 'album_discovery', 'track_discovery', 
                          'producer_identification', 'social_enrichment'])
    LOOP
      SELECT public.reset_stuck_messages(queue_name, 15) INTO fixed_count;
      IF fixed_count > 0 THEN
        RAISE NOTICE 'Fixed % stuck messages in queue %', fixed_count, queue_name;
      END IF;
    END LOOP;
  END $$;
  $$
);
