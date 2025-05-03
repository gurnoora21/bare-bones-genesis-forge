
-- Queue management functions

-- Create PGMQ queues for worker system
SELECT pgmq.create('artist_discovery');
SELECT pgmq.create('album_discovery');
SELECT pgmq.create('track_discovery');
SELECT pgmq.create('producer_identification');
SELECT pgmq.create('social_enrichment');

-- Function to enqueue a message with JSONB type support
DROP FUNCTION IF EXISTS pg_enqueue(TEXT, TEXT);

CREATE OR REPLACE FUNCTION pg_enqueue(
  queue_name TEXT,
  message_body JSONB  -- Accept JSONB directly
) RETURNS BIGINT AS $$
DECLARE
  msg_id BIGINT;
BEGIN
  -- Call pgmq.send with the JSONB parameter
  SELECT send
    FROM pgmq.send(queue_name, message_body)
    INTO msg_id;
  RETURN msg_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to dequeue a batch of messages
CREATE OR REPLACE FUNCTION pg_dequeue(
  queue_name TEXT,
  batch_size INT DEFAULT 5,
  visibility_timeout INT DEFAULT 60
) RETURNS JSONB AS $$
DECLARE
  result JSONB;
BEGIN
  SELECT jsonb_agg(row_to_json(msg))
  FROM pgmq.read(queue_name, batch_size, visibility_timeout) AS msg
  INTO result;
  
  RETURN result;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Improved message deletion function with multiple approaches
CREATE OR REPLACE FUNCTION pg_delete_message(
  queue_name TEXT,
  message_id TEXT -- Accept string to handle various ID formats
) RETURNS BOOLEAN AS $$
DECLARE
  queue_table TEXT;
  success BOOLEAN := FALSE;
  numeric_id BIGINT;
  uuid_id UUID;
BEGIN
  -- Get the actual queue table name
  BEGIN
    SELECT pgmq.get_queue_table_name(queue_name) INTO STRICT queue_table;
  EXCEPTION WHEN OTHERS THEN
    queue_table := 'pgmq_' || queue_name;
  END;
  
  -- Try standard pgmq.delete first if message_id can be converted to UUID
  BEGIN
    uuid_id := message_id::UUID;
    SELECT pgmq.delete(queue_name, uuid_id) INTO success;
    IF success THEN
      RETURN TRUE;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    -- Not a UUID, continue to other methods
  END;
  
  -- Try with numeric ID if possible
  BEGIN
    numeric_id := message_id::BIGINT;
    
    -- Try deleting with numeric msg_id
    EXECUTE format('DELETE FROM %I WHERE msg_id = $1', queue_table)
    USING numeric_id;
    GET DIAGNOSTICS success = ROW_COUNT;
    
    IF success > 0 THEN
      RETURN TRUE;
    END IF;
    
    -- Try deleting with numeric id
    EXECUTE format('DELETE FROM %I WHERE id = $1', queue_table)
    USING numeric_id;
    GET DIAGNOSTICS success = ROW_COUNT;
    
    IF success > 0 THEN
      RETURN TRUE;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    -- Not a numeric ID or other failure, continue to text comparison
  END;
  
  -- Final attempt with text comparison - most flexible but potentially slower
  BEGIN
    EXECUTE format('DELETE FROM %I WHERE msg_id::TEXT = $1 OR id::TEXT = $1', queue_table)
    USING message_id;
    GET DIAGNOSTICS success = ROW_COUNT;
    
    IF success > 0 THEN
      RETURN TRUE;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'All deletion attempts for message % in queue % failed', message_id, queue_name;
  END;
  
  RETURN FALSE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to release a message (return to queue)
CREATE OR REPLACE FUNCTION pg_release_message(
  queue_name TEXT,
  message_id UUID
) RETURNS BOOLEAN AS $$
DECLARE
  success BOOLEAN;
BEGIN
  -- Use archive + re-send pattern since there's no direct "release" in PGMQ
  SELECT pgmq.delete(queue_name, message_id) INTO success;
  IF success THEN
    -- Re-send the message with original body
    PERFORM pgmq.send(queue_name, (
      SELECT message_body 
      FROM pgmq.get_queue_table_name(queue_name) 
      WHERE id = message_id
    ));
  END IF;
  RETURN success;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to get queue status
CREATE OR REPLACE FUNCTION pg_queue_status(
  queue_name TEXT
) RETURNS JSON AS $$
DECLARE
  msg_count INT;
  oldest_msg TIMESTAMP WITH TIME ZONE;
BEGIN
  SELECT COUNT(*), MIN(created_at)
  INTO msg_count, oldest_msg
  FROM pgmq.get_queue_table_name(queue_name);
  
  RETURN json_build_object(
    'count', msg_count,
    'oldest_message', oldest_msg
  );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to manually start the discovery process for an artist
CREATE OR REPLACE FUNCTION start_artist_discovery(artist_name TEXT) RETURNS BIGINT AS $$
DECLARE
  msg_id BIGINT;
BEGIN
  -- Create a proper JSONB payload
  SELECT pg_enqueue('artist_discovery', jsonb_build_object('artistName', artist_name)) INTO msg_id;
  RETURN msg_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to manually trigger any worker using explicit URLs
CREATE OR REPLACE FUNCTION manual_trigger_worker(worker_name TEXT) RETURNS JSONB AS $$
DECLARE
  response JSONB;
  worker_url TEXT;
BEGIN
  -- Construct the worker URL with explicit URL
  worker_url := 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/' || worker_name;
  
  -- Trigger the worker manually with explicit anon key
  SELECT 
    net.http_post(
      url:= worker_url,
      headers:= json_build_object(
        'Content-type', 'application/json', 
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{}'::jsonb,
      timeout_milliseconds:= 0
    ) INTO response;
    
  RETURN response;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to reset visibility timeout for stuck messages
CREATE OR REPLACE FUNCTION reset_stuck_message(
  queue_name TEXT,
  message_id TEXT
) RETURNS BOOLEAN AS $$
DECLARE
  queue_table TEXT;
  success BOOLEAN := FALSE;
BEGIN
  -- Get the actual queue table name
  BEGIN
    SELECT pgmq.get_queue_table_name(queue_name) INTO STRICT queue_table;
  EXCEPTION WHEN OTHERS THEN
    queue_table := 'pgmq_' || queue_name;
  END;
  
  -- Try with text comparison for maximum flexibility
  BEGIN
    EXECUTE format('UPDATE %I SET vt = NULL WHERE msg_id::TEXT = $1 OR id::TEXT = $1', queue_table)
    USING message_id;
    GET DIAGNOSTICS success = ROW_COUNT;
    
    IF success > 0 THEN
      RETURN TRUE;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Failed to reset visibility timeout for message % in queue %', message_id, queue_name;
  END;
  
  RETURN FALSE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
