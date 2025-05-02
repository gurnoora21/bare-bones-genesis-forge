
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

-- Function to delete a message (mark as processed)
-- Fix: Create two overloads to handle parameters in any order
DROP FUNCTION IF EXISTS pg_delete_message;

CREATE OR REPLACE FUNCTION pg_delete_message(
  message_id UUID,
  queue_name TEXT
) RETURNS BOOLEAN AS $$
DECLARE
  success BOOLEAN;
BEGIN
  SELECT pgmq.delete(queue_name, message_id) INTO success;
  RETURN success;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION pg_delete_message(
  queue_name TEXT,
  message_id UUID
) RETURNS BOOLEAN AS $$
BEGIN
  -- Call the first version with parameters swapped
  RETURN pg_delete_message(message_id, queue_name);
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

