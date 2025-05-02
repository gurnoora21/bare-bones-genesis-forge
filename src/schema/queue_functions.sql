
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
CREATE OR REPLACE FUNCTION pg_delete_message(
  queue_name TEXT,
  message_id UUID
) RETURNS BOOLEAN AS $$
DECLARE
  success BOOLEAN;
BEGIN
  SELECT pgmq.delete(queue_name, message_id) INTO success;
  RETURN success;
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

-- Schedule cron jobs to process each queue regularly

-- Artist discovery - every 2 minutes
SELECT cron.schedule(
  'artist-discovery-worker',
  '*/2 * * * *',
  $$
  SELECT
    net.http_post(
      url:= (SELECT value FROM vault.secrets WHERE name = 'SUPABASE_URL') || '/functions/v1/artistDiscovery',
      headers:= '{\"Content-type\":\"application/json\", \"Authorization\": \"Bearer ' || (SELECT value FROM vault.secrets WHERE name = 'SUPABASE_ANON_KEY') || '\"}'::jsonb,
      body:= '{}'::jsonb,
      timeout_milliseconds:= 0
    );
  $$
);

-- Album discovery - every 5 minutes
SELECT cron.schedule(
  'album-discovery-worker',
  '*/5 * * * *',
  $$
  SELECT
    net.http_post(
      url:= (SELECT value FROM vault.secrets WHERE name = 'SUPABASE_URL') || '/functions/v1/albumDiscovery',
      headers:= '{\"Content-type\":\"application/json\", \"Authorization\": \"Bearer ' || (SELECT value FROM vault.secrets WHERE name = 'SUPABASE_ANON_KEY') || '\"}'::jsonb,
      body:= '{}'::jsonb,
      timeout_milliseconds:= 0
    );
  $$
);

-- Track discovery - every 5 minutes
SELECT cron.schedule(
  'track-discovery-worker',
  '*/5 * * * *',
  $$
  SELECT
    net.http_post(
      url:= (SELECT value FROM vault.secrets WHERE name = 'SUPABASE_URL') || '/functions/v1/trackDiscovery',
      headers:= '{\"Content-type\":\"application/json\", \"Authorization\": \"Bearer ' || (SELECT value FROM vault.secrets WHERE name = 'SUPABASE_ANON_KEY') || '\"}'::jsonb,
      body:= '{}'::jsonb,
      timeout_milliseconds:= 0
    );
  $$
);

-- Producer identification - every 10 minutes
SELECT cron.schedule(
  'producer-identification-worker',
  '*/10 * * * *',
  $$
  SELECT
    net.http_post(
      url:= (SELECT value FROM vault.secrets WHERE name = 'SUPABASE_URL') || '/functions/v1/producerIdentification',
      headers:= '{\"Content-type\":\"application/json\", \"Authorization\": \"Bearer ' || (SELECT value FROM vault.secrets WHERE name = 'SUPABASE_ANON_KEY') || '\"}'::jsonb,
      body:= '{}'::jsonb,
      timeout_milliseconds:= 0
    );
  $$
);

-- Social enrichment - every hour
SELECT cron.schedule(
  'social-enrichment-worker',
  '0 * * * *',
  $$
  SELECT
    net.http_post(
      url:= (SELECT value FROM vault.secrets WHERE name = 'SUPABASE_URL') || '/functions/v1/socialEnrichment',
      headers:= '{\"Content-type\":\"application/json\", \"Authorization\": \"Bearer ' || (SELECT value FROM vault.secrets WHERE name = 'SUPABASE_ANON_KEY') || '\"}'::jsonb,
      body:= '{}'::jsonb,
      timeout_milliseconds:= 0
    );
  $$
);

-- System monitoring - every 30 minutes
SELECT cron.schedule(
  'system-monitoring',
  '*/30 * * * *',
  $$
  SELECT
    net.http_post(
      url:= (SELECT value FROM vault.secrets WHERE name = 'SUPABASE_URL') || '/functions/v1/monitorSystem',
      headers:= '{\"Content-type\":\"application/json\", \"Authorization\": \"Bearer ' || (SELECT value FROM vault.secrets WHERE name = 'SUPABASE_ANON_KEY') || '\"}'::jsonb,
      body:= '{}'::jsonb,
      timeout_milliseconds:= 0
    );
  $$
);

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

-- Function to manually trigger any worker
CREATE OR REPLACE FUNCTION manual_trigger_worker(worker_name TEXT) RETURNS JSONB AS $$
DECLARE
  response JSONB;
  worker_url TEXT;
  supabase_url TEXT;
  supabase_anon_key TEXT;
BEGIN
  -- Get settings values 
  SELECT value INTO supabase_url FROM vault.secrets WHERE name = 'SUPABASE_URL';
  SELECT value INTO supabase_anon_key FROM vault.secrets WHERE name = 'SUPABASE_ANON_KEY';
  
  -- Check if settings exist
  IF supabase_url IS NULL OR supabase_anon_key IS NULL THEN
    RAISE EXCEPTION 'Missing Supabase settings';
  END IF;
  
  -- Construct the worker URL using settings
  worker_url := supabase_url || '/functions/v1/' || worker_name;
  
  -- Trigger the worker manually
  SELECT 
    net.http_post(
      url:= worker_url,
      headers:= json_build_object(
        'Content-type', 'application/json', 
        'Authorization', 'Bearer ' || supabase_anon_key
      )::jsonb,
      body:= '{}'::jsonb,
      timeout_milliseconds:= 0
    ) INTO response;
    
  RETURN response;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
