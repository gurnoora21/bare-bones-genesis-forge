
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
  message_body TEXT  -- We'll keep this as TEXT but cast to JSONB internally
) RETURNS UUID AS $$
DECLARE
  msg_id UUID;
  jsonb_message JSONB;
BEGIN
  -- Convert the text message to JSONB
  BEGIN
    jsonb_message := message_body::JSONB;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Invalid JSON format: %', message_body;
  END;
  
  -- Now call pgmq.send with the properly typed parameters
  SELECT pgmq.send(queue_name, jsonb_message) INTO msg_id;
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
  SELECT pgmq.read(queue_name, batch_size, visibility_timeout) INTO result;
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
      SELECT message_body::JSONB
      FROM pgmq.get_queue_table_name(queue_name) 
      WHERE id = message_id
    ));
  END IF;
  RETURN success;
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

-- Function to manually start the discovery process for an artist
CREATE OR REPLACE FUNCTION start_artist_discovery(artist_name TEXT) RETURNS UUID AS $$
DECLARE
  msg_id UUID;
  payload JSONB;
BEGIN
  -- Create a proper JSONB payload
  payload := jsonb_build_object('artistName', artist_name);
  -- Send it to the queue
  SELECT pgmq.send('artist_discovery', payload) INTO msg_id;
  RETURN msg_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
