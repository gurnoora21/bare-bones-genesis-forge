
-- Create a registry table for active queues
CREATE TABLE IF NOT EXISTS public.queue_registry (
  id SERIAL PRIMARY KEY,
  queue_name TEXT NOT NULL UNIQUE,
  display_name TEXT,
  description TEXT,
  active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Add standard queues to the registry if they don't exist
INSERT INTO public.queue_registry (queue_name, display_name, description)
VALUES 
  ('artist_discovery', 'Artist Discovery', 'Discovers new artists from Spotify and other sources'),
  ('album_discovery', 'Album Discovery', 'Retrieves album data for discovered artists'),
  ('track_discovery', 'Track Discovery', 'Retrieves track data from discovered albums'),
  ('producer_identification', 'Producer Identification', 'Identifies producers and writers from track data'),
  ('social_enrichment', 'Social Enrichment', 'Enriches producer profiles with social media information')
ON CONFLICT (queue_name) DO NOTHING;

-- Create a function to register a new queue
CREATE OR REPLACE FUNCTION public.register_queue(
  p_queue_name TEXT,
  p_display_name TEXT DEFAULT NULL,
  p_description TEXT DEFAULT NULL,
  p_active BOOLEAN DEFAULT TRUE
) RETURNS BOOLEAN AS $$
BEGIN
  -- Create the queue in pgmq if it doesn't exist
  BEGIN
    PERFORM pgmq.create(p_queue_name);
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Queue % might already exist: %', p_queue_name, SQLERRM;
  END;
  
  -- Register the queue in our registry
  INSERT INTO public.queue_registry (queue_name, display_name, description, active)
  VALUES (p_queue_name, COALESCE(p_display_name, p_queue_name), p_description, p_active)
  ON CONFLICT (queue_name) 
  DO UPDATE SET 
    display_name = COALESCE(p_display_name, queue_registry.display_name),
    description = COALESCE(p_description, queue_registry.description),
    active = p_active,
    updated_at = now();
    
  RETURN TRUE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Set up cron jobs to process queues regularly
DO $$
BEGIN
  -- Remove existing cron jobs if any
  PERFORM cron.unschedule('cron-queue-processor-job');
  PERFORM cron.unschedule('artist-discovery-queue-job');
  PERFORM cron.unschedule('album-discovery-queue-job');
  PERFORM cron.unschedule('track-discovery-queue-job');
  PERFORM cron.unschedule('producer-identification-queue-job');
  PERFORM cron.unschedule('social-enrichment-queue-job');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Some jobs may not exist yet: %', SQLERRM;
END $$;

-- Schedule the general queue processor to run every 2 minutes
SELECT cron.schedule(
  'cron-queue-processor-job',
  '*/2 * * * *',  -- Every 2 minutes
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/cronQueueProcessor',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);

-- Schedule individual queue processors for important queues with staggered schedules
-- Artist Discovery - every 3 minutes
SELECT cron.schedule(
  'artist-discovery-queue-job',
  '*/3 * * * *', 
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/cronQueueProcessor',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"queues":["artist_discovery"],"batchSize":15}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);

-- Album Discovery - every 4 minutes at 1 minute offset
SELECT cron.schedule(
  'album-discovery-queue-job',
  '1-59/4 * * * *', 
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/cronQueueProcessor',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"queues":["album_discovery"],"batchSize":15}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);

-- Track Discovery - every 4 minutes at 2 minute offset
SELECT cron.schedule(
  'track-discovery-queue-job',
  '2-59/4 * * * *', 
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/cronQueueProcessor',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"queues":["track_discovery"],"batchSize":20}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);

-- Producer Identification - every 5 minutes
SELECT cron.schedule(
  'producer-identification-queue-job',
  '*/5 * * * *', 
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/cronQueueProcessor',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"queues":["producer_identification"],"batchSize":10}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);

-- Social Enrichment - every 7 minutes
SELECT cron.schedule(
  'social-enrichment-queue-job',
  '*/7 * * * *', 
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/cronQueueProcessor',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"queues":["social_enrichment"],"batchSize":10}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);
