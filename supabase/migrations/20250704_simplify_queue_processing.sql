-- Simplify queue processing by directly calling worker functions
-- This removes the dependency on the cronQueueProcessor

-- First, unschedule the existing cron jobs
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

-- Schedule direct calls to worker functions with staggered schedules
-- Artist Discovery - every 3 minutes
SELECT cron.schedule(
  'artist-discovery-direct-job',
  '*/3 * * * *', 
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/artistDiscovery',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"batchSize":15}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);

-- Album Discovery - every 4 minutes at 1 minute offset
SELECT cron.schedule(
  'album-discovery-direct-job',
  '1-59/4 * * * *', 
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/albumDiscovery',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"batchSize":15}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);

-- Track Discovery - every 4 minutes at 2 minute offset
SELECT cron.schedule(
  'track-discovery-direct-job',
  '2-59/4 * * * *', 
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/trackDiscovery',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"batchSize":20}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);

-- Producer Identification - every 5 minutes
SELECT cron.schedule(
  'producer-identification-direct-job',
  '*/5 * * * *', 
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/producerIdentification',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"batchSize":10}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);

-- Social Enrichment - every 7 minutes
SELECT cron.schedule(
  'social-enrichment-direct-job',
  '*/7 * * * *', 
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/socialEnrichment',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"batchSize":10}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);
