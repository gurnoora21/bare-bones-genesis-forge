
-- Optimize worker cron schedules for better throughput and scalability
DO $$
BEGIN
  -- Unschedule existing jobs
  PERFORM cron.unschedule('artist-discovery-queue-job');
  PERFORM cron.unschedule('album-discovery-queue-job');
  PERFORM cron.unschedule('track-discovery-queue-job');
  PERFORM cron.unschedule('producer-identification-queue-job');
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Some jobs may not exist yet: %', SQLERRM;
END $$;

-- Artist Discovery - run every minute instead of every 3 minutes for 3x throughput
SELECT cron.schedule(
  'artist-discovery-queue-job',
  '* * * * *', -- Every minute
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/artistDiscovery',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"batchSize":5}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);

-- Album Discovery - run every 2 minutes instead of every 4 minutes for 2x throughput
SELECT cron.schedule(
  'album-discovery-queue-job',
  '*/2 * * * *', -- Every 2 minutes
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/albumDiscovery',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"batchSize":5}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);

-- Track Discovery - deploy two staggered workers for 2x throughput
-- First worker runs at 0,10,20,30,40,50 minutes
SELECT cron.schedule(
  'track-discovery-queue-job-a',
  '0,10,20,30,40,50 * * * *',
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/trackDiscovery',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"batchSize":5,"instanceId":"worker-a"}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);

-- Second worker runs at 5,15,25,35,45,55 minutes
SELECT cron.schedule(
  'track-discovery-queue-job-b',
  '5,15,25,35,45,55 * * * *',
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/trackDiscovery',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"batchSize":5,"instanceId":"worker-b"}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);

-- Producer Identification - run every 5 minutes (increased from 10 minutes)
SELECT cron.schedule(
  'producer-identification-queue-job',
  '*/5 * * * *',
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/producerIdentification',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"batchSize":5}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);

-- New Pipeline Monitor cron job - runs every 15 minutes to check system health
SELECT cron.schedule(
  'pipeline-monitor-job',
  '*/15 * * * *',
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/pipelineMonitor',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"operation":"recommendations"}'::jsonb,
      timeout_milliseconds:= 10000
    ) as request_id;
  $$
);

-- Create table for storing scaling recommendations
CREATE TABLE IF NOT EXISTS monitoring.scaling_recommendations (
  id SERIAL PRIMARY KEY,
  timestamp TIMESTAMPTZ DEFAULT now(),
  recommendations JSONB,
  applied BOOLEAN DEFAULT FALSE
);

-- Create function to record scaling recommendations
CREATE OR REPLACE FUNCTION public.record_scaling_recommendations()
RETURNS VOID AS $$
DECLARE
  recommendation_data JSONB;
BEGIN
  -- Call the monitor function to get recommendations
  SELECT
    content::jsonb INTO recommendation_data
  FROM
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/pipelineMonitor?operation=recommendations',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{}'::jsonb
    );

  -- Store the recommendations
  INSERT INTO monitoring.scaling_recommendations (recommendations)
  VALUES (recommendation_data);
  
  -- Log the recommendations were recorded
  RAISE NOTICE 'Recorded scaling recommendations: %', recommendation_data;
END;
$$ LANGUAGE plpgsql;

-- Schedule daily scaling recommendations recording
SELECT cron.schedule(
  'record-scaling-recommendations',
  '0 0 * * *',  -- Midnight every day
  $$SELECT public.record_scaling_recommendations()$$
);
