
-- Set up a cron job to run the queue monitor every 5 minutes
-- First make sure the cron job doesn't already exist
DO $$
BEGIN
  PERFORM cron.unschedule('auto-queue-monitor-job');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Job did not exist yet, will be created';
END $$;

-- Create the cron job
SELECT cron.schedule(
  'auto-queue-monitor-job',
  '*/5 * * * *',  -- Run every 5 minutes
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
