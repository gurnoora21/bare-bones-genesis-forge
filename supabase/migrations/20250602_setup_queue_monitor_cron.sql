
-- Set up automatic queue monitoring cronjob

-- First, clean up any existing jobs to avoid duplicates
DO $$
BEGIN
  PERFORM cron.unschedule('queue-monitor-job');
  PERFORM cron.unschedule('auto-queue-monitor-job');
  PERFORM cron.unschedule('queue-auto-fix-job');
EXCEPTION WHEN OTHERS THEN
  -- Jobs didn't exist yet
END $$;

-- Create the cron job to run every 5 minutes
SELECT cron.schedule(
  'queue-monitor-job',
  '*/5 * * * *',  -- Run every 5 minutes
  $$
  SELECT
    net.http_post(
      url:= current_setting('app.settings.supabase_url') || '/functions/v1/queueMonitor',
      headers:= json_build_object(
        'Content-type', 'application/json', 
        'Authorization', 'Bearer ' || current_setting('app.settings.supabase_anon_key')
      )::jsonb,
      body:= '{"threshold_minutes":10,"auto_fix":true}'::jsonb,
      timeout_milliseconds:= 30000
    ) as request_id;
  $$
);

-- Store necessary settings for the HTTP call if they don't exist
INSERT INTO settings (key, value)
VALUES 
  ('supabase_url', 'https://wshetxovyxtfqohhbvpg.supabase.co')
ON CONFLICT (key) DO NOTHING;

INSERT INTO settings (key, value)
VALUES 
  ('supabase_anon_key', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs')
ON CONFLICT (key) DO NOTHING;

-- Set the settings for the cron jobs
ALTER DATABASE postgres SET "app.settings.supabase_url" = 'https://wshetxovyxtfqohhbvpg.supabase.co';
ALTER DATABASE postgres SET "app.settings.supabase_anon_key" = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs';

-- Set up a cron job to periodically reset stuck processing states as well
SELECT cron.schedule(
  'processing-states-monitor-job',
  '10 * * * *',  -- Run at 10 minutes past every hour
  $$
  -- Reset processing states that have been stuck for more than 2 hours
  UPDATE public.processing_status
  SET 
    state = 'PENDING',
    metadata = jsonb_set(
      COALESCE(metadata, '{}'::jsonb),
      '{auto_cleanup}',
      jsonb_build_object(
        'previous_state', state,
        'cleaned_at', now()
      )
    ),
    updated_at = now()
  WHERE 
    state = 'IN_PROGRESS'
    AND last_processed_at < now() - interval '2 hours';
  $$
);
