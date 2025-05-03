
-- Create a cron job to run the queue monitor every 5 minutes
SELECT cron.schedule(
  'queue-monitor-job',
  '*/5 * * * *',  -- Every 5 minutes
  $$
  SELECT
    net.http_post(
        url:='https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/queueMonitor',
        headers:='{
          "Content-Type": "application/json",
          "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs"
        }'::jsonb,
        body:='{
          "threshold_minutes": 10,
          "auto_fix": true
        }'::jsonb
    ) as request_id;
  $$
);

-- Immediately run the emergency fix for message 4
DO $$
BEGIN
  -- Try direct deletion
  IF (SELECT enhanced_delete_message('artist_discovery', '4', TRUE)) THEN
    RAISE NOTICE 'Successfully deleted message 4';
  -- Fallback to emergency reset
  ELSIF (SELECT emergency_reset_message('artist_discovery', '4', TRUE)) THEN
    RAISE NOTICE 'Successfully reset message 4';
  ELSE
    RAISE NOTICE 'Could not delete or reset message 4';
  END IF;
END
$$;
