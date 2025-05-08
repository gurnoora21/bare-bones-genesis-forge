
-- Add a scheduled job for the queue monitor to run every 5 minutes
SELECT cron.schedule(
  'queue-monitor',
  '*/5 * * * *', -- Run every 5 minutes
  $$
  SELECT net.http_post(
    url:='https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/queueMonitor',
    headers:='{
      "Content-Type": "application/json",
      "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs"
    }'::jsonb,
    body:='{}'::jsonb
  );
  $$
);
