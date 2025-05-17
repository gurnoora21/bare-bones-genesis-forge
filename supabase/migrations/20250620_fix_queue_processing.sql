
-- First, remove duplicate cron jobs that might be processing the same queues
DO $$
DECLARE
    queue_names TEXT[] := ARRAY['artist_discovery', 'album_discovery', 'track_discovery', 'producer_identification', 'social_enrichment'];
    queue TEXT;
BEGIN
    -- Unschedule any duplicate jobs that process the same queue
    FOR queue IN SELECT unnest(queue_names) LOOP
        -- Find and remove duplicate cron jobs, keeping only the newest one
        PERFORM cron.unschedule(jobid)
        FROM cron.job
        WHERE command LIKE '%' || queue || '%'
        AND jobname NOT IN (
            SELECT jobname 
            FROM cron.job 
            WHERE command LIKE '%' || queue || '%'
            ORDER BY jobid DESC 
            LIMIT 1
        );
    END LOOP;
END $$;

-- Ensure we have properly spaced out cron jobs for each worker
-- This ensures they don't run exactly at the same time, preventing conflicts

-- Artist discovery runs every 2 minutes
SELECT cron.schedule(
  'artist-discovery-worker',
  '*/2 * * * *',
  $$ SELECT supabase.functions.invoke('artistDiscovery') $$
);

-- Album discovery runs every 2 minutes, but offset by 30 seconds
SELECT cron.schedule(
  'album-discovery-worker',
  '1-59/2 * * * *', -- Every odd minute: 1, 3, 5, etc.
  $$ SELECT supabase.functions.invoke('albumDiscovery') $$
);

-- Track discovery runs every 3 minutes 
SELECT cron.schedule(
  'track-discovery-worker',
  '*/3 * * * *',
  $$ SELECT supabase.functions.invoke('trackDiscovery') $$
);

-- Producer identification runs every 5 minutes
SELECT cron.schedule(
  'producer-identification-worker',
  '*/5 * * * *',
  $$ SELECT supabase.functions.invoke('producerIdentification') $$
);

-- Social enrichment runs every 10 minutes
SELECT cron.schedule(
  'social-enrichment-worker',
  '*/10 * * * *',
  $$ SELECT supabase.functions.invoke('socialEnrichment') $$
);

-- Queue monitor runs every 5 minutes
SELECT cron.schedule(
  'queue-monitor-job',
  '*/5 * * * *',
  $$ SELECT supabase.functions.invoke('queueMonitor') $$
);

-- Create function to monitor which jobs are running for each queue
CREATE OR REPLACE FUNCTION public.check_worker_crons()
RETURNS TABLE(
  jobid bigint, 
  jobname text,
  schedule text, 
  command text, 
  last_run timestamp with time zone,
  next_run timestamp with time zone, 
  active boolean
)
LANGUAGE plpgsql
SECURITY DEFINER AS $$
BEGIN
  RETURN QUERY
  SELECT 
    j.jobid,
    j.jobname,
    j.schedule,
    j.command,
    j.last_run,
    j.next_run,
    j.active
  FROM 
    cron.job j
  WHERE 
    j.jobname LIKE '%-worker' OR
    j.jobname LIKE '%-job'
  ORDER BY j.next_run;
END;
$$;
