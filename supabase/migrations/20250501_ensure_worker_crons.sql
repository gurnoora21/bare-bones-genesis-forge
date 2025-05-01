
-- First, ensure the required extensions are available
CREATE EXTENSION IF NOT EXISTS "pg_net";
CREATE EXTENSION IF NOT EXISTS "pg_cron";

-- Ensure the crons are properly set up by dropping and recreating them

-- Drop existing cron jobs if they exist
SELECT cron.unschedule('artist-discovery-worker');
SELECT cron.unschedule('album-discovery-worker');
SELECT cron.unschedule('track-discovery-worker');
SELECT cron.unschedule('producer-identification-worker');
SELECT cron.unschedule('social-enrichment-worker');

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

-- For testing, let's also create a function to manually trigger workers
CREATE OR REPLACE FUNCTION public.manual_trigger_worker(worker_name text) RETURNS jsonb AS $$
DECLARE
  response jsonb;
  worker_url text;
BEGIN
  -- Construct the worker URL
  worker_url := (SELECT value FROM vault.secrets WHERE name = 'SUPABASE_URL') || '/functions/v1/' || worker_name;
  
  -- Trigger the worker manually
  SELECT 
    net.http_post(
      url:= worker_url,
      headers:= '{\"Content-type\":\"application/json\", \"Authorization\": \"Bearer ' || (SELECT value FROM vault.secrets WHERE name = 'SUPABASE_ANON_KEY') || '\"}'::jsonb,
      body:= '{}'::jsonb,
      timeout_milliseconds:= 0
    ) INTO response;
    
  RETURN response;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Add a function to check cron job status
CREATE OR REPLACE FUNCTION public.check_worker_crons() RETURNS TABLE (
  jobid bigint,
  schedule text,
  command text,
  nodename text,
  nodeport integer,
  database text,
  username text,
  active boolean,
  next_run timestamp with time zone
) AS $$
BEGIN
  RETURN QUERY
  SELECT * FROM cron.job
  WHERE jobname IN (
    'artist-discovery-worker',
    'album-discovery-worker',
    'track-discovery-worker',
    'producer-identification-worker',
    'social-enrichment-worker'
  );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
