
-- First, ensure the required extensions are available
CREATE EXTENSION IF NOT EXISTS "pg_net";
CREATE EXTENSION IF NOT EXISTS "pg_cron";

-- Ensure the crons are properly set up by dropping and recreating them

-- Drop existing cron jobs if they exist
SELECT cron.unschedule('artist-discovery-worker') WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'artist-discovery-worker');
SELECT cron.unschedule('album-discovery-worker') WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'album-discovery-worker');
SELECT cron.unschedule('track-discovery-worker') WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'track-discovery-worker');
SELECT cron.unschedule('producer-identification-worker') WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'producer-identification-worker');
SELECT cron.unschedule('social-enrichment-worker') WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'social-enrichment-worker');

-- Artist discovery - every 2 minutes
SELECT cron.schedule(
  'artist-discovery-worker',
  '*/2 * * * *',
  $$
  SELECT
    net.http_post(
      url:= current_setting('app.settings.supabase_url') || '/functions/v1/artistDiscovery',
      headers:= json_build_object('Content-type', 'application/json', 'Authorization', 'Bearer ' || current_setting('app.settings.supabase_anon_key'))::jsonb,
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
      url:= current_setting('app.settings.supabase_url') || '/functions/v1/albumDiscovery',
      headers:= json_build_object('Content-type', 'application/json', 'Authorization', 'Bearer ' || current_setting('app.settings.supabase_anon_key'))::jsonb,
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
      url:= current_setting('app.settings.supabase_url') || '/functions/v1/trackDiscovery',
      headers:= json_build_object('Content-type', 'application/json', 'Authorization', 'Bearer ' || current_setting('app.settings.supabase_anon_key'))::jsonb,
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
      url:= current_setting('app.settings.supabase_url') || '/functions/v1/producerIdentification',
      headers:= json_build_object('Content-type', 'application/json', 'Authorization', 'Bearer ' || current_setting('app.settings.supabase_anon_key'))::jsonb,
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
      url:= current_setting('app.settings.supabase_url') || '/functions/v1/socialEnrichment',
      headers:= json_build_object('Content-type', 'application/json', 'Authorization', 'Bearer ' || current_setting('app.settings.supabase_anon_key'))::jsonb,
      body:= '{}'::jsonb,
      timeout_milliseconds:= 0
    );
  $$
);

-- First, insert the settings if they don't exist
INSERT INTO public.settings (key, value)
VALUES 
  ('supabase_url', (SELECT value FROM vault.secrets WHERE name = 'SUPABASE_URL')),
  ('supabase_anon_key', (SELECT value FROM vault.secrets WHERE name = 'SUPABASE_ANON_KEY'))
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- Create a function to get settings
CREATE OR REPLACE FUNCTION app.get_setting(setting_name text)
RETURNS text AS $$
BEGIN
  RETURN (SELECT value FROM public.settings WHERE key = setting_name);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Set search_path to make settings available
DO $$
BEGIN
  EXECUTE 'ALTER DATABASE postgres SET "app.settings.supabase_url" = ' || quote_literal((SELECT value FROM public.settings WHERE key = 'supabase_url'));
  EXECUTE 'ALTER DATABASE postgres SET "app.settings.supabase_anon_key" = ' || quote_literal((SELECT value FROM public.settings WHERE key = 'supabase_anon_key'));
END $$;

-- Create function to manually trigger workers
CREATE OR REPLACE FUNCTION public.manual_trigger_worker(worker_name text) RETURNS jsonb AS $$
DECLARE
  response jsonb;
  worker_url text;
BEGIN
  -- Construct the worker URL using settings
  worker_url := current_setting('app.settings.supabase_url') || '/functions/v1/' || worker_name;
  
  -- Trigger the worker manually
  SELECT 
    net.http_post(
      url:= worker_url,
      headers:= json_build_object('Content-type', 'application/json', 'Authorization', 'Bearer ' || current_setting('app.settings.supabase_anon_key'))::jsonb,
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

-- Create settings table if it doesn't exist
CREATE TABLE IF NOT EXISTS public.settings (
  key text PRIMARY KEY,
  value text NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now()
);
