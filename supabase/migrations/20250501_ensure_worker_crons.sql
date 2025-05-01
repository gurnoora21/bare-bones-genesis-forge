
-- First, ensure the required extensions are available
CREATE EXTENSION IF NOT EXISTS "pg_net";
CREATE EXTENSION IF NOT EXISTS "pg_cron";

-- Make sure the cron schema exists and has the tables it needs
DO $$
BEGIN
  -- Create the cfg table if it doesn't exist (pg_cron uses this)
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'cron' AND table_name = 'cfg') THEN
    CREATE TABLE IF NOT EXISTS cron.cfg (
      singleton text PRIMARY KEY,
      fx_schema text,
      ax_schema text,
      ex_schema text,
      database_name text
    );
  END IF;
END$$;

-- Ensure the settings table exists with proper values
CREATE TABLE IF NOT EXISTS public.settings (
  key text PRIMARY KEY,
  value text NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now()
);

-- Insert/update the Supabase URL and anon key settings
INSERT INTO public.settings (key, value)
VALUES 
  ('supabase_url', 'https://wshetxovyxtfqohhbvpg.supabase.co'),
  ('supabase_anon_key', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- Set search_path to make settings available
DO $$
BEGIN
  EXECUTE 'ALTER DATABASE postgres SET "app.settings.supabase_url" = ' || quote_literal((SELECT value FROM public.settings WHERE key = 'supabase_url'));
  EXECUTE 'ALTER DATABASE postgres SET "app.settings.supabase_anon_key" = ' || quote_literal((SELECT value FROM public.settings WHERE key = 'supabase_anon_key'));
END $$;

-- Drop existing cron jobs if they exist
DO $$
BEGIN
  PERFORM cron.unschedule('artist-discovery-worker');
  PERFORM cron.unschedule('album-discovery-worker');
  PERFORM cron.unschedule('track-discovery-worker');
  PERFORM cron.unschedule('producer-identification-worker');
  PERFORM cron.unschedule('social-enrichment-worker');
EXCEPTION 
  WHEN OTHERS THEN
    RAISE NOTICE 'Error unscheduling cron jobs: %', SQLERRM;
END $$;

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
