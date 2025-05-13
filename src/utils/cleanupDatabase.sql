
-- First, drop the view that depends on the queue tables
DROP VIEW IF EXISTS public.queue_monitoring_view CASCADE;

-- First, disable triggers to avoid cascading issues
SET session_replication_role = 'replica';

-- Clear all application data tables in correct order
TRUNCATE TABLE public.track_producers CASCADE;
TRUNCATE TABLE public.tracks CASCADE;
TRUNCATE TABLE public.albums CASCADE;
TRUNCATE TABLE public.normalized_tracks CASCADE;
TRUNCATE TABLE public.producers CASCADE;
TRUNCATE TABLE public.artists CASCADE;
TRUNCATE TABLE public.worker_issues CASCADE;
TRUNCATE TABLE public.queue_metrics CASCADE;
TRUNCATE TABLE public.processing_status CASCADE;
TRUNCATE TABLE public.processing_locks CASCADE;
TRUNCATE TABLE public.monitoring_events CASCADE;

-- Re-enable triggers
SET session_replication_role = 'origin';

-- Drop all existing queues - the CASCADE parameter will drop dependent objects
DO $$
BEGIN
  -- Try dropping each queue with exception handling
  BEGIN
    PERFORM pgmq.drop_queue('album_discovery', true); -- true = cascade
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error dropping album_discovery queue: %', SQLERRM;
  END;
  
  BEGIN
    PERFORM pgmq.drop_queue('track_discovery', true);
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error dropping track_discovery queue: %', SQLERRM;
  END;
  
  BEGIN
    PERFORM pgmq.drop_queue('producer_identification', true);
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error dropping producer_identification queue: %', SQLERRM;
  END;
  
  BEGIN
    PERFORM pgmq.drop_queue('social_enrichment', true);
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error dropping social_enrichment queue: %', SQLERRM;
  END;
  
  BEGIN
    PERFORM pgmq.drop_queue('artist_discovery', true);
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error dropping artist_discovery queue: %', SQLERRM;
  END;
  
  BEGIN
    PERFORM pgmq.drop_queue('producer_identification_dlq', true);
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error dropping producer_identification_dlq queue: %', SQLERRM;
  END;
END $$;

-- Recreate all queues
SELECT pgmq.create('album_discovery');
SELECT pgmq.create('track_discovery');
SELECT pgmq.create('producer_identification');
SELECT pgmq.create('social_enrichment');
SELECT pgmq.create('artist_discovery');
SELECT pgmq.create('producer_identification_dlq');

-- Re-create the queue monitoring view
CREATE OR REPLACE VIEW public.queue_monitoring_view AS
WITH queue_tables AS (
  SELECT 
    CASE 
      WHEN table_name LIKE 'q\_%' THEN SUBSTRING(table_name FROM 3)
      ELSE SUBSTRING(table_name FROM 6) 
    END AS queue_name,
    CASE 
      WHEN table_schema = 'pgmq' THEN format('%I.%I', table_schema, table_name)
      ELSE format('%I.%I', table_schema, table_name)
    END AS full_table_name
  FROM information_schema.tables
  WHERE (table_schema = 'pgmq' AND table_name LIKE 'q\_%')
     OR (table_schema = 'public' AND table_name LIKE 'pgmq\_%')
)
SELECT
  qt.queue_name,
  COUNT(*) AS total_messages,
  COUNT(*) FILTER (WHERE vt IS NOT NULL AND vt < NOW()) AS stuck_messages,
  NULL::INTEGER AS messages_fixed, -- Will be updated by the monitor
  NOW() AS last_check_time
FROM queue_tables qt
LEFT JOIN LATERAL (
  SELECT vt 
  FROM pgmq.q_album_discovery
  WHERE qt.queue_name = 'album_discovery'
  UNION ALL
  SELECT vt 
  FROM pgmq.q_artist_discovery
  WHERE qt.queue_name = 'artist_discovery'
  UNION ALL
  SELECT vt 
  FROM pgmq.q_track_discovery
  WHERE qt.queue_name = 'track_discovery'
  UNION ALL
  SELECT vt 
  FROM pgmq.q_producer_identification
  WHERE qt.queue_name = 'producer_identification'
  UNION ALL
  SELECT vt 
  FROM pgmq.q_social_enrichment
  WHERE qt.queue_name = 'social_enrichment'
) q ON true
GROUP BY qt.queue_name;

-- Register the queues in the registry
INSERT INTO public.queue_registry (queue_name, display_name, description, active)
VALUES 
  ('artist_discovery', 'Artist Discovery', 'Discovers new artists from Spotify and other sources', true),
  ('album_discovery', 'Album Discovery', 'Retrieves album data for discovered artists', true),
  ('track_discovery', 'Track Discovery', 'Retrieves track data from discovered albums', true),
  ('producer_identification', 'Producer Identification', 'Identifies producers and writers from track data', true),
  ('social_enrichment', 'Social Enrichment', 'Enriches producer profiles with social media information', true),
  ('producer_identification_dlq', 'Producer Identification DLQ', 'Dead-letter queue for producer identification', true)
ON CONFLICT (queue_name) DO UPDATE SET active = true;
