
-- This migration removes complex Redis operations and focuses on direct database operations
-- for album and artist discovery processes

-- Create a better start_album_discovery function that does not rely on Redis
CREATE OR REPLACE FUNCTION public.start_album_discovery(
  artist_id UUID,
  offset_val INTEGER DEFAULT 0
)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER AS $$
DECLARE
  msg_id BIGINT;
  artist_name TEXT;
  dedup_exists BOOLEAN := FALSE;
BEGIN
  -- Get the artist name for logging
  SELECT name INTO artist_name FROM artists WHERE id = artist_id;
  
  -- Log the attempt but do not block on Redis operations
  RAISE NOTICE 'Attempting to start album discovery for artist % (ID: %)', artist_name, artist_id;
  
  -- Create a direct message without relying on deduplication
  SELECT pg_enqueue(
    'album_discovery',
    jsonb_build_object(
      'artistId', artist_id,
      'offset', offset_val,
      '_timestamp', extract(epoch from now())::text
    )
  ) INTO msg_id;
  
  -- If we can't queue, raise an exception to make the issue visible
  IF msg_id IS NULL THEN
    RAISE EXCEPTION 'Failed to enqueue album discovery message for artist % (ID: %)', artist_name, artist_id;
  END IF;
  
  RETURN msg_id;
END;
$$;

-- Create a function to manually clear stuck messages in all queues
CREATE OR REPLACE FUNCTION public.fix_all_queues()
RETURNS TABLE(queue_name TEXT, messages_cleared INTEGER)
LANGUAGE plpgsql
SECURITY DEFINER AS $$
DECLARE
  queue_rec RECORD;
  cleared INTEGER;
BEGIN
  -- Get all queue tables
  FOR queue_rec IN SELECT queue_name FROM queue_registry WHERE active = TRUE
  LOOP
    BEGIN
      -- Reset stuck messages for this queue
      SELECT reset_stuck_messages(queue_rec.queue_name, 15) INTO cleared;
      
      IF cleared > 0 THEN
        queue_name := queue_rec.queue_name;
        messages_cleared := cleared;
        RETURN NEXT;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Error clearing queue %: %', queue_rec.queue_name, SQLERRM;
    END;
  END LOOP;
  
  RETURN;
END;
$$;

-- Create a simple function to get artist progress status
CREATE OR REPLACE FUNCTION public.check_artist_pipeline_progress(artist_id UUID)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER AS $$
DECLARE
  artist_rec RECORD;
  album_count INTEGER;
  track_count INTEGER;
  producer_count INTEGER;
  result JSONB;
BEGIN
  -- Get basic artist info
  SELECT name, spotify_id INTO artist_rec
  FROM artists 
  WHERE id = artist_id;
  
  -- Count related entities
  SELECT COUNT(*) INTO album_count
  FROM albums
  WHERE artist_id = check_artist_pipeline_progress.artist_id;
  
  SELECT COUNT(*) INTO track_count
  FROM tracks t
  JOIN albums a ON t.album_id = a.id
  WHERE a.artist_id = check_artist_pipeline_progress.artist_id;
  
  SELECT COUNT(DISTINCT p.id) INTO producer_count
  FROM producers p
  JOIN track_producers tp ON p.id = tp.producer_id
  JOIN tracks t ON tp.track_id = t.id
  JOIN albums a ON t.album_id = a.id
  WHERE a.artist_id = check_artist_pipeline_progress.artist_id;
  
  -- Build status result
  result := jsonb_build_object(
    'artist_id', artist_id,
    'artist_name', artist_rec.name,
    'spotify_id', artist_rec.spotify_id,
    'album_count', album_count,
    'track_count', track_count,
    'producer_count', producer_count,
    'timestamp', now(),
    'status', CASE
      WHEN album_count = 0 THEN 'no_albums'
      WHEN track_count = 0 THEN 'no_tracks'
      WHEN producer_count = 0 THEN 'no_producers'
      ELSE 'complete'
    END
  );
  
  RETURN result;
END;
$$;

-- Set up cron job to fix any stuck messages every 5 minutes
DO $$
BEGIN
  PERFORM cron.unschedule('fix-stuck-messages-job');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Job does not exist yet, will be created';
END $$;

SELECT cron.schedule(
  'fix-stuck-messages-job',
  '*/5 * * * *',  -- Run every 5 minutes
  $$
  SELECT * FROM public.fix_all_queues();
  $$
);
