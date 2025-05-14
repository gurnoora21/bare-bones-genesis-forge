
-- Fix album discovery enqueuing process
-- Create a database function to enqueue album discovery messages
CREATE OR REPLACE FUNCTION public.start_album_discovery(artist_id UUID, offset_val INT DEFAULT 0)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  msg_id BIGINT;
  idempotency_key TEXT;
BEGIN
  -- Create idempotency key
  idempotency_key := 'artist:' || artist_id || ':albums:offset:' || offset_val;
  
  -- Call pgmq.send directly with a properly formatted JSONB object
  -- Note that pg_enqueue expects JSONB, not a string
  SELECT pg_enqueue(
    'album_discovery',
    jsonb_build_object(
      'artistId', artist_id,
      'offset', offset_val,
      '_idempotencyKey', idempotency_key
    )
  ) INTO msg_id;
  
  -- If no message ID was returned, raise an error
  IF msg_id IS NULL THEN
    RAISE EXCEPTION 'Failed to enqueue album discovery message';
  END IF;
  
  -- Log the operation to help with debugging
  INSERT INTO public.queue_metrics (
    queue_name, 
    operation, 
    success_count, 
    details
  ) VALUES (
    'album_discovery',
    'enqueue',
    1,
    jsonb_build_object(
      'artist_id', artist_id,
      'offset', offset_val,
      'message_id', msg_id,
      'idempotency_key', idempotency_key
    )
  );
  
  RETURN msg_id;
EXCEPTION WHEN OTHERS THEN
  -- Log the error
  INSERT INTO public.worker_issues (
    worker_name, 
    issue_type, 
    message, 
    details, 
    resolved
  ) VALUES (
    'start_album_discovery',
    'database_error',
    'Failed to enqueue album discovery: ' || SQLERRM,
    jsonb_build_object(
      'artist_id', artist_id,
      'offset', offset_val,
      'error', SQLERRM
    ),
    false
  );
  
  RAISE;
END;
$$;

-- Create a view to monitor album discovery processing
CREATE OR REPLACE VIEW public.album_discovery_status AS
WITH enqueued AS (
  SELECT 
    count(*) as total_enqueued,
    jsonb_agg(distinct details->>'artist_id') as artist_ids
  FROM public.queue_metrics
  WHERE queue_name = 'album_discovery' AND operation = 'enqueue'
),
processed AS (
  SELECT 
    count(*) as total_processed
  FROM public.albums
)
SELECT 
  e.total_enqueued,
  p.total_processed,
  (e.total_enqueued - p.total_processed) as pending,
  e.artist_ids
FROM enqueued e, processed p;

-- Fix potential issues with existing queue tables
DO $$
BEGIN
  -- Check if the album_discovery queue exists
  IF NOT EXISTS (
    SELECT FROM pgmq.get_queues() 
    WHERE queue_name = 'album_discovery'
  ) THEN
    PERFORM pgmq.create('album_discovery');
    RAISE NOTICE 'Created album_discovery queue';
  END IF;
  
  -- Check if there are any stuck messages
  BEGIN
    CREATE TEMPORARY TABLE tmp_stuck_messages AS
    SELECT 
      msg_id, 
      message,
      read_ct,
      vt
    FROM pgmq.q_album_discovery
    WHERE vt IS NOT NULL AND vt < NOW() - INTERVAL '5 minutes';
    
    -- Log stuck messages
    WITH msgs AS (
      SELECT count(*) as count FROM tmp_stuck_messages
    )
    INSERT INTO public.worker_issues (
      worker_name, 
      issue_type, 
      message, 
      details, 
      resolved
    )
    SELECT
      'album_discovery',
      'stuck_messages',
      'Found ' || count || ' stuck messages in album_discovery queue',
      jsonb_build_object(
        'count', count,
        'timestamp', NOW()
      ),
      false
    FROM msgs
    WHERE count > 0;
    
    -- Reset stuck messages
    UPDATE pgmq.q_album_discovery
    SET vt = NULL
    WHERE vt IS NOT NULL AND vt < NOW() - INTERVAL '5 minutes';
    
    DROP TABLE IF EXISTS tmp_stuck_messages;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error checking for stuck messages: %', SQLERRM;
  END;
END;
$$;
