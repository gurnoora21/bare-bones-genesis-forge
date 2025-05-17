

-- Create a more reliable function that directly uses pgmq.send for artist discovery
-- This will ensure the message gets created in the queue without any intermediate steps
CREATE OR REPLACE FUNCTION public.start_artist_discovery(artist_name TEXT)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  msg_id BIGINT;
  idempotency_key TEXT;
BEGIN
  -- Create idempotency key
  idempotency_key := 'artist:name:' || lower(artist_name);
  
  -- Call pgmq.send directly to avoid any potential issues
  SELECT pgmq.send(
    'artist_discovery',
    jsonb_build_object(
      'artistName', artist_name,
      '_idempotencyKey', idempotency_key
    )
  ) INTO msg_id;
  
  -- If no message ID was returned, raise an error
  IF msg_id IS NULL THEN
    RAISE EXCEPTION 'Failed to enqueue artist discovery message';
  END IF;
  
  RETURN msg_id;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Error enqueueing artist discovery: %', SQLERRM;
  RAISE;
END;
$$;

-- Ensure the PGMQ queue exists
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pgmq.get_queues() WHERE queue_name = 'artist_discovery'
  ) THEN
    PERFORM pgmq.create('artist_discovery');
    RAISE NOTICE 'Created artist_discovery queue';
  END IF;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Error checking/creating queue: %', SQLERRM;
END;
$$;

