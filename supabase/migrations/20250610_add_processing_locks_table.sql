
-- Create a dedicated table for lock heartbeats
CREATE TABLE IF NOT EXISTS public.processing_locks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  correlation_id TEXT,
  acquired_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  last_heartbeat TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  metadata JSONB DEFAULT '{}'::jsonb,
  ttl_seconds INTEGER NOT NULL DEFAULT 1800,
  UNIQUE (entity_type, entity_id)
);

-- Add index for efficient lookups
CREATE INDEX IF NOT EXISTS idx_processing_locks_entities 
  ON public.processing_locks (entity_type, entity_id);

-- Add index for stale lock lookups
CREATE INDEX IF NOT EXISTS idx_processing_locks_heartbeats 
  ON public.processing_locks (last_heartbeat);

-- Add column for dead-letter status to processing_status table
ALTER TABLE public.processing_status 
  ADD COLUMN IF NOT EXISTS dead_lettered BOOLEAN DEFAULT FALSE;

-- Create function to update heartbeats with conflict handling
CREATE OR REPLACE FUNCTION public.update_lock_heartbeat(
  p_entity_type TEXT,
  p_entity_id TEXT,
  p_worker_id TEXT,
  p_correlation_id TEXT DEFAULT NULL
) RETURNS BOOLEAN AS $$
DECLARE
  updated_rows INTEGER;
BEGIN
  -- Only update if the lock is owned by this worker
  UPDATE public.processing_locks
  SET 
    last_heartbeat = NOW(),
    metadata = jsonb_set(
      COALESCE(metadata, '{}'::jsonb),
      '{heartbeats}',
      COALESCE(metadata->'heartbeats', '0')::jsonb + '1'::jsonb
    )
  WHERE 
    entity_type = p_entity_type
    AND entity_id = p_entity_id
    AND worker_id = p_worker_id;
    
  GET DIAGNOSTICS updated_rows = ROW_COUNT;
  
  RETURN updated_rows > 0;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create function to release a lock
CREATE OR REPLACE FUNCTION public.release_lock(
  p_entity_type TEXT,
  p_entity_id TEXT,
  p_worker_id TEXT DEFAULT NULL
) RETURNS BOOLEAN AS $$
DECLARE
  deleted_rows INTEGER;
BEGIN
  -- Delete the lock, optionally checking the worker_id
  IF p_worker_id IS NULL THEN
    DELETE FROM public.processing_locks
    WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
  ELSE
    DELETE FROM public.processing_locks
    WHERE 
      entity_type = p_entity_type 
      AND entity_id = p_entity_id
      AND worker_id = p_worker_id;
  END IF;
  
  GET DIAGNOSTICS deleted_rows = ROW_COUNT;
  
  RETURN deleted_rows > 0;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create function to find and claim stale locks
CREATE OR REPLACE FUNCTION public.claim_stale_lock(
  p_entity_type TEXT,
  p_entity_id TEXT,
  p_new_worker_id TEXT,
  p_correlation_id TEXT DEFAULT NULL,
  p_stale_threshold_seconds INTEGER DEFAULT 60
) RETURNS JSONB AS $$
DECLARE
  stale_lock RECORD;
  claim_result JSONB;
BEGIN
  -- First check if lock exists and is stale
  SELECT * INTO stale_lock
  FROM public.processing_locks
  WHERE 
    entity_type = p_entity_type
    AND entity_id = p_entity_id
    AND last_heartbeat < NOW() - (p_stale_threshold_seconds * INTERVAL '1 second');
    
  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'claimed', FALSE,
      'reason', 'Lock not found or not stale'
    );
  END IF;
  
  -- Lock exists and is stale, attempt to claim it
  UPDATE public.processing_locks
  SET
    worker_id = p_new_worker_id,
    correlation_id = COALESCE(p_correlation_id, correlation_id),
    acquired_at = NOW(),
    last_heartbeat = NOW(),
    metadata = jsonb_set(
      COALESCE(metadata, '{}'::jsonb),
      '{stolen}',
      jsonb_build_object(
        'stolen_at', NOW(),
        'stolen_from', stale_lock.worker_id,
        'previous_acquired_at', stale_lock.acquired_at,
        'previous_heartbeat', stale_lock.last_heartbeat,
        'stale_seconds', EXTRACT(EPOCH FROM (NOW() - stale_lock.last_heartbeat))
      )
    )
  WHERE 
    entity_type = p_entity_type
    AND entity_id = p_entity_id
    AND worker_id = stale_lock.worker_id  -- Ensure we're updating the same record we checked
    AND last_heartbeat = stale_lock.last_heartbeat;  -- Extra precaution against race conditions
  
  -- Check if we successfully claimed it
  IF FOUND THEN
    RETURN jsonb_build_object(
      'claimed', TRUE,
      'previous_worker', stale_lock.worker_id,
      'stale_seconds', EXTRACT(EPOCH FROM (NOW() - stale_lock.last_heartbeat))
    );
  ELSE
    RETURN jsonb_build_object(
      'claimed', FALSE,
      'reason', 'Race condition - lock was updated by another process'
    );
  END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create function to find and clean up stale locks
CREATE OR REPLACE FUNCTION public.cleanup_stale_locks(
  p_stale_threshold_seconds INTEGER DEFAULT 300
) RETURNS TABLE (
  entity_type TEXT,
  entity_id TEXT,
  worker_id TEXT,
  stale_seconds NUMERIC
) AS $$
BEGIN
  RETURN QUERY
  WITH deleted_locks AS (
    DELETE FROM public.processing_locks
    WHERE last_heartbeat < NOW() - (p_stale_threshold_seconds * INTERVAL '1 second')
    RETURNING 
      entity_type,
      entity_id,
      worker_id,
      EXTRACT(EPOCH FROM (NOW() - last_heartbeat)) AS stale_seconds
  )
  SELECT 
    dl.entity_type,
    dl.entity_id,
    dl.worker_id,
    dl.stale_seconds
  FROM deleted_locks dl;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create dead-letter tables for each queue
DO $$
DECLARE
  queue_record RECORD;
BEGIN
  FOR queue_record IN 
    SELECT DISTINCT queue_name FROM pgmq.list_queues()
  LOOP
    -- Check if DLQ already exists
    IF NOT EXISTS (SELECT 1 FROM pgmq.list_queues() WHERE queue_name = queue_record.queue_name || '_dlq') THEN
      -- Create DLQ for this queue
      PERFORM pgmq.create(queue_record.queue_name || '_dlq');
      RAISE NOTICE 'Created dead-letter queue: %', queue_record.queue_name || '_dlq';
    ELSE
      RAISE NOTICE 'Dead-letter queue already exists: %', queue_record.queue_name || '_dlq';
    END IF;
  END LOOP;
END $$;

-- Add a cronjob to clean up stale locks
DO $$
BEGIN
  -- Remove any existing job first to avoid duplicates
  PERFORM cron.unschedule('stale-locks-cleanup-job');
  
  -- Create a new scheduled job
  PERFORM cron.schedule(
    'stale-locks-cleanup-job',
    '*/10 * * * *',  -- Run every 10 minutes
    $$
    SELECT * FROM public.cleanup_stale_locks(300);
    $$
  );
END $$;
