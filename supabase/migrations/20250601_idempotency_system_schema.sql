
-- ENHANCED IDEMPOTENCY SYSTEM
-- Implement dual-system idempotency with database as source of truth

-- Create processing_status table if it doesn't exist
CREATE TABLE IF NOT EXISTS public.processing_status (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL, 
  state TEXT NOT NULL,
  last_processed_at TIMESTAMPTZ DEFAULT now(),
  last_error TEXT,
  metadata JSONB DEFAULT '{}'::jsonb,
  attempts INTEGER DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(entity_type, entity_id)
);

-- Create indices for performance
CREATE INDEX IF NOT EXISTS idx_processing_status_lookup 
ON public.processing_status(entity_type, entity_id);

CREATE INDEX IF NOT EXISTS idx_processing_status_state 
ON public.processing_status(state);

CREATE INDEX IF NOT EXISTS idx_processing_status_updated 
ON public.processing_status(updated_at);

-- Function to acquire a processing lock
CREATE OR REPLACE FUNCTION public.acquire_processing_lock(
  p_entity_type TEXT,
  p_entity_id TEXT,
  p_timeout_minutes INT DEFAULT 30,
  p_correlation_id TEXT DEFAULT NULL
)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_current_state TEXT;
  v_last_processed TIMESTAMPTZ;
  v_lock_acquired BOOLEAN := FALSE;
BEGIN
  -- Use advisory lock to prevent concurrent execution of this function
  -- for the same entity to avoid race conditions
  PERFORM pg_advisory_xact_lock(
    ('x' || substr(md5(p_entity_type || ':' || p_entity_id), 1, 16))::bit(64)::bigint
  );
  
  -- Check if entity exists and get current state
  SELECT state, last_processed_at 
  INTO v_current_state, v_last_processed
  FROM public.processing_status
  WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
  
  -- Handle based on current state
  IF v_current_state IS NULL THEN
    -- Entity doesn't exist yet, create it in IN_PROGRESS state
    INSERT INTO public.processing_status(
      entity_type, entity_id, state, last_processed_at, metadata
    ) VALUES (
      p_entity_type, p_entity_id, 'IN_PROGRESS', NOW(), 
      jsonb_build_object('correlation_id', p_correlation_id)
    );
    v_lock_acquired := TRUE;
  
  ELSIF v_current_state = 'PENDING' THEN
    -- Entity exists and is in PENDING state, transition to IN_PROGRESS
    UPDATE public.processing_status
    SET 
      state = 'IN_PROGRESS',
      last_processed_at = NOW(),
      metadata = jsonb_set(
        COALESCE(metadata, '{}'::jsonb),
        '{correlation_id}',
        to_jsonb(COALESCE(p_correlation_id, metadata->>'correlation_id'))
      ),
      attempts = attempts + 1,
      updated_at = NOW()
    WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
    v_lock_acquired := TRUE;
    
  ELSIF v_current_state = 'IN_PROGRESS' THEN
    -- Check if the lock has timed out
    IF v_last_processed < (NOW() - (p_timeout_minutes * INTERVAL '1 minute')) THEN
      -- Lock has timed out, reset to IN_PROGRESS
      UPDATE public.processing_status
      SET 
        state = 'IN_PROGRESS',
        last_processed_at = NOW(),
        metadata = jsonb_set(
          COALESCE(metadata, '{}'::jsonb),
          '{timeout_recovery}',
          jsonb_build_object(
            'previous_lock_time', v_last_processed,
            'timeout_minutes', p_timeout_minutes,
            'recovered_at', NOW(),
            'correlation_id', COALESCE(p_correlation_id, metadata->>'correlation_id')
          )
        ),
        attempts = attempts + 1,
        updated_at = NOW()
      WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
      v_lock_acquired := TRUE;
    ELSE
      -- Lock is still valid, cannot acquire
      v_lock_acquired := FALSE;
    END IF;
    
  ELSIF v_current_state = 'COMPLETED' THEN
    -- Already completed, no need to process again
    v_lock_acquired := FALSE;
    
  ELSIF v_current_state = 'FAILED' THEN
    -- Failed previously, allow retry
    UPDATE public.processing_status
    SET 
      state = 'IN_PROGRESS',
      last_processed_at = NOW(),
      metadata = jsonb_set(
        COALESCE(metadata, '{}'::jsonb),
        '{retry_info}',
        jsonb_build_object(
          'previous_attempt', metadata,
          'retried_at', NOW(),
          'correlation_id', COALESCE(p_correlation_id, metadata->>'correlation_id')
        )
      ),
      attempts = attempts + 1,
      updated_at = NOW()
    WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
    v_lock_acquired := TRUE;
  END IF;
  
  RETURN v_lock_acquired;
END;
$$;

-- Function to release a processing lock
CREATE OR REPLACE FUNCTION public.release_processing_lock(
  p_entity_type TEXT,
  p_entity_id TEXT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_current_state TEXT;
BEGIN
  -- Use advisory lock to prevent concurrent execution
  PERFORM pg_advisory_xact_lock(
    ('x' || substr(md5(p_entity_type || ':' || p_entity_id), 1, 16))::bit(64)::bigint
  );
  
  -- Check current state
  SELECT state INTO v_current_state
  FROM public.processing_status
  WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
  
  -- Only release if in IN_PROGRESS state
  IF v_current_state = 'IN_PROGRESS' THEN
    -- Set to PENDING state to allow reprocessing
    UPDATE public.processing_status
    SET 
      state = 'PENDING',
      last_processed_at = NOW(),
      updated_at = NOW()
    WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
    
    RETURN TRUE;
  END IF;
  
  RETURN FALSE;
END;
$$;

-- Function to identify inconsistencies
CREATE OR REPLACE FUNCTION public.find_inconsistent_states(
  p_entity_type TEXT DEFAULT NULL,
  p_older_than_minutes INT DEFAULT 60
)
RETURNS TABLE (
  entity_type TEXT,
  entity_id TEXT,
  db_state TEXT,
  redis_state TEXT,
  last_processed_at TIMESTAMPTZ,
  minutes_since_update NUMERIC
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  -- This function requires a Redis check in application code
  -- Here we just identify candidates in the database that might be stale
  
  RETURN QUERY
  SELECT 
    ps.entity_type,
    ps.entity_id,
    ps.state AS db_state,
    NULL AS redis_state, -- To be filled by application code
    ps.last_processed_at,
    EXTRACT(EPOCH FROM (NOW() - ps.last_processed_at))/60 AS minutes_since_update
  FROM 
    public.processing_status ps
  WHERE 
    (p_entity_type IS NULL OR ps.entity_type = p_entity_type)
    AND ps.last_processed_at < NOW() - (p_older_than_minutes * INTERVAL '1 minute')
    AND ps.state IN ('IN_PROGRESS', 'PENDING')
  ORDER BY 
    ps.last_processed_at ASC;
END;
$$;

-- Function to reset entity processing state
CREATE OR REPLACE FUNCTION public.reset_entity_processing_state(
  p_entity_type TEXT DEFAULT NULL,
  p_older_than_minutes INT DEFAULT 60,
  p_target_states TEXT[] DEFAULT ARRAY['COMPLETED', 'FAILED']
)
RETURNS SETOF public.processing_status
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  WITH updated_entities AS (
    UPDATE public.processing_status ps
    SET 
      state = 'PENDING',
      last_processed_at = NOW(),
      metadata = jsonb_set(
        COALESCE(ps.metadata, '{}'::jsonb), 
        '{reset_info}',
        jsonb_build_object(
          'previous_state', ps.state, 
          'reset_at', NOW(),
          'previous_metadata', ps.metadata
        )
      ),
      updated_at = NOW()
    WHERE 
      (p_entity_type IS NULL OR entity_type = p_entity_type)
      AND last_processed_at < NOW() - (p_older_than_minutes * INTERVAL '1 minute')
      AND state = ANY(p_target_states)
    RETURNING *
  )
  SELECT * FROM updated_entities;
END;
$$;

-- Function to get all queue tables
CREATE OR REPLACE FUNCTION public.get_all_queue_tables()
RETURNS TABLE (
  schema_name TEXT,
  table_name TEXT,
  queue_name TEXT,
  record_count BIGINT
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  WITH queue_tables AS (
    -- Get tables in pgmq schema if it exists
    SELECT 
      'pgmq' AS schema_name,
      table_name,
      CASE 
        WHEN table_name LIKE 'q\_%' THEN substring(table_name FROM 3)
        ELSE table_name
      END AS queue_name
    FROM information_schema.tables
    WHERE table_schema = 'pgmq'
    AND table_name LIKE 'q\_%'
    
    UNION ALL
    
    -- Get tables in public schema with pgmq_ prefix
    SELECT 
      'public' AS schema_name,
      table_name,
      CASE 
        WHEN table_name LIKE 'pgmq\_%' THEN substring(table_name FROM 6)
        ELSE table_name
      END AS queue_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_name LIKE 'pgmq\_%'
  )
  SELECT 
    qt.schema_name,
    qt.table_name,
    qt.queue_name,
    (SELECT COALESCE(COUNT(*), 0) 
     FROM information_schema.tables t
     WHERE t.table_schema = qt.schema_name AND t.table_name = qt.table_name)::BIGINT AS record_count
  FROM queue_tables qt
  ORDER BY qt.schema_name, qt.queue_name;
END;
$$;

-- Set up automatic cleanup for stuck processing states
DO $$
BEGIN
  PERFORM cron.schedule(
    'auto-cleanup-processing-states',
    '0 * * * *',  -- Run every hour
    $$
    -- Identify IN_PROGRESS entities stuck for more than 2 hours
    UPDATE public.processing_status
    SET 
      state = 'PENDING',
      metadata = jsonb_set(
        COALESCE(metadata, '{}'::jsonb),
        '{auto_cleanup}',
        jsonb_build_object(
          'previous_state', state,
          'cleaned_at', NOW()
        )
      )
    WHERE 
      state = 'IN_PROGRESS'
      AND last_processed_at < NOW() - INTERVAL '2 hours';
    $$
  );
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Cron job already exists or failed to create: %', SQLERRM;
END $$;
