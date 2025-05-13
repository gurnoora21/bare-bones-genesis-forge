
-- Fix for stale_locks_cleanup job that's failing
-- Drop existing maintenance function and replace with improved version
DROP FUNCTION IF EXISTS public.maintenance_clear_stale_entities;

CREATE OR REPLACE FUNCTION public.maintenance_clear_stale_entities(
  p_stale_threshold_minutes INT DEFAULT 60
)
RETURNS TABLE(entity_type TEXT, entity_id TEXT, state TEXT, action TEXT)
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  cleanup_error TEXT;
BEGIN
  -- Use exception handling to prevent failures
  BEGIN
    -- Step 1: Clear stale locks
    RETURN QUERY
    WITH deleted_locks AS (
      DELETE FROM public.processing_locks
      WHERE last_heartbeat < NOW() - (p_stale_threshold_minutes * INTERVAL '1 minute')
      RETURNING entity_type, entity_id, 'lock_deleted' AS action, 'IN_PROGRESS' AS state
    )
    SELECT * FROM deleted_locks;
  EXCEPTION WHEN OTHERS THEN
    -- Log the error but continue with the next step
    RAISE WARNING 'Error clearing stale locks: %', SQLERRM;
    cleanup_error := SQLERRM;
  END;

  -- Step 2: Reset stuck processing statuses (with separate exception handling)
  BEGIN
    RETURN QUERY
    WITH updated_statuses AS (
      UPDATE public.processing_status
      SET 
        state = 'PENDING',
        last_processed_at = NOW(),
        metadata = jsonb_set(
          COALESCE(metadata, '{}'::jsonb),
          '{auto_maintenance}',
          jsonb_build_object(
            'previous_state', state,
            'reset_at', NOW(),
            'previous_error', cleanup_error
          )
        )
      WHERE 
        state = 'IN_PROGRESS'
        AND last_processed_at < NOW() - (p_stale_threshold_minutes * INTERVAL '1 minute')
      RETURNING entity_type, entity_id, state, 'status_reset' AS action
    )
    SELECT * FROM updated_statuses;
  EXCEPTION WHEN OTHERS THEN
    -- Log this error too
    RAISE WARNING 'Error resetting processing statuses: %', SQLERRM;
    
    -- Return at least one row with the error information
    entity_type := 'error';
    entity_id := 'maintenance';
    state := 'ERROR';
    action := 'Error: ' || SQLERRM;
    RETURN NEXT;
  END;
END;
$$;

-- Update the cron schedule for maintenance to be more resilient
DO $$
BEGIN
  PERFORM cron.unschedule('maintenance-clear-stale-entities');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'No existing cron job to unschedule';
END $$;

SELECT cron.schedule(
  'maintenance-clear-stale-entities',
  '*/30 * * * *',  -- Run every 30 minutes
  $$
  BEGIN;
    -- Use transaction to prevent partial execution
    SELECT * FROM public.maintenance_clear_stale_entities(60);
    -- Log successful execution
    INSERT INTO monitoring_events (event_type, details) 
    VALUES ('maintenance_job', jsonb_build_object(
      'job', 'stale_locks_cleanup',
      'status', 'success',
      'executed_at', now()
    ));
  EXCEPTION WHEN OTHERS THEN
    -- Log failed execution
    INSERT INTO monitoring_events (event_type, details) 
    VALUES ('maintenance_job', jsonb_build_object(
      'job', 'stale_locks_cleanup',
      'status', 'error',
      'error', SQLERRM,
      'executed_at', now()
    ));
  END;
  $$
);

-- Create a dedicated helper function for the cronCleanupStaleEntities edge function to use
CREATE OR REPLACE FUNCTION public.cleanup_stale_entities(
  p_stale_threshold_minutes INT DEFAULT 60
)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  result JSONB;
  deleted_locks INT := 0;
  reset_statuses INT := 0;
  cleanup_error TEXT;
BEGIN
  -- Clean up stale locks
  BEGIN
    WITH deleted AS (
      DELETE FROM public.processing_locks
      WHERE last_heartbeat < NOW() - (p_stale_threshold_minutes * INTERVAL '1 minute')
      RETURNING entity_type, entity_id
    )
    SELECT COUNT(*) INTO deleted_locks FROM deleted;
  EXCEPTION WHEN OTHERS THEN
    cleanup_error := SQLERRM;
  END;

  -- Reset stuck processing statuses
  BEGIN
    WITH updated AS (
      UPDATE public.processing_status
      SET 
        state = 'PENDING',
        last_processed_at = NOW(),
        metadata = jsonb_set(
          COALESCE(metadata, '{}'::jsonb),
          '{auto_maintenance}',
          jsonb_build_object(
            'previous_state', state,
            'reset_at', NOW(),
            'previous_error', cleanup_error
          )
        )
      WHERE 
        state = 'IN_PROGRESS'
        AND last_processed_at < NOW() - (p_stale_threshold_minutes * INTERVAL '1 minute')
      RETURNING entity_type, entity_id
    )
    SELECT COUNT(*) INTO reset_statuses FROM updated;
  EXCEPTION WHEN OTHERS THEN
    IF cleanup_error IS NULL THEN
      cleanup_error := SQLERRM;
    ELSE
      cleanup_error := cleanup_error || ' | ' || SQLERRM;
    END IF;
  END;

  -- Build result
  result := jsonb_build_object(
    'deleted_locks', deleted_locks,
    'reset_statuses', reset_statuses,
    'timestamp', NOW()
  );
  
  IF cleanup_error IS NOT NULL THEN
    result := result || jsonb_build_object('error', cleanup_error);
  END IF;
  
  RETURN result;
END;
$$;
