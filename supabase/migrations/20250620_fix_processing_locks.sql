
-- Fix any potential lock acquisition issues

-- First, clear any stale locks that might be preventing proper acquisition
DELETE FROM public.processing_locks
WHERE last_heartbeat < NOW() - INTERVAL '15 minutes';

-- Then, ensure the processing_status table also reflects the cleared locks
UPDATE public.processing_status
SET 
    state = 'PENDING',
    last_processed_at = NOW(),
    metadata = jsonb_set(
        COALESCE(metadata, '{}'::jsonb),
        '{cleanup}',
        jsonb_build_object(
            'previous_state', state,
            'cleaned_at', NOW(),
            'reason', 'Stale lock cleanup'
        )
    )
WHERE 
    state = 'IN_PROGRESS' 
    AND last_processed_at < NOW() - INTERVAL '30 minutes';

-- Create a function to help clear stuck locks when needed
CREATE OR REPLACE FUNCTION public.force_release_entity_lock(
    p_entity_type TEXT,
    p_entity_id TEXT
) RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
    lock_record RECORD;
    result JSONB;
BEGIN
    -- Get current lock info
    SELECT * INTO lock_record 
    FROM public.processing_locks
    WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
    
    -- Delete the lock
    DELETE FROM public.processing_locks
    WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
    
    -- Update processing status
    UPDATE public.processing_status
    SET 
        state = 'PENDING',
        last_processed_at = NOW(),
        metadata = jsonb_set(
            COALESCE(metadata, '{}'::jsonb),
            '{force_released}',
            jsonb_build_object(
                'previous_state', state,
                'released_at', NOW(),
                'previous_lock', lock_record
            )
        )
    WHERE 
        entity_type = p_entity_type
        AND entity_id = p_entity_id;
    
    -- Build result
    IF lock_record IS NULL THEN
        result := jsonb_build_object(
            'released', FALSE,
            'message', 'No lock found for this entity'
        );
    ELSE
        result := jsonb_build_object(
            'released', TRUE,
            'previous_lock', to_jsonb(lock_record)
        );
    END IF;
    
    RETURN result;
END;
$$;

-- Create a maintenance function to run periodically
CREATE OR REPLACE FUNCTION public.maintenance_clear_stale_entities()
RETURNS TABLE(entity_type TEXT, entity_id TEXT, state TEXT, action TEXT)
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    -- Clear stale locks
    DELETE FROM public.processing_locks
    WHERE last_heartbeat < NOW() - INTERVAL '1 hour'
    RETURNING entity_type, entity_id, 'lock_deleted' AS action;
    
    -- Reset stuck processing statuses
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
                    'reset_at', NOW()
                )
            )
        WHERE 
            state = 'IN_PROGRESS'
            AND last_processed_at < NOW() - INTERVAL '1 hour'
        RETURNING entity_type, entity_id, state, 'status_reset' AS action
    )
    SELECT * FROM updated_statuses;
END;
$$;

-- Set up a cronjob to run maintenance periodically
DO $$
BEGIN
    -- Remove any existing job first to avoid duplicates
    PERFORM cron.unschedule('maintenance-clear-stale-entities');
    
    -- Create a new scheduled job that runs every 30 minutes
    PERFORM cron.schedule(
        'maintenance-clear-stale-entities',
        '*/30 * * * *',  -- Run every 30 minutes
        $$
        SELECT * FROM public.maintenance_clear_stale_entities();
        $$
    );
END $$;
