
-- Create a table to track processing status of entities for idempotency
CREATE TABLE IF NOT EXISTS public.processing_status (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL, 
  process_type TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  details JSONB,
  attempts INTEGER DEFAULT 1,
  UNIQUE(entity_type, entity_id, process_type)
);

-- Create index for efficient lookups
CREATE INDEX IF NOT EXISTS idx_processing_status_lookup 
ON public.processing_status(entity_type, entity_id, process_type);

-- Enhance our queue table access function to be more resilient
CREATE OR REPLACE FUNCTION public.get_queue_table_name_safe(p_queue_name TEXT)
RETURNS TEXT AS $$
DECLARE
  table_name TEXT;
  pgmq_table_exists BOOLEAN;
  public_table_exists BOOLEAN;
BEGIN
  -- Check if table exists in pgmq schema
  SELECT EXISTS (
    SELECT FROM pg_tables 
    WHERE schemaname = 'pgmq' 
    AND tablename = 'q_' || p_queue_name
  ) INTO pgmq_table_exists;

  -- Check if table exists in public schema
  SELECT EXISTS (
    SELECT FROM pg_tables 
    WHERE schemaname = 'public' 
    AND tablename = 'pgmq_' || p_queue_name
  ) INTO public_table_exists;
  
  -- First try the pgmq built-in function if table exists in pgmq schema
  IF pgmq_table_exists THEN
    BEGIN
      EXECUTE 'SELECT pgmq.get_queue_table_name($1)' INTO STRICT table_name USING p_queue_name;
      RETURN table_name;
    EXCEPTION WHEN OTHERS THEN
      -- Fall back to the known naming pattern for PGMQ tables
      RETURN 'pgmq.q_' || p_queue_name;
    END;
  ELSIF public_table_exists THEN
    -- If only in public schema, use that
    RETURN 'public.pgmq_' || p_queue_name;
  ELSE
    -- Default to pgmq schema if neither exists yet (for new queues)
    RETURN 'pgmq.q_' || p_queue_name;
  END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create an improved pg_delete_message function
CREATE OR REPLACE FUNCTION public.pg_delete_message(
  queue_name TEXT,
  message_id TEXT
) RETURNS BOOLEAN AS $$
DECLARE
  success BOOLEAN := FALSE;
  pgmq_table_exists BOOLEAN;
  public_table_exists BOOLEAN;
  is_numeric BOOLEAN;
BEGIN
  -- Check if queue tables exist
  SELECT EXISTS (
    SELECT FROM pg_tables 
    WHERE schemaname = 'pgmq' 
    AND tablename = 'q_' || queue_name
  ) INTO pgmq_table_exists;

  SELECT EXISTS (
    SELECT FROM pg_tables 
    WHERE schemaname = 'public' 
    AND tablename = 'pgmq_' || queue_name
  ) INTO public_table_exists;
  
  -- If queue doesn't exist yet, return success to avoid errors
  IF NOT pgmq_table_exists AND NOT public_table_exists THEN
    RAISE NOTICE 'Queue % does not exist yet, returning success', queue_name;
    RETURN TRUE;
  END IF;
  
  -- Determine if ID is numeric
  BEGIN
    PERFORM message_id::NUMERIC;
    is_numeric := TRUE;
  EXCEPTION WHEN OTHERS THEN
    is_numeric := FALSE;
  END;
  
  -- First try using pgmq.delete
  BEGIN
    IF is_numeric THEN
      EXECUTE 'SELECT pgmq.delete($1, $2::BIGINT)' INTO success USING queue_name, message_id::BIGINT;
    ELSE
      EXECUTE 'SELECT pgmq.delete($1, $2::UUID)' INTO success USING queue_name, message_id::UUID;
    END IF;
    
    IF success THEN
      RETURN TRUE;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'pgmq.delete failed: %', SQLERRM;
  END;
  
  -- Try direct table deletion in pgmq schema
  IF pgmq_table_exists THEN
    BEGIN
      IF is_numeric THEN
        EXECUTE 'DELETE FROM pgmq.q_' || queue_name || ' WHERE msg_id = $1::BIGINT RETURNING TRUE' 
          USING message_id::BIGINT INTO success;
      ELSE
        BEGIN
          EXECUTE 'DELETE FROM pgmq.q_' || queue_name || ' WHERE id = $1::UUID RETURNING TRUE' 
            USING message_id INTO success;
        EXCEPTION WHEN OTHERS THEN
          -- If UUID cast fails, try text comparison
          EXECUTE 'DELETE FROM pgmq.q_' || queue_name || ' WHERE id::TEXT = $1 RETURNING TRUE' 
            USING message_id INTO success;
        END;
      END IF;
      
      IF success THEN
        RETURN TRUE;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Direct deletion in pgmq schema failed: %', SQLERRM;
    END;
  END IF;
  
  -- Try direct table deletion in public schema
  IF public_table_exists THEN
    BEGIN
      IF is_numeric THEN
        EXECUTE 'DELETE FROM public.pgmq_' || queue_name || ' WHERE msg_id = $1::BIGINT RETURNING TRUE' 
          USING message_id::BIGINT INTO success;
      ELSE
        BEGIN
          EXECUTE 'DELETE FROM public.pgmq_' || queue_name || ' WHERE id = $1::UUID RETURNING TRUE' 
            USING message_id INTO success;
        EXCEPTION WHEN OTHERS THEN
          -- If UUID cast fails, try text comparison
          EXECUTE 'DELETE FROM public.pgmq_' || queue_name || ' WHERE id::TEXT = $1 RETURNING TRUE' 
            USING message_id INTO success;
        END;
      END IF;
      
      IF success THEN
        RETURN TRUE;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Direct deletion in public schema failed: %', SQLERRM;
    END;
  END IF;
  
  -- Last resort - try text comparison in both schemas
  IF pgmq_table_exists THEN
    BEGIN
      EXECUTE 'DELETE FROM pgmq.q_' || queue_name || ' WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE' 
        USING message_id INTO success;
      
      IF success THEN
        RETURN TRUE;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Text comparison deletion in pgmq schema failed: %', SQLERRM;
    END;
  END IF;
  
  IF public_table_exists THEN
    BEGIN
      EXECUTE 'DELETE FROM public.pgmq_' || queue_name || ' WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE' 
        USING message_id INTO success;
      
      RETURN COALESCE(success, FALSE);
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Text comparison deletion in public schema failed: %', SQLERRM;
      RETURN FALSE;
    END;
  END IF;
  
  RETURN FALSE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create function to check entity processing status
CREATE OR REPLACE FUNCTION public.check_entity_processed(
  p_entity_type TEXT,
  p_entity_id TEXT,
  p_process_type TEXT
) RETURNS BOOLEAN AS $$
DECLARE
  v_exists BOOLEAN;
BEGIN
  SELECT EXISTS(
    SELECT 1 FROM public.processing_status
    WHERE entity_type = p_entity_type
    AND entity_id = p_entity_id
    AND process_type = p_process_type
    AND status = 'completed'
  ) INTO v_exists;
  
  RETURN COALESCE(v_exists, FALSE);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to update entity processing status
CREATE OR REPLACE FUNCTION public.update_processing_status(
  p_entity_type TEXT,
  p_entity_id TEXT,
  p_process_type TEXT,
  p_status TEXT,
  p_details JSONB DEFAULT NULL
) RETURNS BOOLEAN AS $$
DECLARE
  v_exists BOOLEAN;
BEGIN
  SELECT EXISTS(
    SELECT 1 FROM public.processing_status
    WHERE entity_type = p_entity_type
    AND entity_id = p_entity_id
    AND process_type = p_process_type
  ) INTO v_exists;
  
  IF v_exists THEN
    UPDATE public.processing_status
    SET status = p_status,
        updated_at = now(),
        details = CASE WHEN p_details IS NOT NULL THEN p_details ELSE details END,
        attempts = attempts + 1
    WHERE entity_type = p_entity_type
    AND entity_id = p_entity_id
    AND process_type = p_process_type;
  ELSE
    INSERT INTO public.processing_status
      (entity_type, entity_id, process_type, status, details)
    VALUES
      (p_entity_type, p_entity_id, p_process_type, p_status, p_details);
  END IF;
  
  RETURN TRUE;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Error updating processing status: %', SQLERRM;
  RETURN FALSE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Improved reset_stuck_message function with better schema handling
CREATE OR REPLACE FUNCTION public.reset_stuck_message(
  queue_name TEXT,
  message_id TEXT
) RETURNS BOOLEAN AS $$
DECLARE
  pgmq_table_exists BOOLEAN;
  public_table_exists BOOLEAN;
  success BOOLEAN := FALSE;
BEGIN
  -- Check if queue tables exist
  SELECT EXISTS (
    SELECT FROM pg_tables 
    WHERE schemaname = 'pgmq' 
    AND tablename = 'q_' || queue_name
  ) INTO pgmq_table_exists;

  SELECT EXISTS (
    SELECT FROM pg_tables 
    WHERE schemaname = 'public' 
    AND tablename = 'pgmq_' || queue_name
  ) INTO public_table_exists;
  
  -- If queue doesn't exist, return success
  IF NOT pgmq_table_exists AND NOT public_table_exists THEN
    RETURN TRUE;
  END IF;
  
  -- Try pgmq schema
  IF pgmq_table_exists THEN
    BEGIN
      EXECUTE 'UPDATE pgmq.q_' || queue_name || ' SET vt = NULL WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE' 
        USING message_id INTO success;
      
      IF success THEN
        RETURN TRUE;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Failed to reset visibility timeout in pgmq schema: %', SQLERRM;
    END;
  END IF;
  
  -- Try public schema
  IF public_table_exists THEN
    BEGIN
      EXECUTE 'UPDATE public.pgmq_' || queue_name || ' SET vt = NULL WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE' 
        USING message_id INTO success;
      
      RETURN COALESCE(success, FALSE);
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Failed to reset visibility timeout in public schema: %', SQLERRM;
      RETURN FALSE;
    END;
  END IF;
  
  RETURN FALSE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Enhanced cross-schema queue op function
CREATE OR REPLACE FUNCTION public.cross_schema_queue_op(
  p_queue_name TEXT,
  p_message_id TEXT,
  p_operation TEXT
) RETURNS BOOLEAN AS $$
DECLARE
  pgmq_exists BOOLEAN;
  public_exists BOOLEAN;
  success BOOLEAN := FALSE;
  v_attempts INTEGER := 0;
  max_attempts CONSTANT INTEGER := 3;
BEGIN
  -- Check if tables exist in different schemas
  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables 
    WHERE table_schema = 'pgmq' AND table_name = 'q_' || p_queue_name
  ) INTO pgmq_exists;
  
  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name = 'pgmq_' || p_queue_name
  ) INTO public_exists;
  
  -- If neither exists, return true (idempotent)
  IF NOT pgmq_exists AND NOT public_exists THEN
    RETURN TRUE;
  END IF;
  
  -- Execute the requested operation with retries
  WHILE v_attempts < max_attempts AND NOT success LOOP
    v_attempts := v_attempts + 1;
    
    -- Execute operation based on type
    IF p_operation = 'delete' THEN
      -- Try in pgmq schema
      IF pgmq_exists THEN
        BEGIN
          EXECUTE format(
            'DELETE FROM pgmq.q_%I WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE', 
            p_queue_name
          ) USING p_message_id INTO success;
          
          IF success THEN
            RETURN TRUE;
          END IF;
        EXCEPTION WHEN OTHERS THEN
          -- Log the error and continue to next attempt
          RAISE NOTICE 'Error in attempt % deleting from pgmq schema: %', v_attempts, SQLERRM;
        END;
      END IF;
      
      -- Try in public schema
      IF public_exists AND NOT success THEN
        BEGIN
          EXECUTE format(
            'DELETE FROM public.pgmq_%I WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE', 
            p_queue_name
          ) USING p_message_id INTO success;
            
          IF success THEN
            RETURN TRUE;
          END IF;
        EXCEPTION WHEN OTHERS THEN
          RAISE NOTICE 'Error in attempt % deleting from public schema: %', v_attempts, SQLERRM;
        END;
      END IF;
      
    ELSIF p_operation = 'reset' THEN
      -- Try in pgmq schema
      IF pgmq_exists THEN
        BEGIN
          EXECUTE format(
            'UPDATE pgmq.q_%I SET vt = NULL WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE', 
            p_queue_name
          ) USING p_message_id INTO success;
            
          IF success THEN
            RETURN TRUE;
          END IF;
        EXCEPTION WHEN OTHERS THEN
          RAISE NOTICE 'Error in attempt % resetting in pgmq schema: %', v_attempts, SQLERRM;
        END;
      END IF;
      
      -- Try in public schema
      IF public_exists AND NOT success THEN
        BEGIN
          EXECUTE format(
            'UPDATE public.pgmq_%I SET vt = NULL WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE', 
            p_queue_name
          ) USING p_message_id INTO success;
            
          IF success THEN
            RETURN TRUE;
          END IF;
        EXCEPTION WHEN OTHERS THEN
          RAISE NOTICE 'Error in attempt % resetting in public schema: %', v_attempts, SQLERRM;
        END;
      END IF;
    END IF;
    
    -- If we weren't successful, add a small delay before the next attempt (exponential backoff)
    IF NOT success AND v_attempts < max_attempts THEN
      PERFORM pg_sleep(0.1 * power(2, v_attempts - 1));
    END IF;
  END LOOP;
  
  RETURN success;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Setup auto-cleanup for dead letter handling
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'cleanup_stuck_messages' AND prokind = 'f') THEN
    CREATE FUNCTION public.cleanup_stuck_messages() RETURNS INTEGER AS
    $$
    DECLARE
      queue_record RECORD;
      reset_count INTEGER := 0;
      total_reset INTEGER := 0;
      min_stuck_minutes INTEGER := 15; -- Messages stuck for >15 minutes
      queues_processed INTEGER := 0;
    BEGIN
      -- Loop through all pgmq queues in pgmq schema
      FOR queue_record IN 
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'pgmq' AND table_name LIKE 'q_%'
      LOOP
        BEGIN
          -- Reset visibility timeout for stuck messages
          EXECUTE FORMAT('
            UPDATE pgmq.%I SET vt = NULL 
            WHERE vt IS NOT NULL AND vt < NOW() - INTERVAL ''%s minutes''
            RETURNING COUNT(*)', 
            queue_record.table_name, min_stuck_minutes
          ) INTO reset_count;
          
          queues_processed := queues_processed + 1;
          total_reset := total_reset + COALESCE(reset_count, 0);
          
          IF reset_count > 0 THEN
            RAISE NOTICE 'Reset % stuck messages in %', reset_count, queue_record.table_name;
          END IF;
        EXCEPTION WHEN OTHERS THEN
          RAISE WARNING 'Failed to reset messages in %: %', queue_record.table_name, SQLERRM;
        END;
      END LOOP;
      
      -- Loop through all pgmq queues in public schema
      FOR queue_record IN 
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name LIKE 'pgmq_%'
      LOOP
        BEGIN
          -- Reset visibility timeout for stuck messages
          EXECUTE FORMAT('
            UPDATE public.%I SET vt = NULL 
            WHERE vt IS NOT NULL AND vt < NOW() - INTERVAL ''%s minutes''
            RETURNING COUNT(*)', 
            queue_record.table_name, min_stuck_minutes
          ) INTO reset_count;
          
          queues_processed := queues_processed + 1;
          total_reset := total_reset + COALESCE(reset_count, 0);
          
          IF reset_count > 0 THEN
            RAISE NOTICE 'Reset % stuck messages in public.%', reset_count, queue_record.table_name;
          END IF;
        EXCEPTION WHEN OTHERS THEN
          RAISE WARNING 'Failed to reset messages in public.%: %', queue_record.table_name, SQLERRM;
        END;
      END LOOP;
      
      RETURN total_reset;
    END;
    $$ LANGUAGE PLPGSQL SECURITY DEFINER;
  END IF;
END
$$;

-- Update the cron job to be more resilient and efficient
DO $$
BEGIN
  PERFORM cron.unschedule('auto-reset-stuck-messages');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Cron job did not exist yet, will be created';
END $$;

SELECT cron.schedule(
  'auto-reset-stuck-messages',
  '*/5 * * * *',  -- Run every 5 minutes
  $$SELECT public.cleanup_stuck_messages();$$
);
