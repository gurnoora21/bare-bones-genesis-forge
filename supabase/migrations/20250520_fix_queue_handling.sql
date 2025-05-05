
-- Fix queue schema and naming pattern handling in queue functions

-- 1. Create a proper implementation of get_queue_table_name if it doesn't exist
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'get_queue_table_name_safe') THEN
    CREATE OR REPLACE FUNCTION public.get_queue_table_name_safe(p_queue_name TEXT)
    RETURNS TEXT AS $$
    DECLARE
      table_name TEXT;
    BEGIN
      -- First try the pgmq built-in function
      BEGIN
        EXECUTE 'SELECT pgmq.get_queue_table_name($1)' INTO STRICT table_name USING p_queue_name;
        RETURN table_name;
      EXCEPTION WHEN OTHERS THEN
        -- Fall back to the known naming pattern for PGMQ tables
        RETURN 'pgmq.q_' || p_queue_name;
      END;
    END;
    $$ LANGUAGE plpgsql SECURITY DEFINER;
  END IF;
END $$;

-- 2. Update pg_delete_message function to use the correct schema and table pattern
CREATE OR REPLACE FUNCTION public.pg_delete_message(
  queue_name TEXT,
  message_id TEXT
) RETURNS BOOLEAN AS $$
DECLARE
  success BOOLEAN := FALSE;
  queue_table TEXT;
  is_numeric BOOLEAN;
BEGIN
  -- Get the actual queue table name with schema
  SELECT public.get_queue_table_name_safe(queue_name) INTO queue_table;
  
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
  
  -- Try direct table deletion
  BEGIN
    IF is_numeric THEN
      EXECUTE format('DELETE FROM %s WHERE msg_id = $1::BIGINT RETURNING TRUE', queue_table)
        USING message_id::BIGINT INTO success;
    ELSE
      EXECUTE format('DELETE FROM %s WHERE id = $1::UUID RETURNING TRUE', queue_table)
        USING message_id INTO success;
    END IF;
    
    IF success THEN
      RETURN TRUE;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Direct deletion failed: %', SQLERRM;
  END;
  
  -- Last resort - try text comparison
  BEGIN
    EXECUTE format('DELETE FROM %s WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE', queue_table)
      USING message_id INTO success;
    
    RETURN COALESCE(success, FALSE);
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Text comparison deletion failed: %', SQLERRM;
    RETURN FALSE;
  END;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 3. Create function to reset visibility timeout for stuck messages
CREATE OR REPLACE FUNCTION public.reset_stuck_message(
  queue_name TEXT,
  message_id TEXT
) RETURNS BOOLEAN AS $$
DECLARE
  queue_table TEXT;
  success BOOLEAN := FALSE;
BEGIN
  -- Get the actual queue table name with schema
  SELECT public.get_queue_table_name_safe(queue_name) INTO queue_table;
  
  -- Reset visibility timeout for the message
  BEGIN
    EXECUTE format('UPDATE %s SET vt = NULL WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE', queue_table)
      USING message_id INTO success;
    
    RETURN COALESCE(success, FALSE);
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Failed to reset visibility timeout: %', SQLERRM;
    RETURN FALSE;
  END;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 4. Create cross-schema queue operation function
CREATE OR REPLACE FUNCTION public.cross_schema_queue_op(
  p_queue_name TEXT,
  p_message_id TEXT,
  p_operation TEXT
) RETURNS BOOLEAN AS $$
DECLARE
  pgmq_exists BOOLEAN;
  public_exists BOOLEAN;
  success BOOLEAN := FALSE;
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
  
  -- Execute the requested operation
  IF p_operation = 'delete' THEN
    -- Try in pgmq schema
    IF pgmq_exists THEN
      BEGIN
        EXECUTE format('DELETE FROM pgmq.q_%I WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE', p_queue_name)
          USING p_message_id INTO success;
        
        IF success THEN
          RETURN TRUE;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        -- Continue to next attempt
      END;
    END IF;
    
    -- Try in public schema
    IF public_exists THEN
      BEGIN
        EXECUTE format('DELETE FROM public.pgmq_%I WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE', p_queue_name)
          USING p_message_id INTO success;
          
        RETURN COALESCE(success, FALSE);
      EXCEPTION WHEN OTHERS THEN
        RETURN FALSE;
      END;
    END IF;
  ELSIF p_operation = 'reset' THEN
    -- Try in pgmq schema
    IF pgmq_exists THEN
      BEGIN
        EXECUTE format('UPDATE pgmq.q_%I SET vt = NULL WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE', p_queue_name)
          USING p_message_id INTO success;
          
        IF success THEN
          RETURN TRUE;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        -- Continue to next attempt
      END;
    END IF;
    
    -- Try in public schema
    IF public_exists THEN
      BEGIN
        EXECUTE format('UPDATE public.pgmq_%I SET vt = NULL WHERE id::TEXT = $1 OR msg_id::TEXT = $1 RETURNING TRUE', p_queue_name)
          USING p_message_id INTO success;
          
        RETURN COALESCE(success, FALSE);
      EXCEPTION WHEN OTHERS THEN
        RETURN FALSE;
      END;
    END IF;
  END IF;
  
  RETURN FALSE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 5. Create a function to handle raw SQL queries safely
CREATE OR REPLACE FUNCTION public.raw_sql_query(
  sql_query TEXT
) RETURNS JSONB AS $$
DECLARE
  result JSONB;
BEGIN
  EXECUTE sql_query INTO result;
  RETURN result;
EXCEPTION WHEN OTHERS THEN
  RETURN jsonb_build_object('error', SQLERRM);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 6. Set up a cron job to automatically check for and reset stuck messages
DO $$
BEGIN
  PERFORM cron.unschedule('auto-reset-stuck-messages');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Cron job did not exist yet, will be created';
END $$;

SELECT cron.schedule(
  'auto-reset-stuck-messages',
  '*/5 * * * *',  -- Run every 5 minutes
  $$
  DO $$
  DECLARE
    queue_record RECORD;
    reset_count INT;
  BEGIN
    FOR queue_record IN 
      SELECT table_name FROM information_schema.tables 
      WHERE table_schema = 'pgmq' AND table_name LIKE 'q_%'
    LOOP
      BEGIN
        EXECUTE FORMAT('
          UPDATE pgmq.%I SET vt = NULL 
          WHERE vt IS NOT NULL AND vt < NOW() - INTERVAL ''15 minutes''', 
          queue_record.table_name
        );
        GET DIAGNOSTICS reset_count = ROW_COUNT;
        
        IF reset_count > 0 THEN
          RAISE NOTICE 'Reset % stuck messages in %', reset_count, queue_record.table_name;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Failed to reset messages in %: %', queue_record.table_name, SQLERRM;
      END;
    END LOOP;
  END $$;
  $$
);
