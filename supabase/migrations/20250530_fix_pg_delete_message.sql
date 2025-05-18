-- Fix for pg_delete_message function to ensure it's properly installed

-- Create or replace the pg_delete_message function
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

-- Add a comment to the function
COMMENT ON FUNCTION pg_delete_message IS 'Reliably deletes a message by ID from a queue with fallback strategies';
