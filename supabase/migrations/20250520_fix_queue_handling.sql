
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
  
  -- Try with text comparison for maximum flexibility
  BEGIN
    EXECUTE format('UPDATE %s SET vt = NULL WHERE msg_id::TEXT = $1 OR id::TEXT = $1', queue_table)
    USING message_id;
    GET DIAGNOSTICS success = ROW_COUNT;
    
    IF success > 0 THEN
      RETURN TRUE;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Failed to reset visibility timeout for message % in queue %', message_id, queue_name;
  END;
  
  RETURN FALSE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 4. Create cross-schema queue operation function
CREATE OR REPLACE FUNCTION public.cross_schema_queue_op(
  p_operation TEXT,
  p_queue_name TEXT,
  p_message_id TEXT DEFAULT NULL,
  p_params JSONB DEFAULT '{}'::JSONB
) RETURNS JSONB AS $$
DECLARE
  v_result JSONB := '{}'::JSONB;
  v_success BOOLEAN := FALSE;
  v_tables RECORD;
BEGIN
  -- Try operations across all possible queue table locations
  FOR v_tables IN 
    SELECT s.nspname AS schema_name, c.relname AS table_name
    FROM pg_class c 
    JOIN pg_namespace s ON c.relnamespace = s.oid
    WHERE (c.relname LIKE 'q\_' || p_queue_name OR c.relname = 'q_' || p_queue_name 
           OR c.relname LIKE 'pgmq\_%' || p_queue_name)
    AND c.relkind = 'r'
  LOOP
    -- Determine operation to perform
    IF p_operation = 'delete' THEN
      BEGIN
        EXECUTE format('DELETE FROM %I.%I WHERE id::TEXT = $1 OR msg_id::TEXT = $1', 
                      v_tables.schema_name, v_tables.table_name)
        USING p_message_id;
        GET DIAGNOSTICS v_success = ROW_COUNT;
        
        IF v_success > 0 THEN
          RETURN jsonb_build_object(
            'success', TRUE, 
            'operation', 'delete', 
            'table', v_tables.schema_name || '.' || v_tables.table_name, 
            'message_id', p_message_id
          );
        END IF;
      EXCEPTION WHEN OTHERS THEN
        -- Just continue to next table
        NULL;
      END;
    ELSIF p_operation = 'reset' THEN
      BEGIN
        EXECUTE format('UPDATE %I.%I SET vt = NULL WHERE id::TEXT = $1 OR msg_id::TEXT = $1', 
                      v_tables.schema_name, v_tables.table_name)
        USING p_message_id;
        GET DIAGNOSTICS v_success = ROW_COUNT;
        
        IF v_success > 0 THEN
          RETURN jsonb_build_object(
            'success', TRUE, 
            'operation', 'reset', 
            'table', v_tables.schema_name || '.' || v_tables.table_name, 
            'message_id', p_message_id
          );
        END IF;
      EXCEPTION WHEN OTHERS THEN
        -- Just continue to next table
        NULL;
      END;
    END IF;
  END LOOP;
  
  -- If we got here, all attempts failed
  RETURN jsonb_build_object(
    'success', FALSE,
    'error', 'Failed to ' || p_operation || ' message ' || p_message_id || ' in any queue table'
  );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
