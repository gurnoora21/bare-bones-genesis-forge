
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

-- 3. Update enhanced_delete_message to use the correct schema and table pattern
CREATE OR REPLACE FUNCTION public.enhanced_delete_message(
  p_queue_name TEXT,
  p_message_id TEXT,
  p_is_numeric BOOLEAN DEFAULT FALSE
) RETURNS BOOLEAN AS $$
DECLARE
  queue_table TEXT;
  id_numeric BIGINT;
  id_uuid UUID;
  success BOOLEAN := FALSE;
BEGIN
  -- Get the actual queue table name with schema
  SELECT public.get_queue_table_name_safe(p_queue_name) INTO queue_table;
  
  -- Handle numeric IDs
  IF p_is_numeric THEN
    BEGIN
      id_numeric := p_message_id::BIGINT;
    EXCEPTION WHEN OTHERS THEN
      RAISE WARNING 'Could not convert % to BIGINT', p_message_id;
    END;
    
    -- Try numeric ID using msg_id
    IF id_numeric IS NOT NULL THEN
      BEGIN
        EXECUTE format('DELETE FROM %s WHERE msg_id = $1', queue_table)
        USING id_numeric;
        GET DIAGNOSTICS success = ROW_COUNT;
        
        IF success > 0 THEN
          RETURN TRUE;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error deleting by msg_id: %', SQLERRM;
      END;
      
      -- Try numeric ID using id
      BEGIN
        EXECUTE format('DELETE FROM %s WHERE id = $1', queue_table)
        USING id_numeric;
        GET DIAGNOSTICS success = ROW_COUNT;
        
        IF success > 0 THEN
          RETURN TRUE;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error deleting by id: %', SQLERRM;
      END;
    END IF;
  ELSE
    -- Handle UUID IDs
    BEGIN
      id_uuid := p_message_id::UUID;
    EXCEPTION WHEN OTHERS THEN
      RAISE WARNING 'Could not convert % to UUID', p_message_id;
    END;
    
    IF id_uuid IS NOT NULL THEN
      BEGIN
        EXECUTE format('DELETE FROM %s WHERE id = $1', queue_table)
        USING id_uuid;
        GET DIAGNOSTICS success = ROW_COUNT;
        
        IF success > 0 THEN
          RETURN TRUE;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error deleting by UUID: %', SQLERRM;
      END;
    END IF;
  END IF;
  
  -- Final attempt using text comparison
  BEGIN
    EXECUTE format('DELETE FROM %s WHERE msg_id::TEXT = $1 OR id::TEXT = $1', queue_table)
    USING p_message_id;
    GET DIAGNOSTICS success = ROW_COUNT;
    
    RETURN success > 0;
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Error deleting using text comparison: %', SQLERRM;
    RETURN FALSE;
  END;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 4. Update emergency_reset_message to use correct schema and table pattern
CREATE OR REPLACE FUNCTION public.emergency_reset_message(
  p_queue_name TEXT,
  p_message_id TEXT,
  p_is_numeric BOOLEAN DEFAULT FALSE
) RETURNS BOOLEAN AS $$
DECLARE
  queue_table TEXT;
  id_numeric BIGINT;
  id_uuid UUID;
  success BOOLEAN := FALSE;
BEGIN
  -- Get the actual queue table name with schema
  SELECT public.get_queue_table_name_safe(p_queue_name) INTO queue_table;
  
  -- Handle numeric IDs
  IF p_is_numeric THEN
    BEGIN
      id_numeric := p_message_id::BIGINT;
    EXCEPTION WHEN OTHERS THEN
      RAISE WARNING 'Could not convert % to BIGINT', p_message_id;
    END;
    
    IF id_numeric IS NOT NULL THEN
      BEGIN
        EXECUTE format('UPDATE %s SET vt = NULL WHERE msg_id = $1 OR id = $1', queue_table)
        USING id_numeric;
        GET DIAGNOSTICS success = ROW_COUNT;
        
        IF success > 0 THEN
          RETURN TRUE;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error resetting numeric ID: %', SQLERRM;
      END;
    END IF;
  ELSE
    -- Handle UUID IDs
    BEGIN
      id_uuid := p_message_id::UUID;
    EXCEPTION WHEN OTHERS THEN
      RAISE WARNING 'Could not convert % to UUID', p_message_id;
    END;
    
    IF id_uuid IS NOT NULL THEN
      BEGIN
        EXECUTE format('UPDATE %s SET vt = NULL WHERE id = $1', queue_table)
        USING id_uuid;
        GET DIAGNOSTICS success = ROW_COUNT;
        
        IF success > 0 THEN
          RETURN TRUE;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error resetting UUID: %', SQLERRM;
      END;
    END IF;
  END IF;
  
  -- Final attempt using text comparison
  BEGIN
    EXECUTE format('UPDATE %s SET vt = NULL WHERE msg_id::TEXT = $1 OR id::TEXT = $1', queue_table)
    USING p_message_id;
    GET DIAGNOSTICS success = ROW_COUNT;
    
    RETURN success > 0;
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Error resetting using text comparison: %', SQLERRM;
    RETURN FALSE;
  END;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 5. Update list_stuck_messages to use correct schema and table pattern
CREATE OR REPLACE FUNCTION public.list_stuck_messages(
  queue_name TEXT,
  min_minutes_locked INT DEFAULT 10
) RETURNS TABLE (
  id TEXT,
  msg_id TEXT,
  message JSONB,
  locked_since TIMESTAMP WITH TIME ZONE,
  read_count INT,
  minutes_locked NUMERIC
) AS $$
DECLARE
  queue_table TEXT;
BEGIN
  -- Get the actual queue table name with schema
  SELECT public.get_queue_table_name_safe(queue_name) INTO queue_table;
  
  -- Return stuck messages
  RETURN QUERY EXECUTE format('
    SELECT 
      id::TEXT, 
      msg_id::TEXT, 
      message::JSONB, 
      vt, 
      read_ct,
      EXTRACT(EPOCH FROM (NOW() - vt))/60 AS minutes_locked
    FROM %s
    WHERE 
      vt IS NOT NULL 
      AND vt < NOW() - INTERVAL ''%s minutes''
    ORDER BY vt ASC
  ', queue_table, min_minutes_locked);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 6. Create a diagnostic helper function to identify where the queue tables actually exist
CREATE OR REPLACE FUNCTION public.diagnose_queue_tables(
  queue_name TEXT DEFAULT NULL
) RETURNS TABLE (
  schema_name TEXT,
  table_name TEXT,
  full_name TEXT,
  record_count BIGINT
) AS $$
DECLARE
  search_pattern TEXT;
BEGIN
  IF queue_name IS NOT NULL THEN
    search_pattern := '%' || queue_name || '%';
  ELSE
    search_pattern := '%';
  END IF;
  
  RETURN QUERY
  SELECT 
    n.nspname AS schema_name,
    c.relname AS table_name,
    n.nspname || '.' || c.relname AS full_name,
    CASE 
      WHEN n.nspname = 'pgmq' THEN
        (SELECT COUNT(*) FROM pgmq.q_artist_discovery)::BIGINT
      ELSE
        0::BIGINT
    END AS record_count
  FROM 
    pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE 
    c.relkind = 'r' AND
    (c.relname LIKE 'q\_' || search_pattern OR 
     c.relname LIKE 'pgmq\_' || search_pattern)
  ORDER BY 
    n.nspname, c.relname;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 7. Add raw_sql_query function if it doesn't exist
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'raw_sql_query') THEN
    CREATE OR REPLACE FUNCTION public.raw_sql_query(
      sql_query TEXT,
      params TEXT[] DEFAULT '{}'::TEXT[]
    ) RETURNS JSONB AS $$
    DECLARE
      result JSONB;
    BEGIN
      EXECUTE sql_query INTO result USING params;
      RETURN result;
    EXCEPTION WHEN OTHERS THEN
      RETURN jsonb_build_object('error', SQLERRM);
    END;
    $$ LANGUAGE plpgsql SECURITY DEFINER;
  END IF;
END $$;

-- 8. Implement function to manage queue across schemas
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
  v_count INT := 0;
BEGIN
  -- Try operations against all potential queue tables
  FOR v_tables IN 
    SELECT * FROM public.diagnose_queue_tables(p_queue_name)
  LOOP
    v_count := v_count + 1;
    
    -- Determine operation to perform
    IF p_operation = 'delete' AND p_message_id IS NOT NULL THEN
      BEGIN
        EXECUTE format('DELETE FROM %s WHERE id::TEXT = $1 OR msg_id::TEXT = $1', v_tables.full_name)
        USING p_message_id;
        GET DIAGNOSTICS v_success = ROW_COUNT;
        
        IF v_success > 0 THEN
          v_result := jsonb_build_object(
            'success', TRUE, 
            'operation', 'delete', 
            'table', v_tables.full_name, 
            'message_id', p_message_id
          );
          RETURN v_result;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        v_result := jsonb_build_object(
          'error', SQLERRM,
          'table', v_tables.full_name,
          'operation', 'delete'
        );
      END;
    ELSIF p_operation = 'reset' AND p_message_id IS NOT NULL THEN
      BEGIN
        EXECUTE format('UPDATE %s SET vt = NULL WHERE id::TEXT = $1 OR msg_id::TEXT = $1', v_tables.full_name)
        USING p_message_id;
        GET DIAGNOSTICS v_success = ROW_COUNT;
        
        IF v_success > 0 THEN
          v_result := jsonb_build_object(
            'success', TRUE, 
            'operation', 'reset', 
            'table', v_tables.full_name, 
            'message_id', p_message_id
          );
          RETURN v_result;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        v_result := jsonb_build_object(
          'error', SQLERRM,
          'table', v_tables.full_name,
          'operation', 'reset'
        );
      END;
    ELSIF p_operation = 'purge' THEN
      BEGIN
        EXECUTE format('TRUNCATE TABLE %s', v_tables.full_name);
        v_result := jsonb_build_object(
          'success', TRUE, 
          'operation', 'purge', 
          'table', v_tables.full_name
        );
        RETURN v_result;
      EXCEPTION WHEN OTHERS THEN
        v_result := jsonb_build_object(
          'error', SQLERRM,
          'table', v_tables.full_name,
          'operation', 'purge'
        );
      END;
    END IF;
  END LOOP;
  
  -- If we got here, all attempts failed
  IF v_count = 0 THEN
    RETURN jsonb_build_object(
      'success', FALSE,
      'error', format('No queue tables found matching: %s', p_queue_name)
    );
  ELSE
    RETURN jsonb_build_object(
      'success', FALSE,
      'error', format('Operation %s failed on all %s matching tables', p_operation, v_count)
    );
  END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 9. Force delete the stuck message 5 using the improved schema-aware approach
DO $$
DECLARE
  result JSONB;
BEGIN
  SELECT public.cross_schema_queue_op('delete', 'artist_discovery', '5') INTO result;
  RAISE NOTICE 'Cross-schema deletion result: %', result;
  
  -- If direct delete failed, try reset
  IF (result->>'success')::BOOLEAN IS NOT TRUE THEN
    SELECT public.cross_schema_queue_op('reset', 'artist_discovery', '5') INTO result;
    RAISE NOTICE 'Cross-schema reset result: %', result;
  END IF;
END $$;
