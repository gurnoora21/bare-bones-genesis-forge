
-- Fix PGMQ table naming pattern in queue functions
DO $$
DECLARE
  table_exists BOOLEAN;
BEGIN
  -- Check if we're using the correct table name pattern
  SELECT EXISTS (
    SELECT 1 
    FROM information_schema.tables 
    WHERE table_schema = 'pgmq' AND table_name = 'q_artist_discovery'
  ) INTO table_exists;
  
  IF NOT table_exists THEN
    RAISE NOTICE 'PGMQ table pattern not found: pgmq.q_artist_discovery';
  ELSE
    RAISE NOTICE 'Confirmed correct PGMQ table pattern: pgmq.q_artist_discovery exists';
  END IF;
END $$;

-- Update get_queue_table_name_safe function to use the correct naming pattern
CREATE OR REPLACE FUNCTION public.get_queue_table_name_safe(p_queue_name TEXT)
RETURNS TEXT AS $$
DECLARE
  table_exists BOOLEAN;
  correct_table_name TEXT;
BEGIN
  -- The correct PGMQ table naming pattern is pgmq.q_queue_name
  correct_table_name := 'pgmq.q_' || p_queue_name;
  
  -- Verify the table exists
  EXECUTE format('
    SELECT EXISTS (
      SELECT 1 FROM information_schema.tables 
      WHERE table_schema = ''pgmq'' AND table_name = ''q_%s''
    )', p_queue_name) INTO table_exists;
    
  IF table_exists THEN
    RETURN correct_table_name;
  ELSE
    -- Fall back to checking alternate patterns
    EXECUTE format('
      SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = ''public'' AND table_name = ''pgmq_%s''
      )', p_queue_name) INTO table_exists;
      
    IF table_exists THEN
      RETURN 'public.pgmq_' || p_queue_name;
    ELSE
      -- Return the correct pattern anyway as PGMQ might create it
      RETURN correct_table_name;
    END IF;
  END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Update cross_schema_queue_op function to handle message deletion correctly
CREATE OR REPLACE FUNCTION public.cross_schema_queue_op(
  p_operation TEXT,
  p_queue_name TEXT,
  p_message_id TEXT DEFAULT NULL,
  p_params JSONB DEFAULT '{}'::JSONB
) RETURNS JSONB AS $$
DECLARE
  v_result JSONB := '{}'::JSONB;
  v_success BOOLEAN := FALSE;
  v_table_name TEXT;
  v_table_exists BOOLEAN;
BEGIN
  -- First try with the correct PGMQ table pattern
  v_table_name := 'pgmq.q_' || p_queue_name;
  
  -- Check if table exists
  EXECUTE format('
    SELECT EXISTS (
      SELECT 1 FROM information_schema.tables 
      WHERE table_schema = ''pgmq'' AND table_name = ''q_%s''
    )', p_queue_name) INTO v_table_exists;
  
  IF v_table_exists THEN
    -- Determine operation to perform
    IF p_operation = 'delete' AND p_message_id IS NOT NULL THEN
      BEGIN
        EXECUTE format('DELETE FROM %s WHERE id::TEXT = $1 OR msg_id::TEXT = $1', v_table_name)
        USING p_message_id;
        GET DIAGNOSTICS v_success = ROW_COUNT;
        
        IF v_success > 0 THEN
          v_result := jsonb_build_object(
            'success', TRUE, 
            'operation', 'delete', 
            'table', v_table_name, 
            'message_id', p_message_id
          );
          RETURN v_result;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        v_result := jsonb_build_object(
          'error', SQLERRM,
          'table', v_table_name,
          'operation', 'delete'
        );
      END;
    ELSIF p_operation = 'reset' AND p_message_id IS NOT NULL THEN
      BEGIN
        EXECUTE format('UPDATE %s SET vt = NULL WHERE id::TEXT = $1 OR msg_id::TEXT = $1', v_table_name)
        USING p_message_id;
        GET DIAGNOSTICS v_success = ROW_COUNT;
        
        IF v_success > 0 THEN
          v_result := jsonb_build_object(
            'success', TRUE, 
            'operation', 'reset', 
            'table', v_table_name, 
            'message_id', p_message_id
          );
          RETURN v_result;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        v_result := jsonb_build_object(
          'error', SQLERRM,
          'table', v_table_name,
          'operation', 'reset'
        );
      END;
    ELSIF p_operation = 'purge' THEN
      BEGIN
        EXECUTE format('TRUNCATE TABLE %s', v_table_name);
        v_result := jsonb_build_object(
          'success', TRUE, 
          'operation', 'purge', 
          'table', v_table_name
        );
        RETURN v_result;
      EXCEPTION WHEN OTHERS THEN
        v_result := jsonb_build_object(
          'error', SQLERRM,
          'table', v_table_name,
          'operation', 'purge'
        );
      END;
    END IF;
  END IF;
  
  -- Try fallback to public schema pattern if primary attempt failed
  v_table_name := 'public.pgmq_' || p_queue_name;
  
  -- Check if fallback table exists
  EXECUTE format('
    SELECT EXISTS (
      SELECT 1 FROM information_schema.tables 
      WHERE table_schema = ''public'' AND table_name = ''pgmq_%s''
    )', p_queue_name) INTO v_table_exists;
  
  IF v_table_exists THEN
    -- Determine operation to perform using fallback pattern
    IF p_operation = 'delete' AND p_message_id IS NOT NULL THEN
      BEGIN
        EXECUTE format('DELETE FROM %s WHERE id::TEXT = $1 OR msg_id::TEXT = $1', v_table_name)
        USING p_message_id;
        GET DIAGNOSTICS v_success = ROW_COUNT;
        
        IF v_success > 0 THEN
          v_result := jsonb_build_object(
            'success', TRUE, 
            'operation', 'delete', 
            'table', v_table_name, 
            'message_id', p_message_id
          );
          RETURN v_result;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        v_result := jsonb_build_object(
          'error', SQLERRM,
          'table', v_table_name,
          'operation', 'delete'
        );
      END;
    ELSIF p_operation = 'reset' AND p_message_id IS NOT NULL THEN
      BEGIN
        EXECUTE format('UPDATE %s SET vt = NULL WHERE id::TEXT = $1 OR msg_id::TEXT = $1', v_table_name)
        USING p_message_id;
        GET DIAGNOSTICS v_success = ROW_COUNT;
        
        IF v_success > 0 THEN
          v_result := jsonb_build_object(
            'success', TRUE, 
            'operation', 'reset', 
            'table', v_table_name, 
            'message_id', p_message_id
          );
          RETURN v_result;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        v_result := jsonb_build_object(
          'error', SQLERRM,
          'table', v_table_name,
          'operation', 'reset'
        );
      END;
    ELSIF p_operation = 'purge' THEN
      BEGIN
        EXECUTE format('TRUNCATE TABLE %s', v_table_name);
        v_result := jsonb_build_object(
          'success', TRUE, 
          'operation', 'purge', 
          'table', v_table_name
        );
        RETURN v_result;
      EXCEPTION WHEN OTHERS THEN
        v_result := jsonb_build_object(
          'error', SQLERRM,
          'table', v_table_name,
          'operation', 'purge'
        );
      END;
    END IF;
  END IF;
  
  -- If both attempts failed, return diagnostic information
  IF NOT v_success THEN
    RETURN jsonb_build_object(
      'success', FALSE,
      'error', format('Operation %s failed for message %s in queue %s', p_operation, p_message_id, p_queue_name),
      'details', jsonb_build_object(
        'checked_tables', jsonb_build_array(
          'pgmq.q_' || p_queue_name,
          'public.pgmq_' || p_queue_name
        )
      )
    );
  END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Direct fix for message ID 5 using the correct table name pattern
DO $$
DECLARE
  result JSONB;
BEGIN
  -- Try to delete message 5 from the correct table
  EXECUTE 'DELETE FROM pgmq.q_artist_discovery WHERE msg_id = 5 OR id = 5 OR msg_id::TEXT = ''5'' OR id::TEXT = ''5''';
  
  -- Check if deletion was successful
  IF FOUND THEN
    RAISE NOTICE 'Successfully deleted message 5 from pgmq.q_artist_discovery';
  ELSE
    -- Try reset if deletion failed
    EXECUTE 'UPDATE pgmq.q_artist_discovery SET vt = NULL WHERE msg_id = 5 OR id = 5 OR msg_id::TEXT = ''5'' OR id::TEXT = ''5''';
    
    IF FOUND THEN
      RAISE NOTICE 'Reset visibility timeout for message 5 in pgmq.q_artist_discovery';
    ELSE
      RAISE NOTICE 'Message 5 not found in pgmq.q_artist_discovery';
    END IF;
  END IF;
END $$;

-- Emergency fix to ensure all PGMQ functions work with correct table names
CREATE OR REPLACE FUNCTION public.pg_delete_message(
  queue_name TEXT,
  message_id TEXT
) RETURNS BOOLEAN AS $$
DECLARE
  success BOOLEAN := FALSE;
  queue_table TEXT;
BEGIN
  -- Get the actual queue table name using the correct pattern
  queue_table := 'pgmq.q_' || queue_name;
  
  -- First try using pgmq.delete directly
  BEGIN
    EXECUTE 'SELECT pgmq.delete($1, $2::UUID)' INTO success USING queue_name, message_id::UUID;
    
    IF success THEN
      RETURN TRUE;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'pgmq.delete failed (% %): %', queue_name, message_id, SQLERRM;
  END;
  
  -- Try direct deletion from the correct table pattern
  BEGIN
    EXECUTE format('DELETE FROM %s WHERE id::TEXT = $1 OR msg_id::TEXT = $1', queue_table)
    USING message_id;
    GET DIAGNOSTICS success = ROW_COUNT;
    
    IF success > 0 THEN
      RETURN TRUE;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Direct deletion failed (% %): %', queue_name, message_id, SQLERRM;
  END;
  
  RETURN FALSE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Update the diagnose_queue_tables function to check both schema patterns
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
  specific_queue TEXT;
  count_query TEXT;
BEGIN
  IF queue_name IS NOT NULL THEN
    specific_queue := queue_name;
    
    -- First check pgmq.q_queue_name pattern
    RETURN QUERY
    SELECT 
      'pgmq'::TEXT AS schema_name,
      'q_' || specific_queue AS table_name,
      'pgmq.q_' || specific_queue AS full_name,
      (SELECT COUNT(*)::BIGINT FROM pgmq.q_artist_discovery LIMIT 1) AS record_count 
    WHERE EXISTS (
      SELECT 1 
      FROM information_schema.tables 
      WHERE table_schema = 'pgmq' AND table_name = 'q_' || specific_queue
    )
    
    UNION
    
    -- Then check public.pgmq_queue_name pattern
    SELECT 
      'public'::TEXT AS schema_name,
      'pgmq_' || specific_queue AS table_name,
      'public.pgmq_' || specific_queue AS full_name,
      (SELECT COUNT(*)::BIGINT FROM public.pgmq_artist_discovery LIMIT 1) AS record_count
    WHERE EXISTS (
      SELECT 1 
      FROM information_schema.tables 
      WHERE table_schema = 'public' AND table_name = 'pgmq_' || specific_queue
    );
  ELSE
    -- Return all PGMQ tables in both schemas
    RETURN QUERY
    SELECT 
      table_schema::TEXT AS schema_name,
      table_name::TEXT,
      (table_schema || '.' || table_name)::TEXT AS full_name,
      0::BIGINT AS record_count -- Just a placeholder, would need dynamic SQL to get actual counts
    FROM 
      information_schema.tables
    WHERE 
      (table_schema = 'pgmq' AND table_name LIKE 'q_%') OR
      (table_schema = 'public' AND table_name LIKE 'pgmq_%')
    ORDER BY
      table_schema, table_name;
  END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
