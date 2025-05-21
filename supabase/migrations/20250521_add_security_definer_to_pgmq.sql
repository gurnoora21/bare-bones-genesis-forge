
-- Add SECURITY DEFINER to pgmq.delete_message_robust function
-- This ensures the function runs with the privileges of the owner rather than the caller
-- which resolves the "relation does not exist" errors when accessing pgmq schema objects

CREATE OR REPLACE FUNCTION pgmq.delete_message_robust(
  queue_name TEXT,
  message_id TEXT
) RETURNS BOOLEAN AS $$
DECLARE
  queue_table TEXT;
  result BOOLEAN;
  rows_affected INTEGER;
BEGIN
  -- Normalize queue name
  IF queue_name LIKE 'pgmq.%' THEN
    queue_table := queue_name;
  ELSIF queue_name LIKE 'q_%' THEN
    queue_table := 'pgmq.' || queue_name;
  ELSE
    queue_table := 'pgmq.q_' || queue_name;
  END IF;

  -- Try to delete by msg_id first (most common case)
  EXECUTE format('
    DELETE FROM %I
    WHERE msg_id = $1
    RETURNING true', queue_table)
  INTO result
  USING message_id;

  GET DIAGNOSTICS rows_affected = ROW_COUNT;
  
  -- If no rows affected, try by id
  IF rows_affected = 0 THEN
    EXECUTE format('
      DELETE FROM %I
      WHERE id = $1
      RETURNING true', queue_table)
    INTO result
    USING message_id;
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
  END IF;
  
  -- If still no rows affected, try by id as text (in case of type mismatch)
  IF rows_affected = 0 THEN
    EXECUTE format('
      DELETE FROM %I
      WHERE id::text = $1::text
      RETURNING true', queue_table)
    INTO result
    USING message_id;
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
  END IF;
  
  -- If still no rows affected, try by msg_id as text
  IF rows_affected = 0 THEN
    EXECUTE format('
      DELETE FROM %I
      WHERE msg_id::text = $1::text
      RETURNING true', queue_table)
    INTO result
    USING message_id;
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
  END IF;

  -- Return true if any rows were affected, false otherwise
  RETURN rows_affected > 0;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;  -- Added SECURITY DEFINER here

-- Update the pg_delete_message function to ensure it also has SECURITY DEFINER
CREATE OR REPLACE FUNCTION pg_delete_message(
  queue_name TEXT,
  message_id TEXT
) RETURNS BOOLEAN AS $$
BEGIN
  RETURN pgmq.delete_message_robust(queue_name, message_id);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;  -- Added SECURITY DEFINER here
