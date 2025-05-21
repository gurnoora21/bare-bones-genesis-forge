
-- Fix table identifier handling in pgmq.delete_message_robust function
-- This resolves the "relation does not exist" errors by correctly formatting schema and table separately

CREATE OR REPLACE FUNCTION pgmq.delete_message_robust(
  queue_name TEXT,
  message_id TEXT
) RETURNS BOOLEAN AS $$
DECLARE
  schema_name TEXT := 'pgmq';
  table_name TEXT;
  result BOOLEAN;
  rows_affected INTEGER;
BEGIN
  -- Normalize queue name
  IF queue_name LIKE 'pgmq.%' THEN
    -- Extract table part from fully qualified name
    table_name := substring(queue_name from 6); -- After 'pgmq.'
  ELSIF queue_name LIKE 'q_%' THEN
    table_name := queue_name;
  ELSE
    table_name := 'q_' || queue_name;
  END IF;

  -- Try to delete by msg_id first (most common case)
  EXECUTE format('
    DELETE FROM %I.%I
    WHERE msg_id = $1
    RETURNING true', schema_name, table_name)
  INTO result
  USING message_id;

  GET DIAGNOSTICS rows_affected = ROW_COUNT;
  
  -- If no rows affected, try by id
  IF rows_affected = 0 THEN
    EXECUTE format('
      DELETE FROM %I.%I
      WHERE id = $1
      RETURNING true', schema_name, table_name)
    INTO result
    USING message_id;
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
  END IF;
  
  -- If still no rows affected, try by id as text (in case of type mismatch)
  IF rows_affected = 0 THEN
    EXECUTE format('
      DELETE FROM %I.%I
      WHERE id::text = $1::text
      RETURNING true', schema_name, table_name)
    INTO result
    USING message_id;
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
  END IF;
  
  -- If still no rows affected, try by msg_id as text
  IF rows_affected = 0 THEN
    EXECUTE format('
      DELETE FROM %I.%I
      WHERE msg_id::text = $1::text
      RETURNING true', schema_name, table_name)
    INTO result
    USING message_id;
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
  END IF;

  -- Return true if any rows were affected, false otherwise
  RETURN rows_affected > 0;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

COMMENT ON FUNCTION pgmq.delete_message_robust IS 'Robustly delete a message from a queue using properly quoted schema and table identifiers';

-- Ensure pg_delete_message also uses the fixed function and remains SECURITY DEFINER
CREATE OR REPLACE FUNCTION pg_delete_message(
  queue_name TEXT,
  message_id TEXT
) RETURNS BOOLEAN AS $$
BEGIN
  RETURN pgmq.delete_message_robust(queue_name, message_id);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

COMMENT ON FUNCTION pg_delete_message IS 'Delete a message from a queue using properly quoted schema and table identifiers';
