-- Fix Message ID Handling in Queue Operations
-- This migration adds a function to handle message ID extraction and deletion more robustly

-- Create a function to delete a message from a queue with better ID handling
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
$$ LANGUAGE plpgsql;

-- Create a function to get message ID from various formats
CREATE OR REPLACE FUNCTION pgmq.extract_message_id(
  message JSONB
) RETURNS TEXT AS $$
DECLARE
  result TEXT;
BEGIN
  -- Try different possible message ID fields
  result := message->>'id';
  
  IF result IS NULL THEN
    result := message->>'msg_id';
  END IF;
  
  IF result IS NULL THEN
    result := message->>'msgId';
  END IF;
  
  IF result IS NULL THEN
    result := message->>'messageId';
  END IF;
  
  RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Update the pg_delete_message function to use our robust function
CREATE OR REPLACE FUNCTION pg_delete_message(
  queue_name TEXT,
  message_id TEXT
) RETURNS BOOLEAN AS $$
BEGIN
  RETURN pgmq.delete_message_robust(queue_name, message_id);
END;
$$ LANGUAGE plpgsql;
