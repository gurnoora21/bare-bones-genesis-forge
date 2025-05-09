
-- Create a function to ensure a message is deleted from a queue with robust error handling
CREATE OR REPLACE FUNCTION public.ensure_message_deleted(
  queue_name TEXT,
  message_id TEXT,
  max_attempts INT DEFAULT 3
) RETURNS BOOLEAN AS $$
DECLARE
  queue_table TEXT;
  attempt_count INT := 0;
  msg_deleted BOOLEAN := FALSE;
BEGIN
  -- Get the actual queue table name
  BEGIN
    SELECT get_queue_table_name_safe(queue_name) INTO STRICT queue_table;
  EXCEPTION WHEN OTHERS THEN
    queue_table := 'pgmq_' || queue_name;
  END;
  
  -- Try multiple deletion methods with retries
  WHILE attempt_count < max_attempts AND NOT msg_deleted LOOP
    attempt_count := attempt_count + 1;
    
    -- Try using standard pgmq.delete first
    BEGIN
      BEGIN
        -- Try UUID conversion
        SELECT pgmq.delete(queue_name, message_id::UUID) INTO msg_deleted;
        IF msg_deleted THEN RETURN TRUE; END IF;
      EXCEPTION WHEN OTHERS THEN
        -- Try numeric conversion
        BEGIN
          SELECT pgmq.delete(queue_name, message_id::BIGINT) INTO msg_deleted;
          IF msg_deleted THEN RETURN TRUE; END IF;
        EXCEPTION WHEN OTHERS THEN
          -- Continue to direct deletion
        END;
      END;
    EXCEPTION WHEN OTHERS THEN
      -- Just continue to next method
    END;
    
    -- Direct deletion with text comparison
    BEGIN
      EXECUTE format('DELETE FROM %I WHERE msg_id::TEXT = $1 OR id::TEXT = $1 RETURNING TRUE', queue_table)
        USING message_id INTO msg_deleted;
        
      IF msg_deleted THEN
        RETURN TRUE;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      -- Continue to next attempt
    END;
    
    -- Wait a bit before next attempt (exponential backoff)
    IF NOT msg_deleted AND attempt_count < max_attempts THEN
      PERFORM pg_sleep(0.1 * (2 ^ attempt_count));
    END IF;
  END LOOP;
  
  -- Final verification to see if the message still exists
  BEGIN
    EXECUTE format('SELECT NOT EXISTS(SELECT 1 FROM %I WHERE id::TEXT = $1 OR msg_id::TEXT = $1)', queue_table)
      USING message_id INTO msg_deleted;
      
    RETURN msg_deleted; -- TRUE if message is gone (deleted or never existed)
  EXCEPTION WHEN OTHERS THEN
    RETURN FALSE; -- Something went wrong with verification
  END;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
