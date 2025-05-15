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

-- Function to list potentially stuck messages
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
  -- Get the actual queue table name
  BEGIN
    SELECT get_queue_table_name_safe(queue_name) INTO STRICT queue_table;
  EXCEPTION WHEN OTHERS THEN
    queue_table := 'pgmq_' || queue_name;
  END;
  
  -- Return stuck messages
  RETURN QUERY EXECUTE format('
    SELECT 
      id::TEXT, 
      msg_id::TEXT, 
      message::JSONB, 
      vt, 
      read_ct,
      EXTRACT(EPOCH FROM (NOW() - vt))/60 AS minutes_locked
    FROM %I
    WHERE 
      vt IS NOT NULL 
      AND vt < NOW() - INTERVAL ''%s minutes''
    ORDER BY vt ASC
  ', queue_table, min_minutes_locked);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to reset visibility timeout for stuck messages
CREATE OR REPLACE FUNCTION public.reset_stuck_messages(
  queue_name TEXT,
  min_minutes_locked INT DEFAULT 10
) RETURNS INT AS $$
DECLARE
  queue_table TEXT;
  reset_count INT;
BEGIN
  -- Get the actual queue table name
  BEGIN
    SELECT get_queue_table_name_safe(queue_name) INTO STRICT queue_table;
  EXCEPTION WHEN OTHERS THEN
    queue_table := 'pgmq_' || queue_name;
  END;
  
  -- Reset visibility timeout for stuck messages
  EXECUTE format('
    UPDATE %I
    SET vt = NULL
    WHERE 
      vt IS NOT NULL 
      AND vt < NOW() - INTERVAL ''%s minutes''
  ', queue_table, min_minutes_locked);
  
  GET DIAGNOSTICS reset_count = ROW_COUNT;
  RETURN reset_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to move a message to a dead-letter queue
CREATE OR REPLACE FUNCTION public.move_to_dead_letter_queue(
  source_queue TEXT,
  dlq_name TEXT,
  message_id TEXT,
  failure_reason TEXT,
  metadata JSONB DEFAULT NULL
) RETURNS BOOLEAN AS $$
DECLARE
  message_body JSONB;
  dlq_msg_id BIGINT;
BEGIN
  -- Check if the DLQ exists, create if not
  BEGIN
    PERFORM pgmq.create(dlq_name);
  EXCEPTION WHEN OTHERS THEN
    -- Queue might already exist, which is fine
    RAISE NOTICE 'Ensuring DLQ exists: %', SQLERRM;
  END;
  
  -- Get the message body from the source queue
  BEGIN
    SELECT message
    INTO message_body
    FROM pgmq.get_queue_table_name(source_queue)
    WHERE id::TEXT = message_id OR msg_id::TEXT = message_id;
    
    IF message_body IS NULL THEN
      RAISE NOTICE 'Message % not found in queue %', message_id, source_queue;
      RETURN FALSE;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error fetching message %: %', message_id, SQLERRM;
    RETURN FALSE;
  END;
  
  -- Add failure metadata
  message_body := jsonb_set(
    message_body,
    '{_dlq_metadata}',
    jsonb_build_object(
      'source_queue', source_queue,
      'original_message_id', message_id,
      'moved_at', now(),
      'failure_reason', failure_reason,
      'custom_metadata', metadata
    )
  );
  
  -- Send to DLQ
  BEGIN
    SELECT pgmq.send(dlq_name, message_body)
    INTO dlq_msg_id;
    
    IF dlq_msg_id IS NULL THEN
      RAISE NOTICE 'Failed to send message to DLQ %', dlq_name;
      RETURN FALSE;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error sending to DLQ %: %', dlq_name, SQLERRM;
    RETURN FALSE;
  END;
  
  -- Try to delete the original message
  PERFORM ensure_message_deleted(source_queue, message_id);
  
  -- Log the DLQ action
  INSERT INTO monitoring_events (
    event_type, 
    details
  ) VALUES (
    'message_moved_to_dlq',
    jsonb_build_object(
      'source_queue', source_queue,
      'dlq_name', dlq_name,
      'source_message_id', message_id,
      'dlq_message_id', dlq_msg_id,
      'failure_reason', failure_reason,
      'timestamp', now()
    )
  );
  
  RETURN TRUE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to reprocess a message from a DLQ back to original queue
CREATE OR REPLACE FUNCTION public.reprocess_from_dlq(
  dlq_name TEXT,
  message_id TEXT,
  override_body JSONB DEFAULT NULL
) RETURNS BOOLEAN AS $$
DECLARE
  message_body JSONB;
  source_queue TEXT;
  reprocessed_id BIGINT;
  metadata JSONB;
BEGIN
  -- Get the message from the DLQ
  BEGIN
    SELECT message
    INTO message_body
    FROM pgmq.get_queue_table_name(dlq_name)
    WHERE id::TEXT = message_id OR msg_id::TEXT = message_id;
    
    IF message_body IS NULL THEN
      RAISE NOTICE 'Message % not found in DLQ %', message_id, dlq_name;
      RETURN FALSE;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error fetching message %: %', message_id, SQLERRM;
    RETURN FALSE;
  END;
  
  -- Get source queue from metadata
  source_queue := message_body->'_dlq_metadata'->>'source_queue';
  metadata := message_body->'_dlq_metadata';
  
  -- Use override body if provided
  IF override_body IS NOT NULL THEN
    message_body := override_body;
  END IF;
  
  -- Remove DLQ metadata for clean reprocessing
  message_body := message_body - '_dlq_metadata';
  
  -- Add reprocessing metadata
  message_body := jsonb_set(
    message_body,
    '{_reprocessing_metadata}',
    jsonb_build_object(
      'reprocessed_from_dlq', dlq_name,
      'original_message_id', message_id,
      'reprocessed_at', now()
    )
  );
  
  -- Send to original queue
  BEGIN
    SELECT pgmq.send(source_queue, message_body)
    INTO reprocessed_id;
    
    IF reprocessed_id IS NULL THEN
      RAISE NOTICE 'Failed to send message back to queue %', source_queue;
      RETURN FALSE;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error sending to original queue %: %', source_queue, SQLERRM;
    RETURN FALSE;
  END;
  
  -- Delete from DLQ
  PERFORM ensure_message_deleted(dlq_name, message_id);
  
  -- Log the reprocessing action
  INSERT INTO monitoring_events (
    event_type, 
    details
  ) VALUES (
    'message_reprocessed_from_dlq',
    jsonb_build_object(
      'dlq_name', dlq_name,
      'source_queue', source_queue,
      'original_message_id', message_id,
      'new_message_id', reprocessed_id,
      'timestamp', now(),
      'original_metadata', metadata
    )
  );
  
  RETURN TRUE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to purge old messages from a DLQ
CREATE OR REPLACE FUNCTION public.purge_dlq_messages(
  dlq_name TEXT,
  older_than_days INTEGER DEFAULT 30,
  max_messages INTEGER DEFAULT 1000
) RETURNS INTEGER AS $$
DECLARE
  deleted_count INTEGER := 0;
  message_record RECORD;
BEGIN
  -- Track this operation
  INSERT INTO monitoring_events (
    event_type, 
    details
  ) VALUES (
    'dlq_purge_operation',
    jsonb_build_object(
      'dlq_name', dlq_name,
      'older_than_days', older_than_days,
      'max_messages', max_messages,
      'started_at', now()
    )
  );

  -- Loop through and delete old messages
  FOR message_record IN 
    SELECT id::TEXT as msg_id 
    FROM pgmq.get_queue_table_name(dlq_name)
    WHERE created_at < (NOW() - (older_than_days * INTERVAL '1 day'))
    LIMIT max_messages
  LOOP
    -- Delete the message
    PERFORM ensure_message_deleted(dlq_name, message_record.msg_id);
    deleted_count := deleted_count + 1;
  END LOOP;
  
  -- Update monitoring event with results
  UPDATE monitoring_events
  SET details = jsonb_set(
    details,
    '{results}',
    jsonb_build_object(
      'deleted_count', deleted_count,
      'completed_at', now()
    )
  )
  WHERE event_type = 'dlq_purge_operation' 
  AND details->>'dlq_name' = dlq_name
  AND details->>'started_at' = (
    SELECT MAX(details->>'started_at') 
    FROM monitoring_events 
    WHERE event_type = 'dlq_purge_operation'
    AND details->>'dlq_name' = dlq_name
  );
  
  RETURN deleted_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
