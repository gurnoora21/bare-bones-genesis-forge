
# Music Producer Discovery System - Database Schema Documentation

This document provides an overview of the database schema, SQL functions, and scheduled jobs for the music producer discovery system.

## Core Tables

### Artists
Stores information about artists from Spotify.
- `id`: Primary key
- `name`: Artist name
- `spotify_id`: Unique identifier from Spotify API
- `followers`: Number of followers on Spotify
- `popularity`: Popularity score from Spotify (0-100)
- `image_url`: URL to artist's profile image
- `metadata`: Additional data in JSON format
- `created_at` & `updated_at`: Timestamp fields

### Albums
Stores album information related to artists.
- `id`: Primary key
- `artist_id`: Foreign key to artists table
- `name`: Album name
- `spotify_id`: Unique identifier from Spotify API
- `release_date`: Album release date
- `cover_url`: URL to album cover image
- `metadata`: Additional data in JSON format
- `created_at` & `updated_at`: Timestamp fields

### Tracks
Stores track information related to albums.
- `id`: Primary key
- `album_id`: Foreign key to albums table
- `name`: Track name
- `spotify_id`: Unique identifier from Spotify API
- `duration_ms`: Track length in milliseconds
- `popularity`: Popularity score from Spotify (0-100)
- `spotify_preview_url`: URL for 30-second preview
- `metadata`: Additional data in JSON format
- `created_at` & `updated_at`: Timestamp fields

### Normalized Tracks
Handles different versions of the same track (e.g., remixes, radio edits).
- `id`: Primary key
- `artist_id`: Foreign key to artists table
- `normalized_name`: Standardized track name for grouping
- `representative_track_id`: Foreign key to tracks table (main version)
- `created_at` & `updated_at`: Timestamp fields

### Producers
Stores information about music producers.
- `id`: Primary key
- `name`: Producer name
- `normalized_name`: Standardized producer name for matching
- `email`: Contact email (if available)
- `image_url`: URL to producer's profile image
- `instagram_handle`: Instagram username
- `instagram_bio`: Instagram biography
- `metadata`: Additional data in JSON format
- `enriched_at`: Timestamp of last social media enrichment
- `enrichment_failed`: Flag for failed enrichment attempts
- `created_at` & `updated_at`: Timestamp fields

### Track Producers
Junction table establishing many-to-many relationships between tracks and producers.
- `id`: Primary key
- `track_id`: Foreign key to tracks table
- `producer_id`: Foreign key to producers table
- `confidence`: Confidence score for the association (0-1)
- `source`: Source of the producer credit information
- `created_at`: Timestamp field

## Queue Management

### Queue Registry
Manages queue configurations and metadata.
- `id`: Primary key
- `queue_name`: Unique name of the queue
- `display_name`: Human-readable name
- `description`: Purpose of the queue
- `active`: Whether the queue is currently active
- `created_at` & `updated_at`: Timestamp fields

### Processing Status
Tracks the processing state of entities.
- `id`: Primary key
- `entity_type`: Type of entity (e.g., "artist", "album")
- `entity_id`: Identifier of the entity
- `state`: Current state ("PENDING", "IN_PROGRESS", "COMPLETED", "FAILED")
- `attempts`: Number of processing attempts
- `last_processed_at`: Timestamp of last processing attempt
- `metadata`: Additional processing data
- `last_error`: Last error message (if any)
- `created_at` & `updated_at`: Timestamp fields

### Processing Locks
Manages distributed locks for concurrent processing.
- `id`: Primary key
- `entity_type`: Type of entity being locked
- `entity_id`: Identifier of the entity
- `worker_id`: Identifier of the worker holding the lock
- `acquired_at`: When the lock was acquired
- `last_heartbeat`: Last heartbeat timestamp
- `ttl_seconds`: Time-to-live in seconds
- `correlation_id`: Optional correlation identifier
- `metadata`: Additional lock data

## Monitoring Tables

### Worker Issues
Logs issues encountered by background workers.
- `id`: Primary key
- `worker_name`: Name of the worker process
- `issue_type`: Category of the issue
- `message`: Description of the issue
- `details`: Additional details in JSON format
- `resolved`: Flag indicating if the issue is resolved
- `created_at` & `updated_at`: Timestamp fields

### Queue Metrics
Tracks performance metrics for queue processing.
- `id`: Primary key
- `queue_name`: Name of the processed queue
- `operation`: Type of operation performed
- `started_at`: When processing began
- `finished_at`: When processing completed
- `processed_count`: Number of items processed
- `success_count`: Number of successful operations
- `error_count`: Number of failed operations
- `details`: Additional metrics in JSON format
- `created_at`: Timestamp field

### Monitoring Events
Records system-level events for auditing and monitoring.
- `id`: Primary key
- `event_type`: Type of event
- `details`: Event details in JSON format
- `created_at`: Timestamp field

## SQL Functions

### Queue Management Functions

#### register_queue
Registers a new queue in the system.
```sql
CREATE OR REPLACE FUNCTION public.register_queue(
  p_queue_name TEXT, 
  p_display_name TEXT DEFAULT NULL,
  p_description TEXT DEFAULT NULL, 
  p_active BOOLEAN DEFAULT TRUE
) RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  -- Create the queue in pgmq if it doesn't exist
  BEGIN
    PERFORM pgmq.create(p_queue_name);
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Queue % might already exist: %', p_queue_name, SQLERRM;
  END;
  
  -- Register the queue in our registry
  INSERT INTO public.queue_registry (queue_name, display_name, description, active)
  VALUES (p_queue_name, COALESCE(p_display_name, p_queue_name), p_description, p_active)
  ON CONFLICT (queue_name) 
  DO UPDATE SET 
    display_name = COALESCE(p_display_name, queue_registry.display_name),
    description = COALESCE(p_description, queue_registry.description),
    active = p_active,
    updated_at = now();
    
  RETURN TRUE;
END;
$$;
```

#### pg_enqueue
Enqueues a message to a PGMQ queue.
```sql
CREATE OR REPLACE FUNCTION public.pg_enqueue(queue_name TEXT, message_body JSONB)
RETURNS BIGINT LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  msg_id BIGINT;
BEGIN
  -- Call pgmq.send with the JSONB parameter
  SELECT send
    FROM pgmq.send(queue_name, message_body)
    INTO msg_id;
  RETURN msg_id;
END;
$$;
```

#### pg_dequeue
Dequeues messages from a PGMQ queue.
```sql
CREATE OR REPLACE FUNCTION public.pg_dequeue(queue_name TEXT, batch_size INTEGER DEFAULT 5, visibility_timeout INTEGER DEFAULT 60)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  result JSONB;
BEGIN
  SELECT jsonb_agg(row_to_json(msg))
  FROM pgmq.read(queue_name, batch_size, visibility_timeout) AS msg
  INTO result;
  
  RETURN result;
END;
$$;
```

#### pg_delete_message
Deletes a message from a queue with multiple fallback strategies.
```sql
CREATE OR REPLACE FUNCTION public.pg_delete_message(queue_name TEXT, message_id TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  queue_table TEXT;
  is_uuid BOOLEAN := FALSE;
  is_numeric BOOLEAN := FALSE;
  numeric_id BIGINT;
  uuid_id UUID;
  success BOOLEAN := FALSE;
BEGIN
  -- Get the actual queue table name
  BEGIN
    SELECT public.get_queue_table_name_safe(queue_name) INTO STRICT queue_table;
  EXCEPTION WHEN OTHERS THEN
    queue_table := 'pgmq_' || queue_name;
  END;

  -- Check if ID is numeric
  BEGIN
    numeric_id := message_id::BIGINT;
    is_numeric := TRUE;
  EXCEPTION WHEN OTHERS THEN
    is_numeric := FALSE;
  END;
  
  -- Check if ID is UUID
  IF NOT is_numeric THEN
    BEGIN
      uuid_id := message_id::UUID;
      is_uuid := TRUE;
    EXCEPTION WHEN OTHERS THEN
      is_uuid := FALSE;
    END;
  END IF;
  
  -- Try using pgmq.delete first with appropriate type
  BEGIN
    IF is_uuid THEN
      SELECT pgmq.delete(queue_name, uuid_id) INTO success;
      IF success THEN RETURN TRUE; END IF;
    ELSIF is_numeric THEN
      -- Try to use pgmq.delete with numeric id
      BEGIN
        EXECUTE 'SELECT pgmq.delete($1, $2::BIGINT)' 
          USING queue_name, numeric_id INTO success;
        IF success THEN RETURN TRUE; END IF;
      EXCEPTION WHEN OTHERS THEN
        -- Some versions don't support numeric IDs with pgmq.delete
        RAISE NOTICE 'pgmq.delete with numeric ID failed: %', SQLERRM;
      END;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'pgmq.delete failed: %', SQLERRM;
  END;
  
  -- Try direct deletion with known ID type
  IF is_numeric THEN
    -- Try deleting with numeric msg_id
    BEGIN
      EXECUTE format('DELETE FROM %s WHERE msg_id = $1', queue_table)
      USING numeric_id;
      GET DIAGNOSTICS success = ROW_COUNT;
      
      IF success > 0 THEN
        RETURN TRUE;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Error deleting by numeric msg_id: %', SQLERRM;
    END;
    
    -- Try deleting with numeric id
    BEGIN
      EXECUTE format('DELETE FROM %s WHERE id = $1', queue_table)
      USING numeric_id;
      GET DIAGNOSTICS success = ROW_COUNT;
      
      IF success > 0 THEN
        RETURN TRUE;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Error deleting by numeric id: %', SQLERRM;
    END;
  ELSIF is_uuid THEN
    -- Try deleting with UUID
    BEGIN
      EXECUTE format('DELETE FROM %s WHERE id = $1', queue_table)
      USING uuid_id;
      GET DIAGNOSTICS success = ROW_COUNT;
      
      IF success > 0 THEN
        RETURN TRUE;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Error deleting by UUID: %', SQLERRM;
    END;
  END IF;
  
  -- Final attempt using text comparison - most flexible but potentially slower
  BEGIN
    EXECUTE format('DELETE FROM %s WHERE msg_id::TEXT = $1 OR id::TEXT = $1', queue_table)
    USING message_id;
    GET DIAGNOSTICS success = ROW_COUNT;
    
    RETURN success > 0;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error deleting using text comparison: %', SQLERRM;
    RETURN FALSE;
  END;
END;
$$;
```

#### ensure_message_deleted
Robustly ensures a message is deleted with retries and verification.
```sql
CREATE OR REPLACE FUNCTION public.ensure_message_deleted(queue_name TEXT, message_id TEXT, max_attempts INTEGER DEFAULT 3)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
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
$$;
```

#### reset_stuck_message
Resets the visibility timeout for a stuck message.
```sql
CREATE OR REPLACE FUNCTION public.reset_stuck_message(queue_name TEXT, message_id TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  queue_table TEXT;
  success BOOLEAN := FALSE;
BEGIN
  -- Get the actual queue table name
  BEGIN
    SELECT pgmq.get_queue_table_name(queue_name) INTO STRICT queue_table;
  EXCEPTION WHEN OTHERS THEN
    queue_table := 'pgmq_' || queue_name;
  END;
  
  -- Try with text comparison for maximum flexibility
  BEGIN
    EXECUTE format('UPDATE %I SET vt = NULL WHERE msg_id::TEXT = $1 OR id::TEXT = $1', queue_table)
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
$$;
```

#### reset_stuck_messages
Resets visibility timeout for all stuck messages in a queue.
```sql
CREATE OR REPLACE FUNCTION public.reset_stuck_messages(queue_name TEXT, min_minutes_locked INTEGER DEFAULT 10)
RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  queue_table TEXT;
  reset_count INT;
BEGIN
  -- Get the actual queue table name with schema
  SELECT public.get_queue_table_name_safe(queue_name) INTO queue_table;
  
  -- Reset visibility timeout for stuck messages
  EXECUTE format('
    UPDATE %s
    SET vt = NULL
    WHERE 
      vt IS NOT NULL 
      AND vt < NOW() - INTERVAL ''%s minutes''
  ', queue_table, min_minutes_locked);
  
  GET DIAGNOSTICS reset_count = ROW_COUNT;
  RETURN reset_count;
END;
$$;
```

#### reset_all_stuck_messages
Resets stuck messages across all queues in the system.
```sql
CREATE OR REPLACE FUNCTION public.reset_all_stuck_messages(threshold_minutes INTEGER DEFAULT 10)
RETURNS TABLE(queue_name TEXT, messages_reset INTEGER) LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  q RECORD;
  reset_count INT;
BEGIN
  -- Find all queue tables using information_schema
  FOR q IN 
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE 
      (table_name LIKE 'q\_%' OR table_name LIKE 'pgmq\_%') 
      AND table_type = 'BASE TABLE'
  LOOP
    -- Extract queue name from table name
    queue_name := CASE 
      WHEN q.table_name LIKE 'q\_%' THEN substring(q.table_name from 3)
      WHEN q.table_name LIKE 'pgmq\_%' THEN substring(q.table_name from 6)
      ELSE q.table_name
    END;
    
    -- Reset stuck messages
    EXECUTE format('
      UPDATE %I.%I
      SET vt = NULL
      WHERE 
        vt IS NOT NULL 
        AND vt < NOW() - INTERVAL ''%s minutes''
      RETURNING count(*)', 
      q.table_schema, q.table_name, threshold_minutes
    ) INTO reset_count;
    
    IF reset_count > 0 THEN
      RETURN QUERY SELECT queue_name::TEXT, reset_count::INT;
    END IF;
  END LOOP;
END;
$$;
```

#### move_to_dead_letter_queue
Moves a failed message to a dead letter queue.
```sql
CREATE OR REPLACE FUNCTION public.move_to_dead_letter_queue(
  source_queue TEXT, 
  dlq_name TEXT, 
  message_id TEXT, 
  failure_reason TEXT, 
  metadata JSONB DEFAULT NULL
) RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
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
$$;
```

#### reprocess_from_dlq
Reprocesses a message from a dead letter queue.
```sql
CREATE OR REPLACE FUNCTION public.reprocess_from_dlq(
  dlq_name TEXT, 
  message_id TEXT, 
  override_body JSONB DEFAULT NULL
) RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
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
$$;
```

### Process Management Functions

#### start_artist_discovery
Initiates discovery for a new artist.
```sql
CREATE OR REPLACE FUNCTION public.start_artist_discovery(artist_name TEXT)
RETURNS BIGINT LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  msg_id BIGINT;
  idempotency_key TEXT;
BEGIN
  -- Create idempotency key
  idempotency_key := 'artist:name:' || lower(artist_name);
  
  -- Call pgmq.send directly to avoid any potential issues
  SELECT pgmq.send(
    'artist_discovery',
    jsonb_build_object(
      'artistName', artist_name,
      '_idempotencyKey', idempotency_key
    )
  ) INTO msg_id;
  
  -- If no message ID was returned, raise an error
  IF msg_id IS NULL THEN
    RAISE EXCEPTION 'Failed to enqueue artist discovery message';
  END IF;
  
  RETURN msg_id;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Error enqueueing artist discovery: %', SQLERRM;
  RAISE;
END;
$$;
```

#### start_bulk_artist_discovery
Starts discovery for multiple artists based on criteria.
```sql
CREATE OR REPLACE FUNCTION public.start_bulk_artist_discovery(
  genre TEXT DEFAULT NULL,
  min_popularity INTEGER DEFAULT 0,
  limit_count INTEGER DEFAULT 10
) RETURNS SETOF UUID LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  artist_record RECORD;
  msg_id UUID;
BEGIN
  FOR artist_record IN 
    SELECT name 
    FROM artists
    WHERE (genre IS NULL OR metadata->>'genres' ? genre)
    AND (popularity >= min_popularity)
    ORDER BY popularity DESC
    LIMIT limit_count
  LOOP
    SELECT pgmq.send('artist_discovery', json_build_object('artistName', artist_record.name)::TEXT) INTO msg_id;
    RETURN NEXT msg_id;
  END LOOP;
  RETURN;
END;
$$;
```

#### process_track_batch
Process a batch of track data in a single transaction.
```sql
CREATE OR REPLACE FUNCTION public.process_track_batch(
  p_track_data JSONB[],
  p_album_id UUID,
  p_artist_id UUID
) RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_track JSONB;
  v_track_id UUID;
  v_results JSONB = '[]'::JSONB;
  v_normalized_name TEXT;
  v_count INTEGER = 0;
BEGIN
  -- Process all tracks in a single transaction
  FOREACH v_track IN ARRAY p_track_data LOOP
    -- Insert the track
    INSERT INTO tracks(
      album_id,
      spotify_id,
      name,
      duration_ms,
      popularity,
      spotify_preview_url,
      metadata
    ) VALUES (
      p_album_id,
      v_track->>'spotify_id',
      v_track->>'name',
      (v_track->>'duration_ms')::INTEGER,
      (v_track->>'popularity')::INTEGER,
      v_track->>'spotify_preview_url',
      v_track->'metadata'
    )
    ON CONFLICT (spotify_id) 
    DO UPDATE SET
      name = EXCLUDED.name,
      duration_ms = EXCLUDED.duration_ms,
      popularity = EXCLUDED.popularity,
      spotify_preview_url = EXCLUDED.spotify_preview_url,
      metadata = EXCLUDED.metadata,
      updated_at = NOW()
    RETURNING id INTO v_track_id;
    
    -- Normalize the track name
    v_normalized_name = lower(regexp_replace(v_track->>'name', '[^a-zA-Z0-9]', '', 'g'));
    
    -- Create normalized track entry
    INSERT INTO normalized_tracks(
      artist_id,
      normalized_name,
      representative_track_id
    ) VALUES (
      p_artist_id,
      v_normalized_name,
      v_track_id
    )
    ON CONFLICT (artist_id, normalized_name) 
    DO NOTHING;
    
    -- Add to results
    v_results = v_results || jsonb_build_object(
      'track_id', v_track_id,
      'name', v_track->>'name',
      'spotify_id', v_track->>'spotify_id'
    );
    
    v_count = v_count + 1;
  END LOOP;
  
  RETURN jsonb_build_object(
    'processed', v_count,
    'results', v_results
  );
EXCEPTION WHEN OTHERS THEN
  -- Return error information
  RETURN jsonb_build_object(
    'error', SQLERRM,
    'detail', SQLSTATE
  );
END;
$$;
```

### Distributed Locking Functions

#### pg_advisory_lock_timeout
Acquires an advisory lock with timeout.
```sql
CREATE OR REPLACE FUNCTION public.pg_advisory_lock_timeout(p_key TEXT, p_timeout_ms INTEGER)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_key BIGINT;
  v_lock_acquired BOOLEAN := FALSE;
BEGIN
  -- Convert text key to bigint using hash
  v_key := ('x' || substr(md5(p_key), 1, 16))::bit(64)::bigint;
  
  -- Set statement timeout to specified value
  EXECUTE format('SET LOCAL statement_timeout = %s', p_timeout_ms);
  
  -- Try to acquire the lock (blocking)
  BEGIN
    PERFORM pg_advisory_lock(v_key);
    v_lock_acquired := TRUE;
  EXCEPTION 
    WHEN sqlstate '57014' THEN
      -- This is the SQLSTATE code for statement_timeout
      v_lock_acquired := FALSE;
  END;
  
  -- Reset statement timeout to default
  RESET statement_timeout;
  
  RETURN v_lock_acquired;
END;
$$;
```

#### pg_try_advisory_lock
Tries to acquire an advisory lock (non-blocking).
```sql
CREATE OR REPLACE FUNCTION public.pg_try_advisory_lock(p_key TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_key BIGINT;
BEGIN
  -- Convert text key to bigint using hash
  v_key := ('x' || substr(md5(p_key), 1, 16))::bit(64)::bigint;
  
  -- Try to acquire the lock (non-blocking)
  RETURN pg_try_advisory_lock(v_key);
END;
$$;
```

#### pg_advisory_unlock
Releases an advisory lock.
```sql
CREATE OR REPLACE FUNCTION public.pg_advisory_unlock(p_key TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_key BIGINT;
BEGIN
  -- Convert text key to bigint using hash
  v_key := ('x' || substr(md5(p_key), 1, 16))::bit(64)::bigint;
  
  -- Release the lock
  RETURN pg_advisory_unlock(v_key);
END;
$$;
```

#### pg_advisory_lock_exists
Checks if an advisory lock is held by another session.
```sql
CREATE OR REPLACE FUNCTION public.pg_advisory_lock_exists(p_key TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_key BIGINT;
  v_exists BOOLEAN;
BEGIN
  -- Convert text key to bigint using hash
  v_key := ('x' || substr(md5(p_key), 1, 16))::bit(64)::bigint;
  
  -- Check if the lock exists in pg_locks
  SELECT EXISTS(
    SELECT 1 
    FROM pg_locks 
    WHERE locktype = 'advisory' 
    AND objid = v_key
    AND pid != pg_backend_pid()
  ) INTO v_exists;
  
  RETURN v_exists;
END;
$$;
```

#### pg_force_advisory_unlock_all
Forces release of advisory locks for a given key across all sessions.
```sql
CREATE OR REPLACE FUNCTION public.pg_force_advisory_unlock_all(p_key TEXT)
RETURNS TABLE(pid INTEGER, released BOOLEAN) LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_key BIGINT;
  v_rec RECORD;
BEGIN
  -- Convert text key to bigint using hash
  v_key := ('x' || substr(md5(p_key), 1, 16))::bit(64)::bigint;
  
  FOR v_rec IN 
    SELECT pid 
    FROM pg_locks 
    WHERE locktype = 'advisory' 
    AND objid = v_key 
    AND pid != pg_backend_pid()
  LOOP
    -- Use pg_terminate_backend to forcibly terminate the connection
    -- This will release all locks held by that backend
    PERFORM pg_terminate_backend(v_rec.pid);
    
    -- Return the pid and success status
    pid := v_rec.pid;
    released := TRUE;
    RETURN NEXT;
  END LOOP;
  
  RETURN;
END;
$$;
```

### Entity Processing Functions

#### acquire_processing_lock
Acquires a processing lock for an entity with timeout.
```sql
CREATE OR REPLACE FUNCTION public.acquire_processing_lock(
  p_entity_type TEXT, 
  p_entity_id TEXT, 
  p_timeout_minutes INTEGER DEFAULT 30, 
  p_correlation_id TEXT DEFAULT NULL
) RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_current_state TEXT;
  v_last_processed TIMESTAMPTZ;
  v_lock_acquired BOOLEAN := FALSE;
BEGIN
  -- Use advisory lock to prevent concurrent execution of this function
  -- for the same entity to avoid race conditions
  PERFORM pg_advisory_xact_lock(
    ('x' || substr(md5(p_entity_type || ':' || p_entity_id), 1, 16))::bit(64)::bigint
  );
  
  -- Check if entity exists and get current state
  SELECT state, last_processed_at 
  INTO v_current_state, v_last_processed
  FROM public.processing_status
  WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
  
  -- Handle based on current state
  IF v_current_state IS NULL THEN
    -- Entity doesn't exist yet, create it in IN_PROGRESS state
    INSERT INTO public.processing_status(
      entity_type, entity_id, state, last_processed_at, metadata
    ) VALUES (
      p_entity_type, p_entity_id, 'IN_PROGRESS', NOW(), 
      jsonb_build_object('correlation_id', p_correlation_id)
    );
    v_lock_acquired := TRUE;
  
  ELSIF v_current_state = 'PENDING' THEN
    -- Entity exists and is in PENDING state, transition to IN_PROGRESS
    UPDATE public.processing_status
    SET 
      state = 'IN_PROGRESS',
      last_processed_at = NOW(),
      metadata = jsonb_set(
        COALESCE(metadata, '{}'::jsonb),
        '{correlation_id}',
        to_jsonb(COALESCE(p_correlation_id, metadata->>'correlation_id'))
      ),
      attempts = attempts + 1,
      updated_at = NOW()
    WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
    v_lock_acquired := TRUE;
    
  ELSIF v_current_state = 'IN_PROGRESS' THEN
    -- Check if the lock has timed out
    IF v_last_processed < (NOW() - (p_timeout_minutes * INTERVAL '1 minute')) THEN
      -- Lock has timed out, reset to IN_PROGRESS
      UPDATE public.processing_status
      SET 
        state = 'IN_PROGRESS',
        last_processed_at = NOW(),
        metadata = jsonb_set(
          COALESCE(metadata, '{}'::jsonb),
          '{timeout_recovery}',
          jsonb_build_object(
            'previous_lock_time', v_last_processed,
            'timeout_minutes', p_timeout_minutes,
            'recovered_at', NOW(),
            'correlation_id', COALESCE(p_correlation_id, metadata->>'correlation_id')
          )
        ),
        attempts = attempts + 1,
        updated_at = NOW()
      WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
      v_lock_acquired := TRUE;
    ELSE
      -- Lock is still valid, cannot acquire
      v_lock_acquired := FALSE;
    END IF;
    
  ELSIF v_current_state = 'COMPLETED' THEN
    -- Already completed, no need to process again
    v_lock_acquired := FALSE;
    
  ELSIF v_current_state = 'FAILED' THEN
    -- Failed previously, allow retry
    UPDATE public.processing_status
    SET 
      state = 'IN_PROGRESS',
      last_processed_at = NOW(),
      metadata = jsonb_set(
        COALESCE(metadata, '{}'::jsonb),
        '{retry_info}',
        jsonb_build_object(
          'previous_attempt', metadata,
          'retried_at', NOW(),
          'correlation_id', COALESCE(p_correlation_id, metadata->>'correlation_id')
        )
      ),
      attempts = attempts + 1,
      updated_at = NOW()
    WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
    v_lock_acquired := TRUE;
  END IF;
  
  RETURN v_lock_acquired;
END;
$$;
```

#### release_processing_lock
Releases a processing lock for an entity.
```sql
CREATE OR REPLACE FUNCTION public.release_processing_lock(p_entity_type TEXT, p_entity_id TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_current_state TEXT;
BEGIN
  -- Use advisory lock to prevent concurrent execution
  PERFORM pg_advisory_xact_lock(
    ('x' || substr(md5(p_entity_type || ':' || p_entity_id), 1, 16))::bit(64)::bigint
  );
  
  -- Check current state
  SELECT state INTO v_current_state
  FROM public.processing_status
  WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
  
  -- Only release if in IN_PROGRESS state
  IF v_current_state = 'IN_PROGRESS' THEN
    -- Set to PENDING state to allow reprocessing
    UPDATE public.processing_status
    SET 
      state = 'PENDING',
      last_processed_at = NOW(),
      updated_at = NOW()
    WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
    
    RETURN TRUE;
  END IF;
  
  RETURN FALSE;
END;
$$;
```

#### force_release_entity_lock
Forces release of a processing lock for an entity.
```sql
CREATE OR REPLACE FUNCTION public.force_release_entity_lock(p_entity_type TEXT, p_entity_id TEXT)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
    lock_record RECORD;
    result JSONB;
BEGIN
    -- Get current lock info
    SELECT * INTO lock_record 
    FROM public.processing_locks
    WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
    
    -- Delete the lock
    DELETE FROM public.processing_locks
    WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
    
    -- Update processing status
    UPDATE public.processing_status
    SET 
        state = 'PENDING',
        last_processed_at = NOW(),
        metadata = jsonb_set(
            COALESCE(metadata, '{}'::jsonb),
            '{force_released}',
            jsonb_build_object(
                'previous_state', state,
                'released_at', NOW(),
                'previous_lock', lock_record
            )
        )
    WHERE 
        entity_type = p_entity_type
        AND entity_id = p_entity_id;
    
    -- Build result
    IF lock_record IS NULL THEN
        result := jsonb_build_object(
            'released', FALSE,
            'message', 'No lock found for this entity'
        );
    ELSE
        result := jsonb_build_object(
            'released', TRUE,
            'previous_lock', to_jsonb(lock_record)
        );
    END IF;
    
    RETURN result;
END;
$$;
```

### Monitoring Functions

#### get_system_health
Returns a comprehensive system health report.
```sql
CREATE OR REPLACE FUNCTION public.get_system_health()
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  result JSONB;
  queue_health JSONB;
  processing_health JSONB;
  worker_health JSONB;
BEGIN
  -- Check queue health
  WITH queue_stats AS (
    SELECT
      COUNT(*) AS total_queues,
      SUM(CASE WHEN stuck_messages > 0 THEN 1 ELSE 0 END) AS queues_with_stuck_messages,
      SUM(stuck_messages) AS total_stuck_messages,
      MAX(last_check_time) AS last_check_time
    FROM
      public.queue_monitoring_view
  )
  SELECT 
    jsonb_build_object(
      'status', CASE
        WHEN total_stuck_messages > 10 THEN 'critical'
        WHEN total_stuck_messages > 0 THEN 'warning'
        ELSE 'healthy'
      END,
      'queues_checked', total_queues,
      'queues_with_issues', queues_with_stuck_messages,
      'total_stuck_messages', total_stuck_messages,
      'last_check_time', last_check_time
    )
  INTO queue_health
  FROM queue_stats;
  
  -- Check processing health
  WITH processing_stats AS (
    SELECT
      COUNT(*) AS total_entities,
      SUM(CASE WHEN state = 'IN_PROGRESS' AND last_processed_at < NOW() - INTERVAL '30 minutes' THEN 1 ELSE 0 END) AS stuck_entities,
      SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS failed_entities
    FROM
      public.processing_status
    WHERE
      updated_at > NOW() - INTERVAL '24 hours'
  )
  SELECT
    jsonb_build_object(
      'status', CASE
        WHEN stuck_entities > 5 OR failed_entities > 10 THEN 'critical'
        WHEN stuck_entities > 0 OR failed_entities > 0 THEN 'warning'
        ELSE 'healthy'
      END,
      'entities_checked', total_entities,
      'stuck_entities', stuck_entities,
      'failed_entities', failed_entities
    )
  INTO processing_health
  FROM processing_stats;
  
  -- Check worker health
  WITH worker_issues AS (
    SELECT
      COUNT(*) AS total_issues,
      COUNT(DISTINCT worker_name) AS workers_with_issues,
      MAX(created_at) AS latest_issue_time
    FROM
      monitoring.worker_issues
    WHERE
      resolved = FALSE
  )
  SELECT
    jsonb_build_object(
      'status', CASE
        WHEN total_issues > 5 THEN 'critical'
        WHEN total_issues > 0 THEN 'warning'
        ELSE 'healthy'
      END,
      'total_issues', total_issues,
      'workers_with_issues', workers_with_issues,
      'latest_issue_time', latest_issue_time
    )
  INTO worker_health
  FROM worker_issues;
  
  -- Combine all health statuses
  result := jsonb_build_object(
    'timestamp', NOW(),
    'queue_health', queue_health,
    'processing_health', processing_health,
    'worker_health', worker_health,
    'overall_status', CASE
      WHEN (queue_health->>'status' = 'critical') OR 
           (processing_health->>'status' = 'critical') OR 
           (worker_health->>'status' = 'critical') THEN 'critical'
      WHEN (queue_health->>'status' = 'warning') OR 
           (processing_health->>'status' = 'warning') OR 
           (worker_health->>'status' = 'warning') THEN 'warning'
      ELSE 'healthy'
    END
  );
  
  RETURN result;
END;
$$;
```

#### check_artist_pipeline_progress
Checks the progress of the pipeline for a specific artist.
```sql
CREATE OR REPLACE FUNCTION public.check_artist_pipeline_progress(artist_id UUID)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  artist_rec RECORD;
  album_count INTEGER;
  track_count INTEGER;
  producer_count INTEGER;
  result JSONB;
BEGIN
  -- Get basic artist info
  SELECT name, spotify_id INTO artist_rec
  FROM artists 
  WHERE id = artist_id;
  
  -- Count related entities
  SELECT COUNT(*) INTO album_count
  FROM albums
  WHERE artist_id = check_artist_pipeline_progress.artist_id;
  
  SELECT COUNT(*) INTO track_count
  FROM tracks t
  JOIN albums a ON t.album_id = a.id
  WHERE a.artist_id = check_artist_pipeline_progress.artist_id;
  
  SELECT COUNT(DISTINCT p.id) INTO producer_count
  FROM producers p
  JOIN track_producers tp ON p.id = tp.producer_id
  JOIN tracks t ON tp.track_id = t.id
  JOIN albums a ON t.album_id = a.id
  WHERE a.artist_id = check_artist_pipeline_progress.artist_id;
  
  -- Build status result
  result := jsonb_build_object(
    'artist_id', artist_id,
    'artist_name', artist_rec.name,
    'spotify_id', artist_rec.spotify_id,
    'album_count', album_count,
    'track_count', track_count,
    'producer_count', producer_count,
    'timestamp', now(),
    'status', CASE
      WHEN album_count = 0 THEN 'no_albums'
      WHEN track_count = 0 THEN 'no_tracks'
      WHEN producer_count = 0 THEN 'no_producers'
      ELSE 'complete'
    END
  );
  
  RETURN result;
END;
$$;
```

#### record_problematic_message
Records a problematic message for later inspection.
```sql
CREATE OR REPLACE FUNCTION public.record_problematic_message(
  p_queue_name TEXT,
  p_message_id TEXT,
  p_message_body JSONB,
  p_error_type TEXT,
  p_error_details TEXT DEFAULT NULL
) RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  -- Make sure the schema and table exist first
  IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'queue_mgmt') THEN
    CREATE SCHEMA IF NOT EXISTS queue_mgmt;
    
    CREATE TABLE IF NOT EXISTS queue_mgmt.problematic_messages (
      id SERIAL PRIMARY KEY,
      queue_name TEXT NOT NULL,
      message_id TEXT NOT NULL,
      message_body JSONB,
      error_type TEXT NOT NULL,
      error_details TEXT,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
      resolved BOOLEAN DEFAULT FALSE,
      resolution_details JSONB
    );
    
    CREATE INDEX IF NOT EXISTS idx_problematic_messages_queue_message 
      ON queue_mgmt.problematic_messages(queue_name, message_id);
  END IF;

  INSERT INTO queue_mgmt.problematic_messages(
    queue_name, message_id, message_body, error_type, error_details
  ) VALUES (
    p_queue_name, p_message_id, p_message_body, p_error_type, p_error_details
  );
  
  EXCEPTION WHEN OTHERS THEN
    -- If schema doesn't exist yet or any other error, just log and continue
    RAISE NOTICE 'Failed to record problematic message: %', SQLERRM;
END;
$$;
```

#### validate_producer_identification_message
Validates a producer identification message.
```sql
CREATE OR REPLACE FUNCTION public.validate_producer_identification_message(message JSONB)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  -- Accept either trackId or track_id
  RETURN (message ? 'trackId' AND (message->>'trackId') IS NOT NULL AND (message->>'trackId')::TEXT != '') OR
         (message ? 'track_id' AND (message->>'track_id') IS NOT NULL AND (message->>'track_id')::TEXT != '');
END;
$$;
```

### Maintenance Functions

#### maintenance_clear_stale_entities
Clears stale entities from the system.
```sql
CREATE OR REPLACE FUNCTION public.maintenance_clear_stale_entities(
  p_stale_threshold_minutes INT DEFAULT 60
) RETURNS TABLE(entity_type TEXT, entity_id TEXT, state TEXT, action TEXT) LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  cleanup_error TEXT;
BEGIN
  -- Use exception handling to prevent failures
  BEGIN
    -- Step 1: Clear stale locks
    RETURN QUERY
    WITH deleted_locks AS (
      DELETE FROM public.processing_locks
      WHERE last_heartbeat < NOW() - (p_stale_threshold_minutes * INTERVAL '1 minute')
      RETURNING entity_type, entity_id, 'lock_deleted' AS action, 'IN_PROGRESS' AS state
    )
    SELECT * FROM deleted_locks;
  EXCEPTION WHEN OTHERS THEN
    -- Log the error but continue with the next step
    RAISE WARNING 'Error clearing stale locks: %', SQLERRM;
    cleanup_error := SQLERRM;
  END;

  -- Step 2: Reset stuck processing statuses (with separate exception handling)
  BEGIN
    RETURN QUERY
    WITH updated_statuses AS (
      UPDATE public.processing_status
      SET 
        state = 'PENDING',
        last_processed_at = NOW(),
        metadata = jsonb_set(
          COALESCE(metadata, '{}'::jsonb),
          '{auto_maintenance}',
          jsonb_build_object(
            'previous_state', state,
            'reset_at', NOW(),
            'previous_error', cleanup_error
          )
        )
      WHERE 
        state = 'IN_PROGRESS'
        AND last_processed_at < NOW() - (p_stale_threshold_minutes * INTERVAL '1 minute')
      RETURNING entity_type, entity_id, state, 'status_reset' AS action
    )
    SELECT * FROM updated_statuses;
  EXCEPTION WHEN OTHERS THEN
    -- Log this error too
    RAISE WARNING 'Error resetting processing statuses: %', SQLERRM;
    
    -- Return at least one row with the error information
    entity_type := 'error';
    entity_id := 'maintenance';
    state := 'ERROR';
    action := 'Error: ' || SQLERRM;
    RETURN NEXT;
  END;
END;
$$;
```

#### cleanup_stale_entities
Cleans up stale entities and returns summary statistics.
```sql
CREATE OR REPLACE FUNCTION public.cleanup_stale_entities(
  p_stale_threshold_minutes INT DEFAULT 60
) RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  result JSONB;
  deleted_locks INT := 0;
  reset_statuses INT := 0;
  cleanup_error TEXT;
BEGIN
  -- Clean up stale locks
  BEGIN
    WITH deleted AS (
      DELETE FROM public.processing_locks
      WHERE last_heartbeat < NOW() - (p_stale_threshold_minutes * INTERVAL '1 minute')
      RETURNING entity_type, entity_id
    )
    SELECT COUNT(*) INTO deleted_locks FROM deleted;
  EXCEPTION WHEN OTHERS THEN
    cleanup_error := SQLERRM;
  END;

  -- Reset stuck processing statuses
  BEGIN
    WITH updated AS (
      UPDATE public.processing_status
      SET 
        state = 'PENDING',
        last_processed_at = NOW(),
        metadata = jsonb_set(
          COALESCE(metadata, '{}'::jsonb),
          '{auto_maintenance}',
          jsonb_build_object(
            'previous_state', state,
            'reset_at', NOW(),
            'previous_error', cleanup_error
          )
        )
      WHERE 
        state = 'IN_PROGRESS'
        AND last_processed_at < NOW() - (p_stale_threshold_minutes * INTERVAL '1 minute')
      RETURNING entity_type, entity_id
    )
    SELECT COUNT(*) INTO reset_statuses FROM updated;
  EXCEPTION WHEN OTHERS THEN
    IF cleanup_error IS NULL THEN
      cleanup_error := SQLERRM;
    ELSE
      cleanup_error := cleanup_error || ' | ' || SQLERRM;
    END IF;
  END;

  -- Build result
  result := jsonb_build_object(
    'deleted_locks', deleted_locks,
    'reset_statuses', reset_statuses,
    'timestamp', NOW()
  );
  
  IF cleanup_error IS NOT NULL THEN
    result := result || jsonb_build_object('error', cleanup_error);
  END IF;
  
  RETURN result;
END;
$$;
```

#### purge_old_monitoring_events
Purges old monitoring events past a retention period.
```sql
CREATE OR REPLACE FUNCTION public.purge_old_monitoring_events(retention_days INTEGER DEFAULT 30)
RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  deleted_count INTEGER;
BEGIN
  DELETE FROM public.monitoring_events
  WHERE created_at < NOW() - (retention_days * INTERVAL '1 day');
  
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  RETURN deleted_count;
END;
$$;
```

## Scheduled Jobs (Cron)

### Queue Worker Jobs

#### artist-discovery-queue-job
Runs every minute to process the artist discovery queue.
```sql
SELECT cron.schedule(
  'artist-discovery-queue-job',
  '* * * * *', -- Every minute
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/artistDiscovery',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"batchSize":5}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);
```

#### album-discovery-queue-job
Runs every 2 minutes to process the album discovery queue.
```sql
SELECT cron.schedule(
  'album-discovery-queue-job',
  '*/2 * * * *', -- Every 2 minutes
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/albumDiscovery',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"batchSize":5}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);
```

#### track-discovery-queue-job-a
Runs on a schedule to process the track discovery queue (worker A).
```sql
SELECT cron.schedule(
  'track-discovery-queue-job-a',
  '0,10,20,30,40,50 * * * *', -- Every 10 minutes starting at 0
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/trackDiscovery',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"batchSize":5,"instanceId":"worker-a"}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);
```

#### track-discovery-queue-job-b
Runs on a staggered schedule to process the track discovery queue (worker B).
```sql
SELECT cron.schedule(
  'track-discovery-queue-job-b',
  '5,15,25,35,45,55 * * * *', -- Every 10 minutes starting at 5
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/trackDiscovery',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"batchSize":5,"instanceId":"worker-b"}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);
```

#### producer-identification-queue-job
Runs every 5 minutes to process the producer identification queue.
```sql
SELECT cron.schedule(
  'producer-identification-queue-job',
  '*/5 * * * *', -- Every 5 minutes
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/producerIdentification',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"batchSize":5}'::jsonb,
      timeout_milliseconds:= 25000
    ) as request_id;
  $$
);
```

### Monitoring and Maintenance Jobs

#### queue-auto-fix-job
Runs every 3 minutes to monitor and fix queue issues.
```sql
SELECT cron.schedule(
  'queue-auto-fix-job',
  '*/3 * * * *', -- Every 3 minutes
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/queueMonitor',
      headers:= json_build_object(
        'Content-type', 'application/json', 
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"threshold_minutes":5,"auto_fix":true}'::jsonb,
      timeout_milliseconds:= 30000
    ) as request_id;
  $$
);
```

#### maintenance-clear-stale-entities
Runs every 30 minutes to clear stale entities.
```sql
SELECT cron.schedule(
  'maintenance-clear-stale-entities',
  '*/30 * * * *', -- Every 30 minutes
  $$
  BEGIN;
    -- Use transaction to prevent partial execution
    SELECT * FROM public.maintenance_clear_stale_entities(60);
    -- Log successful execution
    INSERT INTO monitoring_events (event_type, details) 
    VALUES ('maintenance_job', jsonb_build_object(
      'job', 'stale_locks_cleanup',
      'status', 'success',
      'executed_at', now()
    ));
  EXCEPTION WHEN OTHERS THEN
    -- Log failed execution
    INSERT INTO monitoring_events (event_type, details) 
    VALUES ('maintenance_job', jsonb_build_object(
      'job', 'stale_locks_cleanup',
      'status', 'error',
      'error', SQLERRM,
      'executed_at', now()
    ));
  END;
  $$
);
```

#### purge-old-monitoring-events
Runs weekly to purge old monitoring events.
```sql
SELECT cron.schedule(
  'purge-old-monitoring-events',
  '0 0 * * 0', -- Midnight every Sunday
  $$SELECT public.purge_old_monitoring_events(30)$$
);
```

#### pipeline-monitor-job
Runs every 15 minutes to check system health.
```sql
SELECT cron.schedule(
  'pipeline-monitor-job',
  '*/15 * * * *', -- Every 15 minutes
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/pipelineMonitor',
      headers:= json_build_object(
        'Content-type', 'application/json',
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{"operation":"recommendations"}'::jsonb,
      timeout_milliseconds:= 10000
    ) as request_id;
  $$
);
```

#### record-scaling-recommendations
Runs daily to record and analyze scaling recommendations.
```sql
SELECT cron.schedule(
  'record-scaling-recommendations',
  '0 0 * * *', -- Midnight every day
  $$SELECT public.record_scaling_recommendations()$$
);
```

#### auto-reset-stuck-messages
Runs every 5 minutes to reset stuck message visibility timeouts.
```sql
SELECT cron.schedule(
  'auto-reset-stuck-messages',
  '*/5 * * * *',  -- Run every 5 minutes
  $$
  SELECT * FROM reset_all_stuck_messages(15);
  $$
);
```

## Database Views

### Producer Popularity
Aggregates data to show producer popularity metrics.
- Producer ID and name
- Track count (total tracks produced)
- Artist count (distinct artists worked with)
- Average track popularity

### Queue Monitoring View
Provides a consolidated view of queue metrics:
- Queue name
- Total message count
- Stuck message count
- Messages fixed count
- Last check time

### DLQ Messages
View for exploring dead letter queue messages:
- Queue name and source queue
- Message ID and original message ID
- Move time and failure reason
- Custom metadata and full message content

## Indexes

Indexes have been created for:
- All foreign keys
- Frequently queried columns
- Text columns used in search operations
- Timestamp columns used for sorting or filtering

## Triggers

Automatic timestamp updates are implemented for all tables with `updated_at` columns.

## Common Patterns

### Idempotency
- Messages include idempotency keys (`_idempotencyKey`) to prevent duplicate processing
- Functions use ON CONFLICT clauses to safely handle repeated insertions
- Redis-based deduplication provides an additional layer of protection

### Error Handling
- Errors are logged to the `worker_issues` table
- Failed messages can be moved to dead letter queues
- Retry mechanisms are in place for transient failures

### Transaction Safety
- Critical operations are wrapped in transactions
- Process functions handle atomicity for batch operations
- DB locks prevent race conditions
