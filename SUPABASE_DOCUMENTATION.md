
# Supabase Database Functions & Cron Jobs Documentation

This document provides an overview of all database functions, cron jobs, and key components of the music producer discovery system implemented in Supabase.

## Table of Contents
- [System Overview](#system-overview)
- [Database Functions](#database-functions)
  - [Queue Management Functions](#queue-management-functions)
  - [Artist Discovery Functions](#artist-discovery-functions)
  - [Album & Track Processing Functions](#album--track-processing-functions)
  - [Producer Functions](#producer-functions)
  - [State Management Functions](#state-management-functions)
  - [Advisory Lock Functions](#advisory-lock-functions)
  - [Monitoring and Diagnostics Functions](#monitoring-and-diagnostics-functions)
- [Cron Jobs](#cron-jobs)
- [Edge Functions](#edge-functions)
- [Database Schema](#database-schema)

## System Overview

This system is a music producer discovery pipeline built on Supabase that:

1. Discovers artists from Spotify
2. Retrieves their albums and tracks
3. Identifies music producers and writers
4. Enriches producer profiles with social media information
5. Makes this data searchable via an API

The system uses a queue-based architecture with Supabase Queues (PGMQ) to process messages asynchronously, ensuring reliability and fault tolerance.

## Database Functions

### Queue Management Functions

#### `pg_enqueue(queue_name TEXT, message_body JSONB)`
Enqueues a JSON message to the specified queue.

```sql
CREATE OR REPLACE FUNCTION public.pg_enqueue(
  queue_name TEXT,
  message_body JSONB
) RETURNS BIGINT AS $$
DECLARE
  msg_id BIGINT;
BEGIN
  -- Attempt to use pgmq.send to send the message
  SELECT pgmq.send(queue_name, message_body) INTO msg_id;
  RETURN msg_id;
EXCEPTION WHEN OTHERS THEN
  RAISE EXCEPTION 'Failed to enqueue message: %', SQLERRM;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `pg_dequeue(queue_name TEXT, batch_size INT DEFAULT 5, visibility_timeout INT DEFAULT 60)`
Retrieves a batch of messages from the specified queue.

```sql
CREATE OR REPLACE FUNCTION public.pg_dequeue(
  queue_name TEXT,
  batch_size INT DEFAULT 5,
  visibility_timeout INT DEFAULT 60
) RETURNS JSONB AS $$
DECLARE
  result JSONB;
BEGIN
  -- Attempt to read messages from the queue
  SELECT jsonb_agg(msg) INTO result
  FROM pgmq.read(queue_name, batch_size, visibility_timeout) AS msg;
  
  RETURN COALESCE(result, '[]'::JSONB);
EXCEPTION WHEN OTHERS THEN
  RAISE WARNING 'Error in pg_dequeue: %', SQLERRM;
  RETURN '[]'::JSONB;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `pg_delete_message(queue_name TEXT, message_id TEXT)`
Deletes a message from the specified queue. Handles both UUID and numeric IDs.

```sql
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
```

#### `pg_release_message(queue_name TEXT, message_id UUID)`
Returns a message to the queue (making it available for processing again).

```sql
CREATE OR REPLACE FUNCTION public.pg_release_message(
  queue_name TEXT,
  message_id UUID
) RETURNS BOOLEAN AS $$
DECLARE
  success BOOLEAN;
BEGIN
  -- Set visibility timeout to NULL to make the message available again
  BEGIN
    EXECUTE format('
      UPDATE pgmq.q_%s
      SET vt = NULL
      WHERE id = $1
      RETURNING TRUE
    ', queue_name) 
    INTO success
    USING message_id;

    RETURN COALESCE(success, FALSE);
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error releasing message: %', SQLERRM;
    RETURN FALSE;
  END;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `reset_stuck_messages(queue_name TEXT, min_minutes_locked INT DEFAULT 10)`
Resets visibility timeout for messages that have been locked for too long.

```sql
CREATE OR REPLACE FUNCTION public.reset_stuck_messages(
  queue_name TEXT,
  min_minutes_locked INT DEFAULT 10
) RETURNS INT AS $$
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
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `ensure_message_deleted(queue_name TEXT, message_id TEXT, max_attempts INT DEFAULT 3)`
Makes multiple attempts to delete a message from a queue, with exponential backoff.

```sql
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
```

#### `register_queue(p_queue_name TEXT, p_display_name TEXT, p_description TEXT, p_active BOOLEAN)`
Registers a queue in the queue registry table and creates it in PGMQ if it doesn't exist.

```sql
CREATE OR REPLACE FUNCTION public.register_queue(
  p_queue_name TEXT,
  p_display_name TEXT,
  p_description TEXT,
  p_active BOOLEAN DEFAULT TRUE
) RETURNS INT AS $$
DECLARE
  v_id INT;
BEGIN
  -- Ensure the queue exists in PGMQ
  BEGIN
    PERFORM pgmq.create(p_queue_name);
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Queue % already exists or could not be created', p_queue_name;
  END;
  
  -- Register or update the queue in registry
  INSERT INTO public.queue_registry(
    queue_name, display_name, description, active
  ) VALUES (
    p_queue_name, p_display_name, p_description, p_active
  )
  ON CONFLICT (queue_name) 
  DO UPDATE SET
    display_name = p_display_name,
    description = p_description,
    active = p_active,
    updated_at = now()
  RETURNING id INTO v_id;
  
  RETURN v_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

### Artist Discovery Functions

#### `start_artist_discovery(artist_name TEXT)`
Enqueues a job to discover an artist by name.

```sql
CREATE OR REPLACE FUNCTION public.start_artist_discovery(artist_name TEXT)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
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

#### `start_bulk_artist_discovery(genre TEXT, min_popularity INT, limit_count INT)`
Starts discovery for multiple artists based on genre and popularity criteria.

```sql
CREATE OR REPLACE FUNCTION public.start_bulk_artist_discovery(
  genre TEXT,
  min_popularity INT DEFAULT 50,
  limit_count INT DEFAULT 20
)
RETURNS TABLE(artist_name TEXT, message_id BIGINT) AS $$
DECLARE
  artist_rec RECORD;
  msg_id BIGINT;
BEGIN
  FOR artist_rec IN (
    SELECT name 
    FROM public.artists 
    WHERE 
      metadata->>'genres' ? genre
      AND popularity >= min_popularity
    ORDER BY popularity DESC
    LIMIT limit_count
  )
  LOOP
    -- For each matching artist, start discovery
    SELECT public.start_artist_discovery(artist_rec.name) INTO msg_id;
    
    artist_name := artist_rec.name;
    message_id := msg_id;
    RETURN NEXT;
  END LOOP;
  
  RETURN;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

### Album & Track Processing Functions

#### `start_album_discovery(artist_id UUID, offset_val INT DEFAULT 0)`
Enqueues a job to discover albums for a specific artist, with pagination support.

```sql
CREATE OR REPLACE FUNCTION public.start_album_discovery(artist_id UUID, offset_val INT DEFAULT 0)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  msg_id BIGINT;
  idempotency_key TEXT;
BEGIN
  -- Create idempotency key
  idempotency_key := 'artist:' || artist_id || ':albums:offset:' || offset_val;
  
  -- Call pg_enqueue with a properly formatted JSONB object
  SELECT pg_enqueue(
    'album_discovery',
    jsonb_build_object(
      'artistId', artist_id,
      'offset', offset_val,
      '_idempotencyKey', idempotency_key
    )
  ) INTO msg_id;
  
  -- If no message ID was returned, raise an error
  IF msg_id IS NULL THEN
    RAISE EXCEPTION 'Failed to enqueue album discovery message';
  END IF;
  
  -- Log the operation to help with debugging
  INSERT INTO public.queue_metrics (
    queue_name, 
    operation, 
    success_count, 
    details
  ) VALUES (
    'album_discovery',
    'enqueue',
    1,
    jsonb_build_object(
      'artist_id', artist_id,
      'offset', offset_val,
      'message_id', msg_id,
      'idempotency_key', idempotency_key
    )
  );
  
  RETURN msg_id;
EXCEPTION WHEN OTHERS THEN
  -- Log the error
  INSERT INTO public.worker_issues (
    worker_name, 
    issue_type, 
    message, 
    details, 
    resolved
  ) VALUES (
    'start_album_discovery',
    'database_error',
    'Failed to enqueue album discovery: ' || SQLERRM,
    jsonb_build_object(
      'artist_id', artist_id,
      'offset', offset_val,
      'error', SQLERRM
    ),
    false
  );
  
  RAISE;
END;
$$;
```

#### `process_artist_atomic(p_artist_data JSONB, p_operation_id TEXT, p_spotify_id TEXT)`
Atomically processes and stores artist data with idempotency controls.

```sql
CREATE OR REPLACE FUNCTION public.process_artist_atomic(
  p_artist_data JSONB,
  p_operation_id TEXT,
  p_spotify_id TEXT
)
RETURNS UUID AS $$
DECLARE
  v_artist_id UUID;
  v_existing_id UUID;
  v_name TEXT;
  v_followers INTEGER;
  v_popularity INTEGER;
  v_metadata JSONB;
  v_image_url TEXT;
BEGIN
  -- Extract values from artist data
  v_name := p_artist_data->>'name';
  v_followers := (p_artist_data->>'followers')::INTEGER;
  v_popularity := (p_artist_data->>'popularity')::INTEGER;
  v_metadata := p_artist_data->'fullData';
  v_image_url := p_artist_data->>'image_url';

  -- Check if artist with this spotify_id already exists
  SELECT id INTO v_existing_id
  FROM public.artists
  WHERE spotify_id = p_spotify_id;

  IF v_existing_id IS NOT NULL THEN
    -- Update existing artist
    UPDATE public.artists
    SET 
      name = v_name,
      followers = v_followers,
      popularity = v_popularity,
      metadata = v_metadata,
      image_url = v_image_url,
      updated_at = now()
    WHERE id = v_existing_id;

    v_artist_id := v_existing_id;
  ELSE
    -- Insert new artist
    INSERT INTO public.artists(
      name, followers, popularity, metadata, spotify_id, image_url
    )
    VALUES (
      v_name, v_followers, v_popularity, v_metadata, p_spotify_id, v_image_url
    )
    RETURNING id INTO v_artist_id;
  END IF;

  -- Log the operation
  INSERT INTO public.queue_metrics(
    queue_name, operation, success_count, details
  )
  VALUES (
    'artist_discovery',
    'process',
    1,
    jsonb_build_object(
      'artist_id', v_artist_id,
      'spotify_id', p_spotify_id,
      'operation_id', p_operation_id,
      'name', v_name
    )
  );

  RETURN v_artist_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `process_album_atomic(p_album_data JSONB, p_operation_id TEXT, p_spotify_id TEXT)`
Atomically processes and stores album data with idempotency controls.

```sql
CREATE OR REPLACE FUNCTION public.process_album_atomic(
  p_album_data JSONB,
  p_operation_id TEXT,
  p_spotify_id TEXT
)
RETURNS UUID AS $$
DECLARE
  v_album_id UUID;
  v_existing_id UUID;
  v_name TEXT;
  v_artist_id UUID;
  v_release_date DATE;
  v_metadata JSONB;
  v_cover_url TEXT;
BEGIN
  -- Extract values from album data
  v_name := p_album_data->>'name';
  v_artist_id := (p_album_data->>'artist_id')::UUID;
  v_release_date := (p_album_data->>'release_date')::DATE;
  v_metadata := p_album_data->'fullData';
  v_cover_url := p_album_data->>'cover_url';

  -- Check if album with this spotify_id already exists
  SELECT id INTO v_existing_id
  FROM public.albums
  WHERE spotify_id = p_spotify_id;

  IF v_existing_id IS NOT NULL THEN
    -- Update existing album
    UPDATE public.albums
    SET 
      name = v_name,
      artist_id = v_artist_id,
      release_date = v_release_date,
      metadata = v_metadata,
      cover_url = v_cover_url,
      updated_at = now()
    WHERE id = v_existing_id;

    v_album_id := v_existing_id;
  ELSE
    -- Insert new album
    INSERT INTO public.albums(
      name, artist_id, release_date, metadata, spotify_id, cover_url
    )
    VALUES (
      v_name, v_artist_id, v_release_date, v_metadata, p_spotify_id, v_cover_url
    )
    RETURNING id INTO v_album_id;
  END IF;

  -- Log the operation
  INSERT INTO public.queue_metrics(
    queue_name, operation, success_count, details
  )
  VALUES (
    'album_discovery',
    'process',
    1,
    jsonb_build_object(
      'album_id', v_album_id,
      'artist_id', v_artist_id,
      'spotify_id', p_spotify_id,
      'operation_id', p_operation_id,
      'name', v_name
    )
  );

  RETURN v_album_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `process_track_atomic(p_track_data JSONB, p_operation_id TEXT, p_spotify_id TEXT)`
Atomically processes and stores track data with idempotency controls.

```sql
CREATE OR REPLACE FUNCTION public.process_track_atomic(
  p_track_data JSONB,
  p_operation_id TEXT,
  p_spotify_id TEXT
)
RETURNS UUID AS $$
DECLARE
  v_track_id UUID;
  v_existing_id UUID;
  v_name TEXT;
  v_album_id UUID;
  v_duration_ms INTEGER;
  v_popularity INTEGER;
  v_metadata JSONB;
  v_preview_url TEXT;
BEGIN
  -- Extract values from track data
  v_name := p_track_data->>'name';
  v_album_id := (p_track_data->>'album_id')::UUID;
  v_duration_ms := (p_track_data->>'duration_ms')::INTEGER;
  v_popularity := (p_track_data->>'popularity')::INTEGER;
  v_metadata := p_track_data->'fullData';
  v_preview_url := p_track_data->>'preview_url';

  -- Check if track with this spotify_id already exists
  SELECT id INTO v_existing_id
  FROM public.tracks
  WHERE spotify_id = p_spotify_id;

  IF v_existing_id IS NOT NULL THEN
    -- Update existing track
    UPDATE public.tracks
    SET 
      name = v_name,
      album_id = v_album_id,
      duration_ms = v_duration_ms,
      popularity = v_popularity,
      metadata = v_metadata,
      spotify_preview_url = v_preview_url,
      updated_at = now()
    WHERE id = v_existing_id;

    v_track_id := v_existing_id;
  ELSE
    -- Insert new track
    INSERT INTO public.tracks(
      name, album_id, duration_ms, popularity, metadata, spotify_id, spotify_preview_url
    )
    VALUES (
      v_name, v_album_id, v_duration_ms, v_popularity, v_metadata, p_spotify_id, v_preview_url
    )
    RETURNING id INTO v_track_id;
  END IF;

  -- Log the operation
  INSERT INTO public.queue_metrics(
    queue_name, operation, success_count, details
  )
  VALUES (
    'track_discovery',
    'process',
    1,
    jsonb_build_object(
      'track_id', v_track_id,
      'album_id', v_album_id,
      'spotify_id', p_spotify_id,
      'operation_id', p_operation_id,
      'name', v_name
    )
  );

  RETURN v_track_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

### Producer Functions

#### `process_producer_atomic(p_producer_data JSONB, p_operation_id TEXT, p_normalized_name TEXT)`
Atomically processes and stores producer data with idempotency controls.

```sql
CREATE OR REPLACE FUNCTION public.process_producer_atomic(
  p_producer_data JSONB,
  p_operation_id TEXT,
  p_normalized_name TEXT
)
RETURNS UUID AS $$
DECLARE
  v_producer_id UUID;
  v_existing_id UUID;
  v_name TEXT;
  v_email TEXT;
  v_image_url TEXT;
  v_instagram_handle TEXT;
  v_instagram_bio TEXT;
  v_metadata JSONB;
BEGIN
  -- Extract values from producer data
  v_name := p_producer_data->>'name';
  v_email := p_producer_data->>'email';
  v_image_url := p_producer_data->>'image_url';
  v_instagram_handle := p_producer_data->>'instagram_handle';
  v_instagram_bio := p_producer_data->>'instagram_bio';
  v_metadata := p_producer_data->'metadata';

  -- Check if producer with this normalized name already exists
  SELECT id INTO v_existing_id
  FROM public.producers
  WHERE normalized_name = p_normalized_name;

  IF v_existing_id IS NOT NULL THEN
    -- Update existing producer with any non-NULL values
    UPDATE public.producers
    SET 
      name = COALESCE(v_name, name),
      email = COALESCE(v_email, email),
      image_url = COALESCE(v_image_url, image_url),
      instagram_handle = COALESCE(v_instagram_handle, instagram_handle),
      instagram_bio = COALESCE(v_instagram_bio, instagram_bio),
      metadata = CASE 
                   WHEN v_metadata IS NOT NULL THEN 
                     COALESCE(metadata, '{}'::jsonb) || v_metadata
                   ELSE metadata
                 END,
      enriched_at = CASE WHEN v_instagram_handle IS NOT NULL THEN now() ELSE enriched_at END,
      updated_at = now()
    WHERE id = v_existing_id;

    v_producer_id := v_existing_id;
  ELSE
    -- Insert new producer
    INSERT INTO public.producers(
      name, normalized_name, email, image_url, instagram_handle, instagram_bio, metadata
    )
    VALUES (
      v_name, p_normalized_name, v_email, v_image_url, v_instagram_handle, v_instagram_bio, v_metadata
    )
    RETURNING id INTO v_producer_id;
  END IF;

  -- Log the operation
  INSERT INTO public.queue_metrics(
    queue_name, operation, success_count, details
  )
  VALUES (
    'producer_identification',
    'process',
    1,
    jsonb_build_object(
      'producer_id', v_producer_id,
      'normalized_name', p_normalized_name,
      'operation_id', p_operation_id,
      'name', v_name
    )
  );

  RETURN v_producer_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `search_producers(search_term TEXT)`
Searches for producers by name and returns counts of their tracks and artist collaborations.

```sql
CREATE OR REPLACE FUNCTION public.search_producers(search_term TEXT)
RETURNS TABLE (
  id UUID, 
  name TEXT, 
  normalized_name TEXT, 
  instagram_handle TEXT,
  image_url TEXT,
  track_count BIGINT,
  artist_count BIGINT
) AS $$
BEGIN
  RETURN QUERY
  WITH producer_stats AS (
    SELECT
      p.id,
      COUNT(DISTINCT tp.track_id) AS track_count,
      COUNT(DISTINCT a.id) AS artist_count
    FROM
      producers p
      LEFT JOIN track_producers tp ON p.id = tp.producer_id
      LEFT JOIN tracks t ON tp.track_id = t.id
      LEFT JOIN albums al ON t.album_id = al.id
      LEFT JOIN artists a ON al.artist_id = a.id
    WHERE
      p.name ILIKE '%' || search_term || '%' OR
      p.normalized_name ILIKE '%' || search_term || '%'
    GROUP BY p.id
  )
  SELECT
    p.id,
    p.name,
    p.normalized_name,
    p.instagram_handle,
    p.image_url,
    COALESCE(ps.track_count, 0) AS track_count,
    COALESCE(ps.artist_count, 0) AS artist_count
  FROM
    producers p
    LEFT JOIN producer_stats ps ON p.id = ps.id
  WHERE
    p.name ILIKE '%' || search_term || '%' OR
    p.normalized_name ILIKE '%' || search_term || '%'
  ORDER BY
    ps.track_count DESC NULLS LAST,
    ps.artist_count DESC NULLS LAST,
    p.name ASC
  LIMIT 50;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `get_producer_collaborations(producer_id UUID)`
Returns a list of artists a producer has collaborated with and the track count.

```sql
CREATE OR REPLACE FUNCTION public.get_producer_collaborations(producer_id UUID)
RETURNS TABLE (
  artist_id UUID,
  artist_name TEXT,
  artist_image_url TEXT,
  track_count BIGINT,
  tracks JSONB
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    a.id AS artist_id,
    a.name AS artist_name,
    a.image_url AS artist_image_url,
    COUNT(DISTINCT t.id) AS track_count,
    jsonb_agg(jsonb_build_object(
      'id', t.id,
      'name', t.name,
      'album', al.name,
      'release_date', al.release_date
    )) AS tracks
  FROM
    track_producers tp
    JOIN tracks t ON tp.track_id = t.id
    JOIN albums al ON t.album_id = al.id
    JOIN artists a ON al.artist_id = a.id
  WHERE
    tp.producer_id = $1
  GROUP BY
    a.id, a.name, a.image_url
  ORDER BY
    track_count DESC,
    a.name ASC;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

### State Management Functions

#### `acquire_processing_lock(p_entity_type TEXT, p_entity_id TEXT, p_timeout_minutes INT DEFAULT 30, p_correlation_id TEXT DEFAULT NULL)`
Acquires a lock for processing an entity, handling concurrent access and timeouts.

```sql
CREATE OR REPLACE FUNCTION public.acquire_processing_lock(
  p_entity_type TEXT,
  p_entity_id TEXT,
  p_timeout_minutes INT DEFAULT 30,
  p_correlation_id TEXT DEFAULT NULL
) RETURNS BOOLEAN AS $$
DECLARE
  v_worker_id TEXT := gen_random_uuid()::TEXT;
  v_ttl_seconds INT := p_timeout_minutes * 60;
  v_existing_lock RECORD;
  v_is_stale BOOLEAN;
BEGIN
  -- Check for existing lock
  SELECT 
    * INTO v_existing_lock 
  FROM 
    processing_locks 
  WHERE 
    entity_type = p_entity_type 
    AND entity_id = p_entity_id;

  IF v_existing_lock IS NOT NULL THEN
    -- Check if existing lock is stale
    v_is_stale := v_existing_lock.last_heartbeat < (now() - interval '5 minutes');
    
    IF v_is_stale THEN
      -- Force release stale lock and acquire new one
      DELETE FROM processing_locks
      WHERE id = v_existing_lock.id;
    ELSE
      -- Still locked
      RETURN FALSE;
    END IF;
  END IF;

  -- Acquire new lock
  INSERT INTO processing_locks(
    entity_type, entity_id, worker_id, ttl_seconds, correlation_id
  ) VALUES (
    p_entity_type, p_entity_id, v_worker_id, v_ttl_seconds, p_correlation_id
  );

  RETURN TRUE;
EXCEPTION WHEN unique_violation THEN
  -- Another process acquired the lock concurrently
  RETURN FALSE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `release_processing_lock(p_entity_type TEXT, p_entity_id TEXT)`
Releases a processing lock for an entity.

```sql
CREATE OR REPLACE FUNCTION public.release_processing_lock(
  p_entity_type TEXT,
  p_entity_id TEXT
) RETURNS BOOLEAN AS $$
BEGIN
  DELETE FROM processing_locks
  WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
  
  RETURN FOUND;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `update_lock_heartbeat(p_entity_type TEXT, p_entity_id TEXT, p_worker_id TEXT, p_correlation_id TEXT)`
Updates heartbeat for a lock to prevent it from timing out during long-running operations.

```sql
CREATE OR REPLACE FUNCTION public.update_lock_heartbeat(
  p_entity_type TEXT,
  p_entity_id TEXT,
  p_worker_id TEXT,
  p_correlation_id TEXT
) RETURNS BOOLEAN AS $$
DECLARE
  v_updated BOOLEAN;
BEGIN
  UPDATE processing_locks
  SET last_heartbeat = now()
  WHERE 
    entity_type = p_entity_type 
    AND entity_id = p_entity_id
    AND worker_id = p_worker_id
    AND (p_correlation_id IS NULL OR correlation_id = p_correlation_id);
  
  GET DIAGNOSTICS v_updated = ROW_COUNT;
  RETURN v_updated > 0;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `claim_stale_lock(p_entity_type TEXT, p_entity_id TEXT, p_new_worker_id TEXT, p_correlation_id TEXT)`
Claims a lock that has become stale due to a failed worker.

```sql
CREATE OR REPLACE FUNCTION public.claim_stale_lock(
  p_entity_type TEXT,
  p_entity_id TEXT,
  p_new_worker_id TEXT,
  p_correlation_id TEXT
) RETURNS BOOLEAN AS $$
DECLARE
  v_claimed BOOLEAN;
BEGIN
  UPDATE processing_locks
  SET 
    worker_id = p_new_worker_id,
    correlation_id = COALESCE(p_correlation_id, correlation_id),
    last_heartbeat = now(),
    metadata = jsonb_set(
      COALESCE(metadata, '{}'::jsonb),
      '{lock_history}',
      COALESCE(metadata->'lock_history', '[]'::jsonb) || 
      jsonb_build_object(
        'previous_worker', worker_id,
        'claimed_at', now(),
        'claimed_by', p_new_worker_id
      )
    )
  WHERE
    entity_type = p_entity_type
    AND entity_id = p_entity_id
    AND last_heartbeat < (now() - interval '5 minutes');
    
  GET DIAGNOSTICS v_claimed = ROW_COUNT;
  RETURN v_claimed > 0;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `force_release_entity_lock(p_entity_type TEXT, p_entity_id TEXT)`
Forces release of an entity lock regardless of its state.

```sql
CREATE OR REPLACE FUNCTION public.force_release_entity_lock(
  p_entity_type TEXT,
  p_entity_id TEXT
) RETURNS BOOLEAN AS $$
DECLARE
  v_success BOOLEAN;
BEGIN
  -- Delete the lock record
  DELETE FROM processing_locks
  WHERE entity_type = p_entity_type AND entity_id = p_entity_id;
  
  -- Reset the processing status if it exists
  UPDATE processing_status
  SET 
    state = 'PENDING',
    metadata = jsonb_set(
      COALESCE(metadata, '{}'::jsonb),
      '{forced_reset}',
      jsonb_build_object(
        'reset_at', now(),
        'previous_state', state
      )
    ),
    updated_at = now()
  WHERE entity_type = p_entity_type AND entity_id = p_entity_id
  AND state = 'IN_PROGRESS';
  
  RETURN TRUE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

### Advisory Lock Functions

#### `pg_try_advisory_lock(p_key TEXT)`
Attempts to acquire a PostgreSQL advisory lock (non-blocking).

```sql
CREATE OR REPLACE FUNCTION public.pg_try_advisory_lock(p_key TEXT)
RETURNS BOOLEAN AS $$
DECLARE
  v_key_hash BIGINT;
BEGIN
  -- Convert the text key to a bigint hash
  v_key_hash := ('x' || md5(p_key))::bit(64)::bigint;
  
  -- Try to acquire the advisory lock
  RETURN pg_try_advisory_lock(v_key_hash);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `pg_advisory_lock_timeout(p_key TEXT, p_timeout_ms INTEGER)`
Attempts to acquire an advisory lock with a specified timeout.

```sql
CREATE OR REPLACE FUNCTION public.pg_advisory_lock_timeout(
  p_key TEXT,
  p_timeout_ms INTEGER
) RETURNS BOOLEAN AS $$
DECLARE
  v_key_hash BIGINT;
  v_start_time TIMESTAMPTZ;
  v_acquired BOOLEAN := FALSE;
BEGIN
  -- Convert the text key to a bigint hash
  v_key_hash := ('x' || md5(p_key))::bit(64)::bigint;
  
  -- Record start time
  v_start_time := clock_timestamp();
  
  -- Try to acquire the lock with timeout
  WHILE clock_timestamp() < v_start_time + (p_timeout_ms || ' milliseconds')::interval LOOP
    v_acquired := pg_try_advisory_lock(v_key_hash);
    
    IF v_acquired THEN
      RETURN TRUE;
    END IF;
    
    -- Sleep briefly before retry
    PERFORM pg_sleep(0.01);
  END LOOP;
  
  RETURN FALSE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `pg_advisory_unlock(p_key TEXT)`
Releases an advisory lock.

```sql
CREATE OR REPLACE FUNCTION public.pg_advisory_unlock(p_key TEXT)
RETURNS BOOLEAN AS $$
DECLARE
  v_key_hash BIGINT;
BEGIN
  -- Convert the text key to a bigint hash
  v_key_hash := ('x' || md5(p_key))::bit(64)::bigint;
  
  -- Release the advisory lock
  RETURN pg_advisory_unlock(v_key_hash);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `pg_advisory_lock_exists(p_key TEXT)`
Checks if an advisory lock exists.

```sql
CREATE OR REPLACE FUNCTION public.pg_advisory_lock_exists(p_key TEXT)
RETURNS BOOLEAN AS $$
DECLARE
  v_key_hash BIGINT;
  v_exists BOOLEAN;
BEGIN
  -- Convert the text key to a bigint hash
  v_key_hash := ('x' || md5(p_key))::bit(64)::bigint;
  
  -- Check if the lock exists
  SELECT EXISTS(
    SELECT 1
    FROM pg_locks
    WHERE locktype = 'advisory'
    AND objid = v_key_hash
    AND granted = TRUE
  ) INTO v_exists;
  
  RETURN v_exists;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `pg_force_advisory_unlock_all(p_key TEXT)`
Forces release of all advisory locks for a given key.

```sql
CREATE OR REPLACE FUNCTION public.pg_force_advisory_unlock_all(p_key TEXT)
RETURNS INTEGER AS $$
DECLARE
  v_key_hash BIGINT;
  v_count INTEGER := 0;
BEGIN
  -- Convert the text key to a bigint hash
  v_key_hash := ('x' || md5(p_key))::bit(64)::bigint;
  
  -- Force unlock all matching advisory locks
  LOOP
    IF pg_try_advisory_unlock(v_key_hash) THEN
      v_count := v_count + 1;
    ELSE
      EXIT;
    END IF;
  END LOOP;
  
  RETURN v_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

### Monitoring and Diagnostics Functions

#### `get_queue_table_name_safe(p_queue_name TEXT)`
Gets the correct table name for a queue, handling different naming patterns.

```sql
CREATE OR REPLACE FUNCTION public.get_queue_table_name_safe(p_queue_name TEXT)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  table_name TEXT;
  schema_table TEXT;
  pgmq_exists BOOLEAN;
  q_exists BOOLEAN;
  pgmq_exists BOOLEAN;
BEGIN
  -- Check if the pgmq schema exists
  SELECT EXISTS (
    SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgmq'
  ) INTO pgmq_exists;
  
  IF pgmq_exists THEN
    -- Check if the table exists in pgmq schema
    SELECT EXISTS (
      SELECT 1 FROM information_schema.tables 
      WHERE table_schema = 'pgmq' AND table_name = 'q_' || p_queue_name
    ) INTO q_exists;
    
    IF q_exists THEN
      RETURN 'pgmq.q_' || p_queue_name;
    END IF;
  END IF;
  
  -- Fallback to public schema with pgmq_ prefix
  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name = 'pgmq_' || p_queue_name
  ) INTO pgmq_exists;
  
  IF pgmq_exists THEN
    RETURN 'public.pgmq_' || p_queue_name;
  END IF;
  
  -- If no table exists, return the preferred naming convention for creation
  IF pgmq_exists THEN
    RETURN 'pgmq.q_' || p_queue_name;
  ELSE
    RETURN 'public.pgmq_' || p_queue_name;
  END IF;
END;
$$;
```

#### `list_stuck_messages(queue_name TEXT, min_minutes_locked INT DEFAULT 10)`
Lists messages that have been locked for longer than the specified time.

```sql
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
```

#### `diagnose_queue_tables(queue_name TEXT DEFAULT NULL)`
Provides diagnostic information about queue tables in the database.

```sql
CREATE OR REPLACE FUNCTION public.diagnose_queue_tables(queue_name TEXT DEFAULT NULL)
RETURNS TABLE (
  queue TEXT,
  total_messages BIGINT,
  in_progress_messages BIGINT,
  stuck_messages BIGINT,
  oldest_message_age_minutes NUMERIC
) AS $$
DECLARE
  q RECORD;
  total BIGINT;
  in_progress BIGINT;
  stuck BIGINT;
  oldest_minutes NUMERIC;
  queue_tables CURSOR FOR
    SELECT 
      table_schema, 
      table_name,
      CASE
        WHEN table_schema = 'pgmq' AND table_name LIKE 'q_%' THEN substring(table_name from 3)
        WHEN table_schema = 'public' AND table_name LIKE 'pgmq_%' THEN substring(table_name from 6)
        ELSE table_name
      END AS queue_name
    FROM information_schema.tables
    WHERE (table_name LIKE 'q_%' OR table_name LIKE 'pgmq_%')
    AND (queue_name IS NULL OR 
         (table_schema = 'pgmq' AND table_name = 'q_' || queue_name) OR
         (table_schema = 'public' AND table_name = 'pgmq_' || queue_name))
    ORDER BY table_schema, table_name;
BEGIN
  FOR q IN queue_tables LOOP
    -- Count total messages
    EXECUTE format('SELECT COUNT(*) FROM %I.%I', q.table_schema, q.table_name) INTO total;
    
    -- Count in progress messages
    EXECUTE format('SELECT COUNT(*) FROM %I.%I WHERE vt IS NOT NULL', q.table_schema, q.table_name) INTO in_progress;
    
    -- Count stuck messages (locked for > 10 minutes)
    EXECUTE format('
      SELECT COUNT(*) 
      FROM %I.%I 
      WHERE vt IS NOT NULL AND vt < NOW() - INTERVAL ''10 minutes''
    ', q.table_schema, q.table_name) INTO stuck;
    
    -- Get age of oldest message in minutes
    EXECUTE format('
      SELECT COALESCE(
        MAX(EXTRACT(EPOCH FROM (NOW() - vt))/60),
        0
      )
      FROM %I.%I
      WHERE vt IS NOT NULL
    ', q.table_schema, q.table_name) INTO oldest_minutes;
    
    queue := q.queue_name;
    total_messages := total;
    in_progress_messages := in_progress;
    stuck_messages := stuck;
    oldest_message_age_minutes := oldest_minutes;
    
    RETURN NEXT;
  END LOOP;
  
  RETURN;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `get_processing_stats()`
Returns statistics about entity processing states.

```sql
CREATE OR REPLACE FUNCTION public.get_processing_stats()
RETURNS TABLE (
  entity_type TEXT,
  total_count BIGINT,
  pending_count BIGINT,
  in_progress_count BIGINT,
  completed_count BIGINT,
  failed_count BIGINT,
  dead_lettered_count BIGINT,
  stalled_count BIGINT
) AS $$
BEGIN
  RETURN QUERY
  WITH base_stats AS (
    SELECT 
      ps.entity_type,
      COUNT(*) AS total_count,
      SUM(CASE WHEN ps.state = 'PENDING' THEN 1 ELSE 0 END) AS pending_count,
      SUM(CASE WHEN ps.state = 'IN_PROGRESS' THEN 1 ELSE 0 END) AS in_progress_count,
      SUM(CASE WHEN ps.state = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_count,
      SUM(CASE WHEN ps.state = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
      SUM(CASE WHEN ps.dead_lettered THEN 1 ELSE 0 END) AS dead_lettered_count
    FROM processing_status ps
    GROUP BY ps.entity_type
  ),
  stalled AS (
    SELECT
      ps.entity_type,
      COUNT(*) AS stalled_count
    FROM processing_status ps
    WHERE 
      ps.state = 'IN_PROGRESS' AND
      ps.last_processed_at < NOW() - INTERVAL '30 minutes'
    GROUP BY ps.entity_type
  )
  SELECT
    bs.entity_type,
    bs.total_count,
    bs.pending_count,
    bs.in_progress_count,
    bs.completed_count,
    bs.failed_count,
    bs.dead_lettered_count,
    COALESCE(s.stalled_count, 0) AS stalled_count
  FROM base_stats bs
  LEFT JOIN stalled s ON bs.entity_type = s.entity_type
  ORDER BY bs.total_count DESC;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `get_worker_issue_stats()`
Returns statistics about worker issues grouped by worker name and issue type.

```sql
CREATE OR REPLACE FUNCTION public.get_worker_issue_stats()
RETURNS TABLE (
  worker_name TEXT,
  issue_type TEXT,
  issue_count BIGINT,
  resolved_count BIGINT,
  unresolved_count BIGINT,
  latest_issue TIMESTAMP WITH TIME ZONE,
  latest_message TEXT
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    wi.worker_name,
    wi.issue_type,
    COUNT(*) AS issue_count,
    SUM(CASE WHEN wi.resolved THEN 1 ELSE 0 END) AS resolved_count,
    SUM(CASE WHEN NOT wi.resolved THEN 1 ELSE 0 END) AS unresolved_count,
    MAX(wi.created_at) AS latest_issue,
    (SELECT w.message FROM worker_issues w 
     WHERE w.worker_name = wi.worker_name 
     AND w.issue_type = wi.issue_type
     ORDER BY w.created_at DESC LIMIT 1) AS latest_message
  FROM worker_issues wi
  GROUP BY wi.worker_name, wi.issue_type
  ORDER BY latest_issue DESC;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `get_queue_monitoring_data()`
Returns monitoring data for all queues.

```sql
CREATE OR REPLACE FUNCTION public.get_queue_monitoring_data()
RETURNS TABLE (
  queue_name TEXT,
  total_messages BIGINT,
  stuck_messages BIGINT,
  last_check_time TIMESTAMP WITH TIME ZONE,
  messages_fixed INTEGER
) AS $$
BEGIN
  RETURN QUERY
  WITH queue_status AS (
    SELECT 
      q.queue_name,
      dt.total_messages,
      dt.stuck_messages
    FROM queue_registry q
    LEFT JOIN LATERAL (
      SELECT * FROM diagnose_queue_tables(q.queue_name) LIMIT 1
    ) dt ON TRUE
  ),
  monitoring_data AS (
    SELECT
      me.details->>'queue_name' AS queue_name,
      MAX(me.created_at) AS last_check_time,
      (array_agg(me.details->>'messagesReset' ORDER BY me.created_at DESC))[1]::INTEGER AS messages_fixed
    FROM monitoring_events me
    WHERE 
      me.event_type = 'queue_monitor' AND
      me.created_at > NOW() - INTERVAL '24 hours'
    GROUP BY me.details->>'queue_name'
  )
  SELECT
    qs.queue_name,
    COALESCE(qs.total_messages, 0) AS total_messages,
    COALESCE(qs.stuck_messages, 0) AS stuck_messages,
    md.last_check_time,
    COALESCE(md.messages_fixed, 0) AS messages_fixed
  FROM queue_status qs
  LEFT JOIN monitoring_data md ON qs.queue_name = md.queue_name
  ORDER BY qs.queue_name;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `get_system_health()`
Returns a comprehensive health check of the entire system.

```sql
CREATE OR REPLACE FUNCTION public.get_system_health()
RETURNS JSONB AS $$
DECLARE
  v_result JSONB;
BEGIN
  -- Gather health check components
  WITH queue_health AS (
    SELECT 
      jsonb_build_object(
        'total_queues', COUNT(*),
        'queues_with_stuck_messages', SUM(CASE WHEN stuck_messages > 0 THEN 1 ELSE 0 END),
        'total_stuck_messages', SUM(stuck_messages),
        'details', jsonb_agg(jsonb_build_object(
          'queue_name', queue_name,
          'total', total_messages,
          'stuck', stuck_messages
        ))
      ) AS data
    FROM diagnose_queue_tables()
  ),
  processing_health AS (
    SELECT
      jsonb_build_object(
        'entities_in_progress', SUM(in_progress_count),
        'entities_stalled', SUM(stalled_count),
        'entities_failed', SUM(failed_count),
        'entities_dead_lettered', SUM(dead_lettered_count),
        'details', jsonb_agg(jsonb_build_object(
          'entity_type', entity_type,
          'in_progress', in_progress_count,
          'stalled', stalled_count,
          'failed', failed_count
        ))
      ) AS data
    FROM get_processing_stats()
  ),
  worker_issues AS (
    SELECT
      jsonb_build_object(
        'total_unresolved_issues', SUM(unresolved_count),
        'workers_with_issues', COUNT(DISTINCT worker_name),
        'latest_issue_time', MAX(latest_issue),
        'details', jsonb_agg(jsonb_build_object(
          'worker_name', worker_name,
          'issue_type', issue_type,
          'unresolved', unresolved_count,
          'latest', latest_issue
        ))
      ) AS data
    FROM get_worker_issue_stats()
  )
  SELECT
    jsonb_build_object(
      'timestamp', now(),
      'overall_status', 
        CASE 
          WHEN (SELECT SUM(stuck_messages) FROM diagnose_queue_tables()) > 10 OR
               (SELECT SUM(stalled_count) FROM get_processing_stats()) > 10 OR
               (SELECT SUM(unresolved_count) FROM get_worker_issue_stats() WHERE latest_issue > now() - interval '1 hour') > 5
            THEN 'unhealthy'
          WHEN (SELECT SUM(stuck_messages) FROM diagnose_queue_tables()) > 0 OR
               (SELECT SUM(stalled_count) FROM get_processing_stats()) > 0 OR
               (SELECT SUM(unresolved_count) FROM get_worker_issue_stats() WHERE latest_issue > now() - interval '1 hour') > 0
            THEN 'warning'
          ELSE 'healthy'
        END,
      'queues', (SELECT data FROM queue_health),
      'processing', (SELECT data FROM processing_health),
      'worker_issues', (SELECT data FROM worker_issues)
    ) INTO v_result;
    
  RETURN v_result;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `cleanup_stale_entities(p_stale_threshold_minutes INT DEFAULT 60)`
Cleans up stale entities in both database and Redis.

```sql
CREATE OR REPLACE FUNCTION public.cleanup_stale_entities(
  p_stale_threshold_minutes INT DEFAULT 60
) RETURNS JSONB AS $$
DECLARE
  v_stale_locks_count INT;
  v_stale_states_count INT;
  v_result JSONB;
BEGIN
  -- Clean up stale locks
  DELETE FROM processing_locks
  WHERE last_heartbeat < now() - (p_stale_threshold_minutes || ' minutes')::interval
  RETURNING count(*) INTO v_stale_locks_count;
  
  -- Reset stale processing states
  UPDATE processing_status
  SET 
    state = 'PENDING',
    metadata = jsonb_set(
      COALESCE(metadata, '{}'::jsonb),
      '{cleanup_history}',
      COALESCE(metadata->'cleanup_history', '[]'::jsonb) || 
      jsonb_build_object(
        'cleaned_at', now(),
        'previous_state', state
      )
    )
  WHERE 
    state = 'IN_PROGRESS'
    AND last_processed_at < now() - (p_stale_threshold_minutes || ' minutes')::interval
  RETURNING count(*) INTO v_stale_states_count;
  
  -- Build result
  SELECT jsonb_build_object(
    'timestamp', now(),
    'stale_threshold_minutes', p_stale_threshold_minutes,
    'stale_locks_cleaned', v_stale_locks_count,
    'stale_states_reset', v_stale_states_count
  ) INTO v_result;
  
  -- Record the operation
  INSERT INTO monitoring_events(
    event_type, details
  ) VALUES (
    'stale_entity_cleanup',
    v_result
  );
  
  RETURN v_result;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `maintenance_clear_stale_entities(p_stale_threshold_minutes INT DEFAULT 60)`
Performs maintenance operations on stale entities.

```sql
CREATE OR REPLACE FUNCTION public.maintenance_clear_stale_entities(
  p_stale_threshold_minutes INT DEFAULT 60
) RETURNS JSONB AS $$
DECLARE
  v_result JSONB;
BEGIN
  -- Call the main cleanup function
  SELECT cleanup_stale_entities(p_stale_threshold_minutes) INTO v_result;
  
  -- Additional maintenance logic could be added here
  
  RETURN v_result;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### `cross_schema_queue_op(p_operation TEXT, p_queue_name TEXT, p_message_id TEXT, p_params JSONB)`
Performs operations on queue tables across different schemas.

```sql
CREATE OR REPLACE FUNCTION public.cross_schema_queue_op(
  p_operation TEXT,
  p_queue_name TEXT,
  p_message_id TEXT DEFAULT NULL,
  p_params JSONB DEFAULT '{}'::JSONB
) RETURNS JSONB AS $$
DECLARE
  v_queue_table TEXT;
  v_result JSONB;
  v_success BOOLEAN := FALSE;
BEGIN
  -- Try to get the queue table name
  BEGIN
    SELECT get_queue_table_name_safe(p_queue_name) INTO v_queue_table;
  EXCEPTION WHEN OTHERS THEN
    -- Default naming patterns
    IF EXISTS (SELECT 1 FROM information_schema.tables 
               WHERE table_schema = 'pgmq' AND table_name = 'q_' || p_queue_name) THEN
      v_queue_table := 'pgmq.q_' || p_queue_name;
    ELSE
      v_queue_table := 'public.pgmq_' || p_queue_name;
    END IF;
  END;
  
  -- Execute the requested operation
  CASE p_operation
    WHEN 'delete' THEN
      BEGIN
        IF p_message_id IS NOT NULL THEN
          EXECUTE format('
            DELETE FROM %s
            WHERE id::TEXT = $1 OR msg_id::TEXT = $1
            RETURNING TRUE
          ', v_queue_table)
          USING p_message_id
          INTO v_success;
          
          v_result := jsonb_build_object(
            'operation', 'delete',
            'queue', p_queue_name,
            'success', COALESCE(v_success, FALSE),
            'message_id', p_message_id,
            'queue_table', v_queue_table,
            'method', 'cross_schema_delete'
          );
        ELSE
          v_result := jsonb_build_object(
            'operation', 'delete',
            'success', FALSE,
            'error', 'Message ID required'
          );
        END IF;
      EXCEPTION WHEN OTHERS THEN
        v_result := jsonb_build_object(
          'operation', 'delete',
          'success', FALSE,
          'error', SQLERRM
        );
      END;
      
    WHEN 'reset' THEN
      BEGIN
        IF p_message_id IS NOT NULL THEN
          EXECUTE format('
            UPDATE %s
            SET vt = NULL
            WHERE id::TEXT = $1 OR msg_id::TEXT = $1
            RETURNING TRUE
          ', v_queue_table)
          USING p_message_id
          INTO v_success;
          
          v_result := jsonb_build_object(
            'operation', 'reset',
            'queue', p_queue_name,
            'success', COALESCE(v_success, FALSE),
            'message_id', p_message_id,
            'queue_table', v_queue_table,
            'method', 'visibility_timeout_reset'
          );
        ELSE
          v_result := jsonb_build_object(
            'operation', 'reset',
            'success', FALSE,
            'error', 'Message ID required'
          );
        END IF;
      EXCEPTION WHEN OTHERS THEN
        v_result := jsonb_build_object(
          'operation', 'reset',
          'success', FALSE,
          'error', SQLERRM
        );
      END;
      
    ELSE
      v_result := jsonb_build_object(
        'success', FALSE,
        'error', 'Unknown operation: ' || p_operation
      );
  END CASE;
  
  RETURN v_result;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

## Cron Jobs

The system uses several scheduled cron jobs to ensure reliable processing:

### Queue Processor Jobs

These jobs run on staggered schedules to process messages from various queues:

1. **General Queue Processor** (`cron-queue-processor-job`)
   - Schedule: Every 2 minutes
   - Invokes: `cronQueueProcessor` edge function
   - Purpose: Processes messages from all queues

2. **Artist Discovery** (`artist-discovery-queue-job`)
   - Schedule: Every 3 minutes
   - Processes: `artist_discovery` queue
   - Batch size: 15 messages

3. **Album Discovery** (`album-discovery-queue-job`)
   - Schedule: Every 4 minutes (offset by 1 minute)
   - Processes: `album_discovery` queue
   - Batch size: 15 messages

4. **Track Discovery** (`track-discovery-queue-job`)
   - Schedule: Every 4 minutes (offset by 2 minutes)
   - Processes: `track_discovery` queue
   - Batch size: 20 messages

5. **Producer Identification** (`producer-identification-queue-job`)
   - Schedule: Every 5 minutes
   - Processes: `producer_identification` queue
   - Batch size: 10 messages

6. **Social Enrichment** (`social-enrichment-queue-job`)
   - Schedule: Every 7 minutes
   - Processes: `social_enrichment` queue
   - Batch size: 10 messages

### Monitoring and Maintenance Jobs

1. **Queue Monitor** (`queue-monitor-job`, `auto-queue-monitor-job`, `queue-auto-fix-job`)
   - Schedule: Every 5 minutes
   - Invokes: `queueMonitor` edge function
   - Purpose: Monitors queues for stuck messages and fixes them

```sql
SELECT cron.schedule(
  'queue-auto-fix-job',
  '*/3 * * * *',  -- Run every 3 minutes
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

2. **Processing States Monitor** (`processing-states-monitor-job`)
   - Schedule: 10 minutes past each hour
   - Purpose: Resets processing states that have been stuck for more than 2 hours

```sql
SELECT cron.schedule(
  'processing-states-monitor-job',
  '10 * * * *',  -- Run at 10 minutes past every hour
  $$
  -- Reset processing states that have been stuck for more than 2 hours
  UPDATE public.processing_status
  SET 
    state = 'PENDING',
    metadata = jsonb_set(
      COALESCE(metadata, '{}'::jsonb),
      '{auto_cleanup}',
      jsonb_build_object(
        'previous_state', state,
        'cleaned_at', now()
      )
    ),
    updated_at = now()
  WHERE 
    state = 'IN_PROGRESS'
    AND last_processed_at < now() - interval '2 hours';
  $$
);
```

3. **Stale Entity Cleanup** (`maintenance-clear-stale-entities`)
   - Schedule: Every 30 minutes
   - Purpose: Cleans up stale entity locks and states

```sql
SELECT cron.schedule(
  'maintenance-clear-stale-entities',
  '*/30 * * * *',  -- Every 30 minutes
  $$
  SELECT public.maintenance_clear_stale_entities(60);
  $$
);
```

4. **Stale Entity Cleanup (Edge Function)** (`cronCleanupStaleEntities`)
   - Schedule: Every hour
   - Purpose: Comprehensive cleanup across database and Redis

```sql
SELECT cron.schedule(
  'stale-entity-cleanup-edge-function',
  '0 * * * *',  -- Every hour
  $$
  SELECT
    net.http_post(
      url:= 'https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/cronCleanupStaleEntities',
      headers:= json_build_object(
        'Content-type', 'application/json', 
        'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndzaGV0eG92eXh0ZnFvaGhidnBnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYwODI4OTIsImV4cCI6MjA2MTY1ODg5Mn0.tCQlhWOa0AFX4rcVUyVXFXBaG9Oeibn7N0cJbdmIwOs'
      )::jsonb,
      body:= '{}'::jsonb,
      timeout_milliseconds:= 30000
    );
  $$
);
```

## Edge Functions

Key edge functions in the system include:

1. **artistDiscovery**: Discovers artists from Spotify

```typescript
// Main logic for discovering artists via Spotify API
// This function processes messages from the artist_discovery queue
import { createClient } from "@supabase/supabase-js";
import { getSpotifyClient } from "../_shared/spotifyClient.ts";
import { StateManager } from "../_shared/stateManager.ts";

Deno.serve(async (req) => {
  console.log("Starting artist discovery worker process...");
  
  // Initialize clients
  const supabase = createClient(
    Deno.env.get("SUPABASE_URL") || "",
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || ""
  );
  
  const spotifyClient = await getSpotifyClient();
  const stateManager = new StateManager(supabase);
  
  // Process a batch of artist discovery messages
  try {
    const { data, error } = await supabase.rpc("pg_dequeue", {
      queue_name: "artist_discovery",
      batch_size: 5,
      visibility_timeout: 60
    });
    
    console.log(`Raw queue data received: ${typeof data} ${data ? data.length : null}`);
    
    if (error) throw error;
    if (!data || !data.length) {
      console.log("Retrieved 0 messages from queue");
      return new Response(JSON.stringify({success: true, processed: 0}));
    }
    
    const processed = [];
    const errors = [];
    
    // Process each artist discovery message
    for (const message of data) {
      try {
        const { artistName } = message.message;
        const messageId = message.id;
        
        // Search for artist on Spotify
        const searchResult = await spotifyClient.searchArtists(artistName);
        
        if (searchResult && searchResult.artists.items.length > 0) {
          const artist = searchResult.artists.items[0];
          
          // Process artist data atomically in database
          const { data: artistId } = await supabase.rpc("process_artist_atomic", {
            p_artist_data: {
              name: artist.name,
              followers: artist.followers.total,
              popularity: artist.popularity,
              fullData: artist,
              image_url: artist.images[0]?.url
            },
            p_operation_id: messageId,
            p_spotify_id: artist.id
          });
          
          // Queue album discovery for this artist
          await supabase.rpc("start_album_discovery", {
            artist_id: artistId,
            offset_val: 0
          });
          
          processed.push({
            id: messageId,
            artistId,
            artistName
          });
        }
        
        // Delete the message from the queue
        await supabase.rpc("pg_delete_message", {
          queue_name: "artist_discovery",
          message_id: messageId
        });
      } catch (messageError) {
        errors.push({
          message: message.id,
          error: messageError.message
        });
      }
    }
    
    return new Response(
      JSON.stringify({
        success: true,
        processed: processed.length,
        errors: errors.length,
        details: { processed, errors }
      })
    );
  } catch (error) {
    console.error("Error processing artist discovery:", error);
    return new Response(
      JSON.stringify({ success: false, error: error.message }),
      { status: 500 }
    );
  }
});
```

2. **albumDiscovery**: Retrieves album data for discovered artists
3. **trackDiscovery**: Retrieves track data from albums
4. **producerIdentification**: Identifies producers from track data
5. **socialEnrichment**: Enriches producer profiles with social data
6. **cronQueueProcessor**: Scheduled processor for all queues
7. **queueMonitor**: Monitors queue health and fixes issues
8. **clearQueueDeduplication**: Clears Redis deduplication keys
9. **checkPipeline**: Provides pipeline status information
10. **cronCleanupStaleEntities**: Scheduled cleanup for stale entities

## Database Schema

The main tables in the system are:

- **artists**: Stores artist information from Spotify
```sql
CREATE TABLE public.artists (
  id UUID PRIMARY KEY DEFAULT extensions.uuid_generate_v4(),
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  followers INTEGER,
  popularity INTEGER,
  metadata JSONB,
  spotify_id TEXT,
  image_url TEXT
);
```

- **albums**: Stores album information linked to artists
```sql
CREATE TABLE public.albums (
  id UUID PRIMARY KEY DEFAULT extensions.uuid_generate_v4(),
  name TEXT NOT NULL,
  artist_id UUID NOT NULL REFERENCES artists(id),
  release_date DATE,
  metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  cover_url TEXT,
  spotify_id TEXT
);
```

- **tracks**: Stores track information linked to albums
```sql
CREATE TABLE public.tracks (
  id UUID PRIMARY KEY DEFAULT extensions.uuid_generate_v4(),
  name TEXT NOT NULL,
  album_id UUID NOT NULL REFERENCES albums(id),
  duration_ms INTEGER,
  popularity INTEGER,
  metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  spotify_id TEXT,
  spotify_preview_url TEXT
);
```

- **producers**: Stores producer information
```sql
CREATE TABLE public.producers (
  id UUID PRIMARY KEY DEFAULT extensions.uuid_generate_v4(),
  name TEXT NOT NULL,
  normalized_name TEXT NOT NULL,
  email TEXT,
  image_url TEXT,
  instagram_handle TEXT,
  instagram_bio TEXT,
  metadata JSONB DEFAULT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  enriched_at TIMESTAMPTZ,
  enrichment_failed BOOLEAN DEFAULT false
);
```

- **track_producers**: Many-to-many relationship between tracks and producers
```sql
CREATE TABLE public.track_producers (
  id UUID PRIMARY KEY DEFAULT extensions.uuid_generate_v4(),
  track_id UUID NOT NULL REFERENCES tracks(id),
  producer_id UUID NOT NULL REFERENCES producers(id),
  confidence NUMERIC NOT NULL,
  source TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);
```

Supporting tables:

- **processing_status**: Tracks processing state of entities
```sql
CREATE TABLE public.processing_status (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  state TEXT NOT NULL,
  attempts INTEGER DEFAULT 0,
  last_processed_at TIMESTAMPTZ,
  last_error TEXT,
  metadata JSONB DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  dead_lettered BOOLEAN DEFAULT false
);
```

- **processing_locks**: Manages locks for concurrent processing
```sql
CREATE TABLE public.processing_locks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  correlation_id TEXT,
  ttl_seconds INTEGER NOT NULL DEFAULT 1800,
  acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB DEFAULT '{}'::jsonb
);
```

- **queue_registry**: Stores information about active queues
```sql
CREATE TABLE public.queue_registry (
  id SERIAL PRIMARY KEY,
  queue_name TEXT NOT NULL UNIQUE,
  display_name TEXT,
  description TEXT,
  active BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);
```

- **queue_metrics**: Records metrics about queue processing
```sql
CREATE TABLE public.queue_metrics (
  id UUID PRIMARY KEY DEFAULT extensions.uuid_generate_v4(),
  queue_name TEXT NOT NULL,
  operation TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  success_count INTEGER,
  error_count INTEGER,
  processed_count INTEGER,
  details JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);
```

- **monitoring_events**: Logs system events for auditing
```sql
CREATE TABLE public.monitoring_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_type TEXT NOT NULL,
  details JSONB DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ DEFAULT now()
);
```

- **worker_issues**: Records issues encountered by workers
```sql
CREATE TABLE public.worker_issues (
  id UUID PRIMARY KEY DEFAULT extensions.uuid_generate_v4(),
  worker_name TEXT NOT NULL,
  issue_type TEXT NOT NULL,
  message TEXT NOT NULL,
  details JSONB,
  resolved BOOLEAN DEFAULT false,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);
```

---

This documentation provides a comprehensive overview of the functions and cron jobs in the system. For more detailed information, refer to the SQL definitions and edge function code in the repository.
