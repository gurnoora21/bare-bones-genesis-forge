
# Database Schema Documentation

This document provides a comprehensive overview of the database structure used in the Music Producer Discovery platform.

## Schema Organization

The database is organized into multiple schemas:

- `public`: Main application data
- `monitoring`: Metrics and monitoring data
- `queue_mgmt`: Queue management and problematic messages
- `pgmq`: PostgreSQL Message Queue implementation
- `cron`: Scheduled jobs
- `idempotency`: Tracks processed operations

## Public Schema

### Core Tables

#### artists
```sql
CREATE TABLE public.artists (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    spotify_id TEXT UNIQUE,
    followers INTEGER,
    popularity INTEGER,
    image_url TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

#### albums
```sql
CREATE TABLE public.albums (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    artist_id UUID NOT NULL REFERENCES artists(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    spotify_id TEXT UNIQUE,
    release_date DATE,
    cover_url TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

#### tracks
```sql
CREATE TABLE public.tracks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    album_id UUID NOT NULL REFERENCES albums(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    spotify_id TEXT UNIQUE,
    duration_ms INTEGER,
    popularity INTEGER,
    spotify_preview_url TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

#### normalized_tracks
```sql
CREATE TABLE public.normalized_tracks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    artist_id UUID NOT NULL REFERENCES artists(id) ON DELETE CASCADE,
    normalized_name TEXT NOT NULL,
    representative_track_id UUID REFERENCES tracks(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(artist_id, normalized_name)
);
```

#### producers
```sql
CREATE TABLE public.producers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    normalized_name TEXT NOT NULL UNIQUE,
    email TEXT,
    image_url TEXT,
    instagram_handle TEXT,
    instagram_bio TEXT,
    metadata JSONB,
    enriched_at TIMESTAMPTZ,
    enrichment_failed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

#### track_producers
```sql
CREATE TABLE public.track_producers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    track_id UUID NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
    producer_id UUID NOT NULL REFERENCES producers(id) ON DELETE CASCADE,
    confidence NUMERIC NOT NULL,
    source TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(track_id, producer_id)
);
```

### Processing Status Tables

#### processing_status
```sql
CREATE TABLE public.processing_status (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL, 
    state TEXT NOT NULL,
    last_processed_at TIMESTAMPTZ DEFAULT now(),
    last_error TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    attempts INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    dead_lettered BOOLEAN DEFAULT FALSE,
    UNIQUE(entity_type, entity_id)
);
```

#### processing_locks
```sql
CREATE TABLE public.processing_locks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    correlation_id TEXT,
    acquired_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    last_heartbeat TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    metadata JSONB DEFAULT '{}'::jsonb,
    ttl_seconds INTEGER NOT NULL DEFAULT 1800,
    UNIQUE (entity_type, entity_id)
);
```

### Queue Management Tables

#### queue_registry
```sql
CREATE TABLE public.queue_registry (
    id SERIAL PRIMARY KEY,
    queue_name TEXT NOT NULL,
    display_name TEXT,
    description TEXT,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

#### queue_metrics
```sql
CREATE TABLE public.queue_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    processed_count INTEGER,
    success_count INTEGER,
    error_count INTEGER,
    details JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Monitoring Tables

#### monitoring_events
```sql
CREATE TABLE public.monitoring_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL,
    details JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT now()
);
```

#### worker_issues
```sql
CREATE TABLE public.worker_issues (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    worker_name TEXT NOT NULL,
    issue_type TEXT NOT NULL,
    message TEXT NOT NULL,
    details JSONB,
    resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Configuration Tables

#### settings
```sql
CREATE TABLE public.settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

## Monitoring Schema

This schema contains tables dedicated to metrics collection and monitoring.

#### pipeline_metrics
```sql
CREATE TABLE monitoring.pipeline_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    tags JSONB DEFAULT '{}'::jsonb,
    timestamp TIMESTAMPTZ DEFAULT now(),
    created_at TIMESTAMPTZ DEFAULT now()
);
```

#### aggregated_metrics
```sql
CREATE TABLE monitoring.aggregated_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    metric_name TEXT NOT NULL,
    period TEXT NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    avg_value DOUBLE PRECISION,
    sum_value DOUBLE PRECISION,
    count_value INTEGER,
    dimensions JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT now()
);
```

#### worker_issues
```sql
CREATE TABLE monitoring.worker_issues (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    worker_name TEXT NOT NULL,
    issue_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    message TEXT,
    details JSONB,
    resolved BOOLEAN DEFAULT FALSE,
    resolution_details TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    resolved_at TIMESTAMPTZ
);
```

## Queue Management Schema

This schema contains queue management utilities.

#### problematic_messages
```sql
CREATE TABLE queue_mgmt.problematic_messages (
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
```

## Views

### producer_popularity
```sql
CREATE VIEW producer_popularity AS
SELECT
    p.id,
    p.name,
    COUNT(DISTINCT tp.track_id) AS track_count,
    COUNT(DISTINCT a.id) AS artist_count,
    AVG(t.popularity) AS avg_track_popularity
FROM
    producers p
    JOIN track_producers tp ON p.id = tp.producer_id
    JOIN tracks t ON tp.track_id = t.id
    JOIN albums al ON t.album_id = al.id
    JOIN artists a ON al.artist_id = a.id
GROUP BY
    p.id, p.name;
```

### queue_monitoring_view
```sql
CREATE OR REPLACE VIEW public.queue_monitoring_view AS
SELECT
  queue_name,
  MAX(details->>'totalMessagesInQueues')::INTEGER AS total_messages,
  MAX(details->>'totalStuckMessages')::INTEGER AS stuck_messages,
  MAX(details->>'messagesFixed')::INTEGER AS messages_fixed,
  MAX(created_at) AS last_check_time
FROM
  public.monitoring_events
WHERE
  event_type = 'queue_monitor'
  AND created_at > NOW() - INTERVAL '24 hours'
GROUP BY
  queue_name;
```

### dlq_messages
```sql
CREATE VIEW dlq_messages AS
SELECT 
  queue_name,
  source_queue,
  message_id,
  original_msg_id,
  moved_to_dlq_at,
  failure_reason,
  custom_metadata,
  full_message
FROM 
  get_dlq_messages();
```

### api_metrics
```sql
CREATE VIEW monitoring.api_metrics AS
SELECT
  tags->>'api' AS api_name,
  date_trunc('hour', timestamp) AS hour,
  COUNT(*) AS total_calls,
  SUM(CASE WHEN tags->>'success' = 'true' THEN 1 ELSE 0 END) AS success_count,
  SUM(CASE WHEN tags->>'success' = 'false' THEN 1 ELSE 0 END) AS error_count,
  AVG(CASE WHEN tags->>'success' = 'true' THEN metric_value ELSE NULL END) AS avg_success_time_ms,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY metric_value) AS p95_time_ms
FROM
  monitoring.pipeline_metrics
WHERE
  metric_name = 'api_call'
  AND timestamp > now() - interval '7 days'
GROUP BY
  tags->>'api', date_trunc('hour', timestamp)
ORDER BY
  api_name, hour DESC;
```

## Important Database Functions

### Queue Management Functions

#### validate_queue_message
```sql
CREATE OR REPLACE FUNCTION public.validate_queue_message(
  p_queue_name TEXT,
  p_message JSONB
) RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_is_valid BOOLEAN := FALSE;
  v_error TEXT := 'Unknown message format';
BEGIN
  -- Switch validation based on queue name
  IF p_queue_name = 'album_discovery' THEN
    v_is_valid := public.validate_album_discovery_message(p_message);
    IF NOT v_is_valid THEN
      v_error := 'Invalid album discovery message: missing or empty artistId';
    END IF;
  ELSIF p_queue_name = 'producer_identification' THEN
    v_is_valid := public.validate_producer_identification_message(p_message);
    IF NOT v_is_valid THEN
      v_error := 'Invalid producer identification message: missing or empty trackId';
    END IF;
  ELSIF p_queue_name = 'track_discovery' THEN
    -- Track discovery validation
    v_is_valid := p_message ? 'albumId' AND (p_message->>'albumId') IS NOT NULL;
    IF NOT v_is_valid THEN
      v_error := 'Invalid track discovery message: missing or empty albumId';
    END IF;
  ELSE
    -- Default validation - just check it's not null or empty object
    v_is_valid := p_message IS NOT NULL AND p_message != '{}'::JSONB;
    IF NOT v_is_valid THEN
      v_error := 'Empty or null message';
    END IF;
  END IF;
  
  -- Return validation result
  IF v_is_valid THEN
    RETURN jsonb_build_object('valid', TRUE);
  ELSE
    RETURN jsonb_build_object(
      'valid', FALSE,
      'error', v_error,
      'queue', p_queue_name
    );
  END IF;
END;
$$;
```

#### move_to_dead_letter_queue
```sql
CREATE OR REPLACE FUNCTION public.move_to_dead_letter_queue(
  source_queue TEXT,
  dlq_name TEXT,
  message_id TEXT,
  failure_reason TEXT,
  metadata JSONB DEFAULT NULL
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
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

### Locking Functions

#### acquire_processing_lock
```sql
CREATE OR REPLACE FUNCTION public.acquire_processing_lock(
  p_entity_type TEXT,
  p_entity_id TEXT,
  p_timeout_minutes INT DEFAULT 30,
  p_correlation_id TEXT DEFAULT NULL
)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
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

### Data Processing Functions

#### process_track_batch
```sql
CREATE OR REPLACE FUNCTION public.process_track_batch(
  p_track_data JSONB[],
  p_album_id UUID,
  p_artist_id UUID
) RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
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

### Monitoring & Maintenance

#### get_system_health
```sql
CREATE OR REPLACE FUNCTION public.get_system_health()
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
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

#### maintenance_clear_stale_entities
```sql
CREATE OR REPLACE FUNCTION public.maintenance_clear_stale_entities()
RETURNS TABLE(entity_type text, entity_id text, state text, action text)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  -- Clear stale locks
  DELETE FROM public.processing_locks
  WHERE last_heartbeat < NOW() - INTERVAL '1 hour'
  RETURNING entity_type, entity_id, 'lock_deleted' AS action;
  
  -- Reset stuck processing statuses
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
          'reset_at', NOW()
        )
      )
    WHERE 
      state = 'IN_PROGRESS'
      AND last_processed_at < NOW() - INTERVAL '1 hour'
    RETURNING entity_type, entity_id, state, 'status_reset' AS action
  )
  SELECT * FROM updated_statuses;
END;
$$;
```

## PostgreSQL Message Queue (pgmq)

This is a PostgreSQL extension that implements message queue functionality directly in the database.

```sql
-- Sample functions available in pgmq
-- pgmq.create(queue_name)      -- Create a new queue
-- pgmq.send(queue_name, msg)   -- Send a message to a queue
-- pgmq.read(queue_name, n)     -- Read n messages from a queue 
-- pgmq.delete(queue_name, id)  -- Delete a message from a queue
-- pgmq.archive(queue_name, id) -- Archive a message
-- pgmq.metadata(queue_name)    -- Get queue metadata
```

## Scheduled Jobs

The system uses PostgreSQL's `pg_cron` extension to schedule regular jobs:

```sql
-- Examples of scheduled jobs
-- Every 2 minutes: Artist discovery
SELECT cron.schedule('artist-discovery', '*/2 * * * *', 'SELECT supabase.functions.invoke(''artistDiscovery'')');

-- Every 5 minutes: Album discovery and Track discovery
SELECT cron.schedule('album-discovery', '*/5 * * * *', 'SELECT supabase.functions.invoke(''albumDiscovery'')');
SELECT cron.schedule('track-discovery', '*/5 * * * *', 'SELECT supabase.functions.invoke(''trackDiscovery'')');

-- Every 10 minutes: Producer identification
SELECT cron.schedule('producer-identification', '*/10 * * * *', 'SELECT supabase.functions.invoke(''producerIdentification'')');

-- Every hour: Social enrichment
SELECT cron.schedule('social-enrichment', '0 * * * *', 'SELECT supabase.functions.invoke(''socialEnrichment'')');

-- Maintenance jobs
SELECT cron.schedule('stale-locks-cleanup-job', '*/10 * * * *', 'SELECT * FROM public.cleanup_stale_locks(300)');
SELECT cron.schedule('purge-old-monitoring-events', '0 0 * * 0', 'SELECT public.purge_old_monitoring_events(30)');
SELECT cron.schedule('hourly-metrics-aggregation', '5 * * * *', 'SELECT monitoring.aggregate_hourly_metrics()');
SELECT cron.schedule('daily-metrics-aggregation', '15 0 * * *', 'SELECT monitoring.aggregate_daily_metrics()');
```

## Common Database Interactions

### Adding an Artist to the Discovery Pipeline

```sql
-- Start artist discovery for a specific artist
SELECT start_artist_discovery('Artist Name');

-- Start bulk discovery for multiple artists based on genre
SELECT * FROM start_bulk_artist_discovery('pop', 70, 10);
```

### Monitoring System Health

```sql
-- Get system health overview
SELECT * FROM get_system_health();

-- Get queue statistics
SELECT * FROM queue_monitoring_view;

-- Check processing status
SELECT * FROM get_processing_stats();

-- Get worker issues
SELECT * FROM get_worker_issue_stats();
```

### Finding Stuck Processing States

```sql
-- Find inconsistent processing states
SELECT * FROM find_inconsistent_states();

-- Reset stuck entities
SELECT * FROM reset_entity_processing_state('track', 60);

-- Clean up stale locks
SELECT * FROM cleanup_stale_locks(300);
```
