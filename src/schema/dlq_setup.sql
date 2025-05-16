
-- Register the album discovery DLQ
SELECT public.register_queue(
  'album_discovery_dlq', 
  'Album Discovery DLQ', 
  'Dead letter queue for failed album discovery messages'
);

-- Create a view to explore DLQ messages
CREATE OR REPLACE VIEW public.dlq_messages AS
SELECT
  q.queue_name,
  (mqt.message->>'_dlq_metadata')::jsonb->'source_queue' as source_queue,
  mqt.id as message_id,
  mqt.msg_id as original_msg_id,
  mqt.created_at as moved_to_dlq_at,
  (mqt.message->>'_dlq_metadata')::jsonb->'failure_reason' as failure_reason,
  (mqt.message->>'_dlq_metadata')::jsonb->'custom_metadata' as custom_metadata,
  mqt.message as full_message
FROM
  queue_registry q
JOIN
  pgmq.get_queue_table_name(q.queue_name) mqt ON true
WHERE
  q.queue_name LIKE '%_dlq'
  AND mqt.message ? '_dlq_metadata';

-- Add a function to list all DLQ messages for analysis
CREATE OR REPLACE FUNCTION public.list_dlq_messages(
  p_queue_name TEXT DEFAULT NULL
)
RETURNS TABLE (
  queue_name TEXT,
  source_queue TEXT,
  message_id TEXT,
  moved_at TIMESTAMP WITH TIME ZONE,
  failure_reason TEXT,
  message JSONB
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  SELECT 
    d.queue_name,
    d.source_queue::TEXT,
    d.message_id::TEXT,
    d.moved_to_dlq_at,
    d.failure_reason::TEXT,
    d.full_message
  FROM 
    dlq_messages d
  WHERE
    (p_queue_name IS NULL OR d.queue_name = p_queue_name)
  ORDER BY
    d.moved_to_dlq_at DESC;
END;
$$;

-- Add stored procedure for atomic track processing
CREATE OR REPLACE FUNCTION public.process_track_batch(
  p_track_data JSONB[],
  p_album_id UUID,
  p_artist_id UUID
) 
RETURNS JSONB
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
