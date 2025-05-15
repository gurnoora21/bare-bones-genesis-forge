
-- Enhanced pipeline progress monitoring

-- Improve pipeline progress checking function with detailed status
CREATE OR REPLACE FUNCTION public.check_artist_pipeline_progress(artist_id UUID)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  artist_name TEXT;
  spotify_id TEXT;
  album_count INT;
  track_count INT;
  producer_count INT;
  expected_album_count INT;
  expected_track_count INT;
  status TEXT;
  progress FLOAT;
  result JSONB;
BEGIN
  -- Get artist info
  SELECT 
    a.name, 
    a.spotify_id, 
    COALESCE((a.metadata->>'total_albums')::INT, 0)
  INTO artist_name, spotify_id, expected_album_count
  FROM artists a
  WHERE id = artist_id;
  
  IF artist_name IS NULL THEN
    RETURN jsonb_build_object(
      'status', 'not_found',
      'message', 'Artist not found'
    );
  END IF;
  
  -- Get album count
  SELECT COUNT(*)
  INTO album_count
  FROM albums
  WHERE artist_id = check_artist_pipeline_progress.artist_id;
  
  -- Get track count
  SELECT COUNT(t.id)
  INTO track_count
  FROM tracks t
  JOIN albums a ON t.album_id = a.id
  WHERE a.artist_id = check_artist_pipeline_progress.artist_id;
  
  -- Expected track count based on albums we have
  SELECT COALESCE(SUM(
    CASE
      WHEN a.metadata->>'total_tracks' IS NOT NULL THEN (a.metadata->>'total_tracks')::INT
      ELSE 10 -- Assume average 10 tracks if not specified
    END
  ), 0)
  INTO expected_track_count
  FROM albums a
  WHERE a.artist_id = check_artist_pipeline_progress.artist_id;
  
  -- Get producer count
  SELECT COUNT(DISTINCT p.id)
  INTO producer_count
  FROM producers p
  JOIN track_producers tp ON p.id = tp.producer_id
  JOIN tracks t ON tp.track_id = t.id
  JOIN albums a ON t.album_id = a.id
  WHERE a.artist_id = check_artist_pipeline_progress.artist_id;
  
  -- Determine status and progress
  IF album_count = 0 THEN
    status := 'no_albums';
    progress := 0;
  ELSIF track_count = 0 THEN
    status := 'no_tracks';
    progress := 0.25;
  ELSIF producer_count = 0 THEN
    status := 'no_producers';
    progress := 0.5;
  ELSIF track_count < expected_track_count * 0.9 THEN
    status := 'tracks_incomplete';
    progress := 0.75;
  ELSE
    status := 'complete';
    progress := 1.0;
  END IF;
  
  -- Build detailed result
  result := jsonb_build_object(
    'artist_id', artist_id,
    'artist_name', artist_name,
    'spotify_id', spotify_id,
    'status', status,
    'progress', progress,
    'albums', jsonb_build_object(
      'count', album_count,
      'expected', expected_album_count,
      'completion', CASE 
        WHEN expected_album_count > 0 THEN 
          ROUND((album_count::FLOAT / expected_album_count) * 100)
        ELSE 100
      END
    ),
    'tracks', jsonb_build_object(
      'count', track_count,
      'expected', expected_track_count,
      'completion', CASE 
        WHEN expected_track_count > 0 THEN 
          ROUND((track_count::FLOAT / expected_track_count) * 100)
        ELSE 100
      END
    ),
    'producers', jsonb_build_object(
      'count', producer_count
    ),
    'timestamp', NOW()
  );
  
  RETURN result;
END;
$$;

-- Create a function to check progress for multiple artists
CREATE OR REPLACE FUNCTION public.check_artists_pipeline_progress(
  status_filter TEXT DEFAULT NULL, 
  limit_count INT DEFAULT 100,
  progress_min FLOAT DEFAULT NULL,
  progress_max FLOAT DEFAULT NULL
)
RETURNS TABLE(
  artist_id UUID,
  artist_name TEXT,
  status TEXT,
  progress FLOAT,
  albums_count INT,
  tracks_count INT,
  producers_count INT
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  WITH artist_stats AS (
    SELECT
      a.id AS artist_id,
      a.name AS artist_name,
      COUNT(DISTINCT al.id) AS albums_count,
      COUNT(DISTINCT t.id) AS tracks_count,
      COUNT(DISTINCT p.id) AS producers_count
    FROM
      artists a
      LEFT JOIN albums al ON a.id = al.artist_id
      LEFT JOIN tracks t ON al.id = t.album_id
      LEFT JOIN track_producers tp ON t.id = tp.track_id
      LEFT JOIN producers p ON tp.producer_id = p.id
    GROUP BY
      a.id, a.name
  ),
  pipeline_status AS (
    SELECT
      artist_id,
      artist_name,
      albums_count,
      tracks_count,
      producers_count,
      CASE
        WHEN albums_count = 0 THEN 'no_albums'
        WHEN tracks_count = 0 THEN 'no_tracks'
        WHEN producers_count = 0 THEN 'no_producers'
        ELSE 'complete'
      END AS status,
      CASE
        WHEN albums_count = 0 THEN 0.0
        WHEN tracks_count = 0 THEN 0.25
        WHEN producers_count = 0 THEN 0.5
        ELSE 1.0
      END AS progress
    FROM
      artist_stats
  )
  SELECT
    ps.artist_id,
    ps.artist_name,
    ps.status,
    ps.progress,
    ps.albums_count,
    ps.tracks_count,
    ps.producers_count
  FROM
    pipeline_status ps
  WHERE
    (status_filter IS NULL OR ps.status = status_filter) AND
    (progress_min IS NULL OR ps.progress >= progress_min) AND
    (progress_max IS NULL OR ps.progress <= progress_max)
  ORDER BY
    CASE 
      WHEN ps.status = 'no_albums' THEN 1
      WHEN ps.status = 'no_tracks' THEN 2
      WHEN ps.status = 'no_producers' THEN 3
      ELSE 4
    END,
    ps.artist_name
  LIMIT limit_count;
END;
$$;

-- Create a dashboard view for pipeline status overview
CREATE OR REPLACE VIEW public.pipeline_status_summary AS
WITH artist_stats AS (
  SELECT
    CASE
      WHEN COUNT(DISTINCT al.id) = 0 THEN 'no_albums'
      WHEN COUNT(DISTINCT t.id) = 0 THEN 'no_tracks'
      WHEN COUNT(DISTINCT p.id) = 0 THEN 'no_producers'
      ELSE 'complete'
    END AS status,
    a.id AS artist_id
  FROM
    artists a
    LEFT JOIN albums al ON a.id = al.artist_id
    LEFT JOIN tracks t ON al.id = t.album_id
    LEFT JOIN track_producers tp ON t.id = tp.track_id
    LEFT JOIN producers p ON tp.producer_id = p.id
  GROUP BY
    a.id
),
status_counts AS (
  SELECT
    status,
    COUNT(*) AS artist_count
  FROM
    artist_stats
  GROUP BY
    status
)
SELECT
  status_counts.status,
  status_counts.artist_count,
  ROUND((status_counts.artist_count::FLOAT / NULLIF(SUM(status_counts.artist_count) OVER (), 0)) * 100, 2) AS percentage
FROM
  status_counts
ORDER BY
  CASE 
    WHEN status = 'no_albums' THEN 1
    WHEN status = 'no_tracks' THEN 2
    WHEN status = 'no_producers' THEN 3
    ELSE 4
  END;

-- Create a function to estimate pipeline completion time
CREATE OR REPLACE FUNCTION public.estimate_pipeline_completion()
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  artists_total INT;
  artists_complete INT;
  artists_remaining INT;
  processing_rate FLOAT;
  estimated_hours FLOAT;
  result JSONB;
BEGIN
  -- Get artist counts
  SELECT
    COUNT(*),
    SUM(CASE WHEN s.status = 'complete' THEN 1 ELSE 0 END)
  INTO
    artists_total, artists_complete
  FROM (
    SELECT
      CASE
        WHEN COUNT(DISTINCT al.id) = 0 THEN 'no_albums'
        WHEN COUNT(DISTINCT t.id) = 0 THEN 'no_tracks'
        WHEN COUNT(DISTINCT p.id) = 0 THEN 'no_producers'
        ELSE 'complete'
      END AS status
    FROM
      artists a
      LEFT JOIN albums al ON a.id = al.artist_id
      LEFT JOIN tracks t ON al.id = t.album_id
      LEFT JOIN track_producers tp ON t.id = tp.track_id
      LEFT JOIN producers p ON tp.producer_id = p.id
    GROUP BY
      a.id
  ) s;
  
  artists_remaining := artists_total - artists_complete;
  
  -- Calculate processing rate based on recent metrics (artists completed per hour)
  -- This is a simplified estimation - in reality would need more complex calculation
  SELECT
    CASE
      WHEN NOW() - MIN(created_at) < INTERVAL '1 hour' THEN 1.0  -- Default if not enough data
      ELSE COUNT(*) / (EXTRACT(EPOCH FROM (NOW() - MIN(created_at))) / 3600.0)
    END
  INTO processing_rate
  FROM (
    SELECT created_at
    FROM artists
    WHERE created_at >= NOW() - INTERVAL '24 hours'
    ORDER BY created_at DESC
    LIMIT 100
  ) recent_artists;
  
  -- Calculate estimated completion time
  IF processing_rate > 0 THEN
    estimated_hours := artists_remaining / processing_rate;
  ELSE
    estimated_hours := NULL;  -- Cannot estimate with zero rate
  END IF;
  
  -- Build result
  result := jsonb_build_object(
    'artists_total', artists_total,
    'artists_complete', artists_complete,
    'artists_remaining', artists_remaining,
    'completion_percentage', CASE
      WHEN artists_total > 0 THEN
        ROUND((artists_complete::FLOAT / artists_total) * 100, 2)
      ELSE 0
    END,
    'processing_rate', ROUND(processing_rate, 2) || ' artists/hour',
    'estimated_completion_time', CASE
      WHEN estimated_hours IS NOT NULL THEN
        CASE
          WHEN estimated_hours < 1 THEN ROUND(estimated_hours * 60) || ' minutes'
          WHEN estimated_hours < 24 THEN ROUND(estimated_hours, 1) || ' hours'
          ELSE ROUND(estimated_hours / 24, 1) || ' days'
        END
      ELSE 'Cannot estimate'
    END,
    'timestamp', NOW()
  );
  
  RETURN result;
END;
$$;

-- Create a function to find incomplete artists
CREATE OR REPLACE FUNCTION public.find_stalled_artists(hours_threshold INT DEFAULT 24)
RETURNS TABLE(
  artist_id UUID,
  artist_name TEXT,
  stage TEXT,
  stalled_hours FLOAT,
  last_update TIMESTAMPTZ
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  WITH artist_stages AS (
    SELECT
      a.id AS artist_id,
      a.name AS artist_name,
      a.updated_at AS artist_updated,
      MAX(al.updated_at) AS album_updated,
      MAX(t.updated_at) AS track_updated,
      MAX(tp.created_at) AS producer_updated,
      CASE
        WHEN COUNT(DISTINCT al.id) = 0 THEN 'albums'
        WHEN COUNT(DISTINCT t.id) = 0 THEN 'tracks'
        WHEN COUNT(DISTINCT p.id) = 0 THEN 'producers'
        ELSE 'complete'
      END AS current_stage
    FROM
      artists a
      LEFT JOIN albums al ON a.id = al.artist_id
      LEFT JOIN tracks t ON al.id = t.album_id
      LEFT JOIN track_producers tp ON t.id = tp.track_id
      LEFT JOIN producers p ON tp.producer_id = p.id
    WHERE
      a.created_at < NOW() - INTERVAL '1 hour'  -- Skip very recently added artists
    GROUP BY
      a.id, a.name, a.updated_at
  ),
  stalled_detection AS (
    SELECT
      artist_id,
      artist_name,
      current_stage AS stage,
      CASE
        WHEN current_stage = 'albums' THEN 
          EXTRACT(EPOCH FROM (NOW() - artist_updated)) / 3600.0
        WHEN current_stage = 'tracks' THEN 
          EXTRACT(EPOCH FROM (NOW() - COALESCE(album_updated, artist_updated))) / 3600.0
        WHEN current_stage = 'producers' THEN 
          EXTRACT(EPOCH FROM (NOW() - COALESCE(track_updated, album_updated, artist_updated))) / 3600.0
        ELSE 0
      END AS hours_since_update,
      CASE
        WHEN current_stage = 'albums' THEN artist_updated
        WHEN current_stage = 'tracks' THEN COALESCE(album_updated, artist_updated)
        WHEN current_stage = 'producers' THEN COALESCE(track_updated, album_updated, artist_updated)
        ELSE NOW()
      END AS last_update_time
    FROM artist_stages
    WHERE current_stage != 'complete'
  )
  SELECT
    artist_id,
    artist_name,
    stage,
    hours_since_update AS stalled_hours,
    last_update_time AS last_update
  FROM
    stalled_detection
  WHERE
    hours_since_update >= hours_threshold
  ORDER BY
    hours_since_update DESC,
    stage;
END;
$$;
