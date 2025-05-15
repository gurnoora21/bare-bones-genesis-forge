
-- Remove obsolete functions and streamline code
DO $$
BEGIN
  -- Attempt to drop unused emergency functions
  BEGIN
    DROP FUNCTION IF EXISTS public.emergency_reset_message(text, text, boolean);
    DROP FUNCTION IF EXISTS public.enhanced_delete_message(text, text, boolean);
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error dropping emergency functions: %', SQLERRM;
  END;
  
  -- Check for processing_status table usage and consider dropping if unused
  IF EXISTS (
    SELECT 1 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
      AND table_name = 'processing_status'
  ) THEN
    -- Only drop if no active locks within the last 24 hours
    IF NOT EXISTS (
      SELECT 1
      FROM public.processing_status
      WHERE updated_at > NOW() - INTERVAL '24 hours'
    ) THEN
      RAISE NOTICE 'processing_status table appears unused; consider dropping manually after verifying';
      -- Commented out: DROP TABLE IF EXISTS public.processing_status CASCADE;
    END IF;
  END IF;
  
  -- Update comments on key functions to clarify current usage
  COMMENT ON FUNCTION pg_enqueue IS 'Main queue insertion function - used by Edge Functions to reliably enqueue JSONB messages';
  COMMENT ON FUNCTION pg_dequeue IS 'Main queue reading function - retrieves messages with visibility timeout for worker processing';
  COMMENT ON FUNCTION pg_delete_message IS 'Reliably deletes a message by ID from a queue with fallback strategies';
  COMMENT ON FUNCTION check_artist_pipeline_progress IS 'Checks detailed pipeline progress for a specific artist';
END;
$$;

-- Create a view for active queue monitoring with enhanced metrics
CREATE OR REPLACE VIEW public.active_queue_monitoring AS
WITH queue_stats AS (
  SELECT
    qr.queue_name,
    qr.display_name,
    COALESCE(qs.count, 0) AS message_count,
    qs.oldest_message,
    -- Calculate rates based on recent metrics
    (
      SELECT AVG(m.processed_count)
      FROM queue_metrics m
      WHERE m.queue_name = qr.queue_name
        AND m.created_at > NOW() - INTERVAL '1 hour'
    ) AS avg_processed_per_batch,
    (
      SELECT AVG(m.error_count)
      FROM queue_metrics m
      WHERE m.queue_name = qr.queue_name
        AND m.created_at > NOW() - INTERVAL '1 hour'
    ) AS avg_errors_per_batch,
    (
      SELECT COUNT(*)
      FROM worker_issues w
      WHERE w.worker_name ILIKE '%' || qr.queue_name || '%'
        AND w.created_at > NOW() - INTERVAL '1 hour'
        AND w.resolved = false
    ) AS recent_issues_count
  FROM
    queue_registry qr
    LEFT JOIN LATERAL (
      SELECT * FROM pg_queue_status(qr.queue_name)
    ) AS qs ON true
  WHERE
    qr.active = true
)
SELECT
  queue_name,
  display_name,
  message_count,
  oldest_message,
  CASE
    WHEN message_count = 0 THEN 'empty'
    WHEN message_count > 1000 THEN 'high'
    WHEN message_count > 100 THEN 'medium'
    ELSE 'normal'
  END AS queue_load,
  avg_processed_per_batch,
  avg_errors_per_batch,
  CASE
    WHEN avg_processed_per_batch IS NULL THEN NULL
    WHEN avg_errors_per_batch / NULLIF(avg_processed_per_batch, 0) > 0.2 THEN 'high'
    WHEN avg_errors_per_batch / NULLIF(avg_processed_per_batch, 0) > 0.05 THEN 'medium'
    ELSE 'normal'
  END AS error_rate_status,
  recent_issues_count,
  CASE
    WHEN oldest_message IS NULL THEN NULL
    WHEN oldest_message < NOW() - INTERVAL '1 day' THEN 'stale'
    ELSE 'normal'
  END AS age_status,
  NOW() AS check_time
FROM
  queue_stats
ORDER BY
  message_count DESC;
