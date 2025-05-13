
-- Create additional monitoring tables

-- Table for logging monitoring events
CREATE TABLE IF NOT EXISTS public.monitoring_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_type TEXT NOT NULL,
  details JSONB DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Create index on event_type and created_at
CREATE INDEX IF NOT EXISTS idx_monitoring_events_type ON public.monitoring_events(event_type);
CREATE INDEX IF NOT EXISTS idx_monitoring_events_created_at ON public.monitoring_events(created_at);

-- Create function to purge old monitoring events
CREATE OR REPLACE FUNCTION public.purge_old_monitoring_events(retention_days INTEGER DEFAULT 30)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  deleted_count INTEGER;
BEGIN
  DELETE FROM public.monitoring_events
  WHERE created_at < NOW() - (retention_days * INTERVAL '1 day');
  
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  RETURN deleted_count;
END;
$$;

-- Create a scheduled job to purge old events weekly
SELECT cron.schedule(
  'purge-old-monitoring-events',
  '0 0 * * 0',  -- Midnight every Sunday
  $$SELECT public.purge_old_monitoring_events(30)$$
);

-- Create a view for queue monitoring
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

-- Create a function to get system health status
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

-- Create a table to store performance metrics
CREATE TABLE IF NOT EXISTS public.queue_metrics (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  queue_name TEXT NOT NULL,
  operation TEXT NOT NULL,
  batch_size INT,
  success_count INT NOT NULL DEFAULT 0,
  error_count INT NOT NULL DEFAULT 0,
  processing_time_ms INT,
  metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_queue_metrics_queue ON public.queue_metrics(queue_name);
CREATE INDEX IF NOT EXISTS idx_queue_metrics_created_at ON public.queue_metrics(created_at);
