
-- Create schema for monitoring and metrics
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Table for storing metrics data
CREATE TABLE IF NOT EXISTS monitoring.pipeline_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    tags JSONB DEFAULT '{}'::jsonb,
    timestamp TIMESTAMPTZ DEFAULT now(),
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Index on metric name for faster queries
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_name ON monitoring.pipeline_metrics(metric_name);
-- Index on timestamp for time-based queries
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_timestamp ON monitoring.pipeline_metrics(timestamp);
-- Index on tags using GIN for JSON querying
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_tags ON monitoring.pipeline_metrics USING GIN (tags);

-- Table for storing worker issues
CREATE TABLE IF NOT EXISTS monitoring.worker_issues (
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

-- Index on worker name
CREATE INDEX IF NOT EXISTS idx_worker_issues_worker ON monitoring.worker_issues(worker_name);
-- Index on issue type
CREATE INDEX IF NOT EXISTS idx_worker_issues_type ON monitoring.worker_issues(issue_type);
-- Index on resolved status
CREATE INDEX IF NOT EXISTS idx_worker_issues_resolved ON monitoring.worker_issues(resolved);

-- Table for storing aggregated metrics
CREATE TABLE IF NOT EXISTS monitoring.aggregated_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    metric_name TEXT NOT NULL,
    period TEXT NOT NULL, -- 'hourly', 'daily', 'weekly'
    period_start TIMESTAMPTZ NOT NULL,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    avg_value DOUBLE PRECISION,
    sum_value DOUBLE PRECISION,
    count_value INTEGER,
    dimensions JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Create unique constraint on metric name, period and start time
CREATE UNIQUE INDEX IF NOT EXISTS idx_aggregated_metrics_unique 
ON monitoring.aggregated_metrics(metric_name, period, period_start, (dimensions::TEXT));

-- Create function to aggregate metrics hourly
CREATE OR REPLACE FUNCTION monitoring.aggregate_hourly_metrics()
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    metrics_aggregated INTEGER := 0;
    last_hour TIMESTAMPTZ := date_trunc('hour', now()) - interval '1 hour';
BEGIN
    -- Delete any existing aggregations for the period to avoid duplicates
    DELETE FROM monitoring.aggregated_metrics
    WHERE period = 'hourly' AND period_start = last_hour;
    
    -- Insert new aggregations
    INSERT INTO monitoring.aggregated_metrics (
        metric_name, period, period_start,
        min_value, max_value, avg_value, sum_value, count_value, dimensions
    )
    SELECT
        metric_name,
        'hourly' AS period,
        last_hour AS period_start,
        MIN(metric_value) AS min_value,
        MAX(metric_value) AS max_value,
        AVG(metric_value) AS avg_value,
        SUM(metric_value) AS sum_value,
        COUNT(*) AS count_value,
        tags AS dimensions
    FROM
        monitoring.pipeline_metrics
    WHERE
        timestamp >= last_hour AND timestamp < date_trunc('hour', now())
    GROUP BY
        metric_name, tags;
        
    GET DIAGNOSTICS metrics_aggregated = ROW_COUNT;
    
    -- Purge raw metrics older than retention period (14 days by default)
    DELETE FROM monitoring.pipeline_metrics
    WHERE timestamp < now() - interval '14 days';
    
    RETURN metrics_aggregated;
END;
$$;

-- Create function to aggregate metrics daily
CREATE OR REPLACE FUNCTION monitoring.aggregate_daily_metrics()
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    metrics_aggregated INTEGER := 0;
    yesterday DATE := (now() - interval '1 day')::DATE;
BEGIN
    -- Delete any existing aggregations for the period to avoid duplicates
    DELETE FROM monitoring.aggregated_metrics
    WHERE period = 'daily' AND period_start = yesterday::TIMESTAMPTZ;
    
    -- Insert new aggregations
    INSERT INTO monitoring.aggregated_metrics (
        metric_name, period, period_start,
        min_value, max_value, avg_value, sum_value, count_value, dimensions
    )
    SELECT
        metric_name,
        'daily' AS period,
        yesterday::TIMESTAMPTZ AS period_start,
        MIN(metric_value) AS min_value,
        MAX(metric_value) AS max_value,
        AVG(metric_value) AS avg_value,
        SUM(metric_value) AS sum_value,
        COUNT(*) AS count_value,
        tags AS dimensions
    FROM
        monitoring.pipeline_metrics
    WHERE
        timestamp >= yesterday::TIMESTAMPTZ AND timestamp < (yesterday + interval '1 day')::TIMESTAMPTZ
    GROUP BY
        metric_name, tags;
        
    GET DIAGNOSTICS metrics_aggregated = ROW_COUNT;
    
    RETURN metrics_aggregated;
END;
$$;

-- Create a view for queue metrics
CREATE OR REPLACE VIEW monitoring.queue_metrics AS
SELECT
    tags->>'queue' AS queue_name,
    date_trunc('hour', timestamp) AS hour,
    COUNT(*) AS total_batches,
    SUM((tags->>'success_count')::integer) AS success_count,
    SUM((tags->>'error_count')::integer) AS error_count,
    AVG(metric_value) AS avg_processing_time_ms,
    SUM((tags->>'batch_size')::integer) AS total_messages_processed
FROM
    monitoring.pipeline_metrics
WHERE
    metric_name = 'queue_processing'
    AND timestamp > now() - interval '7 days'
GROUP BY
    tags->>'queue', date_trunc('hour', timestamp)
ORDER BY
    queue_name, hour DESC;

-- Create a view for API metrics
CREATE OR REPLACE VIEW monitoring.api_metrics AS
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

-- Set up cron job for hourly metric aggregation
SELECT cron.schedule(
    'hourly-metrics-aggregation',
    '5 * * * *',  -- 5 minutes past every hour
    $$SELECT monitoring.aggregate_hourly_metrics()$$
);

-- Set up cron job for daily metric aggregation
SELECT cron.schedule(
    'daily-metrics-aggregation',
    '15 0 * * *',  -- 00:15 every day
    $$SELECT monitoring.aggregate_daily_metrics()$$
);
