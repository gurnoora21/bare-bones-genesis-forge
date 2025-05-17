
# Monitoring System Architecture

The monitoring system in our producer discovery platform is designed to track performance, health, and operational metrics across the various pipeline components. This document outlines the architecture and usage of this system.

## Overview

The monitoring system consists of multiple components:

1. **Metrics Collection** - Gathering performance and operational data 
2. **Metrics Storage** - Persisting metrics in database tables
3. **Aggregation** - Summarizing metrics over time periods
4. **Visualization** - Displaying metrics in dashboards and reports
5. **Alerting** - Notifying operators about issues

## Data Storage

### Schema: monitoring

The main monitoring tables are stored in the `monitoring` schema:

#### pipeline_metrics

The primary time-series metrics collection table:

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

This table stores raw measurements with the following structure:
- `metric_name`: The name of the metric (e.g. 'queue_processing_time', 'api_call_latency')
- `metric_value`: The numeric value of the measurement
- `tags`: Additional dimensions/metadata as JSONB (e.g. queue name, API endpoint)
- `timestamp`: When the measurement occurred

#### aggregated_metrics

Pre-aggregated metrics for faster querying:

```sql
CREATE TABLE monitoring.aggregated_metrics (
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
```

This table contains metrics summarized by time period to support faster dashboard rendering.

#### worker_issues

A log of worker problems:

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

This table tracks operational issues with the pipeline workers.

### Schema: public

Additional monitoring tables in the public schema:

#### monitoring_events

```sql
CREATE TABLE public.monitoring_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL,
    details JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT now()
);
```

This table captures significant system events like:
- Queue monitoring runs
- System resets
- Dead-letter queue operations
- Administrative actions

## Metrics Collection

### How Metrics are Collected

1. **Direct Insertion**: Workers directly insert metrics into `monitoring.pipeline_metrics` when they complete processing batches or make API calls.

2. **Edge Function Reports**: Edge functions report their metrics at completion.

3. **Background Monitoring**: Dedicated monitoring edge functions periodically collect system-wide metrics.

### Common Metric Types

1. **Queue Processing Metrics**
   - Processing time per message
   - Success/failure rates
   - Queue depths

2. **API Call Metrics**
   - Latency
   - Success/failure rates
   - Rate limit hits

3. **Pipeline Flow Metrics**
   - Entity throughput rates
   - Processing times per entity type

## Aggregation System

Metrics are automatically aggregated by scheduled jobs:

```sql
-- Aggregate metrics hourly
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

-- Run aggregation job every hour
SELECT cron.schedule(
    'hourly-metrics-aggregation',
    '5 * * * *',  -- 5 minutes past every hour
    $$SELECT monitoring.aggregate_hourly_metrics()$$
);
```

## Monitoring Views

Several views provide convenient access to commonly needed metrics:

```sql
-- Queue metrics view
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

-- API metrics view
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
```

## Health Check Functions

The system provides a comprehensive health check function:

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
  
  -- Additional health checks for processing and worker statuses
  ...
  
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

## Data Retention

The monitoring system automatically manages data retention:

1. **Raw metrics** are kept for 14 days
2. **Hourly aggregations** are kept for 90 days
3. **Daily aggregations** are kept for 365 days
4. **Monitoring events** are purged after 30 days:

```sql
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

SELECT cron.schedule(
  'purge-old-monitoring-events',
  '0 0 * * 0',  -- Midnight every Sunday
  $$SELECT public.purge_old_monitoring_events(30)$$
);
```

## Usage Examples

### Recording a Processing Metric

```typescript
async function recordProcessingMetric(queueName: string, processingTimeMs: number, 
                                      successCount: number, errorCount: number) {
  try {
    await supabase.from('monitoring.pipeline_metrics').insert({
      metric_name: 'queue_processing',
      metric_value: processingTimeMs,
      tags: {
        queue: queueName,
        success_count: successCount,
        error_count: errorCount,
      }
    });
  } catch (error) {
    console.error('Failed to record metric:', error);
  }
}
```

### Recording an API Call Metric

```typescript
async function recordApiCall(apiName: string, durationMs: number, success: boolean) {
  try {
    await supabase.from('monitoring.pipeline_metrics').insert({
      metric_name: 'api_call',
      metric_value: durationMs,
      tags: {
        api: apiName,
        success: success.toString()
      }
    });
  } catch (error) {
    console.error('Failed to record API metric:', error);
  }
}
```

### Recording a Worker Issue

```typescript
async function logWorkerIssue(workerName: string, issueType: string, message: string, 
                             severity: string = 'warning', details: any = {}) {
  try {
    await supabase.from('monitoring.worker_issues').insert({
      worker_name: workerName,
      issue_type: issueType,
      message: message,
      severity: severity,
      details: details,
      resolved: false
    });
  } catch (error) {
    console.error('Failed to log worker issue:', error);
  }
}
```

### Getting System Health

```typescript
async function checkSystemHealth() {
  try {
    const { data, error } = await supabase.rpc('get_system_health');
    
    if (error) throw error;
    
    console.log('System health:', data);
    
    if (data.overall_status === 'critical') {
      // Handle critical system state
      await sendAlert('System health is CRITICAL!', data);
    }
    
    return data;
  } catch (error) {
    console.error('Failed to check system health:', error);
    return { overall_status: 'unknown', error: error.message };
  }
}
```
