
/**
 * Metrics Collector for storing pipeline performance metrics
 * Aggregates metrics and persists them to Postgres for analysis
 */
import { StructuredLogger } from "./structuredLogger.ts";

export interface MetricPoint {
  name: string;
  value: number;
  tags?: Record<string, string>;
  timestamp?: Date;
}

export class MetricsCollector {
  private supabase: any;
  private logger: StructuredLogger;
  private metricsBuffer: MetricPoint[] = [];
  private bufferSize: number;
  private flushInterval: number | null = null;
  private lastFlushTime: number = Date.now();
  private flushInProgress: boolean = false;
  private fallbackLogEnabled: boolean = true;
  private consecutiveErrors: number = 0;
  private maxConsecutiveErrors: number = 3;
  
  constructor(supabase: any, options: { 
    bufferSize?: number, 
    autoFlushIntervalMs?: number,
    logger?: StructuredLogger,
    fallbackLogEnabled?: boolean
  } = {}) {
    this.supabase = supabase;
    this.logger = options.logger || new StructuredLogger({ service: 'metrics-collector' });
    this.bufferSize = options.bufferSize || 10;
    this.fallbackLogEnabled = options.fallbackLogEnabled !== false;
    
    // Set up auto-flush if interval is provided
    if (options.autoFlushIntervalMs) {
      this.flushInterval = options.autoFlushIntervalMs;
      this.scheduleAutoFlush();
    }
  }
  
  /**
   * Record a single metric point
   */
  record(metric: MetricPoint): void {
    try {
      // Add timestamp if not provided
      const fullMetric = {
        ...metric,
        timestamp: metric.timestamp || new Date()
      };
      
      // Add to buffer
      this.metricsBuffer.push(fullMetric);
      
      // Auto-flush if buffer size threshold is reached
      if (this.metricsBuffer.length >= this.bufferSize) {
        this.flush().catch(err => {
          this.logger.warn("Failed to flush metrics buffer", { error: err.message });
        });
      }
    } catch (err) {
      // Don't let metrics recording break the application
      this.logger.warn("Error recording metric", { error: err.message });
    }
  }
  
  /**
   * Record API call metrics
   */
  recordApiCall(apiName: string, success: boolean, durationMs: number, statusCode?: number): void {
    this.record({
      name: 'api_call',
      value: durationMs,
      tags: {
        api: apiName,
        success: success.toString(),
        status: statusCode ? statusCode.toString() : 'unknown'
      }
    });
    
    // Also increment counter
    this.incrementCounter(`api_calls.${apiName}`, success ? 'success' : 'failure');
  }
  
  /**
   * Record queue processing metrics
   */
  recordQueueProcessing(queueName: string, batchSize: number, successCount: number, errorCount: number, durationMs: number): void {
    this.record({
      name: 'queue_processing',
      value: durationMs,
      tags: {
        queue: queueName,
        batch_size: batchSize.toString(),
        success_count: successCount.toString(),
        error_count: errorCount.toString()
      }
    });
  }
  
  /**
   * Increment a counter metric
   */
  incrementCounter(name: string, category?: string, increment: number = 1): void {
    this.record({
      name: `counter.${name}`,
      value: increment,
      tags: category ? { category } : undefined
    });
  }
  
  /**
   * Record job processing duration
   */
  recordJobDuration(jobType: string, durationMs: number, success: boolean): void {
    this.record({
      name: 'job_duration',
      value: durationMs,
      tags: {
        job_type: jobType,
        success: success.toString()
      }
    });
  }
  
  /**
   * Flush metrics buffer to database
   */
  async flush(): Promise<boolean> {
    if (this.metricsBuffer.length === 0) {
      return true;
    }
    
    // Prevent concurrent flushes
    if (this.flushInProgress) {
      return false;
    }
    
    this.flushInProgress = true;
    
    const metricsToFlush = [...this.metricsBuffer];
    this.metricsBuffer = [];
    this.lastFlushTime = Date.now();
    
    try {
      // Convert metrics to database format
      const metricsRows = metricsToFlush.map(metric => ({
        metric_name: metric.name,
        metric_value: metric.value,
        tags: metric.tags || {},
        timestamp: metric.timestamp
      }));
      
      if (this.supabase) {
        try {
          // First, make sure the monitoring schema and table exist
          await this.ensureMonitoringSchema();
          await this.ensurePipelineMetricsTable();
          
          // Insert metrics into database with proper schema prefix
          const { data, error } = await this.supabase
            .from('monitoring.pipeline_metrics')
            .insert(metricsRows);
            
          if (error) {
            // Try alternative approach with raw SQL if normal insert fails
            try {
              // Using raw_sql_query RPC to create the insert statement
              await this.supabase.rpc('raw_sql_query', {
                sql_query: `
                  INSERT INTO monitoring.pipeline_metrics
                  (metric_name, metric_value, tags, timestamp)
                  SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(
                    metric_name TEXT,
                    metric_value DOUBLE PRECISION,
                    tags JSONB,
                    timestamp TIMESTAMPTZ
                  )
                `,
                params: JSON.stringify(metricsRows)
              });
              
              // Reset error counter on success
              this.consecutiveErrors = 0;
              return true;
            } catch (sqlError) {
              // Log the specific SQL error
              this.logger.warn("Failed to insert metrics via SQL", { 
                error: sqlError.message,
                originalError: error.message
              });
              this.consecutiveErrors++;
              
              // Fall back to local logging
              await this.persistToFallback(metricsToFlush);
              return false;
            }
          }
          
          // Reset error counter on success
          this.consecutiveErrors = 0;
          return true;
        } catch (error) {
          // Handle case where supabase client is invalid or table doesn't exist
          this.logger.warn("Error persisting metrics to database", { 
            error: error.message, 
            retryCount: metricsToFlush.length 
          });
          
          this.consecutiveErrors++;
          await this.persistToFallback(metricsToFlush);
          
          return false;
        }
      } else {
        // Supabase client not available, try fallback
        this.logger.warn("No Supabase client available for metrics persistence");
        await this.persistToFallback(metricsToFlush);
        return false;
      }
    } catch (error) {
      this.logger.warn("Exception persisting metrics", { error: error.message });
      this.consecutiveErrors++;
      await this.persistToFallback(metricsToFlush);
      return false;
    } finally {
      this.flushInProgress = false;
    }
  }

  /**
   * Ensure the monitoring schema exists
   */
  private async ensureMonitoringSchema(): Promise<void> {
    if (this.consecutiveErrors >= this.maxConsecutiveErrors) {
      return; // Skip if we've failed too many times
    }
    
    try {
      // Create monitoring schema if it doesn't exist
      await this.supabase.rpc('raw_sql_query', {
        sql_query: `CREATE SCHEMA IF NOT EXISTS monitoring;`
      });
    } catch (error) {
      this.logger.warn("Error ensuring monitoring schema", { error: error.message });
      // Continue anyway - the schema might already exist
    }
  }
  
  /**
   * Ensure the pipeline_metrics table exists
   */
  private async ensurePipelineMetricsTable(): Promise<void> {
    if (this.consecutiveErrors >= this.maxConsecutiveErrors) {
      return; // Skip if we've failed too many times
    }
    
    try {
      // Create the table in the monitoring schema
      await this.supabase.rpc('raw_sql_query', {
        sql_query: `
          CREATE TABLE IF NOT EXISTS monitoring.pipeline_metrics (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            metric_name TEXT NOT NULL,
            metric_value DOUBLE PRECISION NOT NULL,
            tags JSONB DEFAULT '{}'::jsonb,
            timestamp TIMESTAMPTZ DEFAULT now(),
            created_at TIMESTAMPTZ DEFAULT now()
          );
          
          CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_name 
            ON monitoring.pipeline_metrics(metric_name);
          CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_timestamp 
            ON monitoring.pipeline_metrics(timestamp);
        `
      });
    } catch (error) {
      this.logger.warn("Error ensuring pipeline_metrics table", { error: error.message });
      // Continue - we'll try to insert anyway
    }
  }
  
  /**
   * Persist metrics to a fallback storage (Redis or console)
   * when database persistence fails
   */
  private async persistToFallback(metrics: MetricPoint[]): Promise<void> {
    try {
      if (!this.fallbackLogEnabled && this.consecutiveErrors < this.maxConsecutiveErrors) {
        return; // Skip fallback logging if disabled and we haven't failed too many times
      }
      
      // Log metrics to console as a fallback
      const summary = {
        count: metrics.length,
        names: [...new Set(metrics.map(m => m.name))],
        timestamp: new Date().toISOString()
      };
      
      this.logger.info("Metrics summary (fallback)", { summary });
      
      // We could also attempt to save to Redis here if available
      // or to a local file if running in development
    } catch (error) {
      this.logger.warn("Failed to persist metrics to fallback", { error: error.message });
    }
  }
  
  /**
   * Schedule automatic periodic flushing of metrics
   */
  private scheduleAutoFlush(): void {
    if (!this.flushInterval) return;
    
    setTimeout(() => {
      this.flush().catch(err => {
        this.logger.warn("Auto-flush failed", { error: err.message });
      }).finally(() => {
        this.scheduleAutoFlush();
      });
    }, this.flushInterval);
  }
}
