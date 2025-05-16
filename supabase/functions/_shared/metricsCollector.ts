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
  
  constructor(supabase: any, options: { 
    bufferSize?: number, 
    autoFlushIntervalMs?: number,
    logger?: StructuredLogger 
  } = {}) {
    this.supabase = supabase;
    this.logger = options.logger || new StructuredLogger({ service: 'metrics-collector' });
    this.bufferSize = options.bufferSize || 10;
    
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
          // Insert metrics into database
          const { error } = await this.supabase
            .from('pipeline_metrics')
            .insert(metricsRows);
            
          if (error) {
            // Instead of putting metrics back in buffer, we'll just log the error
            // This prevents repeated failure loops
            this.logger.warn("Failed to persist metrics", { error: error.message });
            
            // As a fallback, we'll persist metrics to Redis if available
            await this.persistToFallback(metricsToFlush);
            
            return false;
          }
        } catch (error) {
          // Handle case where supabase client is invalid or table doesn't exist
          this.logger.warn("Error persisting metrics to database", { 
            error: error.message, 
            retryCount: metricsToFlush.length 
          });
          
          await this.persistToFallback(metricsToFlush);
          
          return false;
        }
      } else {
        // Supabase client not available, try fallback
        this.logger.warn("No Supabase client available for metrics persistence");
        await this.persistToFallback(metricsToFlush);
        return false;
      }
      
      this.logger.debug(`Successfully persisted ${metricsToFlush.length} metrics`);
      return true;
    } catch (error) {
      this.logger.warn("Exception persisting metrics", { error: error.message });
      await this.persistToFallback(metricsToFlush);
      return false;
    } finally {
      this.flushInProgress = false;
    }
  }
  
  /**
   * Persist metrics to a fallback storage (Redis or console)
   * when database persistence fails
   */
  private async persistToFallback(metrics: MetricPoint[]): Promise<void> {
    try {
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
