
/**
 * Enhanced Worker Base Class
 * Provides common functionality for all worker types with logging and metrics
 */
import { EnhancedIdempotentWorker, ProcessOptions } from "./enhancedIdempotentWorker.ts";
import { StructuredLogger } from "./structuredLogger.ts";
import { MetricsCollector } from "./metricsCollector.ts";

export abstract class EnhancedWorkerBase<T = any> extends EnhancedIdempotentWorker<T> {
  protected logger: StructuredLogger;
  protected metrics: MetricsCollector;
  protected workerName: string;
  
  constructor(queue: string, supabase?: any, workerName?: string) {
    super(queue, supabase);
    
    this.workerName = workerName || queue;
    
    // Initialize logger with worker context
    this.logger = new StructuredLogger({
      service: this.workerName,
      queue: queue,
      workerId: this.workerId
    });
    
    // Initialize metrics collector
    this.metrics = new MetricsCollector(this.supabase, {
      bufferSize: 10,
      autoFlushIntervalMs: 60000, // Flush metrics every minute
      logger: this.logger
    });
  }
  
  /**
   * Process a batch of messages with enhanced logging and metrics
   */
  async processBatch(options: ProcessOptions): Promise<{ 
    processed: number, 
    errors: number, 
    messages: any[] 
  }> {
    const startTime = Date.now();
    const batchId = `batch_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
    
    this.logger.info(`Starting ${options.processorName || this.workerName} worker`, {
      batchId,
      batchSize: options.batchSize,
      queue: this.queue
    });
    
    try {
      // Process the batch with parent implementation
      const result = await super.processBatch(options);
      
      const duration = Date.now() - startTime;
      
      // Log completion
      this.logger.info(`Completed batch processing of ${this.queue}`, {
        batchId,
        processed: result.processed,
        errors: result.errors,
        duration,
        queue: this.queue
      });
      
      // Record metrics
      this.metrics.recordQueueProcessing(
        this.queue,
        options.batchSize || 5,
        result.processed,
        result.errors,
        duration
      );
      
      // Capture queue metrics in database
      await this.recordQueueMetrics({
        batchId,
        operation: options.processorName || 'processBatch',
        batchSize: options.batchSize || 5,
        successCount: result.processed,
        errorCount: result.errors,
        processingTimeMs: duration
      });
      
      return result;
    } catch (error) {
      const duration = Date.now() - startTime;
      
      // Log error
      this.logger.error(`Error in batch processing of ${this.queue}`, error, {
        batchId,
        queue: this.queue,
        duration
      });
      
      // Record incident if serious error
      await this.recordWorkerIssue(
        'batch_processing_failed',
        'error',
        `Batch processing failed: ${error.message}`,
        {
          error: {
            message: error.message,
            stack: error.stack
          },
          batchId,
          queue: this.queue,
          duration
        }
      );
      
      // Make sure to flush metrics despite error
      await this.metrics.flush().catch(() => {/* ignore flush error */});
      
      // Re-throw to allow caller to handle
      throw error;
    }
  }
  
  /**
   * Override the processMessage to add logging and metrics
   */
  async processMessage(message: T): Promise<any> {
    const messageId = typeof message === 'object' && message ? 
      (message as any).id || 
      (message as any)._id || 
      `msg_${Math.random().toString(36).substring(2, 9)}` : 
      `msg_${Math.random().toString(36).substring(2, 9)}`;
    
    // Create message-specific logger
    const msgLogger = this.logger.withContext({
      messageId,
      queue: this.queue
    });
    
    msgLogger.info(`Processing message from ${this.queue}`, {
      messageType: typeof message,
      hasId: typeof message === 'object' && message && ((message as any).id !== undefined || (message as any)._id !== undefined)
    });
    
    const startTime = Date.now();
    
    try {
      // Call the actual message implementation (must be implemented by subclasses)
      const result = await this.handleMessage(message, msgLogger);
      
      const duration = Date.now() - startTime;
      
      // Log success and metrics
      msgLogger.info(`Successfully processed message from ${this.queue}`, {
        duration,
        resultType: typeof result
      });
      
      // Record job duration
      this.metrics.recordJobDuration(this.queue, duration, true);
      
      return result;
    } catch (error) {
      const duration = Date.now() - startTime;
      
      // Log error
      msgLogger.error(`Failed to process message from ${this.queue}`, error, {
        duration
      });
      
      // Record metrics for failed job
      this.metrics.recordJobDuration(this.queue, duration, false);
      
      // Re-throw to allow parent error handling
      throw error;
    }
  }
  
  /**
   * Abstract method that subclasses must implement to process a message
   */
  abstract handleMessage(message: T, logger: StructuredLogger): Promise<any>;
  
  /**
   * Record queue processing metrics in database
   */
  protected async recordQueueMetrics(data: {
    batchId: string;
    operation: string;
    batchSize: number;
    successCount: number;
    errorCount: number;
    processingTimeMs: number;
  }): Promise<void> {
    try {
      await this.supabase.from('queue_metrics').insert({
        queue_name: this.queue,
        operation: data.operation,
        batch_size: data.batchSize,
        success_count: data.successCount,
        error_count: data.errorCount,
        processing_time_ms: data.processingTimeMs,
        metadata: { batchId: data.batchId }
      });
    } catch (error) {
      // Log error but don't fail the processing
      this.logger.error("Failed to record queue metrics", error);
    }
  }
  
  /**
   * Record a worker issue in the database
   */
  protected async recordWorkerIssue(
    issueType: string,
    severity: 'warning' | 'error' | 'critical',
    message: string,
    details: any = {}
  ): Promise<void> {
    try {
      await this.supabase.from('monitoring.worker_issues').insert({
        worker_name: this.workerName,
        issue_type: issueType,
        severity,
        message,
        details
      });
    } catch (error) {
      // Log error but don't fail the processing
      this.logger.error("Failed to record worker issue", error);
    }
  }
}
