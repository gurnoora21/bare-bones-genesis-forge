
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
      logger: this.logger,
      fallbackLogEnabled: true
    });
  }
  
  /**
   * Process a batch of messages with enhanced logging and metrics
   */
  async processBatch(options: ProcessOptions): Promise<{ 
    processed: number, 
    errors: number, 
    messages: any[],
    duplicates?: number,
    skipped?: number,
    processingTimeMs?: number
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
        duplicates: result.duplicates || 0,
        skipped: result.skipped || 0,
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
      try {
        await this.recordQueueMetrics({
          batchId,
          operation: options.processorName || 'processBatch',
          batchSize: options.batchSize || 5,
          successCount: result.processed,
          errorCount: result.errors,
          processingTimeMs: duration
        }).catch(err => {
          this.logger.warn("Failed to record queue metrics", { error: err.message });
        });
      } catch (error) {
        this.logger.warn("Error recording queue metrics", { error: error?.message });
      }
      
      // Add processing time and any missing fields to result
      return {
        ...result,
        duplicates: result.duplicates || 0,
        skipped: result.skipped || 0, 
        processingTimeMs: duration
      };
    } catch (error) {
      const duration = Date.now() - startTime;
      
      // Log error
      this.logger.error(`Error in batch processing of ${this.queue}`, error, {
        batchId,
        queue: this.queue,
        duration
      });
      
      // Record incident if serious error
      try {
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
        ).catch(() => {/* ignore recording error */});
      } catch (issueError) {
        this.logger.warn("Failed to record worker issue", { error: issueError?.message });
      }
      
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
    // Generate a message ID if none exists
    const messageId = this.getMessageId(message);
    
    // Create message-specific logger
    const msgLogger = this.logger.withContext({
      messageId,
      queue: this.queue
    });
    
    // Skip invalid messages without required fields
    if (!this.validateMessage(message, msgLogger)) {
      return { skipped: true, reason: "invalid_message" };
    }
    
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
   * Validate that a message has the required fields
   */
  protected validateMessage(message: T, logger: StructuredLogger): boolean {
    // Check message is an object
    if (!message || typeof message !== 'object') {
      logger.error("Skipping invalid message: not an object");
      return false;
    }
    
    // Check if message has an ID for tracking
    const messageId = this.getMessageId(message);
    if (!messageId) {
      logger.error("Skipping invalid message without ID");
      return false;
    }
    
    return true;
  }
  
  /**
   * Get a message ID from various possible sources
   */
  protected getMessageId(message: any): string {
    if (!message) return `unknown_${Date.now()}`;
    
    // Try various common ID fields
    if (typeof message === 'object') {
      return message.id || 
             message._id || 
             message.messageId || 
             message.msg_id || 
             message.message_id || 
             `generated_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
    }
    
    // Fallback to a generated ID
    return `fallback_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
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
      // First, make sure the table exists
      await this.supabase.rpc('raw_sql_query', {
        sql_query: `CREATE TABLE IF NOT EXISTS queue_metrics (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          queue_name TEXT NOT NULL,
          operation TEXT NOT NULL,
          batch_size INTEGER,
          success_count INTEGER,
          error_count INTEGER,
          processing_time_ms INTEGER,
          metadata JSONB DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ DEFAULT now()
        )`
      });
      
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
      throw error; // Let caller handle further
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
      // First, ensure the table exists
      await this.supabase.rpc('raw_sql_query', {
        sql_query: `
          CREATE TABLE IF NOT EXISTS worker_issues (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            worker_name TEXT NOT NULL, 
            issue_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            message TEXT NOT NULL,
            details JSONB,
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now(),
            resolved BOOLEAN DEFAULT FALSE
          )
        `
      });
      
      await this.supabase.from('worker_issues').insert({
        worker_name: this.workerName,
        issue_type: issueType,
        severity,
        message,
        details
      });
    } catch (error) {
      // Log error but don't fail the processing
      this.logger.error("Failed to record worker issue", error);
      throw error; // Let caller handle further
    }
  }
}
