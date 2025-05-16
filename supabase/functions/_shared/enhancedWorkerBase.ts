
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
    
    // Log the message structure to help with debugging
    msgLogger.debug("Received message structure", {
      messageType: typeof message,
      hasId: typeof message === 'object' && message !== null,
      messageKeys: typeof message === 'object' && message !== null ? 
        Object.keys(message as object) : [],
      messageIdFound: messageId
    });
    
    // Skip invalid messages without required fields
    if (!this.validateMessage(message, msgLogger)) {
      return { skipped: true, reason: "invalid_message" };
    }
    
    msgLogger.info(`Processing message from ${this.queue}`, {
      messageId,
      messageType: typeof message
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
      logger.error("Skipping invalid message: not an object", { 
        messageType: typeof message,
        messageValue: message 
      });
      return false;
    }
    
    // For messages from queue, we often get a wrapper object with message property
    let actualMessage = message;
    
    // If message has a nested 'message' property, use that instead
    if (typeof message === 'object' && 
        message !== null && 
        'message' in message && 
        typeof (message as any).message === 'object') {
      actualMessage = (message as any).message;
      logger.debug("Using nested message object", { 
        originalMessageKeys: Object.keys(message as object),
        nestedMessageKeys: Object.keys(actualMessage as object) 
      });
    }
    
    // Check if message has an ID for tracking
    const messageId = this.getMessageId(message);
    if (!messageId) {
      logger.error("Skipping invalid message without ID", {
        messageObject: typeof message === 'object' ? 
          JSON.stringify(message).substring(0, 1000) : 'not_an_object'
      });
      return false;
    }
    
    return true;
  }
  
  /**
   * Get a message ID from various possible sources
   */
  protected getMessageId(message: any): string {
    if (!message) return `unknown_${Date.now()}`;
    
    // First check for direct ID fields in the top-level message
    if (typeof message === 'object' && message !== null) {
      // Check top-level ID fields
      const directId = message.id || 
                      message._id || 
                      message.messageId || 
                      message.msg_id || 
                      message.message_id;
                      
      if (directId !== undefined && directId !== null) {
        return String(directId);
      }
      
      // Check if this is a message wrapper from the queue
      if ('message' in message) {
        // If message contains a nested message object, check for IDs there
        const nestedMessage = message.message;
        if (nestedMessage && typeof nestedMessage === 'object') {
          const nestedId = nestedMessage.id || 
                          nestedMessage._id || 
                          nestedMessage.messageId || 
                          nestedMessage.msg_id || 
                          nestedMessage.message_id;
          
          if (nestedId !== undefined && nestedId !== null) {
            return String(nestedId);
          }
        }
      }
      
      // If we still don't have an ID, but have an actual message object, generate a stable one
      try {
        const stableHash = JSON.stringify(message).substring(0, 100);
        return `hash_${Date.now()}_${stableHash.replace(/[^a-zA-Z0-9]/g, '')}`;
      } catch (e) {
        // In case of JSON stringify error
      }
    }
    
    // Fallback to a generated ID as last resort
    return `generated_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
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
        sql_query: `
          CREATE SCHEMA IF NOT EXISTS monitoring;
          
          CREATE TABLE IF NOT EXISTS monitoring.queue_metrics (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            queue_name TEXT NOT NULL,
            operation TEXT NOT NULL,
            batch_size INTEGER,
            success_count INTEGER,
            error_count INTEGER,
            processing_time_ms INTEGER,
            metadata JSONB DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ DEFAULT now()
          );`
      });
      
      // Use raw SQL for better reliability
      await this.supabase.rpc('raw_sql_query', {
        sql_query: `
          INSERT INTO monitoring.queue_metrics
          (queue_name, operation, batch_size, success_count, error_count, processing_time_ms, metadata)
          VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        params: JSON.stringify([
          this.queue,
          data.operation,
          data.batchSize,
          data.successCount,
          data.errorCount,
          data.processingTimeMs,
          { batchId: data.batchId }
        ])
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
      // First, ensure the schema and table exist
      await this.supabase.rpc('raw_sql_query', {
        sql_query: `
          CREATE SCHEMA IF NOT EXISTS monitoring;
          
          CREATE TABLE IF NOT EXISTS monitoring.worker_issues (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            worker_name TEXT NOT NULL, 
            issue_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            message TEXT NOT NULL,
            details JSONB,
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now(),
            resolved BOOLEAN DEFAULT FALSE
          );
        `
      });
      
      // Use raw SQL for better reliability
      await this.supabase.rpc('raw_sql_query', {
        sql_query: `
          INSERT INTO monitoring.worker_issues
          (worker_name, issue_type, severity, message, details)
          VALUES ($1, $2, $3, $4, $5)`,
        params: JSON.stringify([
          this.workerName,
          issueType,
          severity,
          message,
          details
        ])
      });
    } catch (error) {
      // Log error but don't fail the processing
      this.logger.error("Failed to record worker issue", error);
      throw error; // Let caller handle further
    }
  }
}
