
import { createClient } from '@supabase/supabase-js';
import { EnvConfig } from './EnvConfig';
import { EdgeFunctionAdapter } from './EdgeFunctionAdapter';

interface WorkerOptions {
  queueName: string;
  batchSize?: number;
  visibilityTimeout?: number;
  maxRetries?: number;
}

export abstract class BaseWorker<T = any> {
  protected readonly queueName: string;
  protected readonly batchSize: number;
  protected readonly visibilityTimeout: number;
  protected readonly maxRetries: number;
  protected supabase: ReturnType<typeof createClient>;
  
  constructor(options: WorkerOptions) {
    this.queueName = options.queueName;
    this.batchSize = options.batchSize || 10;
    this.visibilityTimeout = options.visibilityTimeout || 60; // seconds
    this.maxRetries = options.maxRetries || 3;
    
    this.supabase = createClient(
      EnvConfig.SUPABASE_URL,
      EnvConfig.SUPABASE_SERVICE_ROLE_KEY
    );
  }
  
  /**
   * Abstract method that each worker must implement to process a message
   */
  abstract processMessage(message: T): Promise<void>;
  
  /**
   * Process a batch of messages from the queue
   */
  async processBatch(): Promise<{
    processed: number;
    successful: number;
    failed: number;
    duration: number;
  }> {
    const startTime = Date.now();
    
    try {
      // Start metric tracking
      const metricId = await this.startMetric();
      
      // Read from queue
      const messages = await EdgeFunctionAdapter.readFromQueue(
        this.queueName, 
        this.batchSize,
        this.visibilityTimeout
      );
      
      if (!messages || messages.length === 0) {
        await this.completeMetric(metricId, 0, 0, 0);
        return {
          processed: 0,
          successful: 0,
          failed: 0,
          duration: Date.now() - startTime
        };
      }
      
      console.log(`[${this.queueName}] Processing ${messages.length} messages`);
      
      let successful = 0;
      let failed = 0;
      
      // Process each message
      for (const msg of messages) {
        // Parse the message if it's a string
        const messageData: T = typeof msg.message === 'string'
          ? JSON.parse(msg.message) as T
          : msg.message as T;
        
        try {
          await this.processMessage(messageData);
          
          // Delete message from queue on success
          await EdgeFunctionAdapter.deleteFromQueue(this.queueName, msg.id);
          successful++;
          
        } catch (error) {
          failed++;
          console.error(`[${this.queueName}] Error processing message:`, error);
          
          // Retry logic is handled by queue visibility timeout
          // Message will be available again after visibility timeout expires
        }
      }
      
      // Complete metric tracking
      await this.completeMetric(metricId, messages.length, successful, failed);
      
      return {
        processed: messages.length,
        successful,
        failed,
        duration: Date.now() - startTime
      };
      
    } catch (error) {
      console.error(`[${this.queueName}] Error in processBatch:`, error);
      
      return {
        processed: 0,
        successful: 0,
        failed: 0,
        duration: Date.now() - startTime
      };
    }
  }
  
  /**
   * Enqueue a message to a specified queue
   */
  protected async enqueue(queueName: string, message: any): Promise<string> {
    return EdgeFunctionAdapter.sendToQueue(queueName, message);
  }
  
  /**
   * Start tracking metrics for this worker run
   */
  private async startMetric(): Promise<string> {
    try {
      const { data, error } = await this.supabase
        .from('queue_metrics')
        .insert({
          queue_name: this.queueName,
          operation: 'process_batch',
          started_at: new Date().toISOString(),
          processed_count: 0,
          success_count: 0,
          error_count: 0
        })
        .select('id')
        .single();
      
      if (error) {
        console.error(`[${this.queueName}] Error starting metric:`, error);
        return '';
      }
      
      return data.id;
    } catch (error) {
      console.error(`[${this.queueName}] Error starting metric:`, error);
      return '';
    }
  }
  
  /**
   * Complete metrics tracking for this worker run
   */
  private async completeMetric(
    metricId: string,
    processed: number,
    successful: number,
    failed: number
  ): Promise<void> {
    if (!metricId) return;
    
    try {
      await this.supabase
        .from('queue_metrics')
        .update({
          finished_at: new Date().toISOString(),
          processed_count: processed,
          success_count: successful,
          error_count: failed,
          details: {
            duration_ms: Date.now() - new Date().getTime()
          }
        })
        .eq('id', metricId);
    } catch (error) {
      console.error(`[${this.queueName}] Error completing metric:`, error);
    }
  }
  
  /**
   * Log a worker issue to the database
   */
  protected async logIssue(issueType: string, message: string, details?: any): Promise<void> {
    try {
      await this.supabase
        .from('worker_issues')
        .insert({
          worker_name: this.queueName,
          issue_type: issueType,
          message,
          details: details || {}
        });
    } catch (error) {
      console.error(`[${this.queueName}] Error logging issue:`, error);
    }
  }
}
