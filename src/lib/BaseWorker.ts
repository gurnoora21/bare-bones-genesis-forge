
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
  
  // Rate limiting properties
  protected maxRequestsPerWindow: Record<string, number> = {};
  protected windowMs: Record<string, number> = {};
  protected lastRequestTime: Record<string, number> = {};
  protected requestsInWindow: Record<string, number> = {};
  
  // Circuit breaker properties
  protected circuitState: Record<string, 'closed' | 'open' | 'half-open'> = {};
  protected failureCount: Record<string, number> = {};
  protected nextAttempt: Record<string, number> = {};
  
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
          const messageId = typeof msg.id === 'string' ? msg.id : String(msg.id);
          await EdgeFunctionAdapter.deleteFromQueue(this.queueName, messageId);
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
   * Wait for rate limit window to reset
   */
  protected async waitForRateLimit(service: string): Promise<void> {
    const now = Date.now();
    const maxRequests = this.maxRequestsPerWindow[service] || 10;
    const windowTime = this.windowMs[service] || 1000;
    
    // Initialize tracking for this service
    if (!this.lastRequestTime[service]) {
      this.lastRequestTime[service] = now;
      this.requestsInWindow[service] = 0;
    }
    
    // Check if window should reset
    if (now - this.lastRequestTime[service] > windowTime) {
      this.lastRequestTime[service] = now;
      this.requestsInWindow[service] = 0;
      return;
    }
    
    // Check if we need to wait
    if (this.requestsInWindow[service] >= maxRequests) {
      const waitTime = windowTime - (now - this.lastRequestTime[service]);
      if (waitTime > 0) {
        await new Promise(resolve => setTimeout(resolve, waitTime));
        
        // Reset after waiting
        this.lastRequestTime[service] = Date.now();
        this.requestsInWindow[service] = 0;
      }
    }
    
    // Increment request count
    this.requestsInWindow[service]++;
  }
  
  /**
   * Circuit breaker pattern for external services
   */
  protected async withCircuitBreaker<R>(service: string, fn: () => Promise<R>): Promise<R> {
    // Initialize circuit state
    if (!this.circuitState[service]) {
      this.circuitState[service] = 'closed';
      this.failureCount[service] = 0;
      this.nextAttempt[service] = 0;
    }
    
    const now = Date.now();
    
    // Check if circuit is open
    if (this.circuitState[service] === 'open') {
      if (now < this.nextAttempt[service]) {
        throw new Error(`Circuit for ${service} is open`);
      }
      
      // Move to half-open state
      this.circuitState[service] = 'half-open';
    }
    
    try {
      const result = await fn();
      
      // Success, close the circuit
      this.circuitState[service] = 'closed';
      this.failureCount[service] = 0;
      
      return result;
    } catch (error) {
      // Increment failure count
      this.failureCount[service]++;
      
      // Check if we should open the circuit
      if (this.failureCount[service] >= 3 || this.circuitState[service] === 'half-open') {
        this.circuitState[service] = 'open';
        
        // Wait longer each time
        const waitTime = Math.min(60000, 1000 * Math.pow(2, this.failureCount[service]));
        this.nextAttempt[service] = now + waitTime;
      }
      
      throw error;
    }
  }
  
  /**
   * Cached fetch for external HTTP requests
   */
  protected async cachedFetch<R>(url: string, options: RequestInit = {}, cacheTtl = 3600000): Promise<R> {
    // Simple in-memory cache (in production you'd use Redis or similar)
    const cacheKey = `${url}:${JSON.stringify(options)}`;
    const cache = (global as any).__fetchCache = (global as any).__fetchCache || {};
    
    const cachedItem = cache[cacheKey];
    if (cachedItem && cachedItem.expires > Date.now()) {
      return cachedItem.data;
    }
    
    const response = await fetch(url, options);
    
    if (!response.ok) {
      throw new Error(`HTTP error ${response.status}: ${response.statusText}`);
    }
    
    const data = await response.json();
    
    // Cache the result
    cache[cacheKey] = {
      data,
      expires: Date.now() + cacheTtl
    };
    
    return data;
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
