
import { createClient } from '@supabase/supabase-js';
import { EnvConfig } from './EnvConfig';

// Interface for metrics tracking
export interface ProcessingMetrics {
  queueName: string;
  operation: string;
  startedAt: Date;
  finishedAt?: Date;
  processedCount: number;
  successCount: number;
  errorCount: number;
  details?: Record<string, any>;
}

// Options for configuring the worker
export interface WorkerOptions {
  queueName: string;
  batchSize?: number;
  visibilityTimeout?: number;
  maxRetries?: number;
  baseDelayMs?: number;
}

// Standard error handling interface
export interface WorkerError {
  message: string;
  code?: string;
  details?: Record<string, any>;
  retryable: boolean;
}

export class BaseWorker<T = any> {
  protected supabase: ReturnType<typeof createClient>;
  protected queueName: string;
  protected batchSize: number;
  protected visibilityTimeout: number;
  protected maxRetries: number;
  protected baseDelayMs: number;
  protected cache: Map<string, { data: any; timestamp: number }> = new Map();
  protected cacheTTLMs = 3600000; // Default cache TTL: 1 hour
  
  // Rate limiting properties
  protected requestCounts: Record<string, { count: number; resetTime: number }> = {};
  protected maxRequestsPerWindow: Record<string, number> = {
    spotify: 100,  // Default: 100 requests per window for Spotify
    genius: 5,     // Default: 5 requests per window for Genius
  };
  protected windowMs: Record<string, number> = {
    spotify: 60000,  // Default: 1 minute window for Spotify
    genius: 1000,    // Default: 1 second window for Genius
  };
  
  // Circuit breaker properties
  protected circuitState: Record<string, { state: 'closed' | 'open' | 'half-open', failureCount: number, lastFailureTime: number }> = {};
  protected failureThreshold = 5;
  protected resetTimeoutMs = 30000; // 30 seconds
  
  constructor(options: WorkerOptions) {
    // Get Supabase credentials from environment
    this.supabase = createClient(
      EnvConfig.SUPABASE_URL,
      EnvConfig.SUPABASE_SERVICE_ROLE_KEY
    );
    
    this.queueName = options.queueName;
    this.batchSize = options.batchSize || 5;
    this.visibilityTimeout = options.visibilityTimeout || 60; // 60 seconds
    this.maxRetries = options.maxRetries || 3;
    this.baseDelayMs = options.baseDelayMs || 1000; // 1 second base delay for retries
    
    // Initialize circuit breakers for APIs
    this.circuitState.spotify = { state: 'closed', failureCount: 0, lastFailureTime: 0 };
    this.circuitState.genius = { state: 'closed', failureCount: 0, lastFailureTime: 0 };
  }

  // Process a batch of messages from the queue
  async processBatch(): Promise<ProcessingMetrics> {
    const metrics: ProcessingMetrics = {
      queueName: this.queueName,
      operation: 'batch_processing',
      startedAt: new Date(),
      processedCount: 0,
      successCount: 0,
      errorCount: 0,
      details: {}
    };
    
    try {
      // Get messages from queue
      const { data: messages, error } = await this.supabase.rpc('pg_dequeue', {
        queue_name: this.queueName,
        batch_size: this.batchSize,
        visibility_timeout: this.visibilityTimeout
      });
      
      if (error) throw error;
      
      if (!messages || !Array.isArray(messages) || messages.length === 0) {
        metrics.finishedAt = new Date();
        return metrics;
      }
      
      metrics.processedCount = messages.length;
      
      // Process each message in the batch
      const processPromises = messages.map(async (message: any) => {
        try {
          const payload = JSON.parse(message.message_body);
          
          // Check if the message has been retried too many times
          const retryCount = message.retry_count || 0;
          if (retryCount > this.maxRetries) {
            await this.handleDeadLetter(message.id, payload, 'max_retries_exceeded');
            await this.deleteMessage(message.id);
            return false;
          }
          
          // Process the message using derived class implementation
          await this.processMessage(payload);
          
          // Delete the message from the queue after successful processing
          await this.deleteMessage(message.id);
          return true;
        } catch (err) {
          const workerError = this.normalizeError(err);
          
          // Log the error
          await this.logWorkerIssue(workerError);
          
          // Handle retry or dead letter queue based on error
          if (workerError.retryable) {
            // Return to the queue with increased retry count
            await this.releaseMessage(message.id);
          } else {
            // Move to dead letter queue
            await this.handleDeadLetter(message.id, JSON.parse(message.message_body), workerError.message);
            await this.deleteMessage(message.id);
          }
          
          return false;
        }
      });
      
      const results = await Promise.allSettled(processPromises);
      metrics.successCount = results.filter(r => r.status === 'fulfilled' && r.value === true).length;
      metrics.errorCount = metrics.processedCount - metrics.successCount;
      
    } catch (err) {
      console.error('Error processing batch:', err);
      metrics.errorCount = metrics.processedCount;
    }
    
    metrics.finishedAt = new Date();
    
    // Record metrics
    await this.recordMetrics(metrics);
    
    return metrics;
  }
  
  // This method must be implemented by derived classes
  async processMessage(message: T): Promise<void> {
    throw new Error('processMessage must be implemented by derived class');
  }
  
  // Enqueue a new message
  async enqueue<M>(queueName: string, message: M): Promise<void> {
    const { error } = await this.supabase.rpc('pg_enqueue', {
      queue_name: queueName,
      message_body: JSON.stringify(message)
    });
    
    if (error) {
      console.error(`Error enqueuing message to ${queueName}:`, error);
      throw error;
    }
  }
  
  // Delete a message from the queue
  protected async deleteMessage(messageId: string): Promise<void> {
    const { error } = await this.supabase.rpc('pg_delete_message', {
      queue_name: this.queueName,
      message_id: messageId
    });
    
    if (error) {
      console.error(`Error deleting message ${messageId}:`, error);
      throw error;
    }
  }
  
  // Release a message back to the queue (for retries)
  protected async releaseMessage(messageId: string): Promise<void> {
    const { error } = await this.supabase.rpc('pg_release_message', {
      queue_name: this.queueName,
      message_id: messageId
    });
    
    if (error) {
      console.error(`Error releasing message ${messageId}:`, error);
      throw error;
    }
  }
  
  // Record worker metrics
  protected async recordMetrics(metrics: ProcessingMetrics): Promise<void> {
    const { error } = await this.supabase.from('queue_metrics').insert({
      queue_name: metrics.queueName,
      operation: metrics.operation,
      started_at: metrics.startedAt.toISOString(),
      finished_at: metrics.finishedAt?.toISOString(),
      processed_count: metrics.processedCount,
      success_count: metrics.successCount,
      error_count: metrics.errorCount,
      details: metrics.details
    });
    
    if (error) {
      console.error('Error recording metrics:', error);
    }
  }
  
  // Log worker issues
  protected async logWorkerIssue(error: WorkerError): Promise<void> {
    const { error: dbError } = await this.supabase.from('worker_issues').insert({
      worker_name: this.queueName,
      issue_type: error.code || 'unknown',
      message: error.message,
      details: error.details
    });
    
    if (dbError) {
      console.error('Error logging worker issue:', dbError);
    }
  }
  
  // Handle dead letter situations
  protected async handleDeadLetter(messageId: string, payload: T, reason: string): Promise<void> {
    // Log the dead letter message
    await this.logWorkerIssue({
      message: `Message moved to dead letter: ${reason}`,
      code: 'dead_letter',
      details: { messageId, payload },
      retryable: false
    });
  }
  
  // Normalize different error types to WorkerError
  protected normalizeError(error: any): WorkerError {
    if (error.retryable !== undefined) {
      // Already a WorkerError
      return error as WorkerError;
    }
    
    const workerError: WorkerError = {
      message: error.message || 'Unknown error',
      retryable: true
    };
    
    if (error.status === 429) {
      // Rate limit error
      workerError.code = 'rate_limited';
      workerError.retryable = true;
    } else if (error.status >= 500) {
      // Server error, can retry
      workerError.code = 'server_error';
      workerError.retryable = true;
    } else if (error.status >= 400 && error.status < 500) {
      // Client error, typically not retryable
      workerError.code = 'client_error';
      workerError.retryable = false;
    }
    
    return workerError;
  }
  
  // Rate limiting implementation
  protected async checkRateLimit(apiName: string): Promise<boolean> {
    const now = Date.now();
    
    if (!this.requestCounts[apiName]) {
      this.requestCounts[apiName] = { count: 0, resetTime: now + this.windowMs[apiName] };
    }
    
    if (now > this.requestCounts[apiName].resetTime) {
      // Reset the counter for a new time window
      this.requestCounts[apiName] = { count: 0, resetTime: now + this.windowMs[apiName] };
    }
    
    if (this.requestCounts[apiName].count >= this.maxRequestsPerWindow[apiName]) {
      return false; // Rate limit reached
    }
    
    this.requestCounts[apiName].count++;
    return true; // Rate limit not reached
  }
  
  // Wait until rate limit reset
  protected async waitForRateLimit(apiName: string): Promise<void> {
    if (!this.checkRateLimit(apiName)) {
      const waitTime = this.requestCounts[apiName].resetTime - Date.now();
      await new Promise(resolve => setTimeout(resolve, waitTime > 0 ? waitTime : 0));
      this.requestCounts[apiName] = { count: 0, resetTime: Date.now() + this.windowMs[apiName] };
    }
  }
  
  // Circuit breaker implementation
  protected async withCircuitBreaker<R>(apiName: string, fn: () => Promise<R>): Promise<R> {
    const circuit = this.circuitState[apiName];
    
    if (!circuit) {
      this.circuitState[apiName] = { state: 'closed', failureCount: 0, lastFailureTime: 0 };
    }
    
    const now = Date.now();
    
    // Check circuit state
    if (circuit?.state === 'open') {
      if (now - circuit.lastFailureTime > this.resetTimeoutMs) {
        // Try to recover by going to half-open
        circuit.state = 'half-open';
      } else {
        throw new Error(`Circuit for ${apiName} is open`);
      }
    }
    
    try {
      const result = await fn();
      
      // Success: reset circuit breaker if in half-open state
      if (circuit?.state === 'half-open') {
        circuit.state = 'closed';
        circuit.failureCount = 0;
      }
      
      return result;
    } catch (error) {
      // Failure: update circuit breaker
      if (!circuit) return Promise.reject(error);
      
      circuit.failureCount++;
      circuit.lastFailureTime = now;
      
      if (circuit.state === 'half-open' || circuit.failureCount >= this.failureThreshold) {
        circuit.state = 'open';
      }
      
      return Promise.reject(error);
    }
  }
  
  // Exponential backoff for retries
  protected async withBackoff<R>(fn: () => Promise<R>, retries = 3): Promise<R> {
    let lastError: any;
    
    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        return await fn();
      } catch (error) {
        lastError = error;
        
        if (attempt < retries) {
          // Calculate delay with exponential backoff and jitter
          const delay = this.baseDelayMs * Math.pow(2, attempt) * (0.5 + Math.random() * 0.5);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }
    
    throw lastError;
  }
  
  // Generic caching mechanism
  protected getCached<R>(key: string): R | null {
    const cached = this.cache.get(key);
    if (cached && Date.now() - cached.timestamp < this.cacheTTLMs) {
      return cached.data as R;
    }
    return null;
  }
  
  protected setCached<R>(key: string, data: R): void {
    this.cache.set(key, { data, timestamp: Date.now() });
  }
  
  // API-specific caching helpers
  protected async cachedFetch<R>(url: string, options: RequestInit, ttlMs?: number): Promise<R> {
    const cacheKey = `fetch:${url}:${JSON.stringify(options.headers || {})}`;
    const cached = this.getCached<R>(cacheKey);
    
    if (cached) return cached;
    
    const response = await fetch(url, options);
    
    if (!response.ok) {
      throw new Error(`HTTP error ${response.status}: ${response.statusText}`);
    }
    
    const result = await response.json() as R;
    
    if (ttlMs !== undefined) {
      // Override default TTL
      this.cacheTTLMs = ttlMs;
    }
    
    this.setCached(cacheKey, result);
    return result;
  }
}
