
/**
 * EnhancedQueueWorker - A scalable worker base class with self-draining capabilities
 * 
 * This worker extends IdempotentWorker with improved throughput handling:
 * - Supports self-draining loops to process multiple batches within one invocation
 * - Implements adaptive backoff based on queue depth
 * - Provides detailed metrics for monitoring
 */

import { createClient, SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { IdempotentWorker, ProcessOptions } from "./idempotentWorker.ts";
import { getDeduplicationService } from "./deduplication.ts";
import { getQueueManager } from "./pgQueueManager.ts";

export interface QueueMetrics {
  processed: number;
  errors: number;
  duplicates: number;
  skipped: number;
  processingTimeMs: number;
}

export interface DrainOptions extends ProcessOptions {
  // Maximum number of batches to process in a single invocation
  maxBatches?: number;
  
  // Maximum total runtime in milliseconds
  maxRuntimeMs?: number;
  
  // Delay between batches in milliseconds
  batchDelayMs?: number;
  
  // Whether to log detailed metrics
  logDetailedMetrics?: boolean;
  
  // Whether to automatically back off if queue is empty
  adaptiveBackoff?: boolean;
}

export class EnhancedQueueWorker<T = any> extends IdempotentWorker<T> {
  protected queueManager: any;
  protected metrics: QueueMetrics;
  protected startTime: number;
  protected abortController: AbortController;
  
  constructor(queue: string, supabase?: SupabaseClient, redis?: Redis) {
    super(queue, supabase, redis);
    
    this.queueManager = getQueueManager(this.supabase);
    this.metrics = {
      processed: 0,
      errors: 0,
      duplicates: 0,
      skipped: 0,
      processingTimeMs: 0
    };
    this.startTime = 0;
    this.abortController = new AbortController();
  }
  
  /**
   * Process multiple batches within time and count constraints
   */
  async drainQueue(options: DrainOptions): Promise<QueueMetrics> {
    const {
      maxBatches = 5,
      maxRuntimeMs = 240000, // 4 minutes default (under the 5-minute cron and most function timeouts)
      batchDelayMs = 250,
      batchSize = 5,
      timeoutSeconds = 60,
      processorName = 'enhanced',
      visibilityTimeoutSeconds = 30,
      logDetailedMetrics = true,
      adaptiveBackoff = true
    } = options;
    
    this.startTime = Date.now();
    this.metrics = {
      processed: 0, 
      errors: 0, 
      duplicates: 0,
      skipped: 0,
      processingTimeMs: 0
    };
    
    // Create a new abort controller for this drain operation
    this.abortController = new AbortController();
    
    let batchCount = 0;
    let queueSize = 0;
    let backoffFactor = 1;
    let consecutiveEmptyBatches = 0;
    
    try {
      // Get initial queue status (if possible)
      try {
        const status = await this.queueManager.getQueueStatus(this.queue);
        queueSize = status?.count || 0;
        
        console.log(`Starting drain of ${this.queue}: ${queueSize} messages pending, max ${maxBatches} batches, max runtime ${maxRuntimeMs}ms`);
      } catch (error) {
        // Non-critical error, just log and continue
        console.warn(`Could not get queue status for ${this.queue}: ${error.message}`);
      }
      
      // Process batches until maxBatches reached, maxRuntimeMs exceeded, or queue empty
      while (batchCount < maxBatches && !this.isTimeExceeded(maxRuntimeMs)) {
        // Check if we need to abort
        if (this.abortController.signal.aborted) {
          console.log(`Processing aborted for ${this.queue} after ${batchCount} batches`);
          break;
        }
        
        // Process a single batch
        const batchStartTime = Date.now();
        const batchResult = await this.processBatch({
          batchSize,
          timeoutSeconds,
          processorName: `${processorName}-${batchCount + 1}`,
          visibilityTimeoutSeconds
        });
        
        const batchTime = Date.now() - batchStartTime;
        
        // Update aggregate metrics
        this.metrics.processed += batchResult.processed;
        this.metrics.errors += batchResult.errors;
        batchCount++;
        
        if (batchResult.processed === 0) {
          consecutiveEmptyBatches++;
          
          // Break early if queue seems empty
          if (consecutiveEmptyBatches >= 2) {
            console.log(`No messages found in ${consecutiveEmptyBatches} consecutive batches, queue may be empty`);
            break;
          }
          
          // Implement backoff if adaptiveBackoff enabled
          if (adaptiveBackoff) {
            backoffFactor = Math.min(backoffFactor * 2, 8);
          }
        } else {
          consecutiveEmptyBatches = 0;
          backoffFactor = 1;
        }
        
        if (logDetailedMetrics) {
          console.log(`Batch ${batchCount}/${maxBatches}: processed ${batchResult.processed}, errors ${batchResult.errors}, time ${batchTime}ms`);
        }
        
        // Wait between batches with adaptive backoff
        if (batchCount < maxBatches) {
          await new Promise(resolve => setTimeout(resolve, batchDelayMs * backoffFactor));
        }
      }
      
      // Calculate total processing time
      this.metrics.processingTimeMs = Date.now() - this.startTime;
      
      // Log final metrics
      console.log(`Completed drain of ${this.queue}: processed ${this.metrics.processed}, errors ${this.metrics.errors}, time ${this.metrics.processingTimeMs}ms`);
      
      // Record metrics in database
      this.recordMetrics();
      
      return this.metrics;
    } catch (error) {
      console.error(`Error during queue drain: ${error.message}`);
      
      // Still record metrics even on error
      this.metrics.processingTimeMs = Date.now() - this.startTime;
      this.recordMetrics();
      
      return {
        ...this.metrics,
        errors: this.metrics.errors + 1 // Add the drain error
      };
    }
  }
  
  /**
   * Check if maximum runtime has been exceeded
   */
  protected isTimeExceeded(maxRuntimeMs: number): boolean {
    const elapsed = Date.now() - this.startTime;
    // Add 10% buffer to ensure we don't exceed function timeout
    return elapsed > (maxRuntimeMs * 0.9);
  }
  
  /**
   * Abort ongoing processing (can be called from outside)
   */
  abort(): void {
    this.abortController.abort();
  }
  
  /**
   * Record metrics to the database
   */
  protected async recordMetrics(): Promise<void> {
    try {
      await this.supabase.from('queue_metrics').insert({
        queue_name: this.queue,
        operation: 'drain',
        started_at: new Date(this.startTime).toISOString(),
        finished_at: new Date().toISOString(),
        processed_count: this.metrics.processed,
        success_count: this.metrics.processed - this.metrics.errors,
        error_count: this.metrics.errors,
        details: {
          duplicates: this.metrics.duplicates,
          skipped: this.metrics.skipped,
          processing_time_ms: this.metrics.processingTimeMs,
          batches: this.metrics.processed > 0 ? Math.ceil(this.metrics.processed / 5) : 0
        }
      });
    } catch (error) {
      console.error(`Failed to record metrics for ${this.queue}: ${error.message}`);
    }
  }
}

/**
 * Create an EnhancedQueueWorker instance
 */
export function createEnhancedWorker<T = any>(
  queue: string, 
  supabase?: SupabaseClient, 
  redis?: Redis
): EnhancedQueueWorker<T> {
  return new EnhancedQueueWorker<T>(queue, supabase, redis);
}
