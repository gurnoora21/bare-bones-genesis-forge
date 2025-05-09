/**
 * Transaction Manager for Supabase
 * Provides proper transaction boundaries for database operations
 */

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

export interface TransactionOptions {
  timeout?: number; // timeout in milliseconds
  isolationLevel?: 'READ COMMITTED' | 'REPEATABLE READ' | 'SERIALIZABLE';
  correlationId?: string;
  retryOnConflict?: boolean;
}

export interface TransactionMetrics {
  startTime: number;
  endTime?: number;
  operationCount: number;
  status: 'pending' | 'committed' | 'rolledback' | 'error';
  error?: string;
}

export class TransactionManager {
  private supabase: any;
  private metrics: Map<string, TransactionMetrics> = new Map();
  
  constructor(supabaseClient?: any) {
    if (supabaseClient) {
      this.supabase = supabaseClient;
    } else {
      this.supabase = createClient(
        Deno.env.get("SUPABASE_URL") || "",
        Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || ""
      );
    }
  }
  
  /**
   * Execute a function within a database transaction
   * Automatically handles BEGIN/COMMIT/ROLLBACK
   */
  async transaction<T>(
    operationFn: (client: any, txId: string) => Promise<T>,
    options: TransactionOptions = {}
  ): Promise<T> {
    const {
      timeout = 30000, // 30 seconds default
      isolationLevel = 'READ COMMITTED',
      correlationId,
      retryOnConflict = true
    } = options;
    
    // Generate transaction ID
    const txId = correlationId || `tx_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
    
    // Initialize metrics
    this.metrics.set(txId, {
      startTime: Date.now(),
      operationCount: 0,
      status: 'pending'
    });
    
    // Set up timeout
    const timeoutPromise = new Promise<never>((_, reject) => {
      setTimeout(() => {
        this.metrics.set(txId, {
          ...this.metrics.get(txId)!,
          status: 'error',
          error: 'Transaction timeout'
        });
        reject(new Error(`Transaction timeout after ${timeout}ms`));
      }, timeout);
    });
    
    // Start transaction
    try {
      // BEGIN transaction with isolation level
      await this.supabase.rpc('raw_sql_query', {
        sql_query: `BEGIN TRANSACTION ISOLATION LEVEL ${isolationLevel}`
      });
      
      // Execute operation function with transaction client
      const result = await Promise.race([
        operationFn(this.supabase, txId),
        timeoutPromise
      ]);
      
      // COMMIT transaction on success
      await this.supabase.rpc('raw_sql_query', { sql_query: 'COMMIT' });
      
      // Update metrics
      this.metrics.set(txId, {
        ...this.metrics.get(txId)!,
        endTime: Date.now(),
        status: 'committed'
      });
      
      return result;
    } catch (error) {
      // ROLLBACK transaction on error
      try {
        await this.supabase.rpc('raw_sql_query', { sql_query: 'ROLLBACK' });
        
        // Update metrics
        this.metrics.set(txId, {
          ...this.metrics.get(txId)!,
          endTime: Date.now(),
          status: 'rolledback',
          error: error.message
        });
      } catch (rollbackError) {
        console.error(`[${txId}] Rollback error:`, rollbackError);
      }
      
      // If it's a serialization failure and retry is enabled, try again
      if (retryOnConflict && 
          error.message && 
          error.message.includes('could not serialize access')) {
        console.log(`[${txId}] Retrying transaction after serialization failure`);
        return this.transaction(operationFn, {
          ...options,
          correlationId: txId + '_retry'
        });
      }
      
      // Otherwise rethrow
      throw error;
    }
  }
  
  /**
   * Get transaction metrics for monitoring
   */
  getMetrics(txId?: string): TransactionMetrics | Map<string, TransactionMetrics> {
    if (txId) {
      return this.metrics.get(txId) || {
        startTime: 0,
        operationCount: 0,
        status: 'error',
        error: 'Transaction not found'
      };
    }
    return this.metrics;
  }
  
  /**
   * Clean up old transaction metrics
   */
  cleanupMetrics(maxAgeMs: number = 3600000): number {
    const now = Date.now();
    let cleaned = 0;
    
    for (const [txId, metrics] of this.metrics.entries()) {
      const age = now - metrics.startTime;
      if (age > maxAgeMs) {
        this.metrics.delete(txId);
        cleaned++;
      }
    }
    
    return cleaned;
  }
}

// Singleton instance for common use
let transactionManagerInstance: TransactionManager | null = null;

export function getTransactionManager(supabaseClient?: any): TransactionManager {
  if (!transactionManagerInstance) {
    transactionManagerInstance = new TransactionManager(supabaseClient);
  }
  return transactionManagerInstance;
}
