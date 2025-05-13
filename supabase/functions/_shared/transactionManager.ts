/**
 * Transaction Manager for Supabase
 * Provides proper transaction boundaries for database operations
 */

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { getIdempotencyManager, IdempotencyOptions, IdempotencyResult } from "./idempotencyManager.ts";

export interface TransactionOptions {
  timeout?: number; // timeout in milliseconds
  isolationLevel?: 'READ COMMITTED' | 'REPEATABLE READ' | 'SERIALIZABLE';
  correlationId?: string;
  retryOnConflict?: boolean;
  maxRetries?: number;
  retryDelay?: number; // base delay in milliseconds for exponential backoff
  idempotency?: IdempotencyOptions; // idempotency options
}

export interface TransactionMetrics {
  startTime: number;
  endTime?: number;
  operationCount: number;
  status: 'pending' | 'committed' | 'rolledback' | 'error';
  error?: string;
  retries?: number;
}

export interface SqlExecutionOptions {
  operationId?: string;
  entityType?: string;
  entityId?: string;
  params?: Record<string, any>;
  retryOnError?: boolean;
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
      retryOnConflict = true,
      maxRetries = 3,
      retryDelay = 100, // exponential backoff starting at 100ms
      idempotency
    } = options;
    
    // Generate transaction ID
    const txId = correlationId || `tx_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
    
    // Check idempotency if options provided
    if (idempotency?.operationId) {
      const idempotencyManager = getIdempotencyManager(this.supabase);
      const checkResult = await idempotencyManager.checkOperation<T>(idempotency);
      
      if (checkResult.alreadyProcessed) {
        console.log(`[${txId}] Operation ${idempotency.operationId} already processed, returning cached result`);
        return checkResult.result as T;
      }
    }
    
    // Initialize metrics
    this.metrics.set(txId, {
      startTime: Date.now(),
      operationCount: 0,
      status: 'pending',
      retries: 0
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
    
    let retries = 0;
    let lastError: Error | null = null;
    
    // Start transaction with retry logic for serialization failures
    while (retries <= maxRetries) {
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
          status: 'committed',
          retries
        });
        
        // If idempotency is enabled, mark as processed
        if (idempotency?.operationId) {
          const idempotencyManager = getIdempotencyManager(this.supabase);
          await idempotencyManager.markProcessed(idempotency, result);
        }
        
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
            error: error.message,
            retries
          });
        } catch (rollbackError) {
          console.error(`[${txId}] Rollback error:`, rollbackError);
        }
        
        // If it's a serialization failure and retry is enabled, try again
        if (retryOnConflict && 
            error.message && 
            (error.message.includes('could not serialize access') || 
             error.message.includes('deadlock detected') ||
             error.message.includes('concurrent update'))) {
          lastError = error;
          retries++;
          
          if (retries <= maxRetries) {
            // Exponential backoff with jitter
            const delay = retryDelay * Math.pow(2, retries - 1) * (0.5 + Math.random() * 0.5);
            console.log(`[${txId}] Retrying transaction after serialization failure (attempt ${retries}/${maxRetries}). Waiting ${delay.toFixed(0)}ms`);
            await new Promise(resolve => setTimeout(resolve, delay));
            continue;
          }
        }
        
        // Mark as error in idempotency if enabled
        if (idempotency?.operationId) {
          const idempotencyManager = getIdempotencyManager(this.supabase);
          await idempotencyManager.markProcessed(
            idempotency, 
            { error: error.message }, 
            'error',
            { stack: error.stack }
          );
        }
        
        // Otherwise rethrow
        throw error;
      }
    }
    
    // If we've exhausted retries, throw the last error
    if (lastError) {
      throw new Error(`Transaction failed after ${retries} retries: ${lastError.message}`);
    }
    
    // This should never happen, but TypeScript needs it
    throw new Error('Unexpected transaction state');
  }
  
  /**
   * Execute SQL within a transaction with idempotency
   */
  async executeSql<T = any>(
    sql: string,
    options: SqlExecutionOptions = {}
  ): Promise<T> {
    const {
      operationId,
      entityType = 'sql',
      entityId,
      params = {},
      retryOnError = true
    } = options;
    
    // If operationId is provided, use idempotency
    if (operationId) {
      // Setup idempotency options
      const idempotencyOptions: IdempotencyOptions = {
        operationId,
        entityType,
        entityId: entityId || operationId
      };
      
      // Use the database's execute_in_transaction function
      const { data, error } = await this.supabase.rpc('execute_in_transaction', {
        p_sql: sql,
        p_params: params,
        p_operation_id: operationId,
        p_entity_type: entityType,
        p_entity_id: entityId || operationId
      });
      
      if (error) {
        throw new Error(`SQL execution error: ${error.message}`);
      }
      
      if (data.status === 'error') {
        throw new Error(`SQL execution failed: ${data.error}`);
      }
      
      return data.result as T;
    } else {
      // Execute without idempotency tracking
      return this.transaction(async (client) => {
        const { data, error } = await client.rpc('raw_sql_query', {
          sql_query: sql,
          params: JSON.stringify(params)
        });
        
        if (error) {
          throw new Error(`SQL execution error: ${error.message}`);
        }
        
        return data as T;
      }, { retryOnConflict: retryOnError });
    }
  }
  
  /**
   * Execute an atomic operation against database function
   * Provides idempotency and transactional guarantees
   */
  async atomicOperation<T = any>(
    functionName: string,
    params: Record<string, any>,
    options: {
      operationId?: string;
      entityType?: string;
      entityId?: string;
    } = {}
  ): Promise<T> {
    const { operationId, entityType, entityId } = options;
    
    // If operationId is provided, check idempotency first
    if (operationId) {
      const idempotencyManager = getIdempotencyManager(this.supabase);
      const checkResult = await idempotencyManager.checkOperation<T>({
        operationId,
        entityType,
        entityId
      });
      
      if (checkResult.alreadyProcessed) {
        console.log(`Operation ${operationId} already processed, returning cached result`);
        return checkResult.result as T;
      }
    }
    
    try {
      // Call the database function
      const { data, error } = await this.supabase.rpc(functionName, params);
      
      if (error) {
        throw new Error(`Atomic operation failed: ${error.message}`);
      }
      
      // If operationId is provided, mark as processed
      if (operationId) {
        const idempotencyManager = getIdempotencyManager(this.supabase);
        await idempotencyManager.markProcessed(
          { operationId, entityType, entityId },
          data
        );
      }
      
      return data as T;
    } catch (error) {
      // If operationId is provided, mark as failed
      if (operationId) {
        const idempotencyManager = getIdempotencyManager(this.supabase);
        await idempotencyManager.markProcessed(
          { operationId, entityType, entityId },
          { error: error.message },
          'error'
        );
      }
      
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
