
/**
 * Idempotency Manager for Supabase
 * Ensures operations are only executed once even if called multiple times
 */

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

export interface IdempotencyOptions {
  operationId?: string; // Unique identifier for this operation
  entityType?: string;  // Type of entity being processed
  entityId?: string;    // ID of the entity being processed
  ttlSeconds?: number;  // How long to keep the idempotency record
}

export interface IdempotencyResult<T = any> {
  alreadyProcessed: boolean;  // Whether the operation was already processed
  result?: T;                // Result from previous operation if already processed
  status: 'success' | 'error' | 'pending'; // Status of the operation
  metadata?: Record<string, any>; // Any additional metadata
  error?: string;            // Error message if status is 'error'
}

export class IdempotencyManager {
  private supabase: any;
  
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
   * Check if an operation has already been processed
   */
  async checkOperation<T = any>(options: IdempotencyOptions): Promise<IdempotencyResult<T>> {
    const { operationId, entityType, entityId } = options;
    
    if (!operationId) {
      return {
        alreadyProcessed: false,
        status: 'pending'
      };
    }
    
    try {
      const { data, error } = await this.supabase.rpc(
        'idempotency.check_operation',
        {
          p_operation_id: operationId,
          p_entity_type: entityType || null,
          p_entity_id: entityId || null
        }
      );
      
      if (error) {
        console.error(`Error checking operation ${operationId}: ${error.message}`);
        // If there's an error checking, we'll assume not processed for safety
        return {
          alreadyProcessed: false,
          status: 'pending',
          error: error.message
        };
      }
      
      if (data && data.exists) {
        return {
          alreadyProcessed: true,
          status: data.status.toLowerCase() as 'success' | 'error',
          result: data.result as T,
          metadata: data.metadata as Record<string, any>
        };
      }
      
      return {
        alreadyProcessed: false,
        status: 'pending'
      };
    } catch (error) {
      console.error(`Exception checking operation ${operationId}: ${error.message}`);
      // If there's an exception, we'll assume not processed for safety
      return {
        alreadyProcessed: false,
        status: 'pending',
        error: error.message
      };
    }
  }
  
  /**
   * Mark an operation as processed with its result
   */
  async markProcessed<T = any>(
    options: IdempotencyOptions,
    result: T,
    status: 'success' | 'error' = 'success',
    metadata?: Record<string, any>
  ): Promise<boolean> {
    const { operationId, entityType, entityId } = options;
    
    if (!operationId) {
      return false; // Can't mark as processed without an operation ID
    }
    
    try {
      const { data, error } = await this.supabase.rpc(
        'idempotency.mark_processed',
        {
          p_operation_id: operationId,
          p_entity_type: entityType || 'general',
          p_entity_id: entityId || operationId,
          p_result: result ? JSON.parse(JSON.stringify(result)) : null,
          p_metadata: metadata ? JSON.parse(JSON.stringify(metadata)) : null,
          p_status: status.toUpperCase()
        }
      );
      
      if (error) {
        console.error(`Error marking operation ${operationId} as processed: ${error.message}`);
        return false;
      }
      
      return true;
    } catch (error) {
      console.error(`Exception marking operation ${operationId} as processed: ${error.message}`);
      return false;
    }
  }
  
  /**
   * Execute a function with idempotency guarantees
   */
  async execute<T = any>(
    options: IdempotencyOptions,
    fn: () => Promise<T>
  ): Promise<IdempotencyResult<T>> {
    // First check if already processed
    const checkResult = await this.checkOperation<T>(options);
    
    // If already processed, return cached result
    if (checkResult.alreadyProcessed) {
      return checkResult;
    }
    
    try {
      // Execute the function
      const result = await fn();
      
      // Mark as processed with success
      await this.markProcessed(options, result);
      
      return {
        alreadyProcessed: false,
        status: 'success',
        result
      };
    } catch (error) {
      // Mark as processed with error
      const errorMessage = error instanceof Error ? error.message : String(error);
      await this.markProcessed(
        options,
        { error: errorMessage },
        'error',
        { stack: error instanceof Error ? error.stack : undefined }
      );
      
      return {
        alreadyProcessed: false,
        status: 'error',
        error: errorMessage
      };
    }
  }
}

// Singleton instance for common use
let idempotencyManagerInstance: IdempotencyManager | null = null;

export function getIdempotencyManager(supabaseClient?: any): IdempotencyManager {
  if (!idempotencyManagerInstance) {
    idempotencyManagerInstance = new IdempotencyManager(supabaseClient);
  }
  return idempotencyManagerInstance;
}
