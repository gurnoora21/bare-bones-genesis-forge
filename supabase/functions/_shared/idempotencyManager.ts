
/**
 * IdempotencyManager
 * Handles idempotent operations using Postgres to track and store operation state
 */
import { createClient, SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

export interface IdempotencyOptions {
  operationId: string;
  entityType?: string;
  entityId?: string;
}

export interface IdempotencyResult<T = any> {
  status: 'success' | 'error';
  result?: T;
  error?: string;
  alreadyProcessed?: boolean;
}

export class IdempotencyManager {
  private supabase: SupabaseClient;
  
  constructor(supabase?: SupabaseClient) {
    if (supabase) {
      this.supabase = supabase;
    } else {
      this.supabase = createClient(
        Deno.env.get("SUPABASE_URL") || "",
        Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || ""
      );
    }
  }
  
  /**
   * Execute an operation with idempotency guarantees
   * @param options Idempotency options
   * @param operation The operation to execute
   * @returns The result of the operation or the cached result if already executed
   */
  async execute<T = any>(
    options: IdempotencyOptions,
    operation: () => Promise<T>
  ): Promise<IdempotencyResult<T>> {
    const { operationId, entityType = 'general', entityId = 'none' } = options;
    
    try {
      // Check if operation already processed
      const { data: existingOp, error: checkError } = await this.supabase
        .rpc('check_operation', {
          p_operation_id: operationId,
          p_entity_type: entityType,
          p_entity_id: entityId
        });
      
      if (checkError) {
        console.error(`[IdempotencyManager] Failed to check operation: ${checkError.message}`);
        // Continue with operation if idempotency check fails
      } else if (existingOp && existingOp.exists) {
        console.log(`[IdempotencyManager] Operation ${operationId} already processed, returning cached result`);
        // Return the cached result
        return {
          status: existingOp.status === 'COMPLETED' ? 'success' : 'error',
          result: existingOp.result,
          error: existingOp.status === 'FAILED' ? existingOp.result?.error : undefined,
          alreadyProcessed: true
        };
      }
      
      // Execute the operation
      console.log(`[IdempotencyManager] Executing operation ${operationId}`);
      const result = await operation();
      
      // Mark operation as processed
      try {
        await this.supabase.rpc('mark_processed', {
          p_operation_id: operationId,
          p_entity_type: entityType,
          p_entity_id: entityId,
          p_result: result !== null && typeof result === 'object' 
            ? result 
            : { value: result },
          p_metadata: { processed_at: new Date().toISOString() },
          p_status: 'COMPLETED'
        });
      } catch (markError) {
        console.error(`[IdempotencyManager] Failed to mark operation as processed: ${markError.message}`, markError);
        // Continue without idempotency marker if marking fails
      }
      
      return {
        status: 'success',
        result,
        alreadyProcessed: false
      };
      
    } catch (error) {
      console.error(`[IdempotencyManager] Operation ${operationId} failed: ${error.message}`, error);
      
      // Attempt to mark as failed
      try {
        await this.supabase.rpc('mark_processed', {
          p_operation_id: operationId,
          p_entity_type: entityType,
          p_entity_id: entityId,
          p_result: { error: error.message },
          p_metadata: { 
            processed_at: new Date().toISOString(),
            error_stack: error.stack
          },
          p_status: 'FAILED'
        });
      } catch (markError) {
        console.error(`[IdempotencyManager] Failed to mark operation as failed: ${markError.message}`);
        // Continue without idempotency marker if marking fails
      }
      
      return {
        status: 'error',
        error: error.message,
        alreadyProcessed: false
      };
    }
  }
  
  /**
   * Get singleton instance
   */
  static getInstance(supabase?: SupabaseClient): IdempotencyManager {
    if (!IdempotencyManager.instance) {
      IdempotencyManager.instance = new IdempotencyManager(supabase);
    }
    return IdempotencyManager.instance;
  }
  
  private static instance: IdempotencyManager;
}

// Export a factory function to get the manager instance
export const getIdempotencyManager = (supabase?: SupabaseClient): IdempotencyManager => {
  return IdempotencyManager.getInstance(supabase);
};
