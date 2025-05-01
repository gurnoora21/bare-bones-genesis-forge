
import { createClient } from '@supabase/supabase-js';
import { EnvConfig } from './EnvConfig';

// Import DenoTypes for type compatibility
import './DenoTypes';

/**
 * Check if we're running in a Deno environment
 */
const isDeno = typeof globalThis !== 'undefined' && 'Deno' in globalThis;

/**
 * Get environment variable safely in either Deno or Node
 */
function getEnvVar(key: string): string {
  if (isDeno) {
    return (globalThis as any).Deno.env.get(key) || '';
  }
  return EnvConfig[key as keyof typeof EnvConfig] || '';
}

/**
 * Adapter for implementing functions that can run both 
 * in Node.js and as Supabase Edge Functions in Deno
 */
export class EdgeFunctionAdapter {
  /**
   * Creates a queue message in Supabase PGMQ
   */
  static async sendToQueue(queueName: string, message: any): Promise<string> {
    // For Edge Function environment
    if (isDeno) {
      const supabase = createClient(
        getEnvVar("SUPABASE_URL"),
        getEnvVar("SUPABASE_SERVICE_ROLE_KEY")
      );
      
      const { data, error } = await supabase.functions.invoke("send-to-queue", {
        body: {
          queue_name: queueName,
          message: typeof message === 'string' ? message : JSON.stringify(message)
        }
      });
      
      if (error) throw error;
      return data.message_id;
    } 
    // For Node.js environment
    else {
      const supabase = createClient(
        EnvConfig.SUPABASE_URL,
        EnvConfig.SUPABASE_SERVICE_ROLE_KEY
      );
      
      const { data, error } = await supabase.rpc('pg_enqueue', {
        queue_name: queueName,
        message_body: typeof message === 'string' ? message : JSON.stringify(message)
      });
      
      if (error) throw error;
      return data;
    }
  }
  
  /**
   * Reads messages from a Supabase PGMQ queue
   */
  static async readFromQueue(queueName: string, batchSize = 5, visibilityTimeout = 60): Promise<any[]> {
    // For Edge Function environment
    if (isDeno) {
      const supabase = createClient(
        getEnvVar("SUPABASE_URL"),
        getEnvVar("SUPABASE_SERVICE_ROLE_KEY")
      );
      
      const { data, error } = await supabase.functions.invoke("read-queue", {
        body: { 
          queue_name: queueName,
          batch_size: batchSize,
          visibility_timeout: visibilityTimeout
        }
      });
      
      if (error) throw error;
      return data;
    } 
    // For Node.js environment
    else {
      const supabase = createClient(
        EnvConfig.SUPABASE_URL,
        EnvConfig.SUPABASE_SERVICE_ROLE_KEY
      );
      
      const { data, error } = await supabase.rpc('pg_dequeue', {
        queue_name: queueName,
        batch_size: batchSize,
        visibility_timeout: visibilityTimeout
      });
      
      if (error) throw error;
      
      // Parse message bodies if they're strings
      return data.messages.map((msg: any) => ({
        ...msg,
        message: typeof msg.message === 'string' ? JSON.parse(msg.message) : msg.message
      }));
    }
  }
  
  /**
   * Deletes a message from a Supabase PGMQ queue (acknowledges completion)
   */
  static async deleteFromQueue(queueName: string, messageId: string): Promise<boolean> {
    // For Edge Function environment
    if (isDeno) {
      const supabase = createClient(
        getEnvVar("SUPABASE_URL"),
        getEnvVar("SUPABASE_SERVICE_ROLE_KEY")
      );
      
      const { data, error } = await supabase.functions.invoke("delete-from-queue", {
        body: { 
          queue_name: queueName,
          message_id: messageId
        }
      });
      
      if (error) throw error;
      return data.success;
    } 
    // For Node.js environment
    else {
      const supabase = createClient(
        EnvConfig.SUPABASE_URL,
        EnvConfig.SUPABASE_SERVICE_ROLE_KEY
      );
      
      const { data, error } = await supabase.rpc('pg_delete_message', {
        queue_name: queueName,
        message_id: messageId
      });
      
      if (error) throw error;
      return data;
    }
  }
}
