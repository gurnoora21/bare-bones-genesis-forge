
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

/**
 * Helper class for system reset operations
 */
export class SystemReset {
  private supabase: any;

  constructor(supabase?: any) {
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
   * Safely execute an SQL query with robust error handling
   */
  async executeSql(sql: string): Promise<{ success: boolean; error?: string; data?: any }> {
    try {
      const { data, error } = await this.supabase.rpc('raw_sql_query', {
        sql_query: sql
      });
      
      if (error) {
        return { success: false, error: error.message };
      }
      
      return { success: true, data };
    } catch (e) {
      return { success: false, error: e.message };
    }
  }

  /**
   * Safely drop a queue with error handling
   */
  async dropQueue(queueName: string, cascade: boolean = true): Promise<{ success: boolean; error?: string }> {
    try {
      const { data, error } = await this.supabase.rpc('raw_sql_query', {
        sql_query: `SELECT pgmq.drop_queue('${queueName}', ${cascade});`
      });
      
      if (error) {
        return { success: false, error: error.message };
      }
      
      return { success: true };
    } catch (e) {
      return { success: false, error: e.message };
    }
  }

  /**
   * Create a new queue with error handling
   */
  async createQueue(queueName: string): Promise<{ success: boolean; error?: string }> {
    try {
      const { data, error } = await this.supabase.rpc('raw_sql_query', {
        sql_query: `SELECT pgmq.create('${queueName}');`
      });
      
      if (error) {
        return { success: false, error: error.message };
      }
      
      return { success: true };
    } catch (e) {
      return { success: false, error: e.message };
    }
  }

  /**
   * Register a queue in the registry with error handling
   */
  async registerQueue(
    queueName: string, 
    displayName: string, 
    description: string
  ): Promise<{ success: boolean; error?: string }> {
    try {
      const { data, error } = await this.supabase.rpc('raw_sql_query', {
        sql_query: `
          INSERT INTO public.queue_registry (queue_name, display_name, description, active)
          VALUES ('${queueName}', '${displayName}', '${description}', true)
          ON CONFLICT (queue_name) DO UPDATE SET 
            display_name = '${displayName}', 
            description = '${description}', 
            active = true,
            updated_at = now();
        `
      });
      
      if (error) {
        return { success: false, error: error.message };
      }
      
      return { success: true };
    } catch (e) {
      return { success: false, error: e.message };
    }
  }
}
