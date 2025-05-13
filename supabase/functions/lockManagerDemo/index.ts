
// Lock Manager Demo
// Demonstrates how to use PostgreSQL advisory locks for distributed locking

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { PgLockManager } from "../_shared/pgLockManager.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req: Request) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Create Supabase client
    const supabase = createClient(
      Deno.env.get('SUPABASE_URL') ?? '',
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
    );

    // Create the lock manager
    const lockManager = new PgLockManager(supabase);
    
    // Parse request body
    const { action, entityType, entityId, timeoutSeconds, wait } = await req.json();
    
    if (!entityType || !entityId) {
      return new Response(
        JSON.stringify({ success: false, error: "Missing entityType or entityId" }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" }, status: 400 }
      );
    }
    
    let result;
    
    // Perform requested action
    switch (action) {
      case "acquire":
        result = await lockManager.acquireLock(entityType, entityId, {
          timeoutSeconds: timeoutSeconds || 0,
          correlationId: `demo_${Date.now()}`
        });
        
        // If wait is specified, simulate a long-running task
        if (wait && result.success) {
          await new Promise(resolve => setTimeout(resolve, parseInt(wait, 10) || 5000));
          // Auto-release after waiting
          const releaseResult = await lockManager.releaseLock(entityType, entityId);
          result.released = releaseResult;
          result.message = "Lock acquired, task processed, and lock released";
        }
        break;
        
      case "release":
        result = {
          success: await lockManager.releaseLock(entityType, entityId)
        };
        break;
        
      case "check":
        result = {
          locked: await lockManager.isLocked(entityType, entityId)
        };
        break;
        
      case "cleanup":
        const minutes = parseInt(timeoutSeconds, 10) || 30;
        const count = await lockManager.cleanupStaleLocks(minutes);
        result = {
          success: true,
          cleaned: count,
          message: `Cleaned up ${count} stale locks older than ${minutes} minutes`
        };
        break;
        
      default:
        result = {
          success: false,
          error: "Unknown action. Use 'acquire', 'release', 'check', or 'cleanup'"
        };
    }
    
    // Return the result
    return new Response(
      JSON.stringify(result),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
    
  } catch (error) {
    console.error(`Error: ${error.message}`);
    return new Response(
      JSON.stringify({ success: false, error: error.message }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" }, status: 500 }
    );
  }
});
