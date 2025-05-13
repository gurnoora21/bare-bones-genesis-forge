
// Edge Function to periodically clean up stale entity processing states
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { getStateManager } from "../_shared/enhancedStateManager.ts";
import { EntityType } from "../_shared/stateManager.ts";

// Setup Supabase client
const supabaseUrl = Deno.env.get("SUPABASE_URL") as string;
const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") as string;
const supabase = createClient(supabaseUrl, supabaseKey);

// Configure CORS
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

// Entry point for cleanup operation
Deno.serve(async (req) => {
  // Handle CORS preflight request
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  // Handle non-GET requests
  if (req.method !== "POST") {
    return new Response(
      JSON.stringify({ error: "Method not allowed" }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" }, status: 405 }
    );
  }

  try {
    // Get request parameters
    const params = await req.json().catch(() => ({}));
    const ageMinutes = params.ageMinutes || 60; // Default to 60 minutes
    const entityType = params.entityType as EntityType | undefined; 
    
    console.log(`Starting stale entity cleanup: age=${ageMinutes}m, type=${entityType || 'all'}`);
    
    // Background processing using EdgeRuntime.waitUntil
    EdgeRuntime.waitUntil(cleanupStaleEntities(ageMinutes, entityType));
    
    // Return immediate response
    return new Response(
      JSON.stringify({ 
        success: true,
        message: "Cleanup process started in background", 
        params: { 
          ageMinutes,
          entityType: entityType || "all" 
        }
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  } catch (error) {
    console.error("Error in cleanup function:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" }, status: 500 }
    );
  }
});

// Main cleanup function to run in background
async function cleanupStaleEntities(ageMinutes: number, entityType?: EntityType): Promise<void> {
  try {
    console.log("Starting background cleanup process");
    
    // Step 1: Find and clean stale locks in database
    const { data: dbCleanupResults, error: dbError } = await supabase.rpc(
      'maintenance_clear_stale_entities'
    );
    
    if (dbError) {
      console.error("Database cleanup error:", dbError);
    } else {
      console.log(`Database cleanup completed, results:`, dbCleanupResults);
    }
    
    // Step 2: Clean up Redis-specific state
    const stateManager = getStateManager(supabase);
    const redisResults = await stateManager.cleanupStaleEntities(ageMinutes, entityType);
    
    console.log(`Redis cleanup completed, released ${redisResults.releasedCount} stale locks`);
    
    // Step 3: Find inconsistent states (DB says one thing, Redis another)
    const { data: inconsistentStates, error: findError } = await supabase.rpc(
      'find_inconsistent_states',
      { 
        p_entity_type: entityType,
        p_older_than_minutes: ageMinutes 
      }
    );
    
    if (findError) {
      console.error("Error finding inconsistent states:", findError);
    } else if (inconsistentStates && inconsistentStates.length > 0) {
      console.log(`Found ${inconsistentStates.length} inconsistent states`);
      
      // Process each inconsistent state
      for (const state of inconsistentStates) {
        console.log(`Resolving inconsistent state for ${state.entity_type}:${state.entity_id}`);
        
        // Force release any locks and reset to PENDING
        const resetResult = await stateManager.forceReset(
          state.entity_type,
          state.entity_id,
          {
            reason: "Inconsistent state cleanup",
            minutesSinceUpdate: state.minutes_since_update
          }
        );
        
        console.log(`Reset result:`, resetResult);
      }
    } else {
      console.log("No inconsistent states found");
    }
    
    console.log("Background cleanup process completed successfully");
  } catch (error) {
    console.error("Error in background cleanup process:", error);
  }
}
