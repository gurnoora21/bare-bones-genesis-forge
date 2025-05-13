
// Scheduled Edge Function to periodically clean up stale entity processing states
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { getStateManager } from "../_shared/enhancedStateManager.ts";

// Setup Supabase client
const supabaseUrl = Deno.env.get("SUPABASE_URL") as string;
const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") as string;
const supabase = createClient(supabaseUrl, supabaseKey);

// Configure CORS
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

// Entry point for scheduled cleanup operation
Deno.serve(async (_req) => {
  try {
    console.log("Starting scheduled stale entity cleanup");
    
    // Use the new dedicated helper function for cleanup
    const { data: dbResult, error: dbError } = await supabase.rpc(
      'cleanup_stale_entities',
      { p_stale_threshold_minutes: 60 }
    );
    
    if (dbError) {
      console.error("Error in database cleanup:", dbError);
      throw new Error(`Database error: ${dbError.message}`);
    } else {
      console.log("Database cleanup results:", dbResult || "No operation performed");
    }
    
    // Run Redis cleanup separately 
    let redisResult = { success: false, message: "Redis cleanup not attempted" };
    try {
      const stateManager = getStateManager(supabase);
      redisResult = await stateManager.cleanupStaleEntities(60); // 60 minutes threshold
      console.log("Redis cleanup results:", redisResult);
    } catch (redisError) {
      console.error("Error in Redis cleanup (non-critical):", redisError);
      redisResult = { 
        success: false, 
        error: redisError.message,
        message: "Redis cleanup failed but continuing"
      };
    }
    
    // Check for any entities marked as IN_PROGRESS but might be stuck in limbo
    // Use try-catch to prevent failures here from stopping the function
    let limboEntities = [];
    let limboProcessed = 0;
    
    try {
      const { data: foundEntities, error: findError } = await supabase
        .from('processing_status')
        .select('*')
        .eq('state', 'IN_PROGRESS')
        .lt('last_processed_at', new Date(Date.now() - 60 * 60 * 1000).toISOString()) // 60 minutes
        .limit(100);
      
      if (findError) {
        console.error("Error finding limbo entities:", findError);
      } else if (foundEntities && foundEntities.length > 0) {
        console.log(`Found ${foundEntities.length} entities in limbo`);
        limboEntities = foundEntities;
        
        // Process each limbo entity (reset to PENDING)
        for (const entity of foundEntities) {
          try {
            console.log(`Resetting limbo entity: ${entity.entity_type}:${entity.entity_id}`);
            
            // Force reset to PENDING
            await supabase.rpc('force_release_entity_lock', {
              p_entity_type: entity.entity_type,
              p_entity_id: entity.entity_id
            });
            
            limboProcessed++;
          } catch (resetError) {
            console.error(`Error resetting entity ${entity.entity_type}:${entity.entity_id}:`, resetError);
          }
        }
      } else {
        console.log("No entities found in limbo");
      }
    } catch (limboError) {
      console.error("Error in limbo entity processing:", limboError);
    }
    
    // Log the run to monitoring_events for tracking
    try {
      await supabase.from('monitoring_events').insert({
        event_type: 'stale_cleanup',
        details: {
          db_cleanup: dbResult,
          redis_cleanup: redisResult,
          limbo_entities: limboEntities.length,
          limbo_processed: limboProcessed,
          timestamp: new Date().toISOString()
        }
      });
    } catch (logError) {
      console.error("Failed to log cleanup event:", logError);
    }
    
    return new Response(
      JSON.stringify({ 
        success: true,
        dbResult,
        redisResult,
        limboEntitiesProcessed: limboProcessed,
        limboEntitiesFound: limboEntities.length,
        timestamp: new Date().toISOString()
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  } catch (error) {
    console.error("Error in scheduled cleanup:", error);
    
    // Try to log the failure
    try {
      await supabase.from('monitoring_events').insert({
        event_type: 'stale_cleanup_error',
        details: {
          error: error.message,
          timestamp: new Date().toISOString()
        }
      });
    } catch (logError) {
      console.error("Failed to log error event:", logError);
    }
    
    return new Response(
      JSON.stringify({ 
        error: error.message,
        timestamp: new Date().toISOString()
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" }, status: 500 }
    );
  }
});
