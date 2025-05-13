
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
    
    // Run database cleanup function
    const { data: dbResult, error: dbError } = await supabase.rpc(
      'maintenance_clear_stale_entities'
    );
    
    if (dbError) {
      console.error("Error in database cleanup:", dbError);
    } else {
      console.log("Database cleanup results:", dbResult || "No stale entities found");
    }
    
    // Run Redis cleanup
    const stateManager = getStateManager(supabase);
    const redisResult = await stateManager.cleanupStaleEntities(60); // 60 minutes threshold
    
    console.log("Redis cleanup results:", redisResult);
    
    // Check for any entities marked as IN_PROGRESS but might be stuck in limbo
    const { data: limboEntities, error: findError } = await supabase
      .from('processing_status')
      .select('*')
      .eq('state', 'IN_PROGRESS')
      .lt('last_processed_at', new Date(Date.now() - 60 * 60 * 1000).toISOString()) // 60 minutes
      .limit(100);
    
    if (findError) {
      console.error("Error finding limbo entities:", findError);
    } else if (limboEntities && limboEntities.length > 0) {
      console.log(`Found ${limboEntities.length} entities in limbo`);
      
      // Process each limbo entity (reset to PENDING)
      for (const entity of limboEntities) {
        console.log(`Resetting limbo entity: ${entity.entity_type}:${entity.entity_id}`);
        
        // Force reset to PENDING
        try {
          await supabase.rpc('force_release_entity_lock', {
            p_entity_type: entity.entity_type,
            p_entity_id: entity.entity_id
          });
        } catch (resetError) {
          console.error(`Error resetting entity ${entity.entity_type}:${entity.entity_id}:`, resetError);
        }
      }
    } else {
      console.log("No entities found in limbo");
    }
    
    return new Response(
      JSON.stringify({ 
        success: true,
        dbResult,
        redisResult,
        limboEntitiesProcessed: limboEntities?.length || 0,
        timestamp: new Date().toISOString()
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  } catch (error) {
    console.error("Error in scheduled cleanup:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" }, status: 500 }
    );
  }
});
