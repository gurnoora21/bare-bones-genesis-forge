
// Enhanced edge function to clear idempotency keys
// Supports queue-specific clearing, age-based filtering, and database state coordination

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Initialize Supabase client
    const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
    const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
    const supabase = createClient(supabaseUrl, supabaseKey);
    
    // Extract parameters from URL or request body
    let queueName = '';
    let entityType = '';
    let ageMinutes = 0;
    let clearDatabase = false; // Flag to coordinate with database state
    let keyPattern = ''; // For advanced pattern-based matching
    
    // Parse the URL for path parameters, e.g., /clearIdempotencyKeys/artist_discovery/60
    const url = new URL(req.url);
    const pathParts = url.pathname.split('/').filter(Boolean);
    
    if (pathParts.length > 1) {
      queueName = pathParts[1]; // e.g., "artist_discovery"
      
      if (pathParts.length > 2) {
        ageMinutes = parseInt(pathParts[2]) || 0; // e.g., 60 (for 60 minutes)
      }
    }
    
    // If not found in path, try query parameters
    if (!queueName) {
      queueName = url.searchParams.get('queue') || '';
    }
    if (!entityType) {
      entityType = url.searchParams.get('entityType') || '';
    }
    if (!ageMinutes) {
      ageMinutes = parseInt(url.searchParams.get('age') || '0');
    }
    if (!keyPattern) {
      keyPattern = url.searchParams.get('pattern') || '';
    }
    clearDatabase = url.searchParams.get('clearDatabase') === 'true';
    
    // If still not found, try request body
    if (req.body) {
      try {
        const body = await req.json();
        if (!queueName && (body.queue || body.queueName)) {
          queueName = body.queue || body.queueName || '';
        }
        if (!entityType && body.entityType) {
          entityType = body.entityType;
        }
        if (!ageMinutes && (body.age || body.ageMinutes)) {
          ageMinutes = body.age || body.ageMinutes || 0;
        }
        if (!keyPattern && body.pattern) {
          keyPattern = body.pattern;
        }
        if (body.clearDatabase !== undefined) {
          clearDatabase = !!body.clearDatabase;
        }
      } catch (e) {
        // Ignore JSON parsing errors
      }
    }
    
    // Set up the request for manageRedis
    const redisOperation = {
      operation: 'clear-idempotency',
      queueName,
      entityType,
      pattern: keyPattern,
      age: ageMinutes
    };
    
    console.log(`Calling manageRedis to clear idempotency keys: ${JSON.stringify(redisOperation)}`);
    
    // Clear Redis idempotency keys
    const { data: redisResult, error: redisError } = await supabase.functions.invoke('manageRedis', {
      method: 'DELETE',
      body: redisOperation
    });
    
    if (redisError) {
      console.error("Error clearing Redis idempotency keys:", redisError);
      throw new Error(`Failed to clear idempotency keys from Redis: ${redisError.message}`);
    }
    
    const result = {
      success: true,
      redisKeysDeleted: redisResult?.keysDeleted || 0,
      databaseEntitiesReset: 0,
      message: `Cleared ${redisResult?.keysDeleted || 0} Redis idempotency keys`,
    };
    
    // If database coordination is requested, also reset database state
    if (clearDatabase && (entityType || queueName)) {
      try {
        // Convert queue name to entity type if needed
        const dbEntityType = entityType || queueName.replace(/_/g, '-');
        
        // Get age in minutes, defaulting to 24 hours if not specified
        const dbAgeMinutes = ageMinutes || 24 * 60;
        
        // Call database function to reset entity processing state
        const { data: dbResult, error: dbError } = await supabase.rpc(
          'reset_entity_processing_state',
          {
            p_entity_type: dbEntityType,
            p_older_than_minutes: dbAgeMinutes,
            p_target_states: ['COMPLETED', 'FAILED']
          }
        );
        
        if (dbError) {
          console.warn("Warning: Database state reset failed:", dbError);
          result.databaseError = dbError.message;
        } else {
          result.databaseEntitiesReset = dbResult?.length || 0;
          result.message = `Successfully cleared ${result.redisKeysDeleted} Redis keys and reset ${result.databaseEntitiesReset} database entities`;
        }
      } catch (dbExc) {
        console.warn("Exception during database reset:", dbExc);
        result.databaseError = dbExc.message;
      }
    }
    
    console.log("Successfully cleared idempotency keys:", result);
    
    return new Response(
      JSON.stringify(result),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 200 
      }
    );
  } catch (error) {
    console.error("Error clearing idempotency keys:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      }
    );
  }
});
