
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { getTransactionManager } from "../_shared/transactionManager.ts";
import { getIdempotencyManager } from "../_shared/idempotencyManager.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Initialize Supabase client
    const supabase = createClient(
      Deno.env.get('SUPABASE_URL') || '',
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') || ''
    );
    
    // Parse request body
    const { operation, data = {}, idempotencyKey } = await req.json();
    
    if (!operation) {
      throw new Error("Operation parameter is required");
    }
    
    // Get managers
    const transactionManager = getTransactionManager(supabase);
    const idempotencyManager = getIdempotencyManager(supabase);
    
    let result;
    
    // Process based on operation type
    switch (operation) {
      case 'process_artist':
        // Demo processing an artist with atomicity
        if (!data.artistData) {
          throw new Error("Artist data is required");
        }
        
        result = await transactionManager.atomicOperation('process_artist_atomic', {
          p_artist_data: data.artistData,
          p_operation_id: idempotencyKey || `artist_${Date.now()}`,
          p_spotify_id: data.spotifyId
        });
        break;
        
      case 'process_album':
        // Demo processing an album with atomicity
        if (!data.albumData) {
          throw new Error("Album data is required");
        }
        
        result = await transactionManager.atomicOperation('process_album_atomic', {
          p_album_data: data.albumData,
          p_operation_id: idempotencyKey || `album_${Date.now()}`,
          p_spotify_id: data.spotifyId
        });
        break;
        
      case 'process_track':
        // Demo processing a track with atomicity
        if (!data.trackData) {
          throw new Error("Track data is required");
        }
        
        result = await transactionManager.atomicOperation('process_track_atomic', {
          p_track_data: data.trackData,
          p_operation_id: idempotencyKey || `track_${Date.now()}`,
          p_spotify_id: data.spotifyId
        });
        break;
        
      case 'custom_sql':
        // Demo executing custom SQL with transaction safety
        if (!data.sql) {
          throw new Error("SQL query is required");
        }
        
        result = await transactionManager.executeSql(
          data.sql,
          data.params || [],
          idempotencyKey ? {
            operationId: idempotencyKey,
            entityType: 'custom_sql',
            entityId: data.entityId || 'custom'
          } : undefined
        );
        break;
        
      case 'idempotent_operation':
        // Demo pure idempotency without DB transaction
        if (!idempotencyKey) {
          throw new Error("Idempotency key is required for this operation");
        }
        
        // Execute with idempotency
        const opResult = await idempotencyManager.execute(
          {
            operationId: idempotencyKey,
            entityType: data.entityType || 'demo',
            entityId: data.entityId || 'operation'
          },
          async () => {
            console.log("Executing idempotent operation");
            
            // Simulate some work
            await new Promise(resolve => setTimeout(resolve, 500));
            
            // Return a result
            return {
              message: "Operation completed successfully",
              timestamp: new Date().toISOString(),
              data: data.input || {}
            };
          }
        );
        
        result = opResult;
        break;
        
      default:
        throw new Error(`Unknown operation: ${operation}`);
    }
    
    // Return successful response
    return new Response(
      JSON.stringify({ 
        success: true, 
        result, 
        operation 
      }),
      { 
        headers: { 
          ...corsHeaders, 
          'Content-Type': 'application/json' 
        } 
      }
    );
  } catch (error) {
    console.error(`Error in transaction demo: ${error.message}`);
    
    // Return error response
    return new Response(
      JSON.stringify({
        success: false,
        error: error.message,
        details: error.stack
      }),
      {
        status: 400,
        headers: {
          ...corsHeaders,
          'Content-Type': 'application/json'
        }
      }
    );
  }
});
