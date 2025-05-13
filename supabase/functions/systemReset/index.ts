
// System reset edge function to manage circuit breakers and clear stale locks

import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { ApiResilienceManager, getApiResilienceManager } from "../_shared/apiResilienceManager.ts";

// Initialize Redis client
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

// Initialize Supabase client
const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const supabase = createClient(supabaseUrl, supabaseKey);

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
};

// Response helper
function createResponse(body: any, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 
      ...corsHeaders,
      'Content-Type': 'application/json' 
    }
  });
}

async function resetCircuitBreakers() {
  try {
    const resilienceManager = getApiResilienceManager(redis);
    const resetServices = await resilienceManager.resetAllCircuitBreakers();
    
    return {
      success: true,
      reset_services: resetServices,
      timestamp: new Date().toISOString()
    };
  } catch (error) {
    console.error("Error resetting circuit breakers:", error);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

async function clearStaleLocks(staleThresholdMinutes = 15) {
  try {
    // Call the database function to clear stale locks
    const { data, error } = await supabase.rpc('cleanup_stale_locks', {
      p_stale_threshold_seconds: staleThresholdMinutes * 60
    });
    
    if (error) {
      throw new Error(`Database error: ${error.message}`);
    }
    
    return {
      success: true,
      cleared_locks: data || [],
      timestamp: new Date().toISOString()
    };
  } catch (error) {
    console.error("Error clearing stale locks:", error);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

async function resetStuckEntities(staleThresholdMinutes = 30) {
  try {
    // Reset stuck processing states
    const { data, error } = await supabase
      .from('processing_status')
      .update({
        state: 'PENDING',
        metadata: { reset_reason: 'manual_reset', reset_at: new Date().toISOString() }
      })
      .eq('state', 'IN_PROGRESS')
      .lt('last_processed_at', new Date(Date.now() - staleThresholdMinutes * 60000).toISOString())
      .select();
    
    if (error) {
      throw new Error(`Database error: ${error.message}`);
    }
    
    return {
      success: true,
      reset_entities: data?.length || 0,
      entities: data || [],
      timestamp: new Date().toISOString()
    };
  } catch (error) {
    console.error("Error resetting stuck entities:", error);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

async function resetStuckMessages(queueNames: string[] = [], staleThresholdMinutes = 15) {
  if (!queueNames || queueNames.length === 0) {
    queueNames = [
      'artist_discovery',
      'album_discovery',
      'track_discovery',
      'producer_identification',
      'social_enrichment'
    ];
  }
  
  const results: Record<string, any> = {};
  
  for (const queueName of queueNames) {
    try {
      // Call the database function to reset stuck messages
      const { data, error } = await supabase.rpc('reset_stuck_messages', {
        queue_name: queueName,
        min_minutes_locked: staleThresholdMinutes
      });
      
      if (error) {
        results[queueName] = { 
          success: false, 
          error: error.message 
        };
      } else {
        results[queueName] = { 
          success: true, 
          reset_count: data 
        };
      }
    } catch (error) {
      results[queueName] = { 
        success: false, 
        error: error.message 
      };
    }
  }
  
  return {
    success: true,
    queue_results: results,
    timestamp: new Date().toISOString()
  };
}

async function resetAll(staleThresholdMinutes = 15) {
  const results = {
    circuit_breakers: await resetCircuitBreakers(),
    stale_locks: await clearStaleLocks(staleThresholdMinutes),
    stuck_entities: await resetStuckEntities(staleThresholdMinutes),
    stuck_messages: await resetStuckMessages([], staleThresholdMinutes),
    timestamp: new Date().toISOString()
  };
  
  return {
    success: true,
    results
  };
}

async function getSystemHealth() {
  try {
    // Get circuit breaker health
    const resilienceManager = getApiResilienceManager(redis);
    const serviceHealth = await resilienceManager.getAllServicesHealth();
    
    // Check stale locks
    const { data: staleLocks, error: locksError } = await supabase.rpc('cleanup_stale_locks', {
      p_stale_threshold_seconds: 3600,
      p_return_only: true
    });
    
    if (locksError) {
      throw new Error(`Error checking stale locks: ${locksError.message}`);
    }
    
    // Check stuck processing states
    const { data: stuckEntities, error: entitiesError } = await supabase
      .from('processing_status')
      .select('*')
      .eq('state', 'IN_PROGRESS')
      .lt('last_processed_at', new Date(Date.now() - 30 * 60000).toISOString());
    
    if (entitiesError) {
      throw new Error(`Error checking stuck entities: ${entitiesError.message}`);
    }
    
    // Check queue health
    const queueNames = [
      'artist_discovery',
      'album_discovery',
      'track_discovery',
      'producer_identification',
      'social_enrichment'
    ];
    
    const queueHealth: Record<string, any> = {};
    for (const queueName of queueNames) {
      try {
        const { data: messages, error: messagesError } = await supabase.rpc('list_stuck_messages', {
          queue_name: queueName,
          min_minutes_locked: 15
        });
        
        if (messagesError) {
          queueHealth[queueName] = { 
            error: messagesError.message 
          };
        } else {
          queueHealth[queueName] = { 
            stuck_messages: messages?.length || 0,
            messages: messages 
          };
        }
      } catch (error) {
        queueHealth[queueName] = { 
          error: error.message 
        };
      }
    }
    
    return {
      success: true,
      api_services: serviceHealth,
      stale_locks: staleLocks || [],
      stale_locks_count: staleLocks?.length || 0,
      stuck_entities: stuckEntities || [],
      stuck_entities_count: stuckEntities?.length || 0,
      queue_health: queueHealth,
      timestamp: new Date().toISOString()
    };
  } catch (error) {
    console.error("Error getting system health:", error);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

// Main request handler
serve(async (req) => {
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }
  
  const url = new URL(req.url);
  const path = url.pathname.split('/').pop() || '';
  
  try {
    if (req.method === 'GET') {
      // Health check
      if (path === 'health') {
        const health = await getSystemHealth();
        return createResponse(health);
      }
      
      return createResponse({ 
        available_actions: [
          'health', 
          'reset-circuits', 
          'clear-locks', 
          'reset-entities', 
          'reset-messages', 
          'reset-all'
        ] 
      });
    }
    
    if (req.method === 'POST') {
      const params = await req.json().catch(() => ({}));
      const staleThresholdMinutes = parseInt(params.staleThresholdMinutes as string || '15');
      
      switch (path) {
        case 'reset-circuits':
          return createResponse(await resetCircuitBreakers());
          
        case 'clear-locks':
          return createResponse(await clearStaleLocks(staleThresholdMinutes));
          
        case 'reset-entities':
          return createResponse(await resetStuckEntities(staleThresholdMinutes));
          
        case 'reset-messages':
          return createResponse(await resetStuckMessages(
            params.queueNames as string[] || [], 
            staleThresholdMinutes
          ));
          
        case 'reset-all':
          return createResponse(await resetAll(staleThresholdMinutes));
          
        default:
          return createResponse({ 
            error: 'Unknown action',
            available_actions: [
              'reset-circuits', 
              'clear-locks', 
              'reset-entities', 
              'reset-messages', 
              'reset-all'
            ] 
          }, 400);
      }
    }
    
    return createResponse({ error: 'Method not allowed' }, 405);
  } catch (error) {
    console.error("Error handling request:", error);
    return createResponse({ 
      error: error.message || 'Unknown error',
      timestamp: new Date().toISOString()
    }, 500);
  }
});
