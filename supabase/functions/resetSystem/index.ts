
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { StructuredLogger } from "../_shared/structuredLogger.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  const logger = new StructuredLogger({ 
    service: 'reset-system',
    functionId: crypto.randomUUID()
  });

  try {
    const supabase = createClient(
      Deno.env.get('SUPABASE_URL') || '',
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') || ''
    );

    logger.info("Starting system reset");
    
    // Read the cleanup SQL script
    const cleanupSQL = `
    -- First, drop the view that depends on the queue tables
    DROP VIEW IF EXISTS public.queue_monitoring_view CASCADE;
    
    -- First, disable triggers to avoid cascading issues
    SET session_replication_role = 'replica';
    
    -- Clear all application data tables in correct order
    TRUNCATE TABLE public.track_producers CASCADE;
    TRUNCATE TABLE public.tracks CASCADE;
    TRUNCATE TABLE public.albums CASCADE;
    TRUNCATE TABLE public.normalized_tracks CASCADE;
    TRUNCATE TABLE public.producers CASCADE;
    TRUNCATE TABLE public.artists CASCADE;
    TRUNCATE TABLE public.worker_issues CASCADE;
    TRUNCATE TABLE public.queue_metrics CASCADE;
    TRUNCATE TABLE public.processing_status CASCADE;
    TRUNCATE TABLE public.processing_locks CASCADE;
    TRUNCATE TABLE public.monitoring_events CASCADE;
    
    -- Re-enable triggers
    SET session_replication_role = 'origin';
    `;
    
    // Execute the cleanup SQL
    logger.info("Executing cleanup SQL");
    const { data: cleanupData, error: cleanupError } = await supabase.rpc('raw_sql_query', {
      sql_query: cleanupSQL
    });
    
    if (cleanupError) {
      logger.error("Error during cleanup", cleanupError);
    }

    // Drop all queues - with error handling for each queue
    logger.info("Dropping existing queues");
    
    const queues = [
      'album_discovery',
      'track_discovery',
      'producer_identification',
      'social_enrichment',
      'artist_discovery',
      'producer_identification_dlq'
    ];
    
    const dropResults = [];
    
    for (const queue of queues) {
      try {
        // Try to drop the queue with cascade option (true)
        const { data, error } = await supabase.rpc('raw_sql_query', {
          sql_query: `SELECT pgmq.drop_queue('${queue}', true);`
        });
        
        dropResults.push({ queue, success: !error, error: error?.message });
        
        if (error) {
          logger.warn(`Error dropping queue ${queue}`, { error: error.message });
        } else {
          logger.info(`Successfully dropped queue ${queue}`);
        }
      } catch (e) {
        logger.error(`Exception dropping queue ${queue}`, e);
        dropResults.push({ queue, success: false, error: e.message });
      }
    }

    // Create new queues
    logger.info("Creating new queues");
    
    const createResults = [];
    
    for (const queue of queues) {
      try {
        const { data, error } = await supabase.rpc('raw_sql_query', {
          sql_query: `SELECT pgmq.create('${queue}');`
        });
        
        createResults.push({ queue, success: !error, error: error?.message });
        
        if (error) {
          logger.warn(`Error creating queue ${queue}`, { error: error.message });
        } else {
          logger.info(`Successfully created queue ${queue}`);
        }
      } catch (e) {
        logger.error(`Exception creating queue ${queue}`, e);
        createResults.push({ queue, success: false, error: e.message });
      }
    }
    
    // Re-create the queue monitoring view
    logger.info("Creating queue monitoring view");
    const viewSQL = `
    -- Create queue monitoring view
    CREATE OR REPLACE VIEW public.queue_monitoring_view AS
    WITH queue_tables AS (
      SELECT 
        CASE 
          WHEN table_name LIKE 'q\\_%' THEN SUBSTRING(table_name FROM 3)
          ELSE SUBSTRING(table_name FROM 6) 
        END AS queue_name,
        CASE 
          WHEN table_schema = 'pgmq' THEN format('%I.%I', table_schema, table_name)
          ELSE format('%I.%I', table_schema, table_name)
        END AS full_table_name
      FROM information_schema.tables
      WHERE (table_schema = 'pgmq' AND table_name LIKE 'q\\_%')
         OR (table_schema = 'public' AND table_name LIKE 'pgmq\\_%')
    )
    SELECT
      qt.queue_name,
      (SELECT COUNT(*) FROM pgmq.read(qt.queue_name, 0, 0)) AS total_messages,
      0 AS stuck_messages,
      NULL::INTEGER AS messages_fixed,
      NOW() AS last_check_time
    FROM queue_tables qt
    GROUP BY qt.queue_name;
    `;
    
    const { data: viewData, error: viewError } = await supabase.rpc('raw_sql_query', {
      sql_query: viewSQL
    });
    
    if (viewError) {
      logger.error("Error creating queue monitoring view", viewError);
    } else {
      logger.info("Successfully created queue monitoring view");
    }
    
    // Register queues in the registry
    logger.info("Registering queues in registry");
    const registrySQL = `
    INSERT INTO public.queue_registry (queue_name, display_name, description, active)
    VALUES 
      ('artist_discovery', 'Artist Discovery', 'Discovers new artists from Spotify and other sources', true),
      ('album_discovery', 'Album Discovery', 'Retrieves album data for discovered artists', true),
      ('track_discovery', 'Track Discovery', 'Retrieves track data from discovered albums', true),
      ('producer_identification', 'Producer Identification', 'Identifies producers and writers from track data', true),
      ('social_enrichment', 'Social Enrichment', 'Enriches producer profiles with social media information', true),
      ('producer_identification_dlq', 'Producer Identification DLQ', 'Dead-letter queue for producer identification', true)
    ON CONFLICT (queue_name) DO UPDATE SET active = true;
    `;
    
    const { data: registryData, error: registryError } = await supabase.rpc('raw_sql_query', {
      sql_query: registrySQL
    });
    
    if (registryError) {
      logger.error("Error registering queues", registryError);
    } else {
      logger.info("Successfully registered queues");
    }

    // Return the results
    return new Response(
      JSON.stringify({
        success: true,
        message: "System reset completed",
        results: {
          cleanup: { success: !cleanupError },
          drop_queues: dropResults,
          create_queues: createResults,
          view: { success: !viewError },
          registry: { success: !registryError }
        }
      }),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 200 
      }
    );
  } catch (error) {
    console.error("Error in resetSystem:", error);
    
    return new Response(
      JSON.stringify({ 
        success: false, 
        message: "System reset failed",
        error: error.message 
      }),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 500 
      }
    );
  }
});
