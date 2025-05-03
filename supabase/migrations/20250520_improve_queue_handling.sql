
-- Clean up old functions that were hardcoded for specific message IDs
DROP FUNCTION IF EXISTS public.emergency_reset_message;
DROP FUNCTION IF EXISTS public.enhanced_delete_message;
DROP FUNCTION IF EXISTS public.ensure_message_deleted;
DROP FUNCTION IF EXISTS public.inspect_queue_message;

-- Create a function to get all queue tables (both in pgmq schema and legacy ones in public)
CREATE OR REPLACE FUNCTION public.get_all_queue_tables() 
RETURNS TABLE(schema_name TEXT, table_name TEXT, full_name TEXT, record_count BIGINT)
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  RETURN QUERY
  WITH queue_tables AS (
    -- Check pgmq schema tables
    SELECT 
      'pgmq' AS schema,
      relname AS table_name,
      'pgmq.' || relname AS full_name
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = 'pgmq' AND relname LIKE 'q_%'
    
    UNION ALL
    
    -- Check public schema tables
    SELECT 
      'public' AS schema,
      relname AS table_name,
      'public.' || relname AS full_name
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = 'public' AND relname LIKE 'pgmq_%'
  )
  SELECT 
    qt.schema,
    qt.table_name,
    qt.full_name,
    (SELECT count(*) FROM (SELECT 1 FROM pg_catalog.pg_class c
                          JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                          WHERE n.nspname = qt.schema AND c.relname = qt.table_name) t)::BIGINT AS record_count
  FROM queue_tables qt
  ORDER BY schema, table_name;
END; $$;

-- Create a function to reset all stuck messages across all queues
CREATE OR REPLACE FUNCTION public.reset_all_stuck_messages(
  threshold_minutes INT DEFAULT 10
) RETURNS TABLE(
  queue_name TEXT,
  messages_reset BIGINT
)
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  qt RECORD;
  reset_count BIGINT;
  q_name TEXT;
BEGIN
  FOR qt IN 
    SELECT schema_name, table_name, full_name
    FROM get_all_queue_tables()
  LOOP
    -- Extract queue name from table name
    IF qt.schema_name = 'pgmq' THEN
      q_name := SUBSTRING(qt.table_name FROM 3); -- Remove 'q_' prefix
    ELSE
      q_name := SUBSTRING(qt.table_name FROM 6); -- Remove 'pgmq_' prefix
    END IF;
    
    -- Reset stuck messages
    EXECUTE format('
      UPDATE %s
      SET vt = NULL
      WHERE vt IS NOT NULL AND vt < NOW() - INTERVAL ''%s minutes''
      RETURNING COUNT(*)',
      qt.full_name, threshold_minutes
    ) INTO reset_count;
    
    IF reset_count > 0 THEN
      queue_name := q_name;
      messages_reset := reset_count;
      RETURN NEXT;
    END IF;
  END LOOP;
  
  RETURN;
END; $$;

-- Set up a cron job to automatically check for and reset stuck messages
DO $$
BEGIN
  PERFORM cron.unschedule('auto-reset-stuck-messages');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Cron job did not exist yet, will be created';
END $$;

SELECT cron.schedule(
  'auto-reset-stuck-messages',
  '*/5 * * * *',  -- Run every 5 minutes
  $$
  SELECT * FROM reset_all_stuck_messages(15);
  $$
);

-- Configure a more frequent run of the discovery workers
DO $$
BEGIN
  PERFORM cron.unschedule('artist-discovery-worker');
EXCEPTION WHEN OTHERS THEN
  NULL;
END $$;

SELECT cron.schedule(
  'artist-discovery-worker',
  '*/2 * * * *',  -- Run every 2 minutes
  $$ SELECT supabase.functions.invoke('artistDiscovery') $$
);
