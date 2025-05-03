
-- Create a function to diagnose queue tables
CREATE OR REPLACE FUNCTION public.diagnose_queue_tables(
  queue_name TEXT DEFAULT NULL
) RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  result JSONB;
BEGIN
  WITH queue_tables AS (
    SELECT 
      n.nspname AS schema_name,
      c.relname AS table_name,
      n.nspname || '.' || c.relname AS full_name,
      pg_table_size(n.nspname || '.' || c.relname) AS table_size,
      (SELECT COUNT(*) FROM pg_catalog.pg_attribute 
       WHERE attrelid = c.oid AND attnum > 0) AS column_count
    FROM 
      pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE 
      c.relkind = 'r' AND
      (
        (n.nspname = 'pgmq' AND c.relname LIKE 'q\_%') OR
        (n.nspname = 'public' AND c.relname LIKE 'pgmq\_%')
      ) AND
      (queue_name IS NULL OR 
       c.relname = 'q_' || queue_name OR 
       c.relname = 'pgmq_' || queue_name)
  )
  SELECT 
    jsonb_build_object(
      'queue_tables', jsonb_agg(
        jsonb_build_object(
          'schema_name', schema_name,
          'table_name', table_name,
          'full_name', full_name,
          'table_size', table_size,
          'column_count', column_count
        )
      ),
      'count', COUNT(*),
      'found_in_pgmq', COUNT(*) FILTER (WHERE schema_name = 'pgmq'),
      'found_in_public', COUNT(*) FILTER (WHERE schema_name = 'public')
    )
  INTO result
  FROM queue_tables;
  
  -- Return empty structure if no tables found
  IF result IS NULL THEN
    RETURN jsonb_build_object(
      'queue_tables', '[]',
      'count', 0,
      'found_in_pgmq', 0,
      'found_in_public', 0,
      'error', 'No queue tables found'
    );
  END IF;
  
  RETURN result;
END;
$$;

-- Set up a cron job to automatically fix stuck messages
DO $$
BEGIN
  PERFORM cron.unschedule('auto-fix-stuck-messages-cron');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Cron job did not exist yet, will be created';
END $$;

SELECT cron.schedule(
  'auto-fix-stuck-messages-cron',
  '*/10 * * * *',  -- Run every 10 minutes
  $$
  SELECT reset_all_stuck_messages(15);
  $$
);
