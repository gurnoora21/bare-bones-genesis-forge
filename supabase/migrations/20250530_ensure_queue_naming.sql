
-- Improve queue table naming consistency and ensure proper queue existence

-- Function to check if a queue exists
CREATE OR REPLACE FUNCTION public.ensure_queue_exists(p_queue_name TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  queue_exists BOOLEAN;
BEGIN
  -- Check if the queue exists in pgmq schema
  BEGIN
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM pgmq.list_queues() WHERE queue_name = %L)', p_queue_name)
    INTO queue_exists;
  EXCEPTION WHEN OTHERS THEN
    -- If pgmq.list_queues doesn't exist, use a different approach
    BEGIN
      EXECUTE format('SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = %L AND table_name = %L)', 
                     'pgmq', 'q_' || p_queue_name)
      INTO queue_exists;
    EXCEPTION WHEN OTHERS THEN
      queue_exists := FALSE;
    END;
  END;

  -- If queue doesn't exist, create it
  IF NOT queue_exists THEN
    BEGIN
      PERFORM pgmq.create(p_queue_name);
      RAISE NOTICE 'Created queue: %', p_queue_name;
      RETURN TRUE;
    EXCEPTION WHEN OTHERS THEN
      RAISE WARNING 'Failed to create queue %: %', p_queue_name, SQLERRM;
      RETURN FALSE;
    END;
  END IF;

  RETURN TRUE;
END;
$$;

-- Function to ensure all required queues exist
CREATE OR REPLACE FUNCTION public.ensure_all_queues_exist()
RETURNS SETOF TEXT LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  queue_name TEXT;
  required_queues TEXT[] := ARRAY[
    'artist_discovery', 
    'album_discovery', 
    'track_discovery', 
    'producer_identification', 
    'social_enrichment',
    'artist_discovery_dlq', 
    'album_discovery_dlq', 
    'track_discovery_dlq', 
    'producer_identification_dlq', 
    'social_enrichment_dlq'
  ];
BEGIN
  FOREACH queue_name IN ARRAY required_queues
  LOOP
    -- Try to ensure the queue exists
    IF ensure_queue_exists(queue_name) THEN
      RETURN NEXT queue_name || ' - OK';
    ELSE
      RETURN NEXT queue_name || ' - FAILED';
    END IF;
  END LOOP;
  
  RETURN;
END;
$$;

-- Ensure all queues exist
SELECT * FROM ensure_all_queues_exist();

-- Ensure required tables and schema exist
CREATE SCHEMA IF NOT EXISTS pgmq;

-- Update the get_queue_table_name_safe function for greater accuracy
CREATE OR REPLACE FUNCTION public.get_queue_table_name_safe(p_queue_name TEXT)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  pgmq_schema_exists BOOLEAN;
  q_table_exists BOOLEAN;
  public_table_exists BOOLEAN;
BEGIN
  -- Check if pgmq schema exists
  SELECT EXISTS(
    SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgmq'
  ) INTO pgmq_schema_exists;
  
  -- If pgmq schema exists, check for q_<name> table
  IF pgmq_schema_exists THEN
    SELECT EXISTS(
      SELECT 1 FROM information_schema.tables 
      WHERE table_schema = 'pgmq' AND table_name = 'q_' || p_queue_name
    ) INTO q_table_exists;
    
    IF q_table_exists THEN
      RETURN 'pgmq.q_' || p_queue_name;
    END IF;
  END IF;
  
  -- Check for public.pgmq_<name> table
  SELECT EXISTS(
    SELECT 1 FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name = 'pgmq_' || p_queue_name
  ) INTO public_table_exists;
  
  IF public_table_exists THEN
    RETURN 'public.pgmq_' || p_queue_name;
  END IF;
  
  -- Return the preferred naming if table doesn't exist yet
  IF pgmq_schema_exists THEN
    -- Try to create the queue
    BEGIN
      PERFORM pgmq.create(p_queue_name);
      RETURN 'pgmq.q_' || p_queue_name;
    EXCEPTION WHEN OTHERS THEN
      -- If creation failed, just return the expected name
      RETURN 'pgmq.q_' || p_queue_name;
    END;
  ELSE
    RETURN 'public.pgmq_' || p_queue_name;
  END IF;
END;
$$;

-- Function to diagnose queue tables across schemas
CREATE OR REPLACE FUNCTION public.diagnose_queue_tables(p_queue_name TEXT DEFAULT NULL)
RETURNS TABLE(
  schema_name TEXT,
  table_name TEXT,
  full_name TEXT,
  record_count BIGINT
) AS $$
DECLARE
  specific_queue TEXT;
  count_query TEXT;
BEGIN
  IF p_queue_name IS NOT NULL THEN
    specific_queue := p_queue_name;
    
    -- First check pgmq.q_queue_name pattern
    RETURN QUERY
    SELECT 
      'pgmq'::TEXT AS schema_name,
      'q_' || specific_queue AS table_name,
      'pgmq.q_' || specific_queue AS full_name,
      (
        SELECT COUNT(*)::BIGINT 
        FROM pgmq.q_album_discovery
        WHERE specific_queue = 'album_discovery'
      ) AS record_count 
    WHERE EXISTS (
      SELECT 1 
      FROM information_schema.tables 
      WHERE table_schema = 'pgmq' AND table_name = 'q_' || specific_queue
    )
    
    UNION
    
    -- Then check public.pgmq_queue_name pattern
    SELECT 
      'public'::TEXT AS schema_name,
      'pgmq_' || specific_queue AS table_name,
      'public.pgmq_' || specific_queue AS full_name,
      (
        SELECT COUNT(*)::BIGINT 
        FROM public.pgmq_album_discovery
        WHERE specific_queue = 'album_discovery'
        LIMIT 1
      ) AS record_count
    WHERE EXISTS (
      SELECT 1 
      FROM information_schema.tables 
      WHERE table_schema = 'public' AND table_name = 'pgmq_' || specific_queue
    );
  ELSE
    -- Return all PGMQ tables in both schemas
    RETURN QUERY
    SELECT 
      table_schema::TEXT AS schema_name,
      table_name::TEXT,
      (table_schema || '.' || table_name)::TEXT AS full_name,
      0::BIGINT AS record_count -- Just a placeholder
    FROM 
      information_schema.tables
    WHERE 
      (table_schema = 'pgmq' AND table_name LIKE 'q_%') OR
      (table_schema = 'public' AND table_name LIKE 'pgmq_%')
    ORDER BY
      table_schema, table_name;
  END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Ensure PGMQ is working properly
DO $$
BEGIN
  -- Try to create the album_discovery queue specifically
  BEGIN
    PERFORM pgmq.create('album_discovery');
    RAISE NOTICE 'Created or confirmed album_discovery queue';
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error with pgmq.create for album_discovery: %', SQLERRM;
  END;
  
  -- Diagnose all queue tables
  RAISE NOTICE 'Queue table diagnosis:';
  FOR r IN SELECT * FROM public.diagnose_queue_tables() LOOP
    RAISE NOTICE 'Table: %.%, Schema: %', r.schema_name, r.table_name, r.schema_name;
  END LOOP;
  
  -- Check album_discovery queue specifically
  RAISE NOTICE 'Album discovery queue tables:';
  FOR r IN SELECT * FROM public.diagnose_queue_tables('album_discovery') LOOP
    RAISE NOTICE 'Found: %.% (count: %)', r.schema_name, r.table_name, r.record_count;
  END LOOP;
END $$;
