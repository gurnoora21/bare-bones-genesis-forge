
-- First, drop the existing view
DROP VIEW IF EXISTS public.queue_monitoring_view CASCADE;

-- Create a more resilient view that uses dynamic SQL
CREATE OR REPLACE FUNCTION public.get_queue_monitoring_data()
RETURNS TABLE (
  queue_name TEXT,
  total_messages BIGINT,
  stuck_messages BIGINT,
  messages_fixed INTEGER,
  last_check_time TIMESTAMP WITH TIME ZONE
) AS $$
DECLARE
  q_name TEXT;
  q_table TEXT;
  total BIGINT;
  stuck BIGINT;
  result_record RECORD;
  count_query TEXT;
  stuck_query TEXT;
BEGIN
  FOR result_record IN
    SELECT 
      CASE 
        WHEN table_name LIKE 'q\_%' THEN SUBSTRING(table_name FROM 3)
        ELSE SUBSTRING(table_name FROM 6) 
      END AS queue_name,
      format('%I.%I', table_schema, table_name) AS full_table_name
    FROM information_schema.tables
    WHERE (table_schema = 'pgmq' AND table_name LIKE 'q\_%')
       OR (table_schema = 'public' AND table_name LIKE 'pgmq\_%')
  LOOP
    q_name := result_record.queue_name;
    q_table := result_record.full_table_name;
    
    -- Count total messages - safely handle if table doesn't exist
    BEGIN
      count_query := format('SELECT COUNT(*) FROM %s', q_table);
      EXECUTE count_query INTO total;
    EXCEPTION WHEN OTHERS THEN
      total := 0;
    END;
    
    -- Count stuck messages - safely handle if table doesn't exist
    BEGIN
      stuck_query := format('SELECT COUNT(*) FROM %s WHERE vt IS NOT NULL AND vt < NOW()', q_table);
      EXECUTE stuck_query INTO stuck;
    EXCEPTION WHEN OTHERS THEN
      stuck := 0;
    END;
    
    -- Return row
    queue_name := q_name;
    total_messages := total;
    stuck_messages := stuck;
    messages_fixed := NULL;
    last_check_time := NOW();
    RETURN NEXT;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Create queue monitoring view that uses the function
CREATE OR REPLACE VIEW public.queue_monitoring_view AS
SELECT * FROM public.get_queue_monitoring_data();

COMMENT ON VIEW public.queue_monitoring_view IS 'Queue monitoring view that safely handles missing tables';
