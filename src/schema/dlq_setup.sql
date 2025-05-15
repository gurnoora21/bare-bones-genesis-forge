
-- Register the album discovery DLQ
SELECT public.register_queue(
  'album_discovery_dlq', 
  'Album Discovery DLQ', 
  'Dead letter queue for failed album discovery messages'
);

-- Create a view to explore DLQ messages
CREATE OR REPLACE VIEW public.dlq_messages AS
SELECT
  q.queue_name,
  (mqt.message->>'_dlq_metadata')::jsonb->'source_queue' as source_queue,
  mqt.id as message_id,
  mqt.msg_id as original_msg_id,
  mqt.created_at as moved_to_dlq_at,
  (mqt.message->>'_dlq_metadata')::jsonb->'failure_reason' as failure_reason,
  (mqt.message->>'_dlq_metadata')::jsonb->'custom_metadata' as custom_metadata,
  mqt.message as full_message
FROM
  queue_registry q
JOIN
  pgmq.get_queue_table_name(q.queue_name) mqt ON true
WHERE
  q.queue_name LIKE '%_dlq'
  AND mqt.message ? '_dlq_metadata';

-- Add a function to list all DLQ messages for analysis
CREATE OR REPLACE FUNCTION public.list_dlq_messages(
  p_queue_name TEXT DEFAULT NULL
)
RETURNS TABLE (
  queue_name TEXT,
  source_queue TEXT,
  message_id TEXT,
  moved_at TIMESTAMP WITH TIME ZONE,
  failure_reason TEXT,
  message JSONB
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  SELECT 
    d.queue_name,
    d.source_queue::TEXT,
    d.message_id::TEXT,
    d.moved_to_dlq_at,
    d.failure_reason::TEXT,
    d.full_message
  FROM 
    dlq_messages d
  WHERE
    (p_queue_name IS NULL OR d.queue_name = p_queue_name)
  ORDER BY
    d.moved_to_dlq_at DESC;
END;
$$;
