
-- Create or update a more flexible message validation function
CREATE OR REPLACE FUNCTION public.validate_producer_identification_message(message JSONB)
RETURNS BOOLEAN AS $$
BEGIN
  -- Accept either trackId or track_id
  RETURN (message ? 'trackId' AND (message->>'trackId') IS NOT NULL AND (message->>'trackId')::TEXT != '') OR
         (message ? 'track_id' AND (message->>'track_id') IS NOT NULL AND (message->>'track_id')::TEXT != '');
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Update function to record problematic messages in the queue_mgmt schema
CREATE OR REPLACE FUNCTION public.record_problematic_message(
  p_queue_name TEXT,
  p_message_id TEXT,
  p_message_body JSONB,
  p_error_type TEXT,
  p_error_details TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
  -- Make sure the schema and table exist first
  IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'queue_mgmt') THEN
    CREATE SCHEMA IF NOT EXISTS queue_mgmt;
    
    CREATE TABLE IF NOT EXISTS queue_mgmt.problematic_messages (
      id SERIAL PRIMARY KEY,
      queue_name TEXT NOT NULL,
      message_id TEXT NOT NULL,
      message_body JSONB,
      error_type TEXT NOT NULL,
      error_details TEXT,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
      resolved BOOLEAN DEFAULT FALSE,
      resolution_details JSONB
    );
    
    CREATE INDEX IF NOT EXISTS idx_problematic_messages_queue_message 
      ON queue_mgmt.problematic_messages(queue_name, message_id);
  END IF;

  INSERT INTO queue_mgmt.problematic_messages(
    queue_name, message_id, message_body, error_type, error_details
  ) VALUES (
    p_queue_name, p_message_id, p_message_body, p_error_type, p_error_details
  );
  
  EXCEPTION WHEN OTHERS THEN
    -- If schema doesn't exist yet or any other error, just log and continue
    RAISE NOTICE 'Failed to record problematic message: %', SQLERRM;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
