
-- Comprehensive fix for queue handling and message validation issues

-- 1. Fix message validation by implementing helper functions
CREATE OR REPLACE FUNCTION public.validate_album_discovery_message(message JSONB)
RETURNS BOOLEAN AS $$
BEGIN
  -- Basic validation for album discovery messages
  RETURN message ? 'artistId' AND 
         (message->>'artistId') IS NOT NULL AND
         (message->>'artistId')::TEXT != '';
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION public.validate_producer_identification_message(message JSONB)
RETURNS BOOLEAN AS $$
BEGIN
  -- Basic validation for producer identification messages
  RETURN message ? 'trackId' AND 
         (message->>'trackId') IS NOT NULL AND
         (message->>'trackId')::TEXT != '';
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 2. Create a more aggressive message deletion function as a last resort
CREATE OR REPLACE FUNCTION public.force_delete_message(
  p_queue_name TEXT,
  p_message_id TEXT
) RETURNS JSONB AS $$
DECLARE
  v_result JSONB;
BEGIN
  -- First try standard deletion
  IF public.pg_delete_message(p_queue_name, p_message_id) THEN
    RETURN jsonb_build_object('success', TRUE, 'method', 'standard_delete');
  END IF;
  
  -- If that fails, try cross-schema operation
  SELECT public.cross_schema_queue_op('delete', p_queue_name, p_message_id) INTO v_result;
  IF (v_result->>'success')::BOOLEAN THEN
    RETURN v_result;
  END IF;
  
  -- As a last resort, try to reset the visibility timeout
  SELECT public.cross_schema_queue_op('reset', p_queue_name, p_message_id) INTO v_result;
  IF (v_result->>'success')::BOOLEAN THEN
    RETURN jsonb_build_object(
      'success', TRUE,
      'method', 'visibility_reset',
      'note', 'Message visibility timeout reset, will be reprocessed'
    );
  END IF;
  
  -- Everything failed
  RETURN jsonb_build_object(
    'success', FALSE,
    'error', 'Failed to delete or reset message using all available methods',
    'queue', p_queue_name,
    'message_id', p_message_id
  );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 3. Create a function to validate queue messages before processing
CREATE OR REPLACE FUNCTION public.validate_queue_message(
  p_queue_name TEXT,
  p_message JSONB
) RETURNS JSONB AS $$
DECLARE
  v_is_valid BOOLEAN := FALSE;
  v_error TEXT := 'Unknown message format';
BEGIN
  -- Switch validation based on queue name
  IF p_queue_name = 'album_discovery' THEN
    v_is_valid := public.validate_album_discovery_message(p_message);
    IF NOT v_is_valid THEN
      v_error := 'Invalid album discovery message: missing or empty artistId';
    END IF;
  ELSIF p_queue_name = 'producer_identification' THEN
    v_is_valid := public.validate_producer_identification_message(p_message);
    IF NOT v_is_valid THEN
      v_error := 'Invalid producer identification message: missing or empty trackId';
    END IF;
  ELSIF p_queue_name = 'track_discovery' THEN
    -- Track discovery validation
    v_is_valid := p_message ? 'albumId' AND (p_message->>'albumId') IS NOT NULL;
    IF NOT v_is_valid THEN
      v_error := 'Invalid track discovery message: missing or empty albumId';
    END IF;
  ELSE
    -- Default validation - just check it's not null or empty object
    v_is_valid := p_message IS NOT NULL AND p_message != '{}'::JSONB;
    IF NOT v_is_valid THEN
      v_error := 'Empty or null message';
    END IF;
  END IF;
  
  -- Return validation result
  IF v_is_valid THEN
    RETURN jsonb_build_object('valid', TRUE);
  ELSE
    RETURN jsonb_build_object(
      'valid', FALSE,
      'error', v_error,
      'queue', p_queue_name
    );
  END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 4. Create support schema for queue management if it doesn't exist
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'queue_mgmt') THEN
    CREATE SCHEMA queue_mgmt;
    
    -- Create a table to track problematic messages
    CREATE TABLE queue_mgmt.problematic_messages (
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
    
    -- Create index for faster lookups
    CREATE INDEX idx_problematic_messages_queue_message 
      ON queue_mgmt.problematic_messages(queue_name, message_id);
  END IF;
END$$;

-- 5. Create a function to record problematic messages
CREATE OR REPLACE FUNCTION public.record_problematic_message(
  p_queue_name TEXT,
  p_message_id TEXT,
  p_message_body JSONB,
  p_error_type TEXT,
  p_error_details TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
  INSERT INTO queue_mgmt.problematic_messages(
    queue_name, message_id, message_body, error_type, error_details
  ) VALUES (
    p_queue_name, p_message_id, p_message_body, p_error_type, p_error_details
  )
  ON CONFLICT (id) DO NOTHING;
  
  EXCEPTION WHEN OTHERS THEN
    -- If schema doesn't exist yet or any other error, just ignore
    NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
