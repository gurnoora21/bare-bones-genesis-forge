-- Add check_operation function for idempotency management
CREATE OR REPLACE FUNCTION public.check_operation(
  p_entity_id TEXT,
  p_entity_type TEXT,
  p_operation_id TEXT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_state TEXT;
BEGIN
  -- Check if the operation exists and get its state
  SELECT state INTO v_state
  FROM public.processing_status
  WHERE entity_type = p_entity_type 
    AND entity_id = p_entity_id
    AND metadata->>'operation_id' = p_operation_id;
    
  -- Return true if operation exists and is completed
  RETURN v_state = 'COMPLETED';
END;
$$;

-- Add index to improve performance of operation checks
CREATE INDEX IF NOT EXISTS idx_processing_status_operation 
ON public.processing_status(entity_type, entity_id, ((metadata->>'operation_id'))); 