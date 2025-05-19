-- Migration to add the raw_sql_query function for direct SQL operations
-- This function allows executing arbitrary SQL with parameters
-- Used by the direct SQL approach for queue operations

-- Drop the function if it already exists to ensure we have the latest version
DROP FUNCTION IF EXISTS public.raw_sql_query(text, jsonb);

-- Create the function with security definer to ensure it has proper permissions
CREATE OR REPLACE FUNCTION public.raw_sql_query(sql_query text, params jsonb DEFAULT '[]'::jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  result jsonb;
  param_values text[];
  i integer;
  sql_with_params text;
BEGIN
  -- Extract parameter values from the jsonb array
  IF jsonb_array_length(params) > 0 THEN
    param_values := array_fill(NULL::text, ARRAY[jsonb_array_length(params)]);
    FOR i IN 0..jsonb_array_length(params)-1 LOOP
      param_values[i+1] := params->i;
    END LOOP;
  END IF;
  
  -- Prepare the SQL with parameters
  sql_with_params := sql_query;
  
  -- Execute the query with parameters
  EXECUTE sql_with_params INTO result USING VARIADIC param_values;
  
  -- Return the result as jsonb
  RETURN result;
EXCEPTION WHEN OTHERS THEN
  RETURN jsonb_build_object('error', SQLERRM, 'detail', SQLSTATE);
END;
$$;

-- Grant execute permission to authenticated users
GRANT EXECUTE ON FUNCTION public.raw_sql_query(text, jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION public.raw_sql_query(text, jsonb) TO service_role;

-- Add a comment to explain the function's purpose
COMMENT ON FUNCTION public.raw_sql_query IS 'Execute arbitrary SQL with parameters (security definer)';
