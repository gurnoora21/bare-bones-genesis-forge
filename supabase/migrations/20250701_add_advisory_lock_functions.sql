
-- Add PostgreSQL advisory lock functions for Edge Functions to use

-- Try to acquire an advisory lock (non-blocking)
CREATE OR REPLACE FUNCTION public.pg_try_advisory_lock(p_key TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_key BIGINT;
BEGIN
  -- Convert text key to bigint using hash
  v_key := ('x' || substr(md5(p_key), 1, 16))::bit(64)::bigint;
  
  -- Try to acquire the lock (non-blocking)
  RETURN pg_try_advisory_lock(v_key);
END;
$$;

-- Acquire an advisory lock with timeout (blocking)
CREATE OR REPLACE FUNCTION public.pg_advisory_lock_timeout(p_key TEXT, p_timeout_ms INTEGER)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_key BIGINT;
  v_start_time TIMESTAMPTZ;
  v_lock_acquired BOOLEAN := FALSE;
BEGIN
  -- Convert text key to bigint using hash
  v_key := ('x' || substr(md5(p_key), 1, 16))::bit(64)::bigint;
  
  -- Set statement timeout to specified value
  EXECUTE format('SET LOCAL statement_timeout = %s', p_timeout_ms);
  
  -- Try to acquire the lock (blocking)
  BEGIN
    PERFORM pg_advisory_lock(v_key);
    v_lock_acquired := TRUE;
  EXCEPTION 
    WHEN statement_timeout THEN
      v_lock_acquired := FALSE;
  END;
  
  -- Reset statement timeout to default
  RESET statement_timeout;
  
  RETURN v_lock_acquired;
END;
$$;

-- Release an advisory lock
CREATE OR REPLACE FUNCTION public.pg_advisory_unlock(p_key TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_key BIGINT;
BEGIN
  -- Convert text key to bigint using hash
  v_key := ('x' || substr(md5(p_key), 1, 16))::bit(64)::bigint;
  
  -- Release the lock
  RETURN pg_advisory_unlock(v_key);
END;
$$;

-- Check if an advisory lock is held
CREATE OR REPLACE FUNCTION public.pg_advisory_lock_exists(p_key TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_key BIGINT;
  v_exists BOOLEAN;
BEGIN
  -- Convert text key to bigint using hash
  v_key := ('x' || substr(md5(p_key), 1, 16))::bit(64)::bigint;
  
  -- Check if the lock exists in pg_locks
  SELECT EXISTS(
    SELECT 1 
    FROM pg_locks 
    WHERE locktype = 'advisory' 
    AND objid = v_key
    AND pid != pg_backend_pid()
  ) INTO v_exists;
  
  RETURN v_exists;
END;
$$;

-- Force release advisory locks for a given key
-- This is an administrative function to clean up stuck locks
CREATE OR REPLACE FUNCTION public.pg_force_advisory_unlock_all(p_key TEXT)
RETURNS TABLE(pid INTEGER, released BOOLEAN)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_key BIGINT;
  v_rec RECORD;
BEGIN
  -- Convert text key to bigint using hash
  v_key := ('x' || substr(md5(p_key), 1, 16))::bit(64)::bigint;
  
  FOR v_rec IN 
    SELECT pid 
    FROM pg_locks 
    WHERE locktype = 'advisory' 
    AND objid = v_key 
    AND pid != pg_backend_pid()
  LOOP
    -- Use pg_terminate_backend to forcibly terminate the connection
    -- This will release all locks held by that backend
    PERFORM pg_terminate_backend(v_rec.pid);
    
    -- Return the pid and success status
    pid := v_rec.pid;
    released := TRUE;
    RETURN NEXT;
  END LOOP;
  
  RETURN;
END;
$$;
