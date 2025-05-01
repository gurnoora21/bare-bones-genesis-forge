
import { supabase } from '../integrations/supabase/client';

/**
 * Trigger the discovery process for a new artist
 * @param artistName The name of the artist to discover
 * @returns The message ID in the queue
 */
export async function triggerArtistDiscovery(artistName: string): Promise<string> {
  console.log(`Starting artist discovery for "${artistName}"`);
  
  const { data, error } = await supabase.rpc('start_artist_discovery', {
    artist_name: artistName
  });
  
  if (error) {
    console.error('Error starting discovery:', error);
    throw error;
  }
  
  console.log(`Artist "${artistName}" discovery triggered with job ID: ${data}`);
  return data;
}

/**
 * Manually trigger a specific worker
 * @param workerName The name of the worker to trigger (e.g., 'artistDiscovery')
 * @returns The response from the worker
 */
export async function triggerWorker(workerName: string): Promise<any> {
  console.log(`Manually triggering worker "${workerName}"`);
  
  // Use a type assertion to bypass the TypeScript error
  // This is necessary because the RPC function isn't in the generated TypeScript definitions yet
  const { data, error } = await supabase.rpc(
    'manual_trigger_worker' as any,
    { worker_name: workerName }
  );
  
  if (error) {
    console.error(`Error triggering worker ${workerName}:`, error);
    throw error;
  }
  
  console.log(`Worker "${workerName}" manually triggered with response:`, data);
  return data;
}

/**
 * Check the status of all worker cron jobs
 * @returns Array of cron job status information
 */
export async function checkWorkerCrons(): Promise<any> {
  console.log("Checking worker cron status...");
  
  // Use a type assertion to bypass the TypeScript error
  // This is necessary because the RPC function isn't in the generated TypeScript definitions yet
  const { data, error } = await supabase.rpc('check_worker_crons' as any);
  
  if (error) {
    console.error('Error checking worker crons:', error);
    throw error;
  }
  
  console.log('Worker cron status:', data);
  return data;
}

/**
 * Get contents of a queue for debugging
 * @param queueName The name of the queue to inspect
 * @returns Queue contents data
 */
export async function debugQueueContents(queueName: string): Promise<any> {
  console.log(`Inspecting contents of queue "${queueName}"`);
  
  // Instead of directly querying the PGMQ table (which isn't in our TypeScript types),
  // use a raw SQL query with the `rpc` method to get queue contents
  const { data, error } = await supabase.rpc(
    'pg_dequeue', 
    { 
      queue_name: queueName,
      batch_size: 100,
      visibility_seconds: 5 // Short visibility so we don't block actual processing
    }
  );
  
  if (error) {
    console.error(`Error inspecting queue ${queueName}:`, error);
    throw error;
  }
  
  console.log(`Queue "${queueName}" contents:`, data);
  
  // Return messages to queue immediately after inspection
  try {
    if (data && Array.isArray(data)) {
      for (const msg of data) {
        await supabase.rpc('pg_release_message', {
          queue_name: queueName,
          message_id: msg.id
        });
      }
      console.log(`Released ${data.length} messages back to queue "${queueName}"`);
    }
  } catch (releaseError) {
    console.error(`Error releasing messages back to queue:`, releaseError);
  }
  
  return data;
}
