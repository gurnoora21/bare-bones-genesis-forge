
// Helper functions for queue operations

// Helper function to delete messages with retries
export async function deleteMessageWithRetries(
  supabase: any,
  queueName: string,
  messageId: string,
  maxRetries: number = 3
): Promise<boolean> {
  console.log(`Attempting to delete message ${messageId} from queue ${queueName} with up to ${maxRetries} retries`);
  
  let deleted = false;
  let attempts = 0;
  
  while (!deleted && attempts < maxRetries) {
    attempts++;
    
    try {
      // First attempt: Try using direct RPC with explicit number conversion
      const numericalId = parseInt(messageId, 10);
      if (!isNaN(numericalId)) {
        const { data, error } = await supabase.rpc(
          'pg_delete_message',
          { 
            queue_name: queueName, 
            message_id: numericalId.toString() 
          }
        );
        
        if (!error && data === true) {
          console.log(`Successfully deleted message ${messageId} from ${queueName} using numerical ID`);
          deleted = true;
          break;
        }
      }
      
      // Second attempt: Try with string ID format
      const { data, error } = await supabase.rpc(
        'pg_delete_message',
        { 
          queue_name: queueName, 
          message_id: messageId.toString()
        }
      );
      
      if (error) {
        console.error(`Delete attempt ${attempts} failed with RPC error:`, error);
      } else if (data === true) {
        console.log(`Successfully deleted message ${messageId} from ${queueName} (attempt ${attempts})`);
        deleted = true;
        break;
      } else {
        console.warn(`Delete attempt ${attempts} returned false for message ${messageId}`);
      }

      // Third attempt: Try direct deletion with Edge Function
      if (!deleted) {
        try {
          const deleteResponse = await fetch(
            `${Deno.env.get("SUPABASE_URL")}/functions/v1/deleteFromQueue`,
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${Deno.env.get("SUPABASE_ANON_KEY")}`
              },
              body: JSON.stringify({ queue_name: queueName, message_id: messageId })
            }
          );
          
          if (deleteResponse.ok) {
            const result = await deleteResponse.json();
            if (result.success) {
              console.log(`Successfully deleted message ${messageId} via Edge Function (attempt ${attempts})`);
              deleted = true;
              break;
            }
          }
        } catch (fetchError) {
          console.error(`Fetch error during deletion attempt ${attempts}:`, fetchError);
        }
      }
      
      // Wait before retrying (exponential backoff with jitter)
      if (!deleted && attempts < maxRetries) {
        const baseDelay = Math.pow(2, attempts) * 100;
        const jitter = Math.floor(Math.random() * 100);
        const delayMs = baseDelay + jitter;
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    } catch (e) {
      console.error(`Error during deletion attempt ${attempts} for message ${messageId}:`, e);
      
      // Wait before retrying
      if (attempts < maxRetries) {
        const delayMs = Math.pow(2, attempts) * 100;
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
  }
  
  if (!deleted) {
    console.error(`Failed to delete message ${messageId} after ${maxRetries} attempts`);
  }
  
  return deleted;
}

// Helper function to log worker issues to the database
export async function logWorkerIssue(
  supabase: any,
  workerName: string,
  issueType: string,
  message: string,
  details: any = {}
) {
  try {
    await supabase.from('worker_issues').insert({
      worker_name: workerName,
      issue_type: issueType,
      message: message,
      details: details,
      resolved: false
    });
    console.error(`[${workerName}] ${issueType}: ${message}`);
  } catch (error) {
    console.error("Failed to log worker issue:", error);
  }
}

// Helper function to check if a track has been processed
export async function checkTrackProcessed(
  supabase: any,
  trackId: string,
  processingType: string
): Promise<boolean> {
  try {
    // Check if this track has already been processed for producers
    const { data, error } = await supabase
      .from('track_producers')
      .select('track_id')
      .eq('track_id', trackId)
      .limit(1);
      
    if (error) {
      console.error(`Error checking if track ${trackId} was processed:`, error);
      return false;
    }
    
    // If we found any producers, this track was already processed
    const alreadyProcessed = data && data.length > 0;
    
    if (alreadyProcessed) {
      console.log(`Track ${trackId} already has producers identified, skipping`);
    }
    
    return alreadyProcessed;
  } catch (error) {
    console.error(`Error in checkTrackProcessed for ${trackId}:`, error);
    return false;
  }
}

// Helper to safely process queue messages with idempotency
export async function processQueueMessageSafely(
  supabase: any,
  queueName: string,
  messageId: string,
  processFn: () => Promise<any>,
  idempotencyKey?: string,
  idempotencyCheckFn?: () => Promise<boolean>
): Promise<boolean> {
  // Check for idempotency if provided
  if (idempotencyCheckFn) {
    try {
      const alreadyProcessed = await idempotencyCheckFn();
      if (alreadyProcessed) {
        // Already processed this item, just delete the message
        await deleteMessageWithRetries(supabase, queueName, messageId);
        return true;
      }
    } catch (error) {
      console.error(`Idempotency check failed:`, error);
      // Continue processing as normal
    }
  }
  
  try {
    // Process the message
    await processFn();
    
    // Delete the message after successful processing
    const deleted = await deleteMessageWithRetries(supabase, queueName, messageId);
    return deleted;
  } catch (error) {
    console.error(`Error processing message ${messageId} from queue ${queueName}:`, error);
    return false;
  }
}
