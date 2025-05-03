
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
      // Try using the improved database function first
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

      // If the first method failed and this isn't the last attempt, try the Edge Function approach
      if (!deleted && attempts < maxRetries) {
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
          } else if (result.reset) {
            console.log(`Reset visibility timeout for message ${messageId} (attempt ${attempts})`);
            // We'll consider this a success in terms of handling the message
            deleted = true;
            break;
          }
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
