
// Helper functions for queue operations

import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";

// Initialize Redis client for distributed locking and caching
const redis = new Redis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL") || "",
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN") || "",
});

// Helper function to delete messages with retries and advanced error recovery
export async function deleteMessageWithRetries(
  supabase: any,
  queueName: string,
  messageId: string,
  maxRetries: number = 5
): Promise<boolean> {
  console.log(`Attempting to delete message ${messageId} from queue ${queueName} with up to ${maxRetries} retries`);
  
  // Check if already deleted (idempotency)
  try {
    const deletedKey = `deleted:${queueName}:${messageId}`;
    const alreadyDeleted = await redis.exists(deletedKey);
    
    if (alreadyDeleted === 1) {
      console.log(`Message ${messageId} was already deleted previously, skipping`);
      return true;
    }
  } catch (redisError) {
    // Continue if Redis fails, idempotency check is not critical
    console.warn(`Redis idempotency check failed for message ${messageId}:`, redisError);
  }
  
  let deleted = false;
  let attempts = 0;
  
  // First, try direct force delete for maximum reliability
  try {
    const deleteResult = await supabase.functions.invoke("forceDeleteMessage", {
      body: { 
        queue_name: queueName, 
        message_id: messageId,
        bypass_checks: attempts >= maxRetries - 1 // Allow bypass on last attempt
      }
    });
    
    if (deleteResult?.data?.success) {
      console.log(`Successfully force-deleted message ${messageId} from ${queueName}`);
      
      // Mark as deleted in Redis for idempotency
      try {
        const deletedKey = `deleted:${queueName}:${messageId}`;
        await redis.set(deletedKey, 'true', { ex: 86400 }); // 24 hour TTL
      } catch (redisError) {
        console.warn(`Failed to mark message ${messageId} as deleted in Redis:`, redisError);
      }
      
      return true;
    }
  } catch (e) {
    console.error(`Force delete attempt failed for message ${messageId}:`, e);
  }
  
  while (!deleted && attempts < maxRetries) {
    attempts++;
    
    try {
      // Try using direct RPC with appropriate ID format
      const { data, error } = await supabase.rpc(
        'pg_delete_message',
        { 
          queue_name: queueName, 
          message_id: messageId.toString() 
        }
      );
      
      if (!error && data === true) {
        console.log(`Successfully deleted message ${messageId} from ${queueName} via RPC (attempt ${attempts})`);
        deleted = true;
        
        // Mark as deleted in Redis for idempotency
        try {
          const deletedKey = `deleted:${queueName}:${messageId}`;
          await redis.set(deletedKey, 'true', { ex: 86400 }); // 24 hour TTL
        } catch (redisError) {
          console.warn(`Failed to mark message ${messageId} as deleted in Redis:`, redisError);
        }
        
        break;
      } else if (error) {
        console.error(`Delete attempt ${attempts} failed with RPC error:`, error);
      }

      // If RPC failed, try Edge Function as fallback
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
        const baseDelay = Math.min(Math.pow(2, attempts) * 100, 2000);
        const jitter = Math.floor(Math.random() * 100);
        const delayMs = baseDelay + jitter;
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    } catch (e) {
      console.error(`Error during deletion attempt ${attempts} for message ${messageId}:`, e);
      
      // Wait before retrying
      if (attempts < maxRetries) {
        const delayMs = Math.min(Math.pow(2, attempts) * 100, 2000); 
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
  }
  
  if (!deleted) {
    console.error(`Failed to delete message ${messageId} after ${maxRetries} attempts`);
    
    // Last resort: Try resetting the message's visibility timeout so it can be reprocessed later
    try {
      const { data: resetResult } = await supabase.rpc('reset_stuck_message', {
        queue_name: queueName,
        message_id: messageId.toString()
      });
      
      if (resetResult === true) {
        console.log(`Reset visibility timeout for message ${messageId} as deletion failed`);
      }
    } catch (resetError) {
      console.error(`Failed to reset visibility for message ${messageId}:`, resetError);
    }
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

// Enhanced idempotency check with distributed locking
export async function acquireProcessingLock(
  entityType: string,
  entityId: string,
  ttlSeconds: number = 300
): Promise<boolean> {
  const lockKey = `lock:${entityType}:${entityId}`;
  const now = Date.now();
  
  try {
    // Use Redis to set a distributed lock with expiration
    const result = await redis.set(lockKey, now.toString(), {
      nx: true, // Only set if not exists
      ex: ttlSeconds // Expire after ttlSeconds
    });
    
    return result === 'OK';
  } catch (error) {
    console.error(`Error acquiring lock for ${entityType} ${entityId}:`, error);
    return false;
  }
}

// Function to check if an entity has already been processed
export async function checkEntityProcessed(
  supabase: any,
  entityType: string,
  entityId: string,
  processingType: string
): Promise<boolean> {
  // First check in Redis cache for faster lookups
  try {
    const processedKey = `processed:${entityType}:${processingType}:${entityId}`;
    const exists = await redis.exists(processedKey);
    
    if (exists === 1) {
      console.log(`Entity ${entityType} ${entityId} was already processed for ${processingType} (Redis cache hit)`);
      return true;
    }
  } catch (error) {
    // Continue to database check if Redis fails
    console.warn(`Redis check failed for ${entityType} ${entityId}:`, error);
  }
  
  try {
    if (entityType === 'track' && processingType === 'producer_identification') {
      // Check if this track has already been processed for producers
      const { data, error } = await supabase
        .from('track_producers')
        .select('track_id')
        .eq('track_id', entityId)
        .limit(1);
        
      if (error) {
        console.error(`Error checking if track ${entityId} was processed:`, error);
        return false;
      }
      
      // If we found any producers, this track was already processed
      const alreadyProcessed = data && data.length > 0;
      
      if (alreadyProcessed) {
        console.log(`Track ${entityId} already has producers identified, skipping`);
        
        // Cache this result in Redis for faster future lookups
        try {
          const processedKey = `processed:track:producer_identification:${entityId}`;
          await redis.set(processedKey, 'true', { ex: 86400 }); // 24 hour TTL
        } catch (redisError) {
          console.warn(`Failed to cache processed status for track ${entityId}:`, redisError);
        }
      }
      
      return alreadyProcessed;
    }
    
    // Album tracks processing check
    if (entityType === 'album' && processingType === 'track_discovery') {
      const { data: albumData, error } = await supabase
        .from('tracks')
        .select('id')
        .eq('album_id', entityId)
        .limit(1);
        
      const alreadyProcessed = albumData && albumData.length > 0;
      
      if (alreadyProcessed) {
        console.log(`Album ${entityId} already has tracks discovered, skipping`);
        
        // Cache this result in Redis for faster future lookups
        try {
          const processedKey = `processed:album:track_discovery:${entityId}`;
          await redis.set(processedKey, 'true', { ex: 86400 }); // 24 hour TTL
        } catch (redisError) {
          console.warn(`Failed to cache processed status for album ${entityId}:`, redisError);
        }
      }
      
      return alreadyProcessed;
    }
    
    return false;
  } catch (error) {
    console.error(`Error in checkEntityProcessed for ${entityType} ${entityId}:`, error);
    return false;
  }
}

// Legacy function for backward compatibility
export async function checkTrackProcessed(
  supabase: any,
  trackId: string,
  processingType: string
): Promise<boolean> {
  return checkEntityProcessed(supabase, 'track', trackId, processingType);
}

// Enhanced helper to safely process queue messages with idempotency and transaction guarantees
export async function processQueueMessageSafely(
  supabase: any,
  queueName: string,
  messageId: string,
  processFn: () => Promise<any>,
  idempotencyKey?: string,
  idempotencyCheckFn?: () => Promise<boolean>,
  options: { maxRetries?: number; circuitBreaker?: boolean; lockTimeout?: number } = {}
): Promise<boolean> {
  const maxRetries = options.maxRetries || 3;
  const lockTimeout = options.lockTimeout || 300; // 5 minutes default
  let retries = 0;
  let success = false;
  
  // Circuit breaker implementation
  if (options.circuitBreaker) {
    try {
      const circuitKey = `circuit:${queueName}`;
      const circuitStatus = await redis.get(circuitKey);
      
      if (circuitStatus === 'open') {
        console.log(`Circuit breaker open for queue ${queueName}, skipping processing`);
        // Don't delete the message, just return false to indicate failure
        return false;
      }
      
      // Increment failure counter
      const failureKey = `failures:${queueName}`;
      const failures = await redis.incr(failureKey);
      
      // Reset failure counter after 60 seconds if no error
      redis.expire(failureKey, 60);
      
      // Open circuit breaker if failures exceed threshold
      if (failures > 10) {
        console.error(`Too many failures in queue ${queueName}, opening circuit breaker`);
        await redis.set(circuitKey, 'open', { ex: 300 }); // Open for 5 minutes
        await logWorkerIssue(
          supabase,
          queueName,
          "circuit_breaker",
          `Circuit breaker opened due to excessive failures`,
          { failures }
        );
        return false;
      }
    } catch (error) {
      // Continue processing even if circuit breaker fails
      console.error(`Error in circuit breaker:`, error);
    }
  }
  
  // Combine queue name and message ID for locking
  const processingLockKey = `processing:${queueName}:${messageId}`;
  
  // Try to acquire a distributed lock for this message
  try {
    const lockAcquired = await redis.set(processingLockKey, Date.now().toString(), {
      nx: true,
      ex: lockTimeout
    });
    
    if (lockAcquired !== 'OK') {
      console.log(`Message ${messageId} is already being processed, skipping`);
      return true; // Return true since another process is handling it
    }
  } catch (redisError) {
    // Continue if Redis fails, locking is not critical
    console.warn(`Redis lock acquisition failed for message ${messageId}:`, redisError);
  }
  
  // If an idempotency key is provided, check Redis first for faster lookups
  if (idempotencyKey) {
    try {
      const processedKey = `processed:${queueName}:${idempotencyKey}`;
      const alreadyProcessed = await redis.get(processedKey);
      
      if (alreadyProcessed) {
        console.log(`Idempotency check via Redis passed: ${idempotencyKey} already processed`);
        await deleteMessageWithRetries(supabase, queueName, messageId);
        
        // Release the lock
        try {
          await redis.del(processingLockKey);
        } catch (redisError) {
          console.warn(`Failed to release processing lock for ${messageId}:`, redisError);
        }
        
        return true;
      }
    } catch (redisError) {
      console.error(`Redis idempotency check failed:`, redisError);
      // Continue to database check if Redis fails
    }
  }
  
  // Check database for idempotency if provided
  if (idempotencyCheckFn) {
    try {
      const alreadyProcessed = await idempotencyCheckFn();
      if (alreadyProcessed) {
        console.log(`Idempotency check passed: item already processed`);
        // Delete the message after confirming it was already processed
        await deleteMessageWithRetries(supabase, queueName, messageId);
        
        // Release the lock
        try {
          await redis.del(processingLockKey);
        } catch (redisError) {
          console.warn(`Failed to release processing lock for ${messageId}:`, redisError);
        }
        
        return true;
      }
    } catch (error) {
      console.error(`Idempotency check failed:`, error);
      // Continue processing as normal
    }
  }
  
  // Process with retries
  while (retries <= maxRetries && !success) {
    try {
      // Process the message
      await processFn();
      
      // Mark as processed in Redis for future idempotency checks
      if (idempotencyKey) {
        try {
          const processedKey = `processed:${queueName}:${idempotencyKey}`;
          await redis.set(processedKey, 'true', { ex: 86400 }); // TTL: 24 hours
        } catch (redisError) {
          // Not critical if Redis fails, continue
          console.warn(`Failed to store idempotency key in Redis:`, redisError);
        }
      }
      
      // Delete the message after successful processing
      const deleted = await deleteMessageWithRetries(supabase, queueName, messageId);
      
      // Reset circuit breaker failures if success
      if (options.circuitBreaker) {
        try {
          await redis.del(`failures:${queueName}`);
        } catch (redisError) {
          // Not critical if Redis fails
          console.warn(`Failed to reset circuit breaker:`, redisError);
        }
      }
      
      // Release the lock
      try {
        await redis.del(processingLockKey);
      } catch (redisError) {
        console.warn(`Failed to release processing lock for ${messageId}:`, redisError);
      }
      
      success = deleted;
      return deleted;
    } catch (error) {
      retries++;
      console.error(`Error processing message ${messageId} from queue ${queueName} (attempt ${retries}/${maxRetries}):`, error);
      
      if (retries <= maxRetries) {
        // Exponential backoff between retries with cap
        const delay = Math.min(Math.pow(2, retries) * 100, 2000);
        await new Promise(resolve => setTimeout(resolve, delay));
      } else {
        // Log the issue if all retries failed
        await logWorkerIssue(
          supabase,
          queueName,
          "processing_failed",
          `Failed to process message ${messageId} after ${maxRetries} attempts: ${error.message}`,
          { 
            error: error.message,
            stack: error.stack,
            messageId
          }
        );
        
        // Release the lock
        try {
          await redis.del(processingLockKey);
        } catch (redisError) {
          console.warn(`Failed to release processing lock for ${messageId}:`, redisError);
        }
      }
    }
  }
  
  return false;
}

// Enhanced function to track processing state of entities
export async function trackProcessingState(
  entityType: string,
  entityId: string,
  state: 'started' | 'completed' | 'failed',
  details: any = {}
): Promise<void> {
  try {
    const stateKey = `state:${entityType}:${entityId}`;
    const stateData = {
      state,
      timestamp: Date.now(),
      details
    };
    
    await redis.set(stateKey, JSON.stringify(stateData), { ex: 86400 }); // 24 hour TTL
  } catch (error) {
    console.warn(`Failed to track processing state for ${entityType} ${entityId}:`, error);
  }
}
