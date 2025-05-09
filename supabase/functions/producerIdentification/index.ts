
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";
import { Redis } from "https://esm.sh/@upstash/redis@1.20.6";
import { 
  ErrorCategory, 
  ErrorSource, 
  createEnhancedError,
  retryWithBackoff,
  trackError,
  createDeadLetterRecord
} from "../_shared/errorHandling.ts";
import { 
  validateProducerIdentificationMessage, 
  createValidationError 
} from "../_shared/dataValidation.ts";
import { ProcessingState, EntityType, generateCorrelationId } from "../_shared/stateManager.ts";
import { getEnhancedStateManager } from "../_shared/enhancedStateManager.ts";
import { getRedis } from "../_shared/upstashRedis.ts";
import { getGeniusClient } from "../_shared/enhancedGeniusClient.ts";

// Message structure for producer identification
interface ProducerIdentificationMsg {
  trackId: string;
  trackName: string;
  artistName: string;
}

// Constants
const QUEUE_NAME = "producer_identification";
const DEAD_LETTER_QUEUE = "producer_identification_dlq";
const WORKER_NAME = "producerIdentification";
const DEFAULT_BATCH_SIZE = 5;
const DEFAULT_VISIBILITY_TIMEOUT = 180; // 3 minutes
const MAX_RETRIES = 3;

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );

  // Initialize Redis client
  const redis = getRedis();
  
  // Initialize state manager
  const stateManager = getEnhancedStateManager(supabase, redis, true);
  
  // Generate batch correlation ID
  const batchCorrelationId = generateCorrelationId('batch');
  
  console.log("Starting producer identification process");

  try {
    // Process queue batch with enhanced error handling
    const { data: messages, error } = await retryWithBackoff(
      async () => {
        return supabase.rpc('pg_dequeue', { 
          queue_name: QUEUE_NAME,
          batch_size: DEFAULT_BATCH_SIZE, 
          visibility_timeout: DEFAULT_VISIBILITY_TIMEOUT
        });
      },
      {
        name: "readQueue",
        source: ErrorSource.DATABASE,
        maxRetries: 2,
        baseDelayMs: 500,
        correlationId: batchCorrelationId
      }
    );

    if (error) {
      console.error(`[${batchCorrelationId}] Error reading from queue:`, error);
      await trackError(error, {
        operationName: "readQueue",
        correlationId: batchCorrelationId
      });
      return new Response(JSON.stringify({ error: error.message }), { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      });
    }

    // Parse messages
    let parsedMessages: any[] =  [];
    try {
      // Check if messages need to be parsed from string
      if (typeof messages === 'string') {
        parsedMessages = JSON.parse(messages);
      } else if (Array.isArray(messages)) {
        parsedMessages = messages;
      } else if (messages) {
        parsedMessages = [messages];
      }
    } catch (parseError) {
      console.error(`[${batchCorrelationId}] Error parsing messages:`, parseError);
      await trackError(parseError, {
        operationName: "parseMessages",
        correlationId: batchCorrelationId
      });
    }

    console.log(`[${batchCorrelationId}] Retrieved ${parsedMessages.length} messages from queue`);

    if (!parsedMessages || parsedMessages.length === 0) {
      return new Response(
        JSON.stringify({ processed: 0, message: "No messages to process" }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Create a quick response to avoid timeout
    const response = new Response(
      JSON.stringify({ 
        processing: true, 
        message_count: parsedMessages.length,
        correlation_id: batchCorrelationId
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );

    // Process messages in background to avoid CPU timeout
    EdgeRuntime.waitUntil((async () => {
      // Initialize Genius client
      let geniusClient;
      try {
        geniusClient = getGeniusClient();
      } catch (clientError) {
        console.error(`[${batchCorrelationId}] Failed to initialize Genius client:`, clientError);
        await trackError(clientError, {
          operationName: "initGeniusClient",
          correlationId: batchCorrelationId
        });
        return;
      }
      
      // Track batch statistics
      let successCount = 0;
      let errorCount = 0;
      let validationErrorCount = 0;
      let deadLetteredCount = 0;
      
      // Process each message
      for (const message of parsedMessages) {
        // Generate message correlation ID
        const messageId = message.id?.toString() || message.msg_id?.toString();
        const correlationId = `${batchCorrelationId}:msg:${messageId}`;
        
        console.log(`[${correlationId}] Processing message ${messageId}`);
        
        try {
          // Parse message
          let msg: ProducerIdentificationMsg;
          try {
            if (typeof message.message === 'string') {
              msg = JSON.parse(message.message);
            } else {
              msg = message.message;
            }
            
            // Validate message format
            const validation = validateProducerIdentificationMessage(msg);
            if (!validation.valid) {
              throw createValidationError("ProducerIdentificationMessage", validation);
            }
          } catch (parseError) {
            console.error(`[${correlationId}] Error parsing message:`, parseError);
            await trackError(parseError, {
              operationName: "parseMessage",
              correlationId
            });
            
            // Send to dead letter queue
            await sendToDeadLetterQueue(
              supabase, 
              message, 
              parseError, 
              correlationId,
              "validation_error"
            );
            
            // Delete from original queue
            await deleteMessage(supabase, QUEUE_NAME, messageId);
            
            errorCount++;
            validationErrorCount++;
            continue;
          }
          
          // Acquire processing lock
          const entityType = EntityType.TRACK;
          const entityId = msg.trackId;
          
          // Check if already processed
          const alreadyProcessed = await stateManager.isProcessed(entityType, entityId);
          if (alreadyProcessed) {
            console.log(`[${correlationId}] Track ${msg.trackId} already processed, skipping`);
            
            // Delete message from queue
            await deleteMessage(supabase, QUEUE_NAME, messageId);
            
            successCount++;
            continue;
          }
          
          // Try to acquire processing lock
          const lockAcquired = await stateManager.acquireProcessingLock(
            entityType,
            entityId,
            {
              correlationId,
              heartbeatIntervalSeconds: 15
            }
          );
          
          if (!lockAcquired) {
            console.log(`[${correlationId}] Could not acquire lock for ${entityId}, skipping`);
            continue;
          }
          
          try {
            // Process the message with retry logic
            await retryWithBackoff(
              async () => await identifyProducers(
                supabase,
                geniusClient,
                msg,
                correlationId
              ),
              {
                name: "identifyProducers",
                source: ErrorSource.WORKER,
                correlationId,
                entityType,
                entityId,
                maxRetries: MAX_RETRIES,
                baseDelayMs: 1000,
                maxDelayMs: 10000,
                jitterFactor: 0.3,
                timeoutMs: 25000, // 25 seconds per attempt
                onRetry: (error, attempt, delay) => {
                  console.log(`[${correlationId}] Retrying producer identification for ${msg.trackId} (attempt ${attempt})`);
                }
              }
            );
            
            // Mark as completed
            await stateManager.markAsCompleted(entityType, entityId, {
              correlationId,
              completedBy: WORKER_NAME
            });
            
            // Delete message from queue
            await deleteMessage(supabase, QUEUE_NAME, messageId);
            
            successCount++;
          } catch (processError) {
            console.error(`[${correlationId}] Error processing track ${msg.trackId}:`, processError);
            
            // Track error
            await trackError(processError, {
              entityType,
              entityId,
              operationName: "identifyProducers",
              correlationId
            });
            
            // Determine if this is a permanent failure or retriable
            const isRetriable = processError.retriable !== false && 
                              !(processError.category === ErrorCategory.PERMANENT_VALIDATION ||
                                processError.category === ErrorCategory.PERMANENT_NOT_FOUND ||
                                processError.category === ErrorCategory.PERMANENT_BAD_REQUEST);
            
            if (!isRetriable) {
              // Mark as dead letter
              await stateManager.markAsDeadLetter(entityType, entityId, processError.message, {
                correlationId,
                errorCategory: processError.category,
                errorSource: processError.source
              });
              
              // Send to dead letter queue
              await sendToDeadLetterQueue(
                supabase, 
                message, 
                processError, 
                correlationId,
                "permanent_error"
              );
              
              // Delete from original queue
              await deleteMessage(supabase, QUEUE_NAME, messageId);
              
              deadLetteredCount++;
            } else {
              // Mark as failed (retriable)
              await stateManager.markAsFailed(entityType, entityId, processError.message, {
                correlationId,
                errorCategory: processError.category,
                errorSource: processError.source,
                retriable: true
              });
              
              // Message visibility will timeout and message will be reprocessed
              console.log(`[${correlationId}] Retriable error, will be reprocessed later`);
            }
            
            errorCount++;
          }
        } catch (messageError) {
          console.error(`[${correlationId}] Uncaught error processing message:`, messageError);
          await trackError(messageError, {
            operationName: "processMessage",
            correlationId
          });
          errorCount++;
        }
      }

      // Log batch results
      console.log(
        `[${batchCorrelationId}] Batch processing complete: ` +
        `${successCount} successful, ${errorCount} failed, ` +
        `${validationErrorCount} validation errors, ${deadLetteredCount} dead-lettered`
      );
      
      // Record metrics
      try {
        await supabase.from('queue_metrics').insert({
          queue_name: QUEUE_NAME,
          operation: "batch_processing",
          started_at: new Date().toISOString(),
          finished_at: new Date().toISOString(),
          processed_count: parsedMessages.length,
          success_count: successCount,
          error_count: errorCount,
          details: { 
            validation_errors: validationErrorCount,
            dead_lettered: deadLetteredCount,
            correlation_id: batchCorrelationId
          }
        });
      } catch (metricsError) {
        console.error(`[${batchCorrelationId}] Failed to record metrics:`, metricsError);
      }
    })());

    return response;
  } catch (mainError) {
    console.error(`[${batchCorrelationId}] Critical error in producer identification worker:`, mainError);
    await trackError(mainError, {
      operationName: WORKER_NAME,
      correlationId: batchCorrelationId
    });
    
    return new Response(JSON.stringify({ error: mainError.message }), { 
      status: 500, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
    });
  }
});

/**
 * Main producer identification logic
 */
async function identifyProducers(
  supabase: any,
  geniusClient: any,
  msg: ProducerIdentificationMsg,
  correlationId: string
): Promise<any> {
  const { trackId, trackName, artistName } = msg;
  
  console.log(`[${correlationId}] Identifying producers for track "${trackName}" by ${artistName}`);
  
  // Get track information from database to double-check
  const { data: track, error: trackError } = await supabase
    .from('tracks')
    .select('id, name')
    .eq('id', trackId)
    .maybeSingle();
  
  if (trackError) {
    throw createEnhancedError(
      `Failed to fetch track from database: ${trackError.message}`,
      ErrorSource.DATABASE,
      ErrorCategory.TRANSIENT_SERVICE
    );
  }
  
  if (!track) {
    throw createEnhancedError(
      `Track with ID ${trackId} not found in database`,
      ErrorSource.DATABASE,
      ErrorCategory.PERMANENT_NOT_FOUND
    );
  }
  
  // Step 1: Search for the song on Genius
  console.log(`[${correlationId}] Searching Genius for "${trackName}" by ${artistName}`);
  
  const song = await geniusClient.search(trackName, artistName);
  
  if (!song) {
    console.log(`[${correlationId}] No song found on Genius for "${trackName}" by ${artistName}`);
    return { trackId, found: false };
  }
  
  console.log(`[${correlationId}] Found song on Genius: "${song.title}" (ID: ${song.id})`);
  
  // Step 2: Get detailed song information
  const songDetails = await geniusClient.getSong(song.id);
  
  if (!songDetails) {
    throw createEnhancedError(
      `Failed to get details for song ID ${song.id}`,
      ErrorSource.GENIUS_API,
      ErrorCategory.TRANSIENT_SERVICE
    );
  }
  
  // Step 3: Extract producers from song details
  const producers = geniusClient.extractProducers(songDetails);
  
  console.log(`[${correlationId}] Found ${producers.length} producers for "${trackName}"`);
  
  if (producers.length === 0) {
    return { trackId, found: true, producersFound: 0 };
  }
  
  // Step 4: Process each producer
  const producerResults = [];
  
  for (const producer of producers) {
    try {
      // Normalize producer name
      const normalizedName = normalizeProducerName(producer.name);
      
      // Check if producer already exists
      const { data: existingProducer } = await supabase
        .from('producers')
        .select('id')
        .eq('normalized_name', normalizedName)
        .maybeSingle();
      
      let producerId;
      
      if (existingProducer) {
        // Producer exists, use existing ID
        producerId = existingProducer.id;
        console.log(`[${correlationId}] Using existing producer: ${producer.name} (${producerId})`);
      } else {
        // Create new producer
        const { data: newProducer, error: insertError } = await supabase
          .from('producers')
          .insert({
            name: producer.name,
            normalized_name: normalizedName,
            metadata: {
              genius_extraction: {
                song_id: song.id,
                extraction_source: producer.source,
                extraction_confidence: producer.confidence
              }
            }
          })
          .select('id')
          .maybeSingle();
        
        if (insertError) {
          throw createEnhancedError(
            `Failed to create producer: ${insertError.message}`,
            ErrorSource.DATABASE,
            ErrorCategory.TRANSIENT_SERVICE
          );
        }
        
        producerId = newProducer.id;
        console.log(`[${correlationId}] Created new producer: ${producer.name} (${producerId})`);
      }
      
      // Create track-producer association
      const { error: associationError } = await supabase
        .from('track_producers')
        .insert({
          track_id: trackId,
          producer_id: producerId,
          confidence: producer.confidence,
          source: producer.source
        });
      
      if (associationError) {
        throw createEnhancedError(
          `Failed to create track-producer association: ${associationError.message}`,
          ErrorSource.DATABASE,
          ErrorCategory.TRANSIENT_SERVICE
        );
      }
      
      // Queue producer for social enrichment
      await supabase.rpc('pg_enqueue', {
        queue_name: 'social_enrichment',
        message_body: {
          producerId,
          producerName: producer.name
        }
      });
      
      producerResults.push({
        name: producer.name,
        id: producerId,
        confidence: producer.confidence
      });
    } catch (producerError) {
      console.warn(`[${correlationId}] Error processing producer ${producer.name}:`, producerError);
      // Continue processing other producers
    }
  }
  
  // Return results
  return {
    trackId,
    trackName,
    found: true,
    producersFound: producerResults.length,
    producers: producerResults
  };
}

/**
 * Normalize producer name for consistent comparison
 */
function normalizeProducerName(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^\w\s]/g, '') // Remove special chars
    .replace(/\s+/g, ' ')    // Normalize whitespace
    .trim();
}

/**
 * Delete a message from queue with enhanced error handling
 */
async function deleteMessage(
  supabase: any,
  queueName: string,
  messageId: string
): Promise<boolean> {
  try {
    const { data: deleted, error } = await supabase.rpc(
      'pg_delete_message',
      { queue_name: queueName, message_id: messageId }
    );
    
    if (error) {
      console.warn(`Error deleting message ${messageId} from ${queueName}:`, error);
      return false;
    }
    
    return !!deleted;
  } catch (deleteError) {
    console.warn(`Failed to delete message ${messageId}:`, deleteError);
    return false;
  }
}

/**
 * Send a message to the dead letter queue
 */
async function sendToDeadLetterQueue(
  supabase: any,
  originalMessage: any,
  error: Error,
  correlationId: string,
  reason: string
): Promise<void> {
  try {
    // Create dead letter record
    const deadLetterRecord = await createDeadLetterRecord(
      error,
      {
        queueName: QUEUE_NAME,
        messageId: originalMessage.id?.toString() || originalMessage.msg_id?.toString(),
        message: originalMessage.message,
        operationName: "producerIdentification",
        correlationId,
        attempts: 1
      }
    );
    
    // Add reason and timestamp
    const deadLetterMessage = {
      ...deadLetterRecord,
      reason,
      sent_to_dlq_at: new Date().toISOString()
    };
    
    // Send to dead letter queue
    await supabase.rpc('pg_enqueue', {
      queue_name: DEAD_LETTER_QUEUE,
      message_body: deadLetterMessage
    });
    
    console.log(`[${correlationId}] Message sent to dead letter queue: ${reason}`);
  } catch (dlqError) {
    console.error(`[${correlationId}] Error sending to dead letter queue:`, dlqError);
  }
}
