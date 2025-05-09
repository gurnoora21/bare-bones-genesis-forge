
/**
 * Enhanced error handling system for music producer pipeline
 * Provides standardized error classification, retry policies, and correlation tracking
 */

import { EntityType, ProcessingState } from "./stateManager.ts";
import { getRedis } from "./upstashRedis.ts";

// Enhanced error categories for more granular handling
export enum ErrorCategory {
  // Transient errors that can be retried
  TRANSIENT_NETWORK = 'TRANSIENT_NETWORK',     // Network connectivity issues
  TRANSIENT_TIMEOUT = 'TRANSIENT_TIMEOUT',     // Request timeouts
  TRANSIENT_RATE_LIMIT = 'TRANSIENT_RATE_LIMIT', // Rate limiting (needs backoff)
  TRANSIENT_SERVICE = 'TRANSIENT_SERVICE',     // Temporary service issues (503, etc)
  
  // Permanent errors that should not be retried
  PERMANENT_AUTH = 'PERMANENT_AUTH',           // Authentication failures
  PERMANENT_VALIDATION = 'PERMANENT_VALIDATION', // Data validation errors
  PERMANENT_NOT_FOUND = 'PERMANENT_NOT_FOUND', // Resource not found (404)
  PERMANENT_BAD_REQUEST = 'PERMANENT_BAD_REQUEST', // Malformed requests (400)
  PERMANENT_FORBIDDEN = 'PERMANENT_FORBIDDEN', // Permission issues (403)
  
  // Special categories
  UNKNOWN = 'UNKNOWN',                      // Default category
  SYSTEM = 'SYSTEM'                         // Internal system errors
}

// Group errors by whether they're retriable
export const RETRIABLE_ERROR_CATEGORIES = [
  ErrorCategory.TRANSIENT_NETWORK,
  ErrorCategory.TRANSIENT_TIMEOUT,
  ErrorCategory.TRANSIENT_RATE_LIMIT,
  ErrorCategory.TRANSIENT_SERVICE,
  ErrorCategory.UNKNOWN // We retry unknown errors, but with limits
];

// Error sources for better tracking
export enum ErrorSource {
  SPOTIFY_API = 'SPOTIFY_API',
  GENIUS_API = 'GENIUS_API',
  DATABASE = 'DATABASE',
  REDIS = 'REDIS',
  QUEUE = 'QUEUE',
  VALIDATION = 'VALIDATION',
  STATE_MANAGER = 'STATE_MANAGER',
  WORKER = 'WORKER',
  SYSTEM = 'SYSTEM'
}

// Enhanced error with metadata
export interface EnhancedError extends Error {
  category: ErrorCategory;
  source: ErrorSource;
  correlationId?: string;
  retriable: boolean;
  statusCode?: number;
  metadata?: Record<string, any>;
  originalError?: Error;
}

// Retry configuration by error category
export interface RetryPolicy {
  maxRetries: number;
  baseDelayMs: number;
  maxDelayMs: number;
  jitterFactor: number;
  timeoutMs: number; // Per-attempt timeout
}

// Default retry policies by error category
export const DEFAULT_RETRY_POLICIES: Record<ErrorCategory, RetryPolicy> = {
  [ErrorCategory.TRANSIENT_NETWORK]: {
    maxRetries: 5,
    baseDelayMs: 200,
    maxDelayMs: 10000,
    jitterFactor: 0.3,
    timeoutMs: 5000
  },
  [ErrorCategory.TRANSIENT_TIMEOUT]: {
    maxRetries: 3,
    baseDelayMs: 500,
    maxDelayMs: 5000,
    jitterFactor: 0.2,
    timeoutMs: 8000
  },
  [ErrorCategory.TRANSIENT_RATE_LIMIT]: {
    maxRetries: 7,
    baseDelayMs: 1000,
    maxDelayMs: 60000, // Up to 1 minute for rate limits
    jitterFactor: 0.1,
    timeoutMs: 3000
  },
  [ErrorCategory.TRANSIENT_SERVICE]: {
    maxRetries: 5,
    baseDelayMs: 2000,
    maxDelayMs: 30000,
    jitterFactor: 0.2,
    timeoutMs: 5000
  },
  [ErrorCategory.PERMANENT_AUTH]: {
    maxRetries: 0,
    baseDelayMs: 0,
    maxDelayMs: 0,
    jitterFactor: 0,
    timeoutMs: 3000
  },
  [ErrorCategory.PERMANENT_VALIDATION]: {
    maxRetries: 0,
    baseDelayMs: 0,
    maxDelayMs: 0,
    jitterFactor: 0,
    timeoutMs: 3000
  },
  [ErrorCategory.PERMANENT_NOT_FOUND]: {
    maxRetries: 0,
    baseDelayMs: 0,
    maxDelayMs: 0,
    jitterFactor: 0,
    timeoutMs: 3000
  },
  [ErrorCategory.PERMANENT_BAD_REQUEST]: {
    maxRetries: 0,
    baseDelayMs: 0,
    maxDelayMs: 0,
    jitterFactor: 0, 
    timeoutMs: 3000
  },
  [ErrorCategory.PERMANENT_FORBIDDEN]: {
    maxRetries: 0,
    baseDelayMs: 0, 
    maxDelayMs: 0,
    jitterFactor: 0,
    timeoutMs: 3000
  },
  [ErrorCategory.UNKNOWN]: {
    maxRetries: 2,
    baseDelayMs: 500,
    maxDelayMs: 5000,
    jitterFactor: 0.3,
    timeoutMs: 5000
  },
  [ErrorCategory.SYSTEM]: {
    maxRetries: 1,
    baseDelayMs: 1000,
    maxDelayMs: 3000,
    jitterFactor: 0.1,
    timeoutMs: 10000
  }
};

// Source-specific error patterns for classification
const ERROR_PATTERNS = {
  [ErrorSource.SPOTIFY_API]: {
    rateLimited: [
      { status: 429, message: ['rate limit', 'too many requests'] },
      { message: ['api rate limit', 'exceeded'] }
    ],
    auth: [
      { status: 401, message: ['unauthorized', 'invalid token'] },
      { status: 403, message: ['forbidden'] }
    ],
    timeout: [
      { message: ['timeout', 'timed out', 'etimedout', 'econnaborted'] }
    ],
    network: [
      { message: ['network error', 'econnrefused', 'econnreset', 'enotfound'] }
    ],
    service: [
      { status: 500, message: [] },
      { status: 502, message: [] },
      { status: 503, message: [] },
      { status: 504, message: [] }
    ]
  },
  [ErrorSource.GENIUS_API]: {
    rateLimited: [
      { status: 429, message: ['rate limit', 'too many requests'] }
    ],
    auth: [
      { status: 401, message: ['unauthorized', 'invalid token', 'auth'] },
      { status: 403, message: ['forbidden'] }
    ],
    notFound: [
      { status: 404, message: ['not found'] }
    ]
  },
  [ErrorSource.DATABASE]: {
    connection: [
      { code: ['ECONNREFUSED', '08006', '57P01'], message: ['connection'] },
    ],
    timeout: [
      { code: ['57014', '57P04'], message: ['timeout', 'statement_timeout'] }
    ],
    constraint: [
      { code: ['23505'], message: ['duplicate key', 'violates unique constraint'] }
    ]
  },
  [ErrorSource.REDIS]: {
    connection: [
      { message: ['connection', 'ECONNREFUSED', 'ENOTFOUND'] }
    ],
    timeout: [
      { message: ['timeout', 'ETIMEDOUT'] }
    ]
  }
};

/**
 * Enhanced error classification with specific patterns and source detection
 */
export function classifyError(error: Error | EnhancedError, source?: ErrorSource): ErrorCategory {
  // If already classified, return the category
  if ('category' in error && error.category) {
    return error.category;
  }
  
  if (!error) return ErrorCategory.UNKNOWN;
  
  const errorMessage = error.message || '';
  const errorName = error.name || '';
  const statusCode = 'statusCode' in error ? error.statusCode : 
                    'status' in (error as any) ? (error as any).status : 
                    undefined;
  const errorCode = 'code' in (error as any) ? (error as any).code : undefined;
  
  // Attempt to detect the source if not provided
  if (!source) {
    if (errorMessage.includes('spotify') || errorName.includes('SpotifyAPI')) {
      source = ErrorSource.SPOTIFY_API;
    } else if (errorMessage.includes('genius') || errorName.includes('GeniusAPI')) {
      source = ErrorSource.GENIUS_API;
    } else if (
      errorMessage.includes('database') || 
      errorMessage.includes('sql') || 
      errorMessage.includes('pg') || 
      errorName.includes('Database')
    ) {
      source = ErrorSource.DATABASE;
    } else if (
      errorMessage.includes('redis') || 
      errorName.includes('Redis')
    ) {
      source = ErrorSource.REDIS;
    } else if (
      errorMessage.includes('queue') || 
      errorName.includes('Queue')
    ) {
      source = ErrorSource.QUEUE;
    }
  }
  
  // Check source-specific patterns if source is known
  if (source && ERROR_PATTERNS[source]) {
    const patterns = ERROR_PATTERNS[source];
    
    // Rate limit checks
    if (
      (patterns.rateLimited && checkErrorPatterns(patterns.rateLimited, errorMessage, errorName, statusCode, errorCode)) ||
      errorMessage.includes('rate limit') || 
      errorMessage.includes('too many requests') || 
      statusCode === 429
    ) {
      return ErrorCategory.TRANSIENT_RATE_LIMIT;
    }
    
    // Auth checks
    if (
      (patterns.auth && checkErrorPatterns(patterns.auth, errorMessage, errorName, statusCode, errorCode)) ||
      errorMessage.includes('unauthorized') ||
      errorMessage.includes('authentication') ||
      errorMessage.includes('not authenticated') ||
      statusCode === 401 ||
      statusCode === 403
    ) {
      return ErrorCategory.PERMANENT_AUTH;
    }
    
    // Timeout checks
    if (
      (patterns.timeout && checkErrorPatterns(patterns.timeout, errorMessage, errorName, statusCode, errorCode)) ||
      errorMessage.includes('timeout') ||
      errorName.includes('TimeoutError')
    ) {
      return ErrorCategory.TRANSIENT_TIMEOUT;
    }
    
    // Network checks
    if (
      (patterns.network && checkErrorPatterns(patterns.network, errorMessage, errorName, statusCode, errorCode)) ||
      errorMessage.includes('network') ||
      errorMessage.includes('ECONNREFUSED') ||
      errorMessage.includes('ENOTFOUND') ||
      errorName.includes('NetworkError')
    ) {
      return ErrorCategory.TRANSIENT_NETWORK;
    }
    
    // Service checks
    if (
      (patterns.service && checkErrorPatterns(patterns.service, errorMessage, errorName, statusCode, errorCode)) ||
      errorMessage.includes('service unavailable') ||
      errorMessage.includes('internal server error') ||
      (statusCode && statusCode >= 500 && statusCode < 600)
    ) {
      return ErrorCategory.TRANSIENT_SERVICE;
    }
    
    // Not found checks
    if (
      (patterns.notFound && checkErrorPatterns(patterns.notFound, errorMessage, errorName, statusCode, errorCode)) ||
      errorMessage.includes('not found') ||
      statusCode === 404
    ) {
      return ErrorCategory.PERMANENT_NOT_FOUND;
    }
  }
  
  // Generic checks for common patterns
  if (
    errorMessage.includes('validation failed') ||
    errorMessage.includes('invalid') ||
    errorMessage.includes('malformed') ||
    errorName.includes('ValidationError')
  ) {
    return ErrorCategory.PERMANENT_VALIDATION;
  }
  
  if (
    (statusCode && statusCode === 400) ||
    errorMessage.includes('bad request')
  ) {
    return ErrorCategory.PERMANENT_BAD_REQUEST;
  }
  
  if (
    errorMessage.includes('system error') ||
    errorMessage.includes('internal error') ||
    errorName.includes('SystemError')
  ) {
    return ErrorCategory.SYSTEM;
  }
  
  // Default to unknown
  return ErrorCategory.UNKNOWN;
}

/**
 * Helper to check if an error matches specific patterns
 */
function checkErrorPatterns(
  patterns: Array<{ status?: number, message?: string[], code?: string[] }>,
  errorMessage: string,
  errorName: string,
  statusCode?: number,
  errorCode?: string
): boolean {
  return patterns.some(pattern => {
    // Check status code if pattern includes one
    if (pattern.status !== undefined && statusCode !== pattern.status) {
      return false;
    }
    
    // Check error code if pattern includes one
    if (pattern.code && errorCode) {
      if (!pattern.code.some(code => errorCode.includes(code))) {
        return false;
      }
    }
    
    // Check message patterns if included
    if (pattern.message && pattern.message.length > 0) {
      const fullText = (errorMessage + ' ' + errorName).toLowerCase();
      return pattern.message.some(msgPattern => fullText.includes(msgPattern));
    }
    
    // If we got here and the pattern had a status or code that matched, it's a match
    return pattern.status !== undefined || (pattern.code !== undefined && errorCode !== undefined);
  });
}

/**
 * Create a properly categorized enhanced error
 */
export function createEnhancedError(
  error: Error | string,
  source: ErrorSource,
  category?: ErrorCategory,
  metadata: Record<string, any> = {}
): EnhancedError {
  // Allow creating from string
  const originalError = typeof error === 'string' ? new Error(error) : error;
  
  // Determine category if not provided
  const errorCategory = category || classifyError(originalError, source);
  
  // Create enhanced error
  const enhancedError = new Error(originalError.message) as EnhancedError;
  enhancedError.name = `${source}Error`;
  enhancedError.stack = originalError.stack;
  enhancedError.category = errorCategory;
  enhancedError.source = source;
  enhancedError.originalError = originalError;
  enhancedError.retriable = RETRIABLE_ERROR_CATEGORIES.includes(errorCategory);
  enhancedError.metadata = metadata;
  
  if ('statusCode' in originalError) {
    enhancedError.statusCode = (originalError as any).statusCode;
  }
  if ('status' in originalError) {
    enhancedError.statusCode = (originalError as any).status;
  }
  
  return enhancedError;
}

/**
 * Calculate delay for retry with exponential backoff and jitter
 */
export function calculateRetryDelay(
  attempt: number,
  policy: RetryPolicy
): number {
  const { baseDelayMs, maxDelayMs, jitterFactor } = policy;
  
  // Calculate base delay with exponential backoff
  const exponentialDelay = baseDelayMs * Math.pow(2, attempt);
  
  // Apply max delay cap
  const cappedDelay = Math.min(exponentialDelay, maxDelayMs);
  
  // Apply jitter to prevent thundering herd problem
  // This adds or subtracts a random percentage (jitterFactor) of the delay
  const jitterRange = cappedDelay * jitterFactor;
  const jitter = (Math.random() * 2 - 1) * jitterRange; // Range: -jitterRange to +jitterRange
  
  // Apply jitter and ensure minimum delay
  return Math.max(baseDelayMs, cappedDelay + jitter);
}

/**
 * Track errors for monitoring and analysis
 */
export async function trackError(
  error: Error | EnhancedError, 
  context: {
    entityType?: EntityType;
    entityId?: string;
    operationName: string;
    correlationId?: string;
  }
): Promise<void> {
  try {
    // Get Redis instance for tracking
    const redis = getRedis();
    
    // Create error tracking data
    const errorData = {
      message: error.message,
      name: error.name,
      timestamp: new Date().toISOString(),
      category: 'category' in error ? error.category : classifyError(error),
      source: 'source' in error ? error.source : ErrorSource.SYSTEM,
      entityType: context.entityType,
      entityId: context.entityId,
      operationName: context.operationName,
      correlationId: context.correlationId || ('correlationId' in error ? error.correlationId : undefined),
      statusCode: 'statusCode' in error ? error.statusCode : undefined,
      metadata: 'metadata' in error ? error.metadata : undefined,
      retriable: 'retriable' in error ? error.retriable : RETRIABLE_ERROR_CATEGORIES.includes(
        'category' in error ? error.category : classifyError(error)
      )
    };
    
    // Store in Redis for monitoring and alerting
    const errorKey = `error:${context.operationName}:${Date.now()}:${Math.random().toString(36).substring(2, 10)}`;
    await redis.set(errorKey, JSON.stringify(errorData), {
      ex: 86400 // Keep for 24 hours
    });
    
    // Increment error counter for monitoring
    const counterKey = `error_count:${context.operationName}:${errorData.category}`;
    await redis.incr(counterKey);
    await redis.expire(counterKey, 86400); // Expire after 24 hours
    
    // Also track the recent errors by entity if available
    if (context.entityType && context.entityId) {
      const entityErrorKey = `recent_errors:${context.entityType}:${context.entityId}`;
      await redis.lpush(entityErrorKey, JSON.stringify({
        message: error.message,
        category: errorData.category,
        timestamp: errorData.timestamp,
        operationName: context.operationName
      }));
      await redis.ltrim(entityErrorKey, 0, 9); // Keep only the 10 most recent
      await redis.expire(entityErrorKey, 86400); // Expire after 24 hours
    }
  } catch (trackingError) {
    // Just log tracking errors without throwing
    console.warn(`Failed to track error: ${trackingError.message}`);
  }
}

/**
 * Retry function with exponential backoff and monitoring
 */
export async function retryWithBackoff<T>(
  operation: () => Promise<T>,
  options: {
    name: string;
    source: ErrorSource;
    maxRetries?: number;
    baseDelayMs?: number;
    maxDelayMs?: number;
    jitterFactor?: number;
    onRetry?: (error: Error, attempt: number, delay: number) => void;
    entityType?: EntityType;
    entityId?: string;
    correlationId?: string;
    timeoutMs?: number;
  }
): Promise<T> {
  const {
    name,
    source,
    maxRetries = 3,
    baseDelayMs = 500,
    maxDelayMs = 10000,
    jitterFactor = 0.3,
    onRetry,
    entityType,
    entityId,
    correlationId,
    timeoutMs = 10000
  } = options;
  
  let attempt = 0;
  
  while (true) {
    try {
      // Add timeout to the operation
      const result = await Promise.race([
        operation(),
        new Promise<never>((_, reject) => {
          setTimeout(() => {
            reject(createEnhancedError(
              `Operation "${name}" timed out after ${timeoutMs}ms`,
              source,
              ErrorCategory.TRANSIENT_TIMEOUT,
              { timeoutMs }
            ));
          }, timeoutMs);
        })
      ]);
      
      return result;
    } catch (error) {
      // Enhance the error if needed
      const enhancedError = 'category' in error ? 
        error as EnhancedError : 
        createEnhancedError(error, source, undefined, { operationName: name });
      
      // Track the error
      await trackError(enhancedError, {
        entityType,
        entityId, 
        operationName: name,
        correlationId
      });
      
      attempt++;
      
      // Check if we've exceeded retries or if error is not retriable
      if (attempt > maxRetries || !enhancedError.retriable) {
        throw enhancedError;
      }
      
      // Calculate delay with exponential backoff
      const delay = calculateRetryDelay(
        attempt,
        {
          baseDelayMs,
          maxDelayMs,
          jitterFactor,
          maxRetries,
          timeoutMs
        }
      );
      
      // Call onRetry callback if provided
      if (onRetry) {
        onRetry(enhancedError, attempt, delay);
      }
      
      // Log retry attempt
      console.warn(
        `Retrying "${name}" after error (attempt ${attempt}/${maxRetries}, ` +
        `delay: ${delay}ms): ${enhancedError.message}`
      );
      
      // Wait before next retry
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

/**
 * Create a dead letter record with proper metadata
 */
export async function createDeadLetterRecord(
  error: Error | EnhancedError,
  context: {
    queueName: string;
    messageId: string;
    message: any;
    entityType?: EntityType;
    entityId?: string;
    operationName: string;
    correlationId?: string;
    attempts: number;
  }
): Promise<any> {
  // Enhance error if needed
  const enhancedError = 'category' in error ? 
    error as EnhancedError : 
    createEnhancedError(
      error,
      ErrorSource.QUEUE,
      undefined,
      { queueName: context.queueName }
    );
  
  // Add record to dead letter queue
  return {
    original_message: context.message,
    original_queue: context.queueName,
    original_message_id: context.messageId,
    error: {
      message: enhancedError.message,
      name: enhancedError.name,
      category: enhancedError.category,
      source: enhancedError.source,
      retriable: enhancedError.retriable,
      statusCode: enhancedError.statusCode,
      metadata: enhancedError.metadata
    },
    context: {
      entityType: context.entityType,
      entityId: context.entityId,
      operationName: context.operationName,
      correlationId: context.correlationId,
      attempts: context.attempts,
      deadLetteredAt: new Date().toISOString()
    }
  };
}

/**
 * Update processing state based on error and retry attempts
 */
export async function handleProcessingError(
  error: Error | EnhancedError,
  supabase: any,
  redis: any,
  context: {
    entityType: EntityType;
    entityId: string;
    operationName: string;
    correlationId?: string;
    attempts: number;
    maxRetries: number;
  }
): Promise<void> {
  // Enhance error if needed
  const enhancedError = 'category' in error ? 
    error as EnhancedError : 
    createEnhancedError(error, ErrorSource.WORKER);
  
  // Track the error for monitoring
  await trackError(enhancedError, {
    entityType: context.entityType,
    entityId: context.entityId,
    operationName: context.operationName,
    correlationId: context.correlationId
  });
  
  // Determine if we've exhausted retries
  const exhaustedRetries = context.attempts >= context.maxRetries;
  
  // Update processing state in database
  try {
    if (exhaustedRetries || !enhancedError.retriable) {
      // Move to dead letter state
      await supabase.from('processing_status')
        .update({
          state: ProcessingState.DEAD_LETTER,
          last_error: error.message,
          metadata: {
            error: {
              message: error.message,
              name: error.name,
              category: enhancedError.category,
              source: enhancedError.source,
              retriable: enhancedError.retriable
            },
            correlation_id: context.correlationId,
            dead_lettered_at: new Date().toISOString(),
            attempts: context.attempts
          },
          updated_at: new Date().toISOString(),
          last_processed_at: new Date().toISOString(),
          dead_lettered: true
        })
        .eq('entity_type', context.entityType)
        .eq('entity_id', context.entityId);
    } else {
      // Move to failed state (but retriable)
      await supabase.from('processing_status')
        .update({
          state: ProcessingState.FAILED,
          last_error: error.message,
          metadata: {
            error: {
              message: error.message,
              name: error.name,
              category: enhancedError.category,
              source: enhancedError.source,
              retriable: enhancedError.retriable
            },
            correlation_id: context.correlationId,
            failed_at: new Date().toISOString(),
            attempts: context.attempts
          },
          updated_at: new Date().toISOString(),
          last_processed_at: new Date().toISOString()
        })
        .eq('entity_type', context.entityType)
        .eq('entity_id', context.entityId);
    }
  } catch (updateError) {
    // Log but don't throw to prevent cascading errors
    console.error(`Failed to update processing state: ${updateError.message}`);
  }
  
  // Update Redis state for fast lookups (non-critical)
  try {
    const stateKey = `state:${context.entityType}:${context.entityId}`;
    await redis.set(stateKey, JSON.stringify({
      state: exhaustedRetries ? ProcessingState.DEAD_LETTER : ProcessingState.FAILED,
      timestamp: new Date().toISOString(),
      errorMessage: error.message,
      correlationId: context.correlationId
    }), {
      ex: 86400 // 24 hour TTL
    });
  } catch (redisError) {
    console.warn(`Failed to update Redis state: ${redisError.message}`);
  }
}
