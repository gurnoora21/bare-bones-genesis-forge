
/**
 * State Manager for database-backed processing state
 * Provides formal state machine transitions for entity processing
 */

// Processing state constants
export enum ProcessingState {
  PENDING = 'PENDING',
  IN_PROGRESS = 'IN_PROGRESS',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
  DEAD_LETTER = 'DEAD_LETTER'  // New state for dead-letter messages
}

// Error classification for better retry handling
export enum ErrorCategory {
  TRANSIENT = 'TRANSIENT',    // Temporary errors (network, timeouts) - can retry
  PERMANENT = 'PERMANENT',    // Permanent errors (bad data) - no retry
  UNKNOWN = 'UNKNOWN'         // Default category
}

// Entity types for state tracking
export enum EntityType {
  ARTIST = 'ARTIST',
  ALBUM = 'ALBUM',
  TRACK = 'TRACK',
  PRODUCER = 'PRODUCER',
  GENERIC = 'GENERIC'
}

// Environment detection for TTL values
export enum Environment {
  DEVELOPMENT = 'development',
  STAGING = 'staging', 
  PRODUCTION = 'production'
}

// TTL Settings by environment (in seconds)
const TTL_SETTINGS = {
  [Environment.DEVELOPMENT]: 10 * 60,     // 10 minutes for development
  [Environment.STAGING]: 60 * 60,         // 60 minutes for staging
  [Environment.PRODUCTION]: 24 * 60 * 60  // 24 hours for production
};

// Valid state transitions map
const VALID_STATE_TRANSITIONS = {
  [ProcessingState.PENDING]: [ProcessingState.IN_PROGRESS, ProcessingState.FAILED],
  [ProcessingState.IN_PROGRESS]: [ProcessingState.COMPLETED, ProcessingState.FAILED, ProcessingState.PENDING, ProcessingState.DEAD_LETTER],
  [ProcessingState.COMPLETED]: [ProcessingState.PENDING], // Allow reset to PENDING
  [ProcessingState.FAILED]: [ProcessingState.PENDING, ProcessingState.DEAD_LETTER],
  [ProcessingState.DEAD_LETTER]: [ProcessingState.PENDING] // Allow retrying from dead-letter
};

/**
 * Validates if a state transition is allowed
 */
export function isValidStateTransition(
  currentState: ProcessingState | null, 
  newState: ProcessingState
): boolean {
  // If no current state (new entity), any new state is valid
  if (currentState === null) {
    return true;
  }
  
  // Check if transition is allowed
  const allowedTransitions = VALID_STATE_TRANSITIONS[currentState];
  return allowedTransitions?.includes(newState) || false;
}

/**
 * Detects the current environment based on configuration
 */
export function detectEnvironment(): Environment {
  const env = Deno.env.get("ENV") || Deno.env.get("ENVIRONMENT") || "development";
  
  if (env.toLowerCase().includes('prod')) {
    return Environment.PRODUCTION;
  } else if (env.toLowerCase().includes('stag') || env.toLowerCase().includes('test')) {
    return Environment.STAGING;
  } else {
    return Environment.DEVELOPMENT;
  }
}

/**
 * Gets the appropriate TTL based on the environment
 */
export function getEnvironmentTTL(): number {
  const environment = detectEnvironment();
  return TTL_SETTINGS[environment] || 3600; // Default to 1 hour
}

/**
 * Classifies an error into a category for retry decisions
 */
export function classifyError(error: Error): ErrorCategory {
  if (!error) return ErrorCategory.UNKNOWN;
  
  const errorMessage = error.message || '';
  const errorName = error.name || '';
  
  // Network or service availability issues - should retry
  if (
    errorMessage.includes('ECONNREFUSED') || 
    errorMessage.includes('ETIMEDOUT') || 
    errorMessage.includes('ENOTFOUND') ||
    errorMessage.includes('timeout') ||
    errorMessage.includes('rate limit') ||
    errorMessage.includes('too many requests') ||
    errorMessage.includes('service unavailable') ||
    errorMessage.includes('internal server error') ||
    errorName.includes('TimeoutError') ||
    errorName.includes('NetworkError') ||
    errorName.includes('ConnectionError')
  ) {
    return ErrorCategory.TRANSIENT;
  }
  
  // Data validation or business logic errors - no retry
  if (
    errorMessage.includes('validation failed') ||
    errorMessage.includes('not found') ||
    errorMessage.includes('invalid') ||
    errorMessage.includes('malformed') ||
    errorMessage.includes('forbidden') ||
    errorMessage.includes('unauthorized') ||
    errorName.includes('ValidationError') ||
    errorName.includes('NotFoundError')
  ) {
    return ErrorCategory.PERMANENT;
  }
  
  // Default to unknown
  return ErrorCategory.UNKNOWN;
}

// State check options
export interface StateCheckOptions {
  useRedisFallback?: boolean;
  timeout?: number;
  strictValidation?: boolean;
}

// Lock acquisition options
export interface LockOptions {
  timeoutMinutes?: number;
  allowRetry?: boolean;
  correlationId?: string;
  retries?: number;
  heartbeatIntervalSeconds?: number;
}

// Result of a state transition operation
export interface StateTransitionResult {
  success: boolean;
  previousState?: ProcessingState;
  newState?: ProcessingState;
  error?: string;
  source?: 'database' | 'redis' | 'memory';
  validationDetails?: {
    isValid: boolean;
    reason?: string;
  };
}

// Heartbeat details for lock management
export interface HeartbeatDetails {
  entityType: string;
  entityId: string;
  workerId: string;
  correlationId?: string;
  lastHeartbeat: Date;
  lockAcquiredAt: Date;
}

/**
 * Creates a unique correlation ID for tracking operations
 */
export function generateCorrelationId(prefix: string = 'corr'): string {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
}
