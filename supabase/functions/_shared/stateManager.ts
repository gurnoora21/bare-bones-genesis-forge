
/**
 * State Manager for database-backed processing state
 * Provides formal state machine transitions for entity processing
 */

// Processing state constants
export enum ProcessingState {
  PENDING = 'PENDING',
  IN_PROGRESS = 'IN_PROGRESS',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED'
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

// State check options
export interface StateCheckOptions {
  useRedisFallback?: boolean;
  timeout?: number;
}

// Lock acquisition options
export interface LockOptions {
  timeoutMinutes?: number;
  allowRetry?: boolean;
  correlationId?: string;
  retries?: number;
}

// Result of a state transition operation
export interface StateTransitionResult {
  success: boolean;
  previousState?: ProcessingState;
  newState?: ProcessingState;
  error?: string;
  source?: 'database' | 'redis' | 'memory';
}
