
/**
 * Debug helper functions for consistent logging and debugging
 */

/**
 * Safely stringify objects for logging, handling circular references
 */
export function safeStringify(obj: any, indent: number = 2): string {
  try {
    const cache = new Set();
    return JSON.stringify(
      obj,
      (key, value) => {
        if (typeof value === 'object' && value !== null) {
          if (cache.has(value)) {
            return '[Circular]';
          }
          cache.add(value);
        }
        return value;
      },
      indent
    );
  } catch (error) {
    return `[Could not stringify: ${error.message}]`;
  }
}

/**
 * Log a debug message with consistent formatting
 */
export function logDebug(component: string, message: string, data?: any): void {
  console.log(`[DEBUG][${component}] ${message}${data ? ': ' + safeStringify(data) : ''}`);
}

/**
 * Log a warning message with consistent formatting
 */
export function logWarning(component: string, message: string, data?: any): void {
  console.warn(`[WARNING][${component}] ${message}${data ? ': ' + safeStringify(data) : ''}`);
}

/**
 * Log an error message with consistent formatting
 */
export function logError(component: string, message: string, error?: any): void {
  console.error(`[ERROR][${component}] ${message}${error ? ': ' + (error.stack || safeStringify(error)) : ''}`);
}
