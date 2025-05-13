
/**
 * Structured Logger for consistent logging across the pipeline
 * Formats logs in a way that's easily queryable in Supabase Logs Explorer
 */

export enum LogLevel {
  DEBUG = 'debug',
  INFO = 'info',
  WARN = 'warn',
  ERROR = 'error',
  CRITICAL = 'critical'
}

export interface LogContext {
  entityType?: string;
  entityId?: string;
  correlationId?: string;
  operation?: string;
  workerId?: string;
  duration?: number;
  queueName?: string;
  messageId?: string;
  attempt?: number;
  [key: string]: any;
}

export class StructuredLogger {
  private context: LogContext;
  
  constructor(defaultContext: LogContext = {}) {
    this.context = defaultContext;
  }
  
  /**
   * Create a child logger with additional default context
   */
  withContext(additionalContext: LogContext): StructuredLogger {
    return new StructuredLogger({
      ...this.context,
      ...additionalContext
    });
  }
  
  /**
   * Log debugging information
   */
  debug(message: string, context: LogContext = {}): void {
    this.log(LogLevel.DEBUG, message, context);
  }
  
  /**
   * Log normal operational information
   */
  info(message: string, context: LogContext = {}): void {
    this.log(LogLevel.INFO, message, context);
  }
  
  /**
   * Log warnings that don't prevent normal operation
   */
  warn(message: string, context: LogContext = {}): void {
    this.log(LogLevel.WARN, message, context);
  }
  
  /**
   * Log errors that impact normal operation
   */
  error(message: string, error?: Error, context: LogContext = {}): void {
    const errorContext = error ? {
      errorMessage: error.message,
      errorName: error.name,
      errorStack: error.stack,
      ...context
    } : context;
    
    this.log(LogLevel.ERROR, message, errorContext);
  }
  
  /**
   * Log critical errors that require immediate attention
   */
  critical(message: string, error?: Error, context: LogContext = {}): void {
    const errorContext = error ? {
      errorMessage: error.message,
      errorName: error.name,
      errorStack: error.stack,
      ...context
    } : context;
    
    this.log(LogLevel.CRITICAL, message, errorContext);
  }
  
  /**
   * Log with timing information
   */
  withTiming<T>(operation: string, fn: () => Promise<T>, context: LogContext = {}): Promise<T> {
    const start = Date.now();
    
    return fn()
      .then(result => {
        const duration = Date.now() - start;
        this.info(`${operation} completed`, { 
          operation, 
          duration,
          success: true,
          ...context
        });
        return result;
      })
      .catch(error => {
        const duration = Date.now() - start;
        this.error(`${operation} failed`, error, {
          operation,
          duration,
          success: false,
          ...context
        });
        throw error;
      });
  }
  
  /**
   * Core logging method that produces structured output
   */
  private log(level: LogLevel, message: string, context: LogContext = {}): void {
    // Construct the structured log entry
    const logEntry = {
      timestamp: new Date().toISOString(),
      level,
      message,
      ...this.context,
      ...context
    };
    
    // Output to the appropriate console method based on log level
    switch (level) {
      case LogLevel.DEBUG:
        console.debug(JSON.stringify(logEntry));
        break;
      case LogLevel.INFO:
        console.info(JSON.stringify(logEntry));
        break;
      case LogLevel.WARN:
        console.warn(JSON.stringify(logEntry));
        break;
      case LogLevel.ERROR:
      case LogLevel.CRITICAL:
        console.error(JSON.stringify(logEntry));
        break;
      default:
        console.log(JSON.stringify(logEntry));
    }
  }
}

// Create a default logger instance
export const logger = new StructuredLogger();
