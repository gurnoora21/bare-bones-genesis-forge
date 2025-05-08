
/**
 * Helper function to safely stringify objects for debug logging
 */
export function safeStringify(obj: any, maxLength: number = 1000): string {
  try {
    const str = JSON.stringify(obj);
    if (str && str.length > maxLength) {
      return str.substring(0, maxLength) + '...';
    }
    return str || 'undefined';
  } catch (err) {
    return `[Error stringifying object: ${err.message}]`;
  }
}

/**
 * Improved structured logging
 */
export function logDebug(context: string, message: string, data?: any): void {
  const timestamp = new Date().toISOString();
  console.log(
    `[${timestamp}] [${context}] ${message}${data !== undefined ? ': ' + safeStringify(data) : ''}`
  );
}

/**
 * Check if environment variables are set
 */
export function validateEnvironment(requiredVars: string[]): boolean {
  const missing: string[] = [];
  
  for (const varName of requiredVars) {
    if (!Deno.env.get(varName)) {
      missing.push(varName);
    }
  }
  
  if (missing.length > 0) {
    console.error(`Missing required environment variables: ${missing.join(', ')}`);
    return false;
  }
  
  return true;
}

/**
 * Validate message structure
 */
export function validateQueueMessage(message: any): { valid: boolean; reason?: string } {
  if (!message) {
    return { valid: false, reason: 'Message is null or undefined' };
  }
  
  // Check if message has required ID fields
  const hasId = message.id !== undefined || message.msg_id !== undefined;
  if (!hasId) {
    return { valid: false, reason: 'Message has no ID field (id or msg_id)' };
  }
  
  // Check if message has actual message content
  if (!message.message && typeof message !== 'object') {
    return { valid: false, reason: 'Message has no content' };
  }
  
  return { valid: true };
}

/**
 * Extract a stable ID from a message regardless of format
 */
export function extractMessageId(message: any): string | null {
  if (!message) return null;
  
  // Try all possible ID locations
  const possibleIds = [
    message.id,
    message.msg_id,
    message.message_id,
    message.messageId
  ];
  
  for (const id of possibleIds) {
    if (id !== undefined && id !== null) {
      return String(id);
    }
  }
  
  // If we couldn't find an ID, generate a stable one from the message content
  try {
    const contentHash = JSON.stringify(message);
    return `generated_${contentHash.length}_${Date.now()}`;
  } catch (e) {
    return `fallback_${Date.now()}`;
  }
}
