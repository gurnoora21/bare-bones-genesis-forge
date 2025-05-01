
/**
 * Type definitions for Deno environment
 * This allows us to use Deno types in Node.js for TypeScript compatibility
 */

// Define EdgeRuntime for TypeScript compatibility
declare global {
  interface Window {
    EdgeRuntime: {
      waitUntil: (promise: Promise<any>) => void;
    };
  }
  
  // Use interface merging instead of redeclaring the variable
  interface GlobalThis {
    EdgeRuntime: Window['EdgeRuntime'];
  }
  
  // Define Deno namespace for TypeScript compatibility
  namespace Deno {
    const env: {
      get(key: string): string | undefined;
    };
  }
}

export {};
