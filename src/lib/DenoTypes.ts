
/**
 * Type definitions for Deno environment
 * This allows us to use Deno types in Node.js for TypeScript compatibility
 */

// Define EdgeRuntime for TypeScript compatibility
declare global {
  // Define EdgeRuntime
  const EdgeRuntime: {
    waitUntil: (promise: Promise<any>) => void;
  };
  
  // Define Deno namespace for TypeScript compatibility
  namespace Deno {
    const env: {
      get(key: string): string | undefined;
    };
    // Add serve function to Deno namespace
    function serve(handler: (req: Request) => Promise<Response>): void;
  }
}

export {};
