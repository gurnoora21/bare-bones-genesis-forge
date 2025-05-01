
/**
 * Type definitions for Deno environment
 * This allows us to use Deno types in Node.js for TypeScript compatibility
 */

// Define EdgeRuntime for TypeScript compatibility
declare global {
  // Define EdgeRuntime interface
  interface EdgeRuntimeInterface {
    waitUntil: (promise: Promise<any>) => void;
  }
  
  // Declare EdgeRuntime as a variable without redeclaration
  // Using var instead of const to avoid block-scoped declaration issues
  var EdgeRuntime: EdgeRuntimeInterface;
  
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
