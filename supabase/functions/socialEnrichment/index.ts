/**
 * Social Enrichment Edge Function
 * 
 * This function has been updated to use the idempotent worker pattern
 * for safe retries and proper transaction management.
 * 
 * The implementation is in idempotentSocialEnrichment.ts, which is imported here.
 * This file serves as the entry point for the edge function.
 */

// Import the idempotent worker implementation
// This automatically sets up the serve handler
import { socialEnrichmentWorker } from "./idempotentSocialEnrichment.ts";

// No additional code needed here as the implementation is in idempotentSocialEnrichment.ts
// The imported file sets up the HTTP handler with serve()
