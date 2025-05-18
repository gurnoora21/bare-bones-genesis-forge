
export interface ProcessingResult {
  processed: number;
  errors: number;
  duplicates?: number;
  skipped?: number;
  processingTimeMs?: number;
}
