
import { z } from "https://deno.land/x/zod@v3.22.4/mod.ts";

// Base message schema that all queue messages must follow
export const BaseMessageSchema = z.object({
  id: z.string().optional(),
  createdAt: z.string().optional(),
  retries: z.number().optional(),
});

// Artist Discovery Queue
export const ArtistDiscoveryMessageSchema = BaseMessageSchema.extend({
  spotifyId: z.string().length(22).optional(),
  artistName: z.string(),
  genres: z.array(z.string()).optional(),
  popularity: z.number().min(0).max(100).optional(),
});

export type ArtistDiscoveryMessage = z.infer<typeof ArtistDiscoveryMessageSchema>;

// Album Discovery Queue
export const AlbumDiscoveryMessageSchema = BaseMessageSchema.extend({
  spotifyId: z.string().length(22),
  artistSpotifyId: z.string().length(22),
  albumName: z.string().optional(),
  releaseDate: z.string().optional(),
  albumType: z.enum(['album', 'single', 'compilation']).optional(),
  
  // Add artistId for better validation
  artistId: z.string().uuid().optional(),
  artist_id: z.string().uuid().optional(),
});

export type AlbumDiscoveryMessage = z.infer<typeof AlbumDiscoveryMessageSchema>;

// Track Discovery Queue
export const TrackDiscoveryMessageSchema = BaseMessageSchema.extend({
  // Support multiple field naming conventions seen in the system
  spotifyId: z.string().length(22).optional(),
  albumSpotifyId: z.string().length(22).optional(),
  artistSpotifyId: z.string().length(22).optional(),
  trackName: z.string().optional(),
  durationMs: z.number().optional(),
  trackNumber: z.number().optional(),
  discNumber: z.number().optional(),
  
  // Support for fields used in actual implementation
  albumId: z.string().optional(),
  artistId: z.string().optional(),
  albumName: z.string().optional(),
  offset: z.number().optional(),
});

export type TrackDiscoveryMessage = z.infer<typeof TrackDiscoveryMessageSchema>;

// Producer Identification Queue
export const ProducerIdentificationMessageSchema = BaseMessageSchema.extend({
  trackSpotifyId: z.string().length(22).optional(),
  trackId: z.string().optional(),
  trackName: z.string(),
  producerName: z.string().optional(),
  producerRole: z.string().optional(),
  confidence: z.number().min(0).max(1).optional(),
  artistId: z.string().optional(),
  albumId: z.string().optional(),
});

export type ProducerIdentificationMessage = z.infer<typeof ProducerIdentificationMessageSchema>;

// Social Enrichment Queue
export const SocialEnrichmentMessageSchema = BaseMessageSchema.extend({
  artistSpotifyId: z.string().length(22),
  socialPlatform: z.enum(['twitter', 'instagram', 'facebook', 'youtube']),
  socialHandle: z.string(),
  verified: z.boolean().optional(),
});

export type SocialEnrichmentMessage = z.infer<typeof SocialEnrichmentMessageSchema>;

// Validation helper functions
export function validateMessage<T extends z.ZodType>(
  schema: T,
  message: unknown
): z.infer<T> {
  try {
    return schema.parse(message);
  } catch (error) {
    if (error instanceof z.ZodError) {
      throw new Error(`Invalid message format: ${JSON.stringify(error.errors, null, 2)}`);
    }
    throw error;
  }
} 

// Helper function to normalize message structure from queue
export function normalizeQueueMessage(message: any): any {
  if (!message) return {};
  
  // Parse message body if it's a string
  let messageBody: any = {};
  
  if (typeof message === 'string') {
    try {
      messageBody = JSON.parse(message);
    } catch (e) {
      console.error('Failed to parse message as string:', e);
      return {};
    }
  } else if (typeof message.message === 'string') {
    try {
      messageBody = JSON.parse(message.message);
    } catch (e) {
      console.error('Failed to parse message.message as string:', e);
      messageBody = message.message || {};
    }
  } else if (message.message && typeof message.message === 'object') {
    messageBody = message.message;
  } else if (typeof message === 'object') {
    messageBody = message;
  }
  
  // Remove wrapper fields if they exist and no content inside
  if (messageBody.message && Object.keys(messageBody).length === 1) {
    return normalizeQueueMessage(messageBody.message);
  }
  
  return messageBody;
}
