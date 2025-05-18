import { z } from "zod";

// Base message schema that all queue messages must follow
export const BaseMessageSchema = z.object({
  id: z.string().optional(),
  createdAt: z.string().optional(),
  retries: z.number().optional(),
});

// Artist Discovery Queue
export const ArtistDiscoveryMessageSchema = BaseMessageSchema.extend({
  spotifyId: z.string().length(22),
  artistName: z.string(),
  genres: z.array(z.string()).optional(),
  popularity: z.number().min(0).max(100).optional(),
});

export type ArtistDiscoveryMessage = z.infer<typeof ArtistDiscoveryMessageSchema>;

// Album Discovery Queue
export const AlbumDiscoveryMessageSchema = BaseMessageSchema.extend({
  spotifyId: z.string().length(22),
  artistSpotifyId: z.string().length(22),
  albumName: z.string(),
  releaseDate: z.string().optional(),
  albumType: z.enum(['album', 'single', 'compilation']).optional(),
});

export type AlbumDiscoveryMessage = z.infer<typeof AlbumDiscoveryMessageSchema>;

// Track Discovery Queue
export const TrackDiscoveryMessageSchema = BaseMessageSchema.extend({
  spotifyId: z.string().length(22),
  albumSpotifyId: z.string().length(22),
  artistSpotifyId: z.string().length(22),
  trackName: z.string(),
  durationMs: z.number().optional(),
  trackNumber: z.number().optional(),
  discNumber: z.number().optional(),
});

export type TrackDiscoveryMessage = z.infer<typeof TrackDiscoveryMessageSchema>;

// Producer Identification Queue
export const ProducerIdentificationMessageSchema = BaseMessageSchema.extend({
  trackSpotifyId: z.string().length(22),
  producerName: z.string(),
  producerRole: z.string(),
  confidence: z.number().min(0).max(1).optional(),
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
      throw new Error(`Invalid message format: ${error.message}`);
    }
    throw error;
  }
} 