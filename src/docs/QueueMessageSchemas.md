# Queue Message Schemas

This document outlines the standardized message formats for all queues in the system.

## Base Message Schema

All queue messages extend from this base schema:

```typescript
{
  id?: string;        // Optional message ID
  createdAt?: string; // Optional creation timestamp
  retries?: number;   // Optional retry count
}
```

## Queue-Specific Schemas

### 1. Artist Discovery Queue

**Queue Name:** `artist_discovery`

**Schema:**
```typescript
{
  spotifyId: string;      // 22-character Spotify ID
  artistName: string;     // Artist name
  genres?: string[];      // Optional array of genres
  popularity?: number;    // Optional popularity score (0-100)
}
```

### 2. Album Discovery Queue

**Queue Name:** `album_discovery`

**Schema:**
```typescript
{
  spotifyId: string;      // 22-character Spotify ID
  artistSpotifyId: string; // 22-character Spotify ID of the artist
  albumName: string;      // Album name
  releaseDate?: string;   // Optional release date
  albumType?: 'album' | 'single' | 'compilation'; // Optional album type
}
```

### 3. Track Discovery Queue

**Queue Name:** `track_discovery`

**Schema:**
```typescript
{
  spotifyId: string;      // 22-character Spotify ID
  albumSpotifyId: string; // 22-character Spotify ID of the album
  artistSpotifyId: string; // 22-character Spotify ID of the artist
  trackName: string;      // Track name
  durationMs?: number;    // Optional duration in milliseconds
  trackNumber?: number;   // Optional track number
  discNumber?: number;    // Optional disc number
}
```

### 4. Producer Identification Queue

**Queue Name:** `producer_identification`

**Schema:**
```typescript
{
  trackSpotifyId: string; // 22-character Spotify ID of the track
  producerName: string;   // Producer's name
  producerRole: string;   // Producer's role
  confidence?: number;    // Optional confidence score (0-1)
}
```

### 5. Social Enrichment Queue

**Queue Name:** `social_enrichment`

**Schema:**
```typescript
{
  artistSpotifyId: string; // 22-character Spotify ID of the artist
  socialPlatform: 'twitter' | 'instagram' | 'facebook' | 'youtube';
  socialHandle: string;   // Social media handle
  verified?: boolean;     // Optional verification status
}
```

## Validation

All messages are validated using Zod schemas. The validation ensures:

1. Required fields are present
2. Field types are correct
3. String lengths match requirements (e.g., Spotify IDs are 22 characters)
4. Enums contain valid values
5. Numbers are within valid ranges

## Error Handling

When a message fails validation:
1. The error is logged with details about the validation failure
2. The message is sent to the Dead Letter Queue (DLQ)
3. The error includes the specific validation errors for debugging

## Usage Example

```typescript
import { validateMessage, ArtistDiscoveryMessageSchema } from '../_shared/types/queueMessages';

// In your worker:
const message = await dequeueMessage();
try {
  const validatedMessage = validateMessage(ArtistDiscoveryMessageSchema, message);
  // Process the validated message
} catch (error) {
  // Handle validation error
  await sendToDLQ(message, error.message);
}
``` 