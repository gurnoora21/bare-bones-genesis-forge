
# Music Producer Discovery Worker System

This document outlines the queue-based worker system that powers the music producer discovery platform.

## System Architecture

The system uses Supabase Queues (PGMQ) to manage asynchronous job processing across five specialized worker types:

1. **Artist Discovery** - Discovers and stores artist information from Spotify
2. **Album Discovery** - Retrieves albums for known artists
3. **Track Discovery** - Gets track information from albums
4. **Producer Identification** - Identifies producers from track credits and external data
5. **Social Enrichment** - Enhances producer profiles with social media information

Each worker type:
- Reads from its own dedicated message queue
- Processes messages in batches for efficiency
- Uses background processing via Edge Functions with `EdgeRuntime.waitUntil()`
- Implements proper error handling, retries, and monitoring
- Archives successfully processed messages

## Message Flow

```
[Artist Name/ID] → Artist Discovery → DB
                                    ↓
                    Album Discovery ← Queue
                                    ↓
                                    DB
                                    ↓
                    Track Discovery ← Queue
                                    ↓
                                    DB
                                    ↓
           Producer Identification ← Queue
                         ↓        ↓
                         DB      Social Enrichment ← Queue
                                                   ↓
                                                   DB
```

## Message Structure

Each queue expects messages with specific fields:

- **artist_discovery**
  ```json
  { 
    "artistId": "[optional-spotify-id]", 
    "artistName": "[optional-artist-name]" 
  }
  ```
  Note: At least one of `artistId` or `artistName` must be provided.

- **album_discovery**
  ```json
  { 
    "artistId": "[database-artist-id]", 
    "offset": 0 
  }
  ```

- **track_discovery**
  ```json
  { 
    "albumId": "[database-album-id]",
    "albumName": "[album-name]",
    "artistId": "[database-artist-id]",
    "offset": 0 
  }
  ```

- **producer_identification**
  ```json
  {
    "trackId": "[database-track-id]",
    "trackName": "[track-name]",
    "albumId": "[database-album-id]",
    "artistId": "[database-artist-id]"
  }
  ```

- **social_enrichment**
  ```json
  {
    "producerId": "[database-producer-id]",
    "producerName": "[producer-name]"
  }
  ```

## Worker Implementation

All workers inherit from the `BaseWorker` class, which provides common functionality:

- Batch processing of queue messages
- API rate limiting for external services
- Circuit breaker pattern for failing dependencies
- Exponential backoff for retries
- Caching to reduce duplicate API calls
- Monitoring and metrics collection

## Required Environment Variables

The system requires the following environment variables to be set in Supabase Secrets:

- `SPOTIFY_CLIENT_ID` - Spotify API client ID
- `SPOTIFY_CLIENT_SECRET` - Spotify API client secret
- `GENIUS_ACCESS_TOKEN` - Genius API token
- `INSTAGRAM_API_KEY` - API key for Instagram data provider

## Cron Schedule

Workers are scheduled to run at regular intervals:

- Artist Discovery: Every 2 minutes
- Album Discovery: Every 5 minutes
- Track Discovery: Every 5 minutes
- Producer Identification: Every 10 minutes
- Social Enrichment: Every hour

## Starting the Discovery Process

To manually start the discovery process for an artist, call the database function:

```sql
SELECT start_artist_discovery('Artist Name');
```

## Monitoring

Worker activity is tracked in two database tables:

1. **queue_metrics** - Records batch processing metrics including success/error counts
2. **worker_issues** - Logs specific errors and issues encountered by workers

## Resilience Features

The system implements several resilience patterns:

- **Idempotent Processing** - Messages can be safely processed multiple times
- **Visibility Timeouts** - Prevents multiple workers from processing the same message
- **Dead-Letter Handling** - Messages that fail processing are logged for inspection
- **Rate Limiting** - Respects API provider limits to avoid service disruption
- **Circuit Breaking** - Automatically disables failing dependencies
- **Batch Processing** - Efficiently processes messages in groups
- **Caching** - Reduces unnecessary API calls and improves performance

## Extending the System

To add new worker types:

1. Create a new queue using `SELECT pgmq.create('[queue_name]');`
2. Implement a worker class that extends `BaseWorker`
3. Deploy the worker as a Supabase Edge Function
4. Schedule the worker using `cron.schedule()`
