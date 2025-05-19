# Music Discovery Pipeline Cleanup

This project is a Supabase-based music discovery pipeline that links artists → albums → tracks → producers. It uses PGMQ queues with custom SQL functions to process messages asynchronously.

## Recent Changes

The pipeline has been cleaned up and simplified to improve reliability and maintainability:

### 1. Direct SQL Operations

- Created a new `pgmqBridge.ts` module that provides reliable queue operations using direct SQL
- Added fallback mechanisms to handle queue operation failures gracefully
- Simplified the queue reading and writing logic to be more consistent and reliable

### 2. Queue Helper Improvements

- Updated `queueHelper.ts` to use direct SQL operations with fallbacks
- Simplified the enqueue, deleteMessage, and sendToDLQ methods
- Removed unnecessary complexity and fallback layers

### 3. Worker Function Updates

- Updated `readQueue` and `sendToQueue` functions to use our simplified approach
- Ensured all worker functions (artistDiscovery, albumDiscovery, trackDiscovery, producerIdentification) use the improved queue operations

### 4. Database Support

- Added a migration to create the `raw_sql_query` function for direct SQL operations
- Ensured proper error handling and logging throughout the pipeline

## Pipeline Flow

The music discovery pipeline follows this flow:

1. **Artist Discovery**: Finds artists on Spotify and stores them in the database
2. **Album Discovery**: For each artist, retrieves their albums from Spotify
3. **Track Discovery**: For each album, retrieves the tracks
4. **Producer Identification**: For each track, identifies the producers using Genius API

Each step in the pipeline uses PGMQ queues to process messages asynchronously, with proper error handling and dead-letter queues for failed messages.

## Key Components

- **Queue Operations**: Simplified and reliable queue operations using direct SQL
- **Worker Functions**: Edge functions that process messages from the queues
- **Database Schema**: Tables for artists, albums, tracks, and producers with relationships
- **API Integration**: Connections to Spotify and Genius APIs for data retrieval

## Running the Pipeline

To start the discovery process, send a message to the `artist_discovery` queue with an artist name:

```json
{
  "artistName": "Artist Name"
}
```

The pipeline will automatically process the message and continue through all the steps, creating the relationships between artists, albums, tracks, and producers.
