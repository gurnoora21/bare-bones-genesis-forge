# Music Discovery Pipeline

A Supabase-based music discovery pipeline that links artists → albums → tracks → producers using PGMQ queues with custom SQL functions to process messages asynchronously.

## Overview

This system uses a queue-based architecture to discover and link music data:

1. **Artist Discovery**: Finds artist information and queues album discovery
2. **Album Discovery**: Processes albums for each artist and queues track discovery
3. **Track Discovery**: Processes tracks for each album and queues producer identification
4. **Producer Identification**: Identifies producers for each track and enriches their profiles

## Recent Cleanup

The pipeline code has been cleaned up and simplified to improve maintainability and reliability. Key changes include:

- Simplified queue reading and writing logic
- Removed unnecessary fallback mechanisms
- Consolidated related functionality
- Improved error handling

For detailed information about the changes, see [CLEANUP_SUMMARY.md](./CLEANUP_SUMMARY.md).

## Testing the Pipeline

A test script is provided to verify that the pipeline works correctly. The script:

1. Starts artist discovery for a test artist
2. Monitors the progress through each queue
3. Verifies that data flows correctly through artist → album → track → producer

### Prerequisites

- Node.js 16+
- Supabase project with the pipeline deployed

### Running the Test

1. Set the required environment variables:

```bash
export SUPABASE_URL=https://your-project.supabase.co
export SUPABASE_ANON_KEY=your-anon-key
```

2. Install dependencies:

```bash
npm install @supabase/supabase-js
```

3. Run the test script:

```bash
node test_pipeline.js
```

The script will output detailed information about the pipeline's progress and verify that data is correctly flowing through the system.

## Project Structure

- `supabase/functions/`: Edge functions for processing queue messages
  - `artistDiscovery/`: Processes artist discovery messages
  - `albumDiscovery/`: Processes album discovery messages
  - `trackDiscovery/`: Processes track discovery messages
  - `producerIdentification/`: Processes producer identification messages
  - `_shared/`: Shared utilities and helpers
    - `pgmqBridge.ts`: Simplified queue operations
    - `queueHelper.ts`: Helper functions for queue operations
- `supabase/migrations/`: SQL migrations for database setup
- `test_pipeline.js`: Script to test the pipeline end-to-end
- `CLEANUP_SUMMARY.md`: Detailed summary of cleanup changes

## Database Schema

The database includes the following main tables:

- `artists`: Artist information
- `albums`: Album information linked to artists
- `tracks`: Track information linked to albums
- `producers`: Producer information
- `track_producers`: Junction table linking tracks to producers

Queue tables are managed by PGMQ and include:

- `pgmq.q_artist_discovery`: Queue for artist discovery messages
- `pgmq.q_album_discovery`: Queue for album discovery messages
- `pgmq.q_track_discovery`: Queue for track discovery messages
- `pgmq.q_producer_identification`: Queue for producer identification messages
