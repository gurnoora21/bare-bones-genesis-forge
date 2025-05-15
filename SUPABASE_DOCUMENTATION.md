
# Supabase Database Functions & Cron Jobs Documentation

This document provides an overview of all database functions, cron jobs, and key components of the music producer discovery system implemented in Supabase.

## Table of Contents
- [System Overview](#system-overview)
- [Database Functions](#database-functions)
  - [Queue Management Functions](#queue-management-functions)
  - [Artist Discovery Functions](#artist-discovery-functions)
  - [Album & Track Processing Functions](#album--track-processing-functions)
  - [Producer Functions](#producer-functions)
  - [State Management Functions](#state-management-functions)
  - [Advisory Lock Functions](#advisory-lock-functions)
  - [Monitoring and Diagnostics Functions](#monitoring-and-diagnostics-functions)
- [Cron Jobs](#cron-jobs)
- [Edge Functions](#edge-functions)
- [Database Schema](#database-schema)

## System Overview

This system is a music producer discovery pipeline built on Supabase that:

1. Discovers artists from Spotify
2. Retrieves their albums and tracks
3. Identifies music producers and writers
4. Enriches producer profiles with social media information
5. Makes this data searchable via an API

The system uses a queue-based architecture with Supabase Queues (PGMQ) to process messages asynchronously, ensuring reliability and fault tolerance.

## Database Functions

### Queue Management Functions

#### `pg_enqueue(queue_name TEXT, message_body JSONB)`
Enqueues a JSON message to the specified queue.

#### `pg_dequeue(queue_name TEXT, batch_size INT DEFAULT 5, visibility_timeout INT DEFAULT 60)`
Retrieves a batch of messages from the specified queue.

#### `pg_delete_message(queue_name TEXT, message_id TEXT)`
Deletes a message from the specified queue. Handles both UUID and numeric IDs.

#### `pg_release_message(queue_name TEXT, message_id UUID)`
Returns a message to the queue (making it available for processing again).

#### `reset_stuck_messages(queue_name TEXT, min_minutes_locked INT DEFAULT 10)`
Resets visibility timeout for messages that have been locked for too long.

#### `ensure_message_deleted(queue_name TEXT, message_id TEXT, max_attempts INT DEFAULT 3)`
Makes multiple attempts to delete a message from a queue, with exponential backoff.

#### `register_queue(p_queue_name TEXT, p_display_name TEXT, p_description TEXT, p_active BOOLEAN)`
Registers a queue in the queue registry table and creates it in PGMQ if it doesn't exist.

### Artist Discovery Functions

#### `start_artist_discovery(artist_name TEXT)`
Enqueues a job to discover an artist by name.

#### `start_bulk_artist_discovery(genre TEXT, min_popularity INT, limit_count INT)`
Starts discovery for multiple artists based on genre and popularity criteria.

### Album & Track Processing Functions

#### `start_album_discovery(artist_id UUID, offset_val INT DEFAULT 0)`
Enqueues a job to discover albums for a specific artist, with pagination support.

#### `process_artist_atomic(p_artist_data JSONB, p_operation_id TEXT, p_spotify_id TEXT)`
Atomically processes and stores artist data with idempotency controls.

#### `process_album_atomic(p_album_data JSONB, p_operation_id TEXT, p_spotify_id TEXT)`
Atomically processes and stores album data with idempotency controls.

#### `process_track_atomic(p_track_data JSONB, p_operation_id TEXT, p_spotify_id TEXT)`
Atomically processes and stores track data with idempotency controls.

### Producer Functions

#### `process_producer_atomic(p_producer_data JSONB, p_operation_id TEXT, p_normalized_name TEXT)`
Atomically processes and stores producer data with idempotency controls.

#### `search_producers(search_term TEXT)`
Searches for producers by name and returns counts of their tracks and artist collaborations.

#### `get_producer_collaborations(producer_id UUID)`
Returns a list of artists a producer has collaborated with and the track count.

### State Management Functions

#### `acquire_processing_lock(p_entity_type TEXT, p_entity_id TEXT, p_timeout_minutes INT DEFAULT 30, p_correlation_id TEXT DEFAULT NULL)`
Acquires a lock for processing an entity, handling concurrent access and timeouts.

#### `release_processing_lock(p_entity_type TEXT, p_entity_id TEXT)`
Releases a processing lock for an entity.

#### `update_lock_heartbeat(p_entity_type TEXT, p_entity_id TEXT, p_worker_id TEXT, p_correlation_id TEXT)`
Updates heartbeat for a lock to prevent it from timing out during long-running operations.

#### `claim_stale_lock(p_entity_type TEXT, p_entity_id TEXT, p_new_worker_id TEXT, p_correlation_id TEXT)`
Claims a lock that has become stale due to a failed worker.

#### `force_release_entity_lock(p_entity_type TEXT, p_entity_id TEXT)`
Forces release of an entity lock regardless of its state.

### Advisory Lock Functions

#### `pg_try_advisory_lock(p_key TEXT)`
Attempts to acquire a PostgreSQL advisory lock (non-blocking).

#### `pg_advisory_lock_timeout(p_key TEXT, p_timeout_ms INTEGER)`
Attempts to acquire an advisory lock with a specified timeout.

#### `pg_advisory_unlock(p_key TEXT)`
Releases an advisory lock.

#### `pg_advisory_lock_exists(p_key TEXT)`
Checks if an advisory lock exists.

#### `pg_force_advisory_unlock_all(p_key TEXT)`
Forces release of all advisory locks for a given key.

### Monitoring and Diagnostics Functions

#### `get_queue_table_name_safe(p_queue_name TEXT)`
Gets the correct table name for a queue, handling different naming patterns.

#### `list_stuck_messages(queue_name TEXT, min_minutes_locked INT DEFAULT 10)`
Lists messages that have been locked for longer than the specified time.

#### `diagnose_queue_tables(queue_name TEXT DEFAULT NULL)`
Provides diagnostic information about queue tables in the database.

#### `get_processing_stats()`
Returns statistics about entity processing states.

#### `get_worker_issue_stats()`
Returns statistics about worker issues grouped by worker name and issue type.

#### `get_queue_monitoring_data()`
Returns monitoring data for all queues.

#### `get_system_health()`
Returns a comprehensive health check of the entire system.

#### `cleanup_stale_entities(p_stale_threshold_minutes INT DEFAULT 60)`
Cleans up stale entities in both database and Redis.

#### `maintenance_clear_stale_entities(p_stale_threshold_minutes INT DEFAULT 60)`
Performs maintenance operations on stale entities.

#### `cross_schema_queue_op(p_operation TEXT, p_queue_name TEXT, p_message_id TEXT, p_params JSONB)`
Performs operations on queue tables across different schemas.

## Cron Jobs

The system uses several scheduled cron jobs to ensure reliable processing:

### Queue Processor Jobs

These jobs run on staggered schedules to process messages from various queues:

1. **General Queue Processor** (`cron-queue-processor-job`)
   - Schedule: Every 2 minutes
   - Invokes: `cronQueueProcessor` edge function
   - Purpose: Processes messages from all queues

2. **Artist Discovery** (`artist-discovery-queue-job`)
   - Schedule: Every 3 minutes
   - Processes: `artist_discovery` queue
   - Batch size: 15 messages

3. **Album Discovery** (`album-discovery-queue-job`)
   - Schedule: Every 4 minutes (offset by 1 minute)
   - Processes: `album_discovery` queue
   - Batch size: 15 messages

4. **Track Discovery** (`track-discovery-queue-job`)
   - Schedule: Every 4 minutes (offset by 2 minutes)
   - Processes: `track_discovery` queue
   - Batch size: 20 messages

5. **Producer Identification** (`producer-identification-queue-job`)
   - Schedule: Every 5 minutes
   - Processes: `producer_identification` queue
   - Batch size: 10 messages

6. **Social Enrichment** (`social-enrichment-queue-job`)
   - Schedule: Every 7 minutes
   - Processes: `social_enrichment` queue
   - Batch size: 10 messages

### Monitoring and Maintenance Jobs

1. **Queue Monitor** (`queue-monitor-job`, `auto-queue-monitor-job`, `queue-auto-fix-job`)
   - Schedule: Every 5 minutes
   - Invokes: `queueMonitor` edge function
   - Purpose: Monitors queues for stuck messages and fixes them

2. **Processing States Monitor** (`processing-states-monitor-job`)
   - Schedule: 10 minutes past each hour
   - Purpose: Resets processing states that have been stuck for more than 2 hours

3. **Stale Entity Cleanup** (`maintenance-clear-stale-entities`)
   - Schedule: Every 30 minutes
   - Purpose: Cleans up stale entity locks and states

4. **Stale Entity Cleanup (Edge Function)** (`cronCleanupStaleEntities`)
   - Schedule: Every hour
   - Purpose: Comprehensive cleanup across database and Redis

## Edge Functions

Key edge functions in the system include:

1. **artistDiscovery**: Discovers artists from Spotify
2. **albumDiscovery**: Retrieves album data for discovered artists
3. **trackDiscovery**: Retrieves track data from albums
4. **producerIdentification**: Identifies producers from track data
5. **socialEnrichment**: Enriches producer profiles with social data
6. **cronQueueProcessor**: Scheduled processor for all queues
7. **queueMonitor**: Monitors queue health and fixes issues
8. **clearQueueDeduplication**: Clears Redis deduplication keys
9. **checkPipeline**: Provides pipeline status information
10. **cronCleanupStaleEntities**: Scheduled cleanup for stale entities

## Database Schema

The main tables in the system are:

- **artists**: Stores artist information from Spotify
- **albums**: Stores album information linked to artists
- **tracks**: Stores track information linked to albums
- **producers**: Stores producer information
- **track_producers**: Many-to-many relationship between tracks and producers

Supporting tables:

- **processing_status**: Tracks processing state of entities
- **processing_locks**: Manages locks for concurrent processing
- **queue_registry**: Stores information about active queues
- **queue_metrics**: Records metrics about queue processing
- **monitoring_events**: Logs system events for auditing
- **worker_issues**: Records issues encountered by workers

---

This documentation provides a high-level overview of the functions and cron jobs in the system. For more detailed information, refer to the SQL definitions and edge function code in the repository.
