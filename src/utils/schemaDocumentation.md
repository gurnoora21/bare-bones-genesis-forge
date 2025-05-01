
# Music Producer Discovery System - Database Schema Documentation

This document provides an overview of the database schema for the music producer discovery system.

## Core Tables

### Artists
Stores information about artists from Spotify.
- `id`: Primary key
- `name`: Artist name
- `spotify_id`: Unique identifier from Spotify API
- `followers`: Number of followers on Spotify
- `popularity`: Popularity score from Spotify (0-100)
- `image_url`: URL to artist's profile image
- `metadata`: Additional data in JSON format
- `created_at` & `updated_at`: Timestamp fields

### Albums
Stores album information related to artists.
- `id`: Primary key
- `artist_id`: Foreign key to artists table
- `name`: Album name
- `spotify_id`: Unique identifier from Spotify API
- `release_date`: Album release date
- `cover_url`: URL to album cover image
- `metadata`: Additional data in JSON format
- `created_at` & `updated_at`: Timestamp fields

### Tracks
Stores track information related to albums.
- `id`: Primary key
- `album_id`: Foreign key to albums table
- `name`: Track name
- `spotify_id`: Unique identifier from Spotify API
- `duration_ms`: Track length in milliseconds
- `popularity`: Popularity score from Spotify (0-100)
- `spotify_preview_url`: URL for 30-second preview
- `metadata`: Additional data in JSON format
- `created_at` & `updated_at`: Timestamp fields

### Normalized Tracks
Handles different versions of the same track (e.g., remixes, radio edits).
- `id`: Primary key
- `artist_id`: Foreign key to artists table
- `normalized_name`: Standardized track name for grouping
- `representative_track_id`: Foreign key to tracks table (main version)
- `created_at` & `updated_at`: Timestamp fields

### Producers
Stores information about music producers.
- `id`: Primary key
- `name`: Producer name
- `normalized_name`: Standardized producer name for matching
- `email`: Contact email (if available)
- `image_url`: URL to producer's profile image
- `instagram_handle`: Instagram username
- `instagram_bio`: Instagram biography
- `metadata`: Additional data in JSON format
- `enriched_at`: Timestamp of last social media enrichment
- `enrichment_failed`: Flag for failed enrichment attempts
- `created_at` & `updated_at`: Timestamp fields

### Track Producers
Junction table establishing many-to-many relationships between tracks and producers.
- `id`: Primary key
- `track_id`: Foreign key to tracks table
- `producer_id`: Foreign key to producers table
- `confidence`: Confidence score for the association (0-1)
- `source`: Source of the producer credit information
- `created_at`: Timestamp field

## Monitoring Tables

### Worker Issues
Logs issues encountered by background workers.
- `id`: Primary key
- `worker_name`: Name of the worker process
- `issue_type`: Category of the issue
- `message`: Description of the issue
- `details`: Additional details in JSON format
- `resolved`: Flag indicating if the issue is resolved
- `created_at` & `updated_at`: Timestamp fields

### Queue Metrics
Tracks performance metrics for queue processing.
- `id`: Primary key
- `queue_name`: Name of the processed queue
- `operation`: Type of operation performed
- `started_at`: When processing began
- `finished_at`: When processing completed
- `processed_count`: Number of items processed
- `success_count`: Number of successful operations
- `error_count`: Number of failed operations
- `details`: Additional metrics in JSON format
- `created_at`: Timestamp field

## Database Views

### Producer Popularity
Aggregates data to show producer popularity metrics.
- Producer ID and name
- Track count (total tracks produced)
- Artist count (distinct artists worked with)
- Average track popularity

## Database Functions

### get_producer_collaborations
Returns a list of artists a producer has worked with, including track count.

### search_producers
Performs fuzzy search on producer names and returns relevant information.

## Indexes

Indexes have been created for:
- All foreign keys
- Frequently queried columns
- Text columns used in search operations
- Timestamp columns used for sorting or filtering

## Triggers

Automatic timestamp updates are implemented for all tables with `updated_at` columns.
