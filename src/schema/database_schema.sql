
-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Artists table
CREATE TABLE artists (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    spotify_id TEXT UNIQUE,
    followers INTEGER,
    popularity INTEGER,
    image_url TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for artists table
CREATE INDEX idx_artists_spotify_id ON artists(spotify_id);
CREATE INDEX idx_artists_name ON artists(name);
CREATE INDEX idx_artists_popularity ON artists(popularity);

-- Albums table
CREATE TABLE albums (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    artist_id UUID NOT NULL REFERENCES artists(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    spotify_id TEXT UNIQUE,
    release_date DATE,
    cover_url TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for albums table
CREATE INDEX idx_albums_artist_id ON albums(artist_id);
CREATE INDEX idx_albums_spotify_id ON albums(spotify_id);
CREATE INDEX idx_albums_release_date ON albums(release_date);

-- Tracks table
CREATE TABLE tracks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    album_id UUID NOT NULL REFERENCES albums(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    spotify_id TEXT UNIQUE,
    duration_ms INTEGER,
    popularity INTEGER,
    spotify_preview_url TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for tracks table
CREATE INDEX idx_tracks_album_id ON tracks(album_id);
CREATE INDEX idx_tracks_spotify_id ON tracks(spotify_id);
CREATE INDEX idx_tracks_name ON tracks(name);
CREATE INDEX idx_tracks_popularity ON tracks(popularity);

-- Normalized tracks table to handle different versions of the same song
CREATE TABLE normalized_tracks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    artist_id UUID NOT NULL REFERENCES artists(id) ON DELETE CASCADE,
    normalized_name TEXT NOT NULL,
    representative_track_id UUID REFERENCES tracks(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(artist_id, normalized_name)
);

-- Create indexes for normalized_tracks table
CREATE INDEX idx_normalized_tracks_artist_id ON normalized_tracks(artist_id);
CREATE INDEX idx_normalized_tracks_normalized_name ON normalized_tracks(normalized_name);
CREATE INDEX idx_normalized_tracks_representative_track_id ON normalized_tracks(representative_track_id);

-- Producers table
CREATE TABLE producers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    normalized_name TEXT NOT NULL UNIQUE,
    email TEXT,
    image_url TEXT,
    instagram_handle TEXT,
    instagram_bio TEXT,
    metadata JSONB,
    enriched_at TIMESTAMPTZ,
    enrichment_failed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for producers table
CREATE INDEX idx_producers_normalized_name ON producers(normalized_name);
CREATE INDEX idx_producers_instagram_handle ON producers(instagram_handle);
CREATE INDEX idx_producers_enriched_at ON producers(enriched_at);
CREATE INDEX idx_producers_enrichment_failed ON producers(enrichment_failed);

-- Junction table for tracks and producers (many-to-many)
CREATE TABLE track_producers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    track_id UUID NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
    producer_id UUID NOT NULL REFERENCES producers(id) ON DELETE CASCADE,
    confidence NUMERIC NOT NULL,
    source TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(track_id, producer_id)
);

-- Create indexes for track_producers table
CREATE INDEX idx_track_producers_track_id ON track_producers(track_id);
CREATE INDEX idx_track_producers_producer_id ON track_producers(producer_id);
CREATE INDEX idx_track_producers_confidence ON track_producers(confidence);
CREATE INDEX idx_track_producers_source ON track_producers(source);

-- Worker monitoring tables
CREATE TABLE worker_issues (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    worker_name TEXT NOT NULL,
    issue_type TEXT NOT NULL,
    message TEXT NOT NULL,
    details JSONB,
    resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for worker_issues table
CREATE INDEX idx_worker_issues_worker_name ON worker_issues(worker_name);
CREATE INDEX idx_worker_issues_issue_type ON worker_issues(issue_type);
CREATE INDEX idx_worker_issues_resolved ON worker_issues(resolved);
CREATE INDEX idx_worker_issues_created_at ON worker_issues(created_at);

-- Queue metrics table for monitoring
CREATE TABLE queue_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    queue_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    processed_count INTEGER,
    success_count INTEGER,
    error_count INTEGER,
    details JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for queue_metrics table
CREATE INDEX idx_queue_metrics_queue_name ON queue_metrics(queue_name);
CREATE INDEX idx_queue_metrics_operation ON queue_metrics(operation);
CREATE INDEX idx_queue_metrics_started_at ON queue_metrics(started_at);

-- Add trigger functions for automatic updated_at timestamps
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = NOW();
   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for all tables with updated_at column
CREATE TRIGGER update_artists_timestamp BEFORE UPDATE ON artists
FOR EACH ROW EXECUTE PROCEDURE update_timestamp();

CREATE TRIGGER update_albums_timestamp BEFORE UPDATE ON albums
FOR EACH ROW EXECUTE PROCEDURE update_timestamp();

CREATE TRIGGER update_tracks_timestamp BEFORE UPDATE ON tracks
FOR EACH ROW EXECUTE PROCEDURE update_timestamp();

CREATE TRIGGER update_normalized_tracks_timestamp BEFORE UPDATE ON normalized_tracks
FOR EACH ROW EXECUTE PROCEDURE update_timestamp();

CREATE TRIGGER update_producers_timestamp BEFORE UPDATE ON producers
FOR EACH ROW EXECUTE PROCEDURE update_timestamp();

CREATE TRIGGER update_worker_issues_timestamp BEFORE UPDATE ON worker_issues
FOR EACH ROW EXECUTE PROCEDURE update_timestamp();

-- Create views for analysis and reporting
CREATE VIEW producer_popularity AS
SELECT
    p.id,
    p.name,
    COUNT(DISTINCT tp.track_id) AS track_count,
    COUNT(DISTINCT a.id) AS artist_count,
    AVG(t.popularity) AS avg_track_popularity
FROM
    producers p
    JOIN track_producers tp ON p.id = tp.producer_id
    JOIN tracks t ON tp.track_id = t.id
    JOIN albums al ON t.album_id = al.id
    JOIN artists a ON al.artist_id = a.id
GROUP BY
    p.id, p.name;

-- Create functions for common operations
CREATE OR REPLACE FUNCTION get_producer_collaborations(producer_id UUID)
RETURNS TABLE (
    artist_id UUID,
    artist_name TEXT,
    track_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        a.id AS artist_id,
        a.name AS artist_name,
        COUNT(DISTINCT t.id) AS track_count
    FROM
        producers p
        JOIN track_producers tp ON p.id = tp.producer_id
        JOIN tracks t ON tp.track_id = t.id
        JOIN albums al ON t.album_id = al.id
        JOIN artists a ON al.artist_id = a.id
    WHERE
        p.id = producer_id
    GROUP BY
        a.id, a.name
    ORDER BY
        track_count DESC;
END;
$$ LANGUAGE plpgsql;

-- Create function to search producers by name with fuzzy matching
CREATE OR REPLACE FUNCTION search_producers(search_term TEXT)
RETURNS TABLE (
    id UUID,
    name TEXT,
    normalized_name TEXT,
    instagram_handle TEXT,
    track_count BIGINT,
    artist_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.id,
        p.name,
        p.normalized_name,
        p.instagram_handle,
        COUNT(DISTINCT tp.track_id) AS track_count,
        COUNT(DISTINCT a.id) AS artist_count
    FROM
        producers p
        LEFT JOIN track_producers tp ON p.id = tp.producer_id
        LEFT JOIN tracks t ON tp.track_id = t.id
        LEFT JOIN albums al ON t.album_id = al.id
        LEFT JOIN artists a ON al.artist_id = a.id
    WHERE
        p.name ILIKE '%' || search_term || '%' OR
        p.normalized_name ILIKE '%' || search_term || '%'
    GROUP BY
        p.id, p.name, p.normalized_name, p.instagram_handle
    ORDER BY
        track_count DESC;
END;
$$ LANGUAGE plpgsql;
