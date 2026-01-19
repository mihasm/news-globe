-- PostgreSQL initialization script for NewsGlobe events database
-- This script creates all necessary tables and indexes

-- Create normalized_items table
CREATE TABLE IF NOT EXISTS normalized_items (
    id SERIAL PRIMARY KEY,
    source VARCHAR(32) NOT NULL,
    source_id TEXT NOT NULL,
    collected_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP WITH TIME ZONE,
    title TEXT,
    text TEXT,
    url TEXT,
    media_urls TEXT,  -- JSON array of media URLs
    entities TEXT,    -- JSON structured data
    location_name TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    cluster_id TEXT,
    author TEXT
);

-- Create clusters table
CREATE TABLE IF NOT EXISTS clusters (
    cluster_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    item_count INTEGER NOT NULL DEFAULT 0,
    tags TEXT,        -- JSON array of descriptive tags
    title TEXT,
    summary TEXT,
    representative_lat DOUBLE PRECISION,
    representative_lon DOUBLE PRECISION,
    representative_location_name TEXT,
    first_seen_at TIMESTAMP WITH TIME ZONE,
    last_seen_at TIMESTAMP WITH TIME ZONE
);

-- Create indexes for normalized_items
CREATE INDEX IF NOT EXISTS idx_normalized_items_source ON normalized_items(source);
CREATE INDEX IF NOT EXISTS idx_normalized_items_source_id ON normalized_items(source_id);
CREATE INDEX IF NOT EXISTS idx_normalized_items_collected_at ON normalized_items(collected_at);
CREATE INDEX IF NOT EXISTS idx_normalized_items_published_at ON normalized_items(published_at);
CREATE INDEX IF NOT EXISTS idx_normalized_items_cluster_id ON normalized_items(cluster_id);
CREATE INDEX IF NOT EXISTS idx_normalized_items_source_source_id ON normalized_items(source, source_id);
CREATE INDEX IF NOT EXISTS idx_normalized_items_lat_lon ON normalized_items(lat, lon);

-- Create indexes for clusters
CREATE INDEX IF NOT EXISTS idx_clusters_created_at ON clusters(created_at);
CREATE INDEX IF NOT EXISTS idx_clusters_updated_at ON clusters(updated_at);
CREATE INDEX IF NOT EXISTS idx_clusters_item_count ON clusters(item_count);