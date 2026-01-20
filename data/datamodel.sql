-- ========================================
-- MovieLens Data Model - PostgreSQL
-- ========================================
-- Simplified schema using arrays and composite keys
-- No views, minimal surrogate keys for efficiency

-- Install required extensions
CREATE EXTENSION IF NOT EXISTS vector;  -- pgvector for embeddings
CREATE EXTENSION IF NOT EXISTS pg_search;  -- ParadeDB for BM25 search

-- Drop existing tables if they exist
DROP TABLE IF EXISTS tags CASCADE;
DROP TABLE IF EXISTS ratings CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS movies CASCADE;

-- Movies table with genre array
CREATE TABLE movies (
    movie_id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    year SMALLINT,
    genres TEXT[],                    -- PostgreSQL array of genres
    imdb_id VARCHAR(20),
    tmdb_id INTEGER,
    content_embedding vector(384),    -- Movie content vector (384-dim from all-MiniLM-L12-v2)
    created_at TIMESTAMP DEFAULT NOW()
);

-- Users table
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,      -- MovieLens user IDs
    embedding vector(384),           -- User preference vector (384-dim)
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Ratings table with composite key (no surrogate key)
CREATE TABLE ratings (
    user_id INTEGER REFERENCES users(user_id),
    movie_id INTEGER REFERENCES movies(movie_id),
    rating DECIMAL(2,1) NOT NULL CHECK (rating >= 0.5 AND rating <= 5.0),
    rating_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, movie_id)  -- One rating per user per movie
);

-- Tags table with composite key (no surrogate key)
CREATE TABLE tags (
    user_id INTEGER REFERENCES users(user_id),
    movie_id INTEGER REFERENCES movies(movie_id),
    tag VARCHAR(200) NOT NULL,
    tag_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, movie_id, tag)  -- Prevent duplicate tags
);

-- ========================================
-- Performance Indexes
-- ========================================

-- Movie indexes
CREATE INDEX idx_movies_title ON movies(title);
CREATE INDEX idx_movies_year ON movies(year);
CREATE INDEX idx_movies_genres ON movies USING GIN(genres);  -- GIN for array searches

-- Ratings indexes
CREATE INDEX idx_ratings_user_id ON ratings(user_id);
CREATE INDEX idx_ratings_movie_id ON ratings(movie_id);
CREATE INDEX idx_ratings_timestamp ON ratings(rating_timestamp);

-- Tags indexes
CREATE INDEX idx_tags_user_id ON tags(user_id);
CREATE INDEX idx_tags_movie_id ON tags(movie_id);
CREATE INDEX idx_tags_tag ON tags(tag);

-- ========================================
-- Sample Queries for Array Operations
-- ========================================

/*
-- Find all Action movies
SELECT title, year FROM movies WHERE 'Action' = ANY(genres);

-- Find movies with multiple genres
SELECT title, array_length(genres, 1) as genre_count
FROM movies
WHERE array_length(genres, 1) > 3;

-- Count movies by genre
SELECT unnest(genres) as genre, COUNT(*) as count
FROM movies
GROUP BY genre
ORDER BY count DESC;

-- Find Comedy-Drama movies
SELECT title, year
FROM movies
WHERE 'Comedy' = ANY(genres) AND 'Drama' = ANY(genres);

-- Get top-rated movies with at least 50 ratings
SELECT m.title, COUNT(r.rating) as rating_count, AVG(r.rating) as avg_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id, m.title
HAVING COUNT(r.rating) >= 50
ORDER BY avg_rating DESC
LIMIT 10;
*/

-- ========================================
-- Schema Statistics
-- ========================================

/*
Expected data size:
- Movies: ~9,742 rows (including genre arrays)
- Users: 610 rows
- Ratings: 100,836 rows
- Tags: 3,683 rows

Estimated storage: ~50-100MB total
Index overhead: ~20-30MB

Performance characteristics:
- GIN index on genres: O(log n) for array searches
- Composite primary keys: Prevent duplicates, fast lookups
- No surrogate keys: Better storage efficiency
*/

-- ========================================
-- Data Loading Strategy
-- ========================================

/*
1. Process movies.csv - extract year, convert genres to array format
2. Load unique users from ratings.csv
3. Load ratings.csv with timestamp conversion
4. Load tags.csv with timestamp conversion
5. Update movies with external links from links.csv

Genre array format for CSV:
{}                     -- Empty array for "(no genres listed)"
{"Action","Comedy"}    -- Multiple genres
{"Drama"}              -- Single genre

Timestamp conversion: Unix epoch → PostgreSQL TIMESTAMP
*/