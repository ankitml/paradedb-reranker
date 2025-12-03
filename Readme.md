### Ingestion
source ~/.venv/bin/activate
export PGPASSWORD="your_password"
python ingest_data.py --data-dir sample_data --batch-size 10000 \
  --db-host localhost --db-port 5432 --db-user postgres

### Reading
kubectl port-forward svc/paradedb-rw 5433:5432 -n ankit31-paradedb

PSQL Connection:
PGPASSWORD="wije1LG3VgSo5npDK3fcGpdQEu6OZJ7Cz1mjUUwSn2mgNPMjowikJm2cCYHOvLS8" psql -h localhost -p 5433 -U postgres -d postgres

Or in separate steps:
export PGPASSWORD="wije1LG3VgSo5npDK3fcGpdQEu6OZJ7Cz1mjUUwSn2mgNPMjowikJm2cCYHOvLS8"
psql -h localhost -p 5433 -U postgres -d postgres

Quick test query:
SELECT table_name, COUNT(*) FROM (
SELECT 'movies' as table_name, COUNT(*) FROM movies UNION ALL
SELECT 'users', COUNT(*) FROM users UNION ALL
SELECT 'ratings', COUNT(*) FROM ratings UNION ALL
SELECT 'tags', COUNT(*) FROM tags
) as counts ORDER BY table_name;

### ParadeDB Full-Text Search & Rating Integration
SELECT paradedb.create_bm25(
    index_name => 'movies_search_idx',
    table_name => 'movies',
    key_field => 'movie_id',
    text_fields => '{title}',
    numeric_fields => '{year,imdb_id,tmdb_id}',
    array_fields => '{genres}'
  );

  🎯 Search Query Examples:

  Basic title search:
  SELECT movie_id, title, year, genres
  FROM movies
  WHERE movies @@@ 'dark knight';

  Genre filtering:
  SELECT movie_id, title, year, genres
  FROM movies
  WHERE movies @@@ 'genres:Action AND genres:Thriller';

  Year range with title:
  SELECT movie_id, title, year, genres
  FROM movies
  WHERE movies @@@ 'title:matrix AND year:[2000 TO 2020]';

  Combined search:
  SELECT movie_id, title, year, genres
  FROM movies
  WHERE movies @@@ '(title:lord OR title:king) AND genres:Fantasy';

#### Create ParadeDB BM25 Index with Ratings

First, ensure ParadeDB pg_search extension is enabled:
```sql
CREATE EXTENSION IF NOT EXISTS pg_search;
```

Add average rating column to movies table for search integration:
```sql
-- Add rating columns to movies table
ALTER TABLE movies ADD COLUMN avg_rating DECIMAL(3,2);
ALTER TABLE movies ADD COLUMN rating_count INTEGER;

-- Calculate and populate initial values
UPDATE movies m
SET avg_rating = COALESCE(sub.avg_rating, 0.0),
    rating_count = COALESCE(sub.rating_count, 0)
FROM (
    SELECT movie_id, AVG(rating) as avg_rating, COUNT(rating) as rating_count
    FROM ratings
    GROUP BY movie_id
) sub WHERE m.movie_id = sub.movie_id;
```

Create materialized view for advanced rating filtering:
```sql
-- Materialized view for comprehensive movie statistics
CREATE MATERIALIZED VIEW movie_stats AS
SELECT
    m.movie_id,
    COUNT(r.rating) as rating_count,
    AVG(r.rating) as avg_rating,
    MIN(r.rating) as min_rating,
    MAX(r.rating) as max_rating
FROM movies m
LEFT JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id;

-- Index for fast filtering
CREATE INDEX idx_movie_stats_avg_rating ON movie_stats (avg_rating);
CREATE INDEX idx_movie_stats_rating_count ON movie_stats (rating_count);
```

Create enhanced ParadeDB BM25 index with ratings:
```sql
-- Enhanced index including rating data
CREATE INDEX movies_search_with_ratings_idx ON movies
USING bm25 (
    movie_id,
    title,
    genres,
    year,
    avg_rating,
    rating_count
)
WITH (key_field='movie_id');
```

#### Search Examples

Basic title search with rating filtering:
```sql
-- Find highly-rated action movies
SELECT m.title, m.year, m.avg_rating, m.rating_count
FROM movies m
WHERE m @@@ 'dark knight'
  AND m.avg_rating >= 4.0
  AND m.rating_count >= 10
ORDER BY m.avg_rating DESC;
```

Advanced search using materialized view:
```sql
-- Complex search with genre and rating criteria
SELECT m.title, m.year, m.genres, s.avg_rating, s.rating_count
FROM movies m
JOIN movie_stats s ON m.movie_id = s.movie_id
WHERE m @@@ 'title:matrix AND genres:Sci-Fi'
  AND s.avg_rating >= 3.5
  AND s.rating_count >= 50
  AND m.year BETWEEN 1990 AND 2024
ORDER BY s.avg_rating DESC, m.year DESC;
```

Genre-based filtering with ratings:
```sql
-- Top-rated comedy movies
SELECT m.title, m.year, s.avg_rating, s.rating_count
FROM movies m
JOIN movie_stats s ON m.movie_id = s.movie_id
WHERE m @@@ 'genres:Comedy'
  AND s.avg_rating >= 4.0
  AND s.rating_count >= 100
ORDER BY s.avg_rating DESC
LIMIT 20;
```

#### Refresh Materialized View

To keep movie statistics current:
```sql
-- Refresh materialized view manually
REFRESH MATERIALIZED VIEW CONCURRENTLY movie_stats;

-- Or set up automatic refresh with triggers (see Option 2 in recommendations)
```
