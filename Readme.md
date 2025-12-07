# Personalized Movie Re-ranking System

Two-Tower recommendation model combining ParadeDB BM25 search with collaborative filtering re-ranking using vector similarity.

## Python Scripts

### Data Ingestion
- **`ingest_data.py`** - MovieLens data ingestion script with bulk PostgreSQL import using COPY command and batch processing

### Embedding Generation
- **`generate_embedding.py`** - Generate movie embeddings using OpenRouter API with all-MiniLM-L12-v2 model and batch processing

### Embedding Storage
- **`ingest_embeddings.py`** - Upload generated embeddings from CSV to PostgreSQL movies table with vector storage

## Ingestion
```
source ~/.venv/bin/activate
export PGPASSWORD="your_password"
python ingest_data.py --data-dir sample_data --batch-size 10000 \
  --db-host localhost --db-port 5432 --db-user postgres
```
### Reading
`kubectl port-forward svc/paradedb-rw 5433:5432 -n ankit31-paradedb`

PSQL Connection:
`PGPASSWORD="<>" psql -h localhost -p 5433 -U postgres -d postgres`

### ParadeDB Full-Text Search & Rating Integration

```
CREATE INDEX movies_search_idx ON movies
  USING bm25 (movie_id, title, year, imdb_id, tmdb_id, genres)
  WITH (key_field='movie_id');
```

  🎯 Search Query Examples:

```
  Basic title search:
  SELECT movie_id, title, year, genres
  FROM movies
  WHERE movies @@@ '"dark knight"';

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
```
### Create ParadeDB BM25 Index with Ratings (Not doing it for now)

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

## Test Users & Search Cases

### Test Users Created

#### Basic Test Users
- **User 10001** (Fantasy Lover): 50 ratings, all 5.0 average for fantasy movies
- **User 10002** (Fantasy Hater): 50 ratings, all 1.0 average for fantasy movies

#### Extreme Test Users
- **User 20001** (Extreme Fantasy Lover): 779 ratings, all 5.0 average for ALL fantasy movies
- **User 20002** (Extreme Fantasy Hater): 779 ratings, all 1.0 average for ALL fantasy movies

### Recommended Search Terms for Testing
These search terms return 3-4 fantasy movies in the top 10 results:

#### 1. "lord" ⭐ BEST FOR TESTING
**Fantasy movies in top 10:**
- Lord of the Rings, The (1978) 🧙‍♂️
- Lord of the Rings: The Fellowship of the Ring (2001) 🧙‍♂️
- Lord of the Rings: The Two Towers (2002) 🧙‍♂️
- Lord of the Rings: The Return of the King (2003) 👑

```sql
SELECT movie_id, title, year, genres
FROM movies
WHERE movies @@@ 'title:lord'
ORDER BY movie_id
LIMIT 10;
```

#### 2. "king" ⭐ GOOD OPTION
**Fantasy movies in top 10:**
- Kid in King Arthur's Court (Fantasy) 🏰
- Aladdin and the King of Thieves (Fantasy) 🧞‍♂️
- King Kong (Fantasy) 🦍

```sql
SELECT movie_id, title, year, genres
FROM movies
WHERE movies @@@ 'title:king'
ORDER BY movie_id
LIMIT 10;
```

#### 3. "ring" ⭐ SIMPLE
**Fantasy movies in results:**
- Lord of the Rings: The Fellowship of the Ring (2001) 💍
- The Magic Ring (1982) ✨

```sql
SELECT movie_id, title, year, genres
FROM movies
WHERE movies @@@ 'title:ring'
ORDER BY movie_id
LIMIT 10;
```

### Test Strategy
1. **Use search term "lord"** - gives you 4 LOTR movies + 6 others
2. **Test with User 10001** (Fantasy Lover) - LOTR movies should rank higher after personalized re-ranking
3. **Test with User 10002** (Fantasy Hater) - LOTR movies should rank lower after personalized re-ranking
4. **Test with User 20001** (Extreme Fantasy Lover) - Should show maximum preference boost for fantasy movies
5. **Test with User 20002** (Extreme Fantasy Hater) - Should show maximum penalty for fantasy movies

1. "king" ⭐⭐⭐⭐⭐ (Best after "lord")

  - Fantasy movies: King Kong (1933, 1976, 2005), The Scorpion King, Fisher King
  - Non-Fantasy: King Arthur, King Ralph, King of Kings, King Solomon's Mines
  - Good mix: 4 fantasy movies in top 15 results

  2. "magic" ⭐⭐⭐⭐⭐

  - Fantasy movies: Practical Magic, Strange Magic, The Magic Ring, Carnival Magic
  - Non-Fantasy: Rough Magic, Magic Mike, Magic in the Moonlight
  - Clear distinction: 4 fantasy vs 5 non-fantasy in results

  3. "witch" ⭐⭐⭐⭐⭐

  - Fantasy movies: Escape to Witch Mountain, Chronicles of Narnia, Hansel & Gretel, The Last Witch Hunter
  - Non-Fantasy: Blair Witch Project, Halloween III, The Witch (2015)
  - Strong contrast: Many horror movies vs fantasy

  4. "dragon" ⭐⭐⭐⭐

  - Fantasy movies: How to Train Your Dragon, Mummy: Tomb of the Dragon Emperor
  - Non-Fantasy: Crouching Tiger Hidden Dragon, Enter the Dragon, Red Dragon
  - Good variety: 2 clear fantasy vs 13 others

  5. "ring" ⭐⭐⭐

  - Fantasy movies: Lord of the Rings: Fellowship of the Ring, The Magic Ring
  - Non-Fantasy: The Ring (horror), Ringu, Bling Ring
  - Limited but clear: LOTR movies should rank very differently

  Why These Keywords Work:

  1. Good fantasy representation: Each has 2-5 fantasy movies in the results
  2. Non-fantasy alternatives: Enough non-fantasy movies to see clear re-ranking
  3. Distinct genres: Fantasy vs horror/action/drama creates clear preference signals
### Database Schema
Users table now includes:
- `user_id` (integer, primary key)
- `created_at` (timestamp, default now())
- `embedding` (vector(384)) - User preference embedding
- `updated_at` (timestamp, default now()) - Last embedding update


