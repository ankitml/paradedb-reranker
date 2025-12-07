# Personalized Movie Re-ranking System

A hybrid search and recommendation system that demonstrates how user preferences can dramatically reorder search results. By combining traditional keyword search (BM25) with collaborative filtering through vector embeddings, we achieve personalized rankings that reflect individual tastes - fantasy lovers see fantasy movies rise to the top while fantasy haters see them penalized.

## Why This Matters

**Traditional search = One-size-fits-all**
- All users get identical results for the same query
- No consideration for personal preferences
- Limited to keyword matching

**Personalized search = Tailored experience**
- Same query yields different results per user
- Incorporates user's viewing history and preferences
- Balances relevance with personal taste

**Real-world applications:**
- **E-commerce**: Product search that prioritizes brands/categories you prefer
- **Streaming**: Content discovery matching your viewing history
- **News**: Articles aligned with your interests and reading patterns
- **Enterprise**: Document search prioritizing relevant teams/projects

## Quick Start

```bash
# 1. Database Connection
kubectl port-forward svc/paradedb-rw 5433:5432 -n ankit31-paradedb
export PGPASSWORD="<password>"

# 2. Test Personalized Search
python search_cli.py --query "king" --user-id 20001  # Fantasy lover
python search_cli.py --query "king" --user-id 20002  # Fantasy hater
```

## Complete Setup Guide

### 1. Data Ingestion (MovieLens Dataset)

```bash
source ~/.venv/bin/activate
python ingest_data.py --data-dir sample_data --batch-size 10000 \
  --db-host localhost --db-port 5433 --db-user postgres
```

Creates tables: `movies`, `users`, `ratings`, `tags` with 27M+ ratings.

### 2. Create BM25 Search Index

```sql
CREATE INDEX movies_search_idx ON movies
  USING bm25 (movie_id, title, year, imdb_id, tmdb_id, genres)
  WITH (key_field='movie_id');
```

### 3. Generate Movie Content Embeddings

```bash
# Requires OPENROUTER_API_KEY environment variable
python generate_embedding.py --model sentence-transformers/all-minilm-l12-v2 \
  --batch-size 100 --limit 1000

# Creates: movie_embeddings.csv
```

### 4. Store Movie Embeddings

```bash
python ingest_embeddings.py --csv-file movie_embeddings.csv \
  --batch-size 1000 --table-name movies
```

Updates `movies` table with `content_embedding` vector(384) column.

### 5. Generate User Preference Embeddings

```bash
# Generate for all users
python generate_user_embeddings.py

# Or generate for specific test users
python generate_user_embeddings.py --user-ids 10001 10002 20001 20002
```

Uses directional vectors: ADD for positive ratings (≥4.0), SUBTRACT for negative ratings (<3.0).

### 6. Run Personalized Search

```bash
# Compare BM25 vs Personalized re-ranking
python search_cli.py --query "lord" --user-id 10001 --show-scores

# Adjust personalization weight (default: 50%)
python search_cli.py --query "magic" --user-id 20001 --partial-weight 30
```

## Architecture

### Score Formula
```
final_score = α × normalized_bm25 + (1-α) × cosine_similarity

Where:
- α = 1.0: Pure BM25 (no personalization)
- α = 0.5: 50/50 hybrid (default)
- α = 0.0: Pure personalization
```

### Vector Direction
- **Positive ratings** (≥4.0): `user_embedding += movie_embedding × (rating-3.0)`
- **Negative ratings** (<3.0): `user_embedding -= movie_embedding × (3.0-rating)`
- **Cosine similarity**: Signed (-1 to 1) preserves preference direction

## Test Users

| User ID | Type | Description |
|---------|------|-------------|
| 10001 | Fantasy Lover | 50 ratings, all 5.0 for fantasy movies |
| 10002 | Fantasy Hater | 50 ratings, all 1.0 for fantasy movies |
| 20001 | Extreme Lover | 779 ratings, 5.0 for ALL fantasy movies |
| 20002 | Extreme Hater | 779 ratings, 1.0 for ALL fantasy movies |

## Search Terms & Expected Behavior

### 1. "lord" ⭐ BEST FOR TESTING

**Fantasy movies in results:**
- Lord of the Rings: The Fellowship of the Ring (2001)
- Lord of the Rings: The Two Towers (2002)
- Lord of the Rings: The Return of the King (2003)
- Lord of the Rings, The (1978)

**Expected rankings:**
- **Fantasy Lover (10001, 20001)**: LOTR movies move to TOP 3 positions
- **Fantasy Hater (10002, 20002)**: LOTR movies drop to BOTTOM 3 positions (7-9)

### 2. "king" ⭐⭐⭐⭐⭐

**Fantasy movies:**
- King Kong (1933, 1976, 2005)
- The Scorpion King (2002)
- Fisher King, The (1991)

**Non-Fantasy movies:**
- King Arthur (2004)
- King Ralph (1991)
- King of Kings (1961)

**Expected rankings:**
- **Fantasy Lovers**: King Kong movies rank 1-3, Fisher King moves up
- **Fantasy Haters**: King Arthur, King Ralph rise to top; King Kong sinks

### 3. "magic" ⭐⭐⭐⭐⭐

**Fantasy movies:**
- Practical Magic (1998)
- Strange Magic (2015)
- The Magic Ring (1982)
- Carnival Magic (1981)

**Non-Fantasy movies:**
- Magic Mike (2012)
- Magic in the Moonlight (2014)
- Rough Magic (1995)

**Expected rankings:**
- **Fantasy Lovers**: All magic-themed fantasy movies in top 5
- **Fantasy Haters**: Magic Mike movies rank higher; fantasy magic movies penalized

### 4. "witch" ⭐⭐⭐⭐⭐

**Fantasy movies:**
- Escape to Witch Mountain (1975)
- Chronicles of Narnia: The Lion, the Witch and the Wardrobe (2005)
- Hansel & Gretel: Witch Hunters (2013)
- The Last Witch Hunter (2015)

**Non-Fantasy movies:**
- Blair Witch Project (1999)
- The Witch (2015)
- Halloween III: Season of the Witch (1982)

**Expected rankings:**
- **Fantasy Lovers**: Narnia and family witch movies rank highest
- **Fantasy Haters**: Horror witch movies (Blair Witch) outrank fantasy ones

### 5. "dragon" ⭐⭐⭐⭐

**Fantasy movies:**
- How to Train Your Dragon (2010)
- Mummy: Tomb of the Dragon Emperor (2008)

**Non-Fantasy movies:**
- Crouching Tiger, Hidden Dragon (2000)
- Enter the Dragon (1973)
- Red Dragon (2002)

**Expected rankings:**
- **Fantasy Lovers**: How to Train Your Dragon ranks #1 or #2
- **Fantasy Haters**: Martial arts dragon movies outrank fantasy ones

## Key Patterns to Observe

1. **Fantasy lovers see fantasy movies rise** in rankings (positive similarity scores)
2. **Fantasy haters see fantasy movies fall** in rankings (negative similarity scores)
3. **Non-fantasy movies with the same keyword** shift opposite direction
4. **Extreme users (20001, 20002)** show stronger re-ranking than basic users (10001, 10002)
5. **BM25 column remains unchanged** - only reranked columns show personalization

## Database Schema

```sql
movies
- movie_id (integer, PK)
- title, year, genres (text)
- content_embedding (vector(384)) - Movie content vector

users
- user_id (integer, PK)
- embedding (vector(384)) - User preference vector
- created_at, updated_at (timestamps)

ratings
- user_id, movie_id (foreign keys)
- rating (0.5 - 5.0)
- timestamp
```

## Beyond Re-ranking: Full Recommendation System

This implementation focuses on personalized re-ranking, but the same infrastructure can extend to complete recommender systems:

### 1. User Similarity Discovery
```sql
-- Find users with similar tastes
SELECT u1.user_id, u2.user_id,
       (1 - (u1.embedding <=> u2.embedding)) as similarity
FROM users u1, users u2
WHERE u1.user_id = 10001
  AND u2.user_id != 10001
ORDER BY similarity DESC
LIMIT 10;
```

### 2. Collaborative Filtering
```sql
-- Get movies liked by similar users
WITH similar_users AS (
  SELECT user_id
  FROM users
  WHERE user_id != 10001
  ORDER BY embedding <=> (SELECT embedding FROM users WHERE user_id = 10001)
  LIMIT 100
)
SELECT DISTINCT m.movie_id, m.title, AVG(r.rating) as avg_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
JOIN similar_users su ON r.user_id = su.user_id
WHERE r.rating >= 4.0
  AND m.movie_id NOT IN (
    SELECT movie_id FROM ratings WHERE user_id = 10001
  )
GROUP BY m.movie_id, m.title
ORDER BY avg_rating DESC
LIMIT 20;
```

### 3. Cold Start Solutions
- **Content-based**: Use movie genres, actors, directors
- **Demographic**: Age, location data when available
- **Hybrid**: Combine with popularity trends

### 4. Real-time Personalization
```sql
-- Update user embedding after new rating
UPDATE users u
SET embedding = u.embedding + (
  CASE
    WHEN NEW.rating >= 4.0 THEN
      (SELECT m.content_embedding FROM movies m WHERE m.movie_id = NEW.movie_id) * (NEW.rating - 3.0)
    ELSE
      -(SELECT m.content_embedding FROM movies m WHERE m.movie_id = NEW.movie_id) * (3.0 - NEW.rating)
  END
)
WHERE u.user_id = NEW.user_id;
```

### Applications Beyond Movies

**E-commerce Platforms:**
- Search results prioritizing previously purchased brands
- "Users who bought X also bought" recommendations
- Dynamic category ranking based on browsing history

**Music Streaming:**
- Playlist generation matching user's mood/history
- Artist discovery through similar listener preferences
- Genre exploration with personal relevance scoring

**Content Platforms:**
- Article recommendations based on reading patterns
- Video suggestions matching watch history
- Podcast discovery through topic affinity

**Enterprise Search:**
- Document search prioritizing relevant departments
- Code search favoring technologies you use
- Internal knowledge base with project context

## Search Examples

```sql
-- Basic BM25 search
SELECT movie_id, title, year, genres
FROM movies
WHERE movies @@@ 'title:dark knight';

-- Genre filtering
SELECT movie_id, title, year, genres
FROM movies
WHERE movies @@@ 'genres:Action AND genres:Thriller';

-- Combined search
SELECT movie_id, title, year, genres
FROM movies
WHERE movies @@@ '(title:lord OR title:king) AND genres:Fantasy';
```