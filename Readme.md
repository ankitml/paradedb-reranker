# Personalized Movie Re-ranking after BM25 from ParadeDB (All in Postgres)

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

#### How User Embeddings Are Calculated

**Directional Vector Approach (Current Implementation):**
```
user_embedding = Σ(positive_ratings) - Σ(negative_ratings)

For each movie rating:
- Rating ≥ 4.0: user_embedding += movie_embedding × (rating - 3.0)
- Rating < 3.0: user_embedding -= movie_embedding × (3.0 - rating)
```

**Logic:**
- **Positive signals** (ratings 4-5) add the movie's characteristics to user's preference vector
- **Negative signals** (ratings 1-2) subtract the movie's characteristics
- **Neutral rating** (3.0) has no effect
- **Weighting**: Higher ratings have stronger influence (5.0 adds 2×, 4.0 adds 1×)

**Example:**
- User rates "Lord of the Rings" 5.0 → +2 × fantasy_vector
- User rates "The Conjuring" 1.0 → -2 × horror_vector
- Result: User preference leans toward fantasy, away from horror

#### Alternative Approaches

**1. Weighted Average (Traditional):**
```sql
user_embedding = Σ(rating × movie_embedding) / Σ(rating)
```
- *Pro*: Simple, stable
- *Con*: Loses preference direction, all users with similar taste patterns get similar embeddings

**2. TF-IDF Weighting:**
```sql
weight = (user_rating / avg_movie_rating) × log(total_users / users_who_rated)
user_embedding = Σ(weight × movie_embedding)
```
- *Pro*: Accounts for rating rarity
- *Con*: More complex, requires global statistics

**3. Neural Network Two-Tower:**
```
User Tower: [user_features, rating_history] → 384-dim embedding
Movie Tower: [movie_features, content] → 384-dim embedding
Trained with: score = dot_product(user_embedding, movie_embedding)
```
- *Pro*: Learns complex patterns
- *Con*: Requires training data, more infrastructure

**4. Matrix Factorization (SVD):**
```
Decompose rating_matrix = U × S × V^T
U contains user embeddings, V contains movie embeddings
```
- *Pro*: Proven recommendation technique
- *Con*: Cold start problem, requires dense rating matrix

The directional approach was chosen for its:
- Simplicity (single SQL operation)
- Clear interpretability
- Ability to create opposite embeddings for opposite users
- No training required

### 6. Run Personalized Search

```bash
# Compare BM25 vs Personalized re-ranking
python search_cli.py --query "lord" --user-id 10001 --show-scores

# Adjust personalization weight (default: 50%)
python search_cli.py --query "magic" --user-id 20001 --partial-weight 30
```

#### How Personalized Search Works

**The Complete SQL Query:**
```sql
WITH first_pass_retrieval AS (
    -- Step 1: BM25 candidate generation
    SELECT
        movie_id, title, year, genres,
        paradedb.score(movie_id) as bm25_score
    FROM movies
    WHERE movies @@@ 'king'
    ORDER BY paradedb.score(movie_id) DESC, movie_id ASC
    LIMIT 20
),
normalization AS (
    -- Step 2: Normalize BM25 scores to [0,1]
    SELECT
        *,
        CASE
            WHEN MAX(bm25_score) OVER() = MIN(bm25_score) OVER() THEN 0.5
            ELSE (bm25_score - MIN(bm25_score) OVER()) /
                 (MAX(bm25_score) OVER() - MIN(bm25_score) OVER())
        END as normalized_bm25
    FROM first_pass_retrieval
),
personalized_ranker AS (
    -- Step 3: Calculate user similarity scores
    SELECT
        n.*,
        (1 - (u.embedding <=> m.content_embedding)) as cosine_similarity
    FROM normalization n
    JOIN movies m ON n.movie_id = m.movie_id
    CROSS JOIN users u WHERE u.user_id = 20001
),
joint_ranker AS (
    -- Step 4: Combine scores with weights
    SELECT
        movie_id, title, year, genres,
        normalized_bm25,
        cosine_similarity,
        (0.5 * normalized_bm25 + 0.5 * cosine_similarity) as combined_score
    FROM personalized_ranker
)
SELECT * FROM joint_ranker
ORDER BY combined_score DESC;
```

**Step-by-Step Process:**

1. **BM25 Retrieval** (`first_pass_retrieval`)
   - Finds movies matching the search query using ParadeDB BM25
   - Orders by relevance score, keeps top candidates
   - Example: "king" finds King Kong, King Arthur, etc.

2. **Score Normalization** (`normalization`)
   - Converts BM25 scores to 0-1 range
   - Handles edge case when all scores are identical (returns 0.5)
   - Formula: `(score - min) / (max - min)`

3. **Personalization** (`personalized_ranker`)
   - Calculates cosine similarity between user and movie embeddings
   - Range: -1 to 1 (negative values indicate opposite preferences)
   - Formula: `1 - (user_embedding <=> movie_embedding)`

4. **Score Fusion** (`joint_ranker`)
   - Combines normalized BM25 with similarity scores
   - Default: 50% BM25 + 50% personalization
   - Formula: `α × bm25 + (1-α) × similarity`

## Architecture

### Score Formula
```
final_score = α × normalized_bm25 + (1-α) × cosine_similarity

Where:
- α = 1.0: Pure BM25 (no personalization)
- α = 0.5: 50/50 hybrid (default)
- α = 0.0: Pure personalization
```

### Why This Two-Stage Approach?

**Stage 1 - BM25 Retrieval:**
- ✅ Fast keyword matching with inverted index
- ✅ Handles large million-item database efficiently
- ✅ Provides relevant candidates
- ❌ No personalization

**Stage 2 - Personalized Re-ranking:**
- ✅ Computes expensive vector similarity on few candidates
- ✅ Incorporates user preferences
- ✅ Enables real-time personalization
- ❌ Computationally expensive (hence limited to top candidates)

**Alternative Approaches:**

**1. Vector Search Only:**
```sql
-- Similarity search without BM25
SELECT movie_id, title, 1 - (embedding <=> user_embedding) as score
FROM movies
ORDER BY embedding <=> user_embedding
LIMIT 20;
```
- *Pro*: Purely personalized
- *Con*: No keyword relevance, may return unrelated movies

**2. Learning to Rank:**
```sql
-- Machine learning model predicts relevance
SELECT *, predict_relevance(movie_features, user_features) as score
FROM movies
WHERE movies @@@ query
ORDER BY score DESC;
```
- *Pro*: Learns optimal ranking patterns
- *Con*: Requires training, feature engineering

**3. Reciprocal Rank Fusion (RRF):**
```sql
-- Combine rankings without score normalization
WITH bm25_rank AS (
  SELECT movie_id, ROW_NUMBER() OVER (ORDER BY bm25_score DESC) as rank
),
similarity_rank AS (
  SELECT movie_id, ROW_NUMBER() OVER (ORDER BY similarity DESC) as rank
)
SELECT movie_id, 1/(60 + bm25_rank) + 1/(60 + similarity_rank) as score
FROM bm25_rank JOIN similarity_rank USING(movie_id);
```
- *Pro*: No score normalization needed
- *Con*: Loses actual score magnitudes

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
<img width="1710" height="220" alt="image" src="https://github.com/user-attachments/assets/e9a5a5da-3682-479d-b7c2-5ba0b19e065e" />
Fig 1. Search for "lord" by fantasy hating user.
<img width="1654" height="229" alt="image" src="https://github.com/user-attachments/assets/bfa1b106-e313-4d90-acc0-de8c2825a040" />
Fig 2. Seach for "lord" by fantasy loving user.

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
