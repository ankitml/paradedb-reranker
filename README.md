# Personalized Movie Re-ranking with ParadeDB BM25 (All in Postgres)

Hybrid search combining BM25 keyword search with user preference embeddings. Same query, different results per user - fantasy lovers see fantasy movies rise, fantasy haters see them sink.

<img width="1710" alt="Fantasy hater search" src="https://github.com/user-attachments/assets/e9a5a5da-3682-479d-b7c2-5ba0b19e065e" />
<img width="1654" alt="Fantasy lover search" src="https://github.com/user-attachments/assets/bfa1b106-e313-4d90-acc0-de8c2825a040" />

## Setup Guide

### Prerequisites

- PostgreSQL 14+ with pgvector and ParadeDB extensions
- Python 3.8+
- OpenRouter API key
- MovieLens dataset (in `data/`)

### 1. Install Dependencies

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Set PGPASSWORD and OPENROUTER_API_KEY
```

### 3. Create Database Schema

Ensure the database specified in `PGDATABASE` exists, then run:

```bash
psql -f data/datamodel.sql
```

### 4. Ingest MovieLens Data

```bash
python ingest_data.py --data-dir data --batch-size 10000
```

### 5. Generate Movie Embeddings

```bash
python generate_embedding.py --data-dir data --batch-size 100
```

Creates `data/embeddings.csv` with 384-dim vectors. Requires `OPENROUTER_API_KEY`.

### 6. Store Movie Embeddings

```bash
python ingest_embeddings.py --batch-size 1000
```

### 7. Create Test Users

```bash
psql -f data/test_users.sql
```

| User ID | Type | Description |
|---------|------|-------------|
| 10001 | Fantasy Lover | 50 fantasy movies rated 5.0 |
| 10002 | Fantasy Hater | 50 fantasy movies rated 1.0 |
| 20001 | Extreme Lover | ALL fantasy movies rated 5.0 |
| 20002 | Extreme Hater | ALL fantasy movies rated 1.0 |

### 8. Generate User Embeddings

```bash
python generate_user_embeddings.py --user-ids 10001 10002 20001 20002
```

User embeddings use directional vectors:
- Rating >= 4.0: `user_embedding += movie_embedding × (rating - 3.0)`
- Rating < 3.0: `user_embedding -= movie_embedding × (3.0 - rating)`

### 9. Run Personalized Search

```bash
python search_cli.py --query "lord" --user-id 20001              # Fantasy lover
python search_cli.py --query "lord" --user-id 20002              # Fantasy hater
python search_cli.py --query "lord" --user-id 10001 --show-scores # Show scores
python search_cli.py --query "magic" --user-id 20001 --partial-weight 30
```

## How It Works

```sql
WITH first_pass_retrieval AS (
    -- BM25 candidate retrieval
    SELECT movie_id, title, paradedb.score(movie_id) as bm25_score
    FROM movies WHERE movies @@@ 'king'
    ORDER BY bm25_score DESC LIMIT 20
),
normalization AS (
    -- Normalize BM25 to [0,1]
    SELECT *, (bm25_score - MIN(bm25_score) OVER()) / 
              (MAX(bm25_score) OVER() - MIN(bm25_score) OVER()) as normalized_bm25
    FROM first_pass_retrieval
),
personalized_ranker AS (
    -- User-movie similarity
    SELECT n.*, (1 - (u.embedding <=> m.content_embedding)) as cosine_similarity
    FROM normalization n
    JOIN movies m ON n.movie_id = m.movie_id
    CROSS JOIN users u WHERE u.user_id = 20001
)
SELECT *, (0.5 * normalized_bm25 + 0.5 * cosine_similarity) as combined_score
FROM personalized_ranker ORDER BY combined_score DESC;
```

**Score formula:** `final_score = α × bm25 + (1-α) × similarity`
- α = 1.0: Pure BM25
- α = 0.5: Hybrid (default)
- α = 0.0: Pure personalization

## Database Schema

```sql
movies (movie_id, title, year, genres, content_embedding vector(384))
users (user_id, embedding vector(384))
ratings (user_id, movie_id, rating, timestamp)
```

## Test Queries

Try: `lord`, `king`, `magic`, `witch`, `dragon`

Expected behavior:
- Fantasy lovers: fantasy movies rise to top
- Fantasy haters: fantasy movies sink to bottom
- Extreme users (20001/20002) show stronger effect than basic users (10001/10002)
