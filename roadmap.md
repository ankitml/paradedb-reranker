# Personalized Movie Re-ranking System

## Overview
Two-Tower recommendation model combining ParadeDB BM25 search with collaborative filtering re-ranking using vector similarity.

## Core Components

### 1. Movie Content Embeddings
- Generate embeddings from movie metadata (title, genres, year)
- **Content format**: `movie_text = "{title} {year} {' '.join(genres)}"`
  - Example: `"Toy Story (1995) Adventure Animation Children Comedy Fantasy"`
- **Embedding Model**: Sentence Transformers all-MiniLM-L12-v2 (384 dimensions)
  - Available on OpenRouter: `sentence-transformers/all-minilm-l12-v2`
  - Optimal balance of performance, size, and cost for movie recommendations
  - 12-layer architecture for better semantic understanding
- Store as vectors in PostgreSQL

### 2. User Preference Embeddings
- **Primary Strategy**: Rating-based collaborative filtering
  - Positive signals: Ratings >=4.0 (user likes)
  - Negative signals: Ratings <3.0 (user dislikes)
  - **Formula**:
    ```
    # Calculate weighted average of liked movies
    like_weights = sum(liked_movie_ratings - 3.5)
    like_embedding = sum((rating - 3.5) * movie_embedding for liked movies) / like_weights

    # Calculate weighted average of disliked movies
    dislike_weights = sum(3.5 - disliked_movie_ratings)
    dislike_embedding = sum((3.5 - rating) * movie_embedding for disliked movies) / dislike_weights

    # Final user embedding combines preferences
    user_embedding = like_embedding - dislike_embedding
    ```
- **Advanced Strategy**: Bi-encoder approach for separate user preference modeling

### 3. Two-Stage Search Pipeline
- **Stage 1**: ParadeDB BM25 retrieval for relevant candidates
- **Stage 2**: Personalized re-ranking using vector dot product similarity
- **Final Scoring**: Linear combination of BM25 relevance + personalized similarity

### 4. Architecture Benefits
- Leverages both content relevance and user preferences
- No vector indexes needed - rank only retrieved candidates
- Cold-start handling through content-based search
- Real-time personalization for existing users