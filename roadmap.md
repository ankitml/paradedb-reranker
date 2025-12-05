# Personalized Movie Re-ranking System

## Overview
Two-Tower recommendation model combining ParadeDB BM25 search with collaborative filtering re-ranking using vector similarity.

## Core Components


### 1. User Preference Embeddings
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

### 2. Two-Stage Search Pipeline
- **Stage 1**: ParadeDB BM25 retrieval for relevant candidates
- **Stage 2**: Personalized re-ranking using vector dot product similarity
- **Final Scoring**: Linear combination of BM25 relevance + personalized similarity
