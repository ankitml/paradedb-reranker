# Python Files Analysis

Based on my exploration, this is a **Personalized Movie Re-ranking System** that implements a sophisticated two-tower recommendation model. Here's what I found:

## Overview
The codebase contains 5 Python files that work together to create a movie recommendation system combining:
- **ParadeDB BM25 search** for initial content retrieval
- **AI-powered embeddings** for personalized re-ranking

## Key Files and Purpose

1. **`utils.py`** - Core utilities with database connection management, configuration handling, and movie data processing utilities

2. **`ingest_data.py`** - High-performance MovieLens data ingestion using PostgreSQL COPY commands for bulk imports

3. **`generate_embedding.py`** - Generates movie content embeddings using OpenRouter API with the `sentence-transformers/all-minilm-l12-v2` model (384-dimensional vectors)

4. **`ingest_embeddings.py`** - Loads embeddings from CSV files into the PostgreSQL database with pgvector support

5. **`generate_user_embeddings.py`** - Creates user preference embeddings using collaborative filtering (two-tower approach with positive/negative signals)

## Architecture Flow
1. **Data Ingestion** → Load MovieLens data (movies, users, ratings, tags) into PostgreSQL
2. **Movie Embeddings** → Generate AI-powered content vectors for each movie
3. **Embedding Storage** → Store movie vectors in database with pgvector
4. **User Embeddings** → Create personalized preference vectors based on user ratings

## Technical Stack
- **PostgreSQL** with **pgvector** extension for vector storage
- **OpenRouter API** for AI embedding generation
- **ParadeDB** for BM25 full-text search
- **Two-tower model** combining content-based and collaborative filtering

The system is designed to re-rank search results by combining BM25 relevance scores with personalized vector similarity scores, creating a hybrid recommendation approach.