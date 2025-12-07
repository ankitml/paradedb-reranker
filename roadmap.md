# Personalized Movie Re-ranking: Technical Approaches

## Current Problem
Users with opposite preferences get identical rankings because current embeddings don't capture preference direction.

## Approaches (Simple → Complex)

### 1. Directional Vectors + Dot Product (Current Plan)
- Positive ratings: ADD movie embedding to user vector
- Negative ratings: SUBTRACT movie embedding from user vector
- Score: Direct dot product (gives signed similarity)
- Timeline: 1-2 days

### 2. Two-Tower Neural Network
- User tower: features + rating history → embedding
- Movie tower: content + metadata → embedding
- Train with triplet loss
- Timeline: 3-4 weeks

### 3. Cohere Cross-Encoder Reranking
- First stage: BM25 + vector search (100 items)
- Second stage: LLM scores top 10
- Pros: Deep semantic understanding
- Cons: API cost, latency
- Timeline: 2-3 weeks

### 4. Graph Neural Networks
- Model users and movies as bipartite graph
- Message passing through connections
- Timeline: 2-3 months

## Current Implementation Plan
1. Fix embeddings with vector addition/subtraction
2. Replace cosine similarity with dot product
3. Test with opposite users (fantasy lovers/haters)
4. Deploy and validate