# Score Interpolation and Weight Optimization for Re-ranking Systems

## Overview
This document summarizes research findings and practical guidelines for optimizing the weight between original retrieval scores (BM25) and re-ranker scores (personalized similarity) in a movie recommendation system.

## Common Formula
Most systems use linear interpolation with a λ (lambda) parameter:
```
final_score = λ × original_score + (1-λ) × reranker_score
```

Where:
- λ = 1.0: Use only original retrieval scores
- λ = 0.0: Use only reranker scores
- λ = 0.5: Equal weighting (50/50)

## Research Findings

### LexBoost (2024 research)
- Uses λ parameter tuned on validation set
- Shows that lexical retrieval (BM25) still provides valuable signals
- Optimal λ varies by dataset and query type

### LambdaMART/LambdaRank
- These are learning-to-rank algorithms (not to be confused with λ weighting)
- They directly optimize ranking metrics like NDCG
- Can learn optimal weights for multiple features

## Practical Guidelines from Industry

### Empirical Results
- **BM25 still valuable**: Pure re-ranking often performs worse than hybrid
- **Typical ranges**:
  - Conservative: λ = 0.7-0.9 (70-90% original, 10-30% reranker)
  - Aggressive: λ = 0.3-0.5 (30-50% original, 50-70% reranker)
- **Query-dependent weighting**:
  - Short/keyword queries: Higher λ (more BM25)
  - Long/semantic queries: Lower λ (more reranker)

### Microsoft Bing Approach
- Two-stage re-ranking
- First stage: λ = 0.7 (70% original)
- Second stage: Pure reranker on top candidates

### Google Research
- Found that maintaining some original score prevents overfitting
- Recommends minimum λ = 0.3 for stability

## Optimization Methods

### 1. Grid Search with Validation Set
```python
lambdas = [0.1, 0.3, 0.5, 0.7, 0.9]
best_lambda = 0.5
best_ndcg = 0

for lambda_val in lambdas:
    # Calculate interpolated scores
    scores = lambda_val * bm25_scores + (1-lambda_val) * rerank_scores
    # Evaluate using NDCG@10
    ndcg = calculate_ndcg(scores, ground_truth)
    if ndcg > best_ndcg:
        best_lambda = lambda_val
```

### 2. Learning-based Optimization
- Use regression to learn optimal λ from features:
  - Query length
  - Query ambiguity
  - Result diversity
  - User click patterns

### 3. Dynamic Weighting
```python
def calculate_lambda(query_type, result_confidence):
    if query_type == "short_keyword":
        return 0.8  # Trust BM25 more
    elif query_type == "long_semantic":
        return 0.3  # Trust reranker more
    else:
        return 0.5  # Default balanced
```

## Recommendations for Movie System

Given the current 50/50 approach, here are data-driven suggestions:

### Start with Conservative Weighting
```sql
-- Try 70/30 favoring BM25 initially
combined_score = 0.7 * normalized_bm25 + 0.3 * cosine_similarity
```

### Evaluate Different Weights
1. **Test queries**: "lord", "king", "ring"
2. **Metrics to track**:
   - Fantasy lover: Do fantasy movies rank higher?
   - Fantasy hater: Do fantasy movies rank lower?
   - Overall ranking quality

### Optimization Strategy
```python
# Test matrix
weights = [
    (0.9, 0.1),  # Very conservative
    (0.7, 0.3),  # Conservative
    (0.5, 0.5),  # Current approach
    (0.3, 0.7),  # Aggressive
    (0.1, 0.9)   # Very aggressive
]

# For each user preference type:
# 1. Calculate NDCG for each weight combination
# 2. Check if preferences are preserved
# 3. Choose weight that maximizes both metrics
```

## Implementation in SQL

### Current Implementation (50/50)
```sql
SELECT
    movie_id,
    title,
    normalized_bm25,
    cosine_similarity,
    (0.5 * normalized_bm25 + 0.5 * cosine_similarity) as combined_score
FROM results
ORDER BY combined_score DESC;
```

### Parameterized Version
```sql
SELECT
    movie_id,
    title,
    normalized_bm25,
    cosine_similarity,
    (%s * normalized_bm25 + %s * cosine_similarity) as combined_score
FROM results
ORDER BY combined_score DESC;
```

## Key Takeaways

1. **50/50 is reasonable** as a starting point
2. **Conservative weights (70/30)** often perform better in practice
3. **BM25 provides precision** (exact matches)
4. **Reranker provides personalization** (preference matching)
5. **Optimal weight depends on**:
   - Your specific user base
   - Query characteristics
   - Quality of user embeddings

## Method to the Madness

The optimization process should be:
1. **Start empirical**: Test multiple weights with real data
2. **Measure performance**: Use ranking metrics (NDCG, precision@k)
3. **Validate preferences**: Ensure personalization still works
4. **Iterate**: Fine-tune based on results
5. **Monitor**: Track performance over time

The "method to this madness" is **empirical evaluation** - test different weights with your actual data and user preferences to find what works best for your specific use case.