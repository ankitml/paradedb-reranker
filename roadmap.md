1. Add embeddings to movies. (use openrouter)
2. Add embeddings to users, both likes (4/5 rating) and dislikes(1/2 rating) need to be used. Use arithmetic to get this. 
3. Add second embeddings to users, use direct biencoder if possible.. 
4. use pg vector's dot product post retrieval for personalized ranking score.. 
5. use linear combination of personalized score and bm25 scoring for a final re-ranking
