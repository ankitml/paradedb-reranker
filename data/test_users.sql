-- ========================================
-- Test Users for Personalized Re-ranking
-- ========================================
-- Creates synthetic users with extreme preferences to demonstrate
-- how personalization affects search rankings.
--
-- Usage:
--   psql -h localhost -p 5433 -U vscode -d movie -f data/test_users.sql

-- Insert test users
INSERT INTO users (user_id) VALUES (10001), (10002), (20001), (20002)
ON CONFLICT (user_id) DO NOTHING;

-- ========================================
-- Fantasy Lover (10001) - 50 fantasy movies rated 5.0
-- ========================================
INSERT INTO ratings (user_id, movie_id, rating, rating_timestamp)
SELECT 10001, movie_id, 5.0, NOW()
FROM movies 
WHERE 'Fantasy' = ANY(genres)
ORDER BY movie_id
LIMIT 50
ON CONFLICT (user_id, movie_id) DO UPDATE SET rating = EXCLUDED.rating;

-- ========================================
-- Fantasy Hater (10002) - 50 fantasy movies rated 1.0
-- ========================================
INSERT INTO ratings (user_id, movie_id, rating, rating_timestamp)
SELECT 10002, movie_id, 1.0, NOW()
FROM movies 
WHERE 'Fantasy' = ANY(genres)
ORDER BY movie_id
LIMIT 50
ON CONFLICT (user_id, movie_id) DO UPDATE SET rating = EXCLUDED.rating;

-- ========================================
-- Extreme Fantasy Lover (20001) - ALL fantasy movies rated 5.0
-- ========================================
INSERT INTO ratings (user_id, movie_id, rating, rating_timestamp)
SELECT 20001, movie_id, 5.0, NOW()
FROM movies 
WHERE 'Fantasy' = ANY(genres)
ON CONFLICT (user_id, movie_id) DO UPDATE SET rating = EXCLUDED.rating;

-- ========================================
-- Extreme Fantasy Hater (20002) - ALL fantasy movies rated 1.0
-- ========================================
INSERT INTO ratings (user_id, movie_id, rating, rating_timestamp)
SELECT 20002, movie_id, 1.0, NOW()
FROM movies 
WHERE 'Fantasy' = ANY(genres)
ON CONFLICT (user_id, movie_id) DO UPDATE SET rating = EXCLUDED.rating;

-- ========================================
-- Verification
-- ========================================
SELECT 
    u.user_id,
    CASE u.user_id
        WHEN 10001 THEN 'Fantasy Lover'
        WHEN 10002 THEN 'Fantasy Hater'
        WHEN 20001 THEN 'Extreme Fantasy Lover'
        WHEN 20002 THEN 'Extreme Fantasy Hater'
    END as description,
    COUNT(r.movie_id) as rating_count,
    AVG(r.rating) as avg_rating
FROM users u
LEFT JOIN ratings r ON u.user_id = r.user_id
WHERE u.user_id IN (10001, 10002, 20001, 20002)
GROUP BY u.user_id
ORDER BY u.user_id;
