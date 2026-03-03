-- ============================================================
-- Movie Analytics Data Warehouse - Example Queries
-- ============================================================


-- 1. Top 10 highest-rated movies (min 1000 votes)
SELECT
    m.title,
    m.release_year,
    f.rating,
    f.vote_count
FROM fact_movie_performance f
JOIN dim_movie m ON f.movie_id = m.movie_id
WHERE f.vote_count >= 1000
ORDER BY f.rating DESC
LIMIT 10;


-- 2. Average rating per genre
SELECT
    g.genre_name,
    ROUND(AVG(f.rating)::NUMERIC, 2) AS avg_rating,
    SUM(f.vote_count) AS total_votes
FROM fact_movie_performance f
JOIN bridge_movie_genre b ON f.movie_id = b.movie_id
JOIN dim_genre g ON b.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY avg_rating DESC;


-- 3. Number of movies released per decade
SELECT
    d.decade,
    COUNT(DISTINCT f.movie_id) AS movie_count
FROM fact_movie_performance f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.decade
ORDER BY d.decade;


-- 4. Most popular genres by number of movies
SELECT
    g.genre_name,
    COUNT(DISTINCT b.movie_id) AS movie_count
FROM bridge_movie_genre b
JOIN dim_genre g ON b.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY movie_count DESC
LIMIT 10;


-- 5. Movies with rating above average
SELECT
    m.title,
    m.release_year,
    f.rating
FROM fact_movie_performance f
JOIN dim_movie m ON f.movie_id = m.movie_id
WHERE f.rating > (SELECT AVG(rating) FROM fact_movie_performance)
ORDER BY f.rating DESC;


-- 6. Decade with the highest average rating
SELECT
    d.decade,
    ROUND(AVG(f.rating)::NUMERIC, 2) AS avg_rating,
    COUNT(*) AS movie_count
FROM fact_movie_performance f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.decade
HAVING COUNT(*) >= 10
ORDER BY avg_rating DESC
LIMIT 5;
