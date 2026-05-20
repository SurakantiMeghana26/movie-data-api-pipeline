-- Movies by decade analysis
SELECT 
    decade,
    COUNT(*) AS movie_count,
    ROUND(AVG(imdb_rating), 2) AS avg_rating,
    ROUND(AVG(runtime_minutes), 0) AS avg_runtime_min
FROM processed_movies
GROUP BY decade
ORDER BY decade;
