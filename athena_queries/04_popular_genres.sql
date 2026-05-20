-- Popular genres analysis
SELECT 
    primary_genre,
    COUNT(*) AS movies,
    ROUND(AVG(imdb_rating), 2) AS avg_rating,
    ROUND(AVG(runtime_minutes), 0) AS avg_runtime
FROM processed_movies
WHERE primary_genre IS NOT NULL
GROUP BY primary_genre
ORDER BY movies DESC;
