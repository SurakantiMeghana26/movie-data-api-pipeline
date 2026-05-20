-- Era comparison (Classic vs Vintage vs Modern vs Contemporary)
SELECT 
    era,
    COUNT(*) AS movies,
    ROUND(AVG(imdb_rating), 2) AS avg_rating,
    ROUND(AVG(runtime_minutes), 0) AS avg_runtime
FROM processed_movies
GROUP BY era;
