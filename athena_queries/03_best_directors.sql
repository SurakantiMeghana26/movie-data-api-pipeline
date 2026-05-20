-- Best directors by average rating
SELECT 
    director,
    COUNT(*) AS movies_in_dataset,
    ROUND(AVG(imdb_rating), 2) AS avg_rating,
    MAX(imdb_rating) AS best_rating
FROM processed_movies
WHERE director IS NOT NULL
GROUP BY director
HAVING COUNT(*) >= 2
ORDER BY avg_rating DESC
LIMIT 10;
