-- Popularity score (rating x votes)
SELECT 
    title,
    year,
    director,
    imdb_rating,
    imdb_votes,
    ROUND(imdb_rating * (imdb_votes / 1000000.0), 2) AS popularity_score
FROM processed_movies
WHERE imdb_votes IS NOT NULL
ORDER BY popularity_score DESC
LIMIT 15;
