-- Top box office hits
SELECT 
    title,
    year,
    director,
    ROUND(box_office_usd/1000000, 2) AS box_office_millions_usd,
    imdb_rating
FROM processed_movies
WHERE box_office_usd IS NOT NULL
ORDER BY box_office_usd DESC
LIMIT 15;
