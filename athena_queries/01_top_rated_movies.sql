-- Top 10 highest-rated movies
SELECT title, year, primary_genre, director, imdb_rating, rating_category
FROM processed_movies
ORDER BY imdb_rating DESC
LIMIT 10;
