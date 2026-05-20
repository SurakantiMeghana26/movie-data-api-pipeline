# Architecture Diagram

## Movie Data Pipeline

```
┌─────────────────┐
│   OMDB API      │  External REST API
└────────┬────────┘
         │ HTTPS
         ▼
┌─────────────────┐
│ Python Script   │  Local laptop
│ fetch_movies.py │  Generates JSON
└────────┬────────┘
         │ aws s3 cp
         ▼
┌─────────────────┐
│   Amazon S3     │  raw/movies/
│   (raw zone)    │  JSON Lines format
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Glue Crawler   │  Catalogs schema
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Glue Data       │  movies_db.movies
│ Catalog         │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Glue ETL Job    │  PySpark
│ - Parse JSON    │  movies_etl_job.py
│ - Clean fields  │
│ - Enrich        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Amazon S3     │  processed/movies/
│ (processed)     │  Parquet, partitioned
└────────┬────────┘  by decade
         │
         ▼
┌─────────────────┐
│  Glue Crawler   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Glue Data       │  movies_db.processed_movies
│ Catalog         │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Amazon Athena   │  SQL Analytics
└─────────────────┘
```

## Components

- **OMDB API**: External source for movie data
- **Python Producer**: Fetches movies via REST API
- **S3 Raw Zone**: Untransformed JSON
- **Glue Catalog**: Centralized schema registry
- **Glue ETL**: PySpark transformations
- **S3 Processed Zone**: Optimized Parquet
- **Athena**: SQL queries for analytics
