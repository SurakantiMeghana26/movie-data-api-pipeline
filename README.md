# Movie Data Engineering Pipeline — AWS + API Integration

[![AWS](https://img.shields.io/badge/AWS-232F3E?style=flat&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Athena](https://img.shields.io/badge/Athena-FF9900?style=flat&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/athena/)

An end-to-end **serverless data pipeline** that ingests movie data from the OMDB REST API, transforms it with **PySpark on AWS Glue**, and exposes it for SQL analytics through **Amazon Athena**. The pipeline demonstrates API integration, JSON parsing, schema cleaning, and partitioned analytics.

## Overview

This project simulates a real-world data engineering use case:

- **Source:** OMDB API (REST API for movie metadata)
- **Storage:** Amazon S3 (medallion architecture: raw + processed zones)
- **Catalog:** AWS Glue Data Catalog
- **Transformation:** AWS Glue (PySpark) ETL job
- **Analytics:** Amazon Athena (SQL queries)

The pipeline processes 100+ popular movies and produces queryable Parquet data partitioned by decade for fast time-based analytics.

## Architecture

OMDB API (Movie Data)
↓
Python Ingestion Script
↓
Amazon S3 (raw zone - JSON Lines)
↓
AWS Glue Crawler → Glue Data Catalog
↓
AWS Glue PySpark ETL Job

Parse nested JSON
Clean messy fields ($, commas, units)
Enrich with calculated columns
Partition by decade
↓
Amazon S3 (processed zone - Parquet, partitioned)
↓
AWS Glue Crawler → Glue Data Catalog
↓
Amazon Athena (SQL Analytics)

## Tech Stack

### Cloud Services
- **Amazon S3** — Object storage (medallion architecture)
- **AWS Glue** — Data catalog, crawlers, ETL jobs
- **Amazon Athena** — Serverless SQL analytics
- **AWS IAM** — Access management

### Languages & Libraries
- **Python 3** — API ingestion script
- **PySpark** — Distributed data transformation
- **SQL** — Athena analytics queries
- **JSON** — Source data format
- **Parquet** — Optimized storage format

### External APIs
- **OMDB API** — Movie metadata (titles, ratings, box office, etc.)

## Project Structure

movie-data-pipeline/
├── README.md
├── lessons_learned.md
├── .gitignore
├── architecture/
│   └── architecture.md
├── scripts/
│   ├── fetch_movies.py          # API ingestion script
│   └── movies_etl_job.py        # PySpark Glue ETL job
├── athena_queries/
│   ├── 01_top_rated_movies.sql
│   ├── 02_movies_by_decade.sql
│   ├── 03_best_directors.sql
│   ├── 04_popular_genres.sql
│   ├── 05_era_comparison.sql
│   ├── 06_box_office_hits.sql
│   └── 07_popularity_score.sql
├── screenshots/
│   └── (AWS console screenshots)
└── movie_json/
└── (sample raw JSON data)

## Key Features

### Data Cleaning Highlights

OMDB API returns text fields that need cleaning before analytics:

| Raw Format | Cleaned Format | Solution |
|------------|----------------|----------|
| `"175 min"` | `175` | `regexp_replace` + cast to INT |
| `"$136,381,073"` | `136381073` | Strip `$` and `,`, cast to DOUBLE |
| `"1,995,567"` | `1995567` | Strip commas, cast to INT |
| `"Crime, Drama"` | `["Crime", "Drama"]` | Split on `, ` to array |
| `"N/A"` | `NULL` | `when().otherwise()` conditional |

### Enrichment Columns

The PySpark ETL adds these calculated columns:

- `decade` — Computed from year (e.g., 1972 → 1970)
- `runtime_hours` — Converted from minutes
- `primary_genre` — First genre from array
- `era` — Classic/Vintage/Modern/Contemporary
- `rating_category` — Masterpiece/Great/Good/Average/Below Average
- `popularity_tier` — Based on vote count

### Partitioning Strategy

- **Raw zone:** Single JSON Lines file
- **Processed zone:** Parquet, partitioned by `decade`
- **Result:** Athena queries can prune partitions, reducing scan costs

## How to Reproduce

### Prerequisites

- AWS account with CLI configured
- Python 3.8+
- Free OMDB API key from [omdbapi.com](http://www.omdbapi.com/apikey.aspx)

### Step 1: Set Up AWS Resources

```bash
# Create S3 bucket and folder structure
aws s3 mb s3://your-bucket-name --region us-east-1
aws s3api put-object --bucket your-bucket-name --key raw/movies/
aws s3api put-object --bucket your-bucket-name --key processed/movies/
aws s3api put-object --bucket your-bucket-name --key athena-results/

# Create Glue database (or via Console)
aws glue create-database --database-input '{"Name":"movies_db"}'
```

### Step 2: Update API Key in Script

In `scripts/fetch_movies.py`, replace:
```python
API_KEY = "YOUR_OMDB_API_KEY_HERE"
```

### Step 3: Run Ingestion

```bash
pip install requests
python scripts/fetch_movies.py
```

This fetches 100+ movies and saves to `movie_json/movies.json`.

### Step 4: Upload to S3

```bash
aws s3 cp movie_json/movies.json s3://your-bucket-name/raw/movies/
```

### Step 5: Run Glue Crawler & ETL

1. Create a Glue Crawler pointing to `s3://your-bucket-name/raw/movies/`
2. Run the crawler — creates `movies` table in Glue Catalog
3. Create a Glue ETL job using `scripts/movies_etl_job.py`
4. Run the job — produces partitioned Parquet in `processed/movies/`
5. Run another crawler on processed data — creates `processed_movies` table

### Step 6: Query in Athena

Use the SQL files in `athena_queries/` for analytical queries:

```sql
-- Find top-rated movies
SELECT title, year, director, imdb_rating
FROM processed_movies
ORDER BY imdb_rating DESC
LIMIT 10;
```

## Sample Insights

Some questions this pipeline can answer:

- 🎬 What are the highest-rated movies of all time in this dataset?
- 📅 How do average runtimes differ across decades?
- 🎥 Which directors have multiple highly-rated films?
- 🌍 Which countries produce the most acclaimed films?
- 💰 What's the relationship between box office and IMDb rating?

## Skills Demonstrated

### Data Engineering
- REST API integration in Python
- Working with semi-structured data (JSON)
- ETL/ELT design patterns
- Medallion architecture (raw → processed)
- Schema cleaning and enrichment
- Hive-style partitioning
- SQL analytics

### AWS Cloud
- Amazon S3 storage design
- AWS Glue Catalog and Crawlers
- AWS Glue PySpark ETL jobs
- Amazon Athena query optimization
- AWS IAM roles and policies

### PySpark
- Reading nested JSON
- `regexp_replace` for text cleaning
- `when().otherwise()` for conditional logic
- `withColumn` for column derivation
- Partition-aware writes

## Implementation Notes

### Why JSON Lines for Raw Data?

JSON Lines (`.jsonl`) is one JSON object per line. This format is:
- ✅ Streamable (read line-by-line)
- ✅ Easy to parse in PySpark
- ✅ Splittable across distributed workers

### Why Partition by Decade?

Most analytical queries filter or group by time. Partitioning by decade enables:
- Partition pruning (Athena scans only relevant partitions)
- 60-90% cost reduction on time-based queries
- Faster query response times

### Handling Missing Values

OMDB returns "N/A" instead of NULL for missing fields. The ETL converts these:
```python
when(col("BoxOffice") == "N/A", None).otherwise(...)
```

## Future Enhancements

- Schedule daily ingestion via AWS Lambda + EventBridge
- Add data quality validation with Great Expectations
- Build dashboards on top of Athena (Streamlit/QuickSight)
- Enrich with additional APIs (TMDB, Rotten Tomatoes)
- Implement incremental loads (only new movies)
- Use AWS Glue Data Quality framework

## Related Projects

- 🚕 [NYC Taxi ETL Pipeline](https://github.com/SurakantiMeghana26/nyc-taxi-aws-etl-piplines) — Batch ETL with 9M+ rows
- 🌬️ [Airflow AWS Orchestration](https://github.com/SurakantiMeghana26/airflow-aws-orchestration) — Pipeline orchestration

## Author

**Surakanti Meghana** — Aspiring Data Engineer


- GitHub: [@SurakantiMeghana26](https://github.com/SurakantiMeghana26)

---

⭐ If you found this useful, give it a star!