# Lessons Learned — Movie Data Pipeline

## What Went Well

### API Integration
- OMDB API was straightforward to integrate — simple authentication, JSON responses
- Free tier (1000 requests/day) was more than enough for the use case
- Python's `requests` library made API calls trivial
- Built-in rate limiting (`time.sleep(0.1)`) avoided hitting API limits

### Data Modeling
- **Decade partitioning** was a natural fit for movies (queries often time-based)
- **Enrichment columns** (era, rating_category, popularity_tier) make queries simpler
- **Primary genre extraction** (first item from array) simplified aggregations

### PySpark Transformations
- `regexp_replace` was perfect for cleaning text fields like `"$136,381,073"` → `136381073`
- `when().otherwise()` handled the "N/A" → NULL conversion elegantly
- Splitting comma-separated genres into arrays enabled flexible querying

## Challenges & Solutions

### Challenge 1: Messy Source Data

OMDB returns text fields that look numeric:
- `"175 min"` — needs to be `175`
- `"$136,381,073"` — needs to be `136381073`
- `"1,995,567"` — needs to be `1995567`

**Solution:** Used PySpark's `regexp_replace` to strip units, dollar signs, and commas before casting to numeric types.

### Challenge 2: "N/A" vs NULL

OMDB returns the string `"N/A"` instead of actual NULL for missing values.

**Solution:** Used `when().otherwise()` to convert "N/A" strings to actual NULLs:
```python
when(col("Metascore") == "N/A", None).otherwise(col("Metascore").cast(IntegerType()))
```

### Challenge 3: Nested Genre Field

The `Genre` field is `"Crime, Drama"` (comma-separated string).

**Solution:** Used `split()` to convert to an array:
```python
split(col("Genre"), ", ").alias("genres")
```

Then extracted `primary_genre` as first item: `col("genres").getItem(0)`

### Challenge 4: Partitioning Decisions

Initially considered partitioning by year, but decided on decade because:
- Most analytical queries are by decade
- Fewer partitions = better performance
- 10-year buckets are more meaningful than individual years

## Key Learnings

### Data Engineering
- **APIs are messy** — always need cleaning before analytics
- **JSON Lines format** is better than single JSON for distributed processing
- **Partitioning strategy** should match query patterns
- **Enrichment at ETL time** simplifies downstream queries

### PySpark
- `regexp_replace` is essential for text cleaning
- `withColumn` is cleaner than alias chains for derived columns
- Partition-aware writes (`partitionBy("decade")`) optimize query performance
- Always cast strings to proper types early in the pipeline

### AWS
- Glue Crawlers auto-detect schema from JSON
- Athena queries are super fast on partitioned Parquet
- Cost depends on data scanned (partition pruning matters!)
- Glue ETL jobs scale automatically with worker count

## What I'd Improve

### Pipeline Enhancements
- Add **Apache Airflow** orchestration (built this in another project!)
- **Incremental loads** — only fetch new movies, not all 100+ each run
- **Multi-API enrichment** — combine OMDB with TMDB or Rotten Tomatoes
- **Real-time ingestion** — stream movie releases as they happen

### Code Quality
- Add **unit tests** for the cleaning functions (pytest)
- Use **configuration files** instead of hardcoded values
- Add **logging** with proper severity levels
- Implement **error handling** for partial API failures

### Operations
- Set up **CI/CD pipeline** for automatic Glue job deployment
- Add **data quality checks** with Great Expectations
- Build **Streamlit dashboard** on top of Athena
- Add **monitoring** with CloudWatch alerts

## Key Takeaway

This project reinforced that **real-world data is messy**. The OMDB API returns text where numbers should be, "N/A" where NULL should be, and comma-separated strings where arrays should be.

PySpark's flexibility makes these transformations elegant. Combined with Glue's auto-scaling and Athena's serverless queries, this is a powerful (and inexpensive) data engineering stack.

The biggest learning: **focus 80% of your effort on data cleaning and structuring**. Once data is clean and well-partitioned, analytics become trivial.

## Next Project Ideas

Building on this foundation:
- **Streaming version** — Kinesis pipeline for live movie release data
- **ML recommendations** — collaborative filtering on the rating data
- **Multi-source enrichment** — combine multiple APIs for richer dataset
- **Data quality framework** — validate data at every stage