"""
Movie Data ETL Job
Transforms raw OMDB JSON into clean Parquet for analytics.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, regexp_replace, split, when, year, 
    to_date, lit, floor, size, round as spark_round,
    coalesce, expr
)
from pyspark.sql.types import IntegerType, DoubleType, StringType

# ============================================================
# STANDARD GLUE BOILERPLATE
# ============================================================
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# CONFIGURATION - CHANGE BUCKET NAME!
# ============================================================
BUCKET_NAME = "movie-data-de-jd2026"  
RAW_PATH = f"s3://{BUCKET_NAME}/raw/movies/"
OUTPUT_PATH = f"s3://{BUCKET_NAME}/processed/movies/"

print(f"Starting movie ETL job. Output: {OUTPUT_PATH}")

# ============================================================
# STEP 1: READ RAW JSON
# ============================================================
print("Reading raw movie data...")

raw_df = spark.read.json(RAW_PATH)
print(f"Total movies read: {raw_df.count()}")
print("\nRaw schema:")
raw_df.printSchema()

# ============================================================
# STEP 2: CLEAN AND TRANSFORM
# ============================================================
print("Cleaning and transforming...")

cleaned_df = raw_df.select(
    # Basic info
    col("Title").alias("title"),
    col("Year").cast(IntegerType()).alias("year"),
    col("Rated").alias("rating_certification"),
    col("Released").alias("released_date"),
    
    # Runtime: convert "175 min" -> 175
    regexp_replace(col("Runtime"), " min", "").cast(IntegerType()).alias("runtime_minutes"),
    
    # Genre: split "Crime, Drama" -> array
    split(col("Genre"), ", ").alias("genres"),
    
    # People
    col("Director").alias("director"),
    col("Writer").alias("writer"),
    col("Actors").alias("actors"),
    col("Country").alias("country"),
    col("Language").alias("language"),
    col("Awards").alias("awards"),
    
    # Plot
    col("Plot").alias("plot"),
    
    # IMDb data
    col("imdbRating").cast(DoubleType()).alias("imdb_rating"),
    regexp_replace(col("imdbVotes"), ",", "").cast(IntegerType()).alias("imdb_votes"),
    col("imdbID").alias("imdb_id"),
    
    # Metascore (some movies don't have it, will be null)
    when(col("Metascore") == "N/A", None)
        .otherwise(col("Metascore").cast(IntegerType()))
        .alias("metascore"),
    
    # Box Office: "$136,381,073" -> 136381073
    when(col("BoxOffice").isNull() | (col("BoxOffice") == "N/A"), None)
        .otherwise(
            regexp_replace(regexp_replace(col("BoxOffice"), "\\$", ""), ",", "")
            .cast(DoubleType())
        )
        .alias("box_office_usd"),
    
    # Type (movie, series, etc.)
    col("Type").alias("type"),
    
    # Poster URL
    col("Poster").alias("poster_url")
)

# ============================================================
# STEP 3: ADD CALCULATED COLUMNS (ENRICHMENT)
# ============================================================
print("Adding calculated columns...")

enriched_df = cleaned_df \
    .withColumn("decade", (floor(col("year") / 10) * 10).cast(IntegerType())) \
    .withColumn("runtime_hours", spark_round(col("runtime_minutes") / 60, 2)) \
    .withColumn("genre_count", size(col("genres"))) \
    .withColumn("primary_genre", col("genres").getItem(0)) \
    .withColumn(
        "rating_category",
        when(col("imdb_rating") >= 8.5, "Masterpiece")
        .when(col("imdb_rating") >= 7.5, "Great")
        .when(col("imdb_rating") >= 6.5, "Good")
        .when(col("imdb_rating") >= 5.0, "Average")
        .otherwise("Below Average")
    ) \
    .withColumn(
        "popularity_tier",
        when(col("imdb_votes") >= 1000000, "Very Popular")
        .when(col("imdb_votes") >= 500000, "Popular")
        .when(col("imdb_votes") >= 100000, "Known")
        .otherwise("Niche")
    ) \
    .withColumn(
        "era",
        when(col("year") < 1970, "Classic")
        .when(col("year") < 1990, "Vintage")
        .when(col("year") < 2010, "Modern")
        .otherwise("Contemporary")
    )

# ============================================================
# STEP 4: FILTER (Remove rows with missing critical data)
# ============================================================
print("Filtering invalid rows...")

final_df = enriched_df.filter(
    (col("title").isNotNull()) &
    (col("year").isNotNull()) &
    (col("imdb_rating").isNotNull())
)

print(f"Final movie count: {final_df.count()}")
print("\nFinal schema:")
final_df.printSchema()

# ============================================================
# STEP 5: WRITE TO PROCESSED ZONE
# ============================================================
print(f"Writing to {OUTPUT_PATH}...")

final_df.write \
    .mode("overwrite") \
    .partitionBy("decade") \
    .parquet(OUTPUT_PATH)

print("✅ Movie ETL job complete!")
job.commit()