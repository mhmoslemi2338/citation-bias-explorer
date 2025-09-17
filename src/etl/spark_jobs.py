import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from .schema import works_schema

def load_raw_works(spark):
    return spark.read.schema(works_schema).json("data/raw/works.jsonl")

def curate_works(df):
    return df.select(
        col("id").alias("work_id"),
        col("title"),
        col("publication_year").alias("year"),
        col("host_venue.display_name").alias("venue"),
        col("cited_by_count"),
        col("open_access.is_oa").alias("is_oa")
    )

def flatten_authorships(df):
    a = df.select(
        col("id").alias("work_id"),
        explode("authorships").alias("auth")
    )
    a = a.select(
        "work_id",
        col("auth.author.id").alias("author_id"),
        col("auth.author.display_name").alias("author_name"),
        col("auth.author_position").alias("author_pos"),
        explode("auth.institutions").alias("inst")
    )
    a = a.select(
        "work_id","author_id","author_name","author_pos",
        col("inst.id").alias("institution_id"),
        col("inst.display_name").alias("institution_name"),
        col("inst.country_code").alias("country_code"),
    )
    return a

def main():
    spark = SparkSession.builder.appName("citation_bias_etl").getOrCreate()
    df_raw = load_raw_works(spark).dropna(subset=["id"])
    works = curate_works(df_raw)
    auth = flatten_authorships(df_raw)

    os.makedirs("data/curated", exist_ok=True)
    works.write.mode("overwrite").parquet("data/curated/works.parquet")
    auth.write.mode("overwrite").parquet("data/curated/authorships.parquet")
    print("ETL complete.")
    spark.stop()

if __name__ == "__main__":
    main()
