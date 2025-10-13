"""
Spark ETL: OpenAlex -> {works.parquet, authorships.parquet} (dataset directories)

Usage:
  python3.11 -m src.etl.spark_job_refine \
    --input data/raw/works.jsonl \
    --works_path data/curated/works.parquet \
    --auth_path  data/curated/authorships.parquet \
    --repartition 8
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField


# ---------------------------------------------------------------------
# Spark bootstrap
# ---------------------------------------------------------------------
def build_spark(app_name: str = "openalex-etl") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.caseSensitive", "false")
        .getOrCreate()
    )


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _has_nested(schema: StructType, path: str) -> bool:
    """
    Return True if a nested field path exists in the schema.
    Example: _has_nested(df.schema, "host_venue.display_name")
    """
    parts = path.split(".")
    cur: Optional[StructType] = schema
    for i, p in enumerate(parts):
        if not isinstance(cur, StructType):
            return False
        fields = {f.name: f for f in cur.fields}
        if p not in fields:
            return False
        f: StructField = fields[p]
        if i == len(parts) - 1:
            return True
        cur = f.dataType  # descend
    return False


# ---------------------------------------------------------------------
# Read JSONL
# ---------------------------------------------------------------------
def read_openalex_jsonl(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Reads OpenAlex works from JSONL (one JSON object per line).
    If you have a folder or wildcard, Spark handles it.
    """
    return (
        spark.read
        .option("mode", "PERMISSIVE")
        .option("multiLine", "false")
        .json(input_path)
    )


# ---------------------------------------------------------------------
# Flatten
# ---------------------------------------------------------------------
def curate_works(df: DataFrame) -> DataFrame:
    """
    Flatten works; keep simple columns + source/open_access.
    If `host_venue.display_name` is missing, fallback to primary_location.source.display_name.
    """
    # Decide venue column based on schema presence
    if _has_nested(df.schema, "host_venue.display_name"):
        venue_col = F.coalesce(
            F.col("host_venue.display_name"),
            F.col("primary_location.source.display_name"),
        )
    else:
        venue_col = F.col("primary_location.source.display_name")

    return df.select(
        F.col("id").alias("work_id"),
        F.col("title").alias("title"),
        F.col("publication_year").cast("int").alias("year"),
        venue_col.alias("venue"),
        F.col("cited_by_count").cast("int").alias("cited_by_count"),
        # F.col("open_access.is_oa").alias("is_oa"),
        # extra but harmless/useful:
        # F.col("primary_location.source.id").alias("source_id"),
        # F.col("primary_location.source.display_name").alias("source_display_name"),
        # F.col("primary_location.source.type").alias("source_type"),
        F.col("primary_location.source.issn_l").alias("source_issn_l"),
        # F.col("open_access.oa_status").alias("oa_status"),
        # F.col("open_access.oa_url").alias("oa_url"),
    )


def flatten_authorships(df: DataFrame) -> DataFrame:
    """
    Explode authorships into a tidy table; join arrays into strings.
    """
    a = F.explode_outer(F.col("authorships")).alias("a")

    countries = F.coalesce(F.array_join(F.col("a.countries"), "; "), F.lit(""))
    inst_names = F.coalesce(
        F.array_join(F.transform(F.col("a.institutions"), lambda x: x["display_name"]), "; "),
        F.lit(""),
    )
    aff_raws = F.coalesce(
        F.array_join(F.transform(F.col("a.affiliations"), lambda x: x["raw_affiliation_string"]), "; "),
        F.lit(""),
    )

    return (
        df.select(
            F.col("id").alias("work_id"),
            F.col("title").alias("work_title"),
            a,
        )
        .select(
            "work_id",
            "work_title",
            F.col("a.author.id").alias("author_id"),
            F.col("a.author.display_name").alias("author_name"),
            F.col("a.author_position").alias("author_pos"),
            F.col("a.is_corresponding").alias("is_corresponding"),
            countries.alias("countries"),
            inst_names.alias("institution_names"),
            aff_raws.alias("affiliation_strings"),
        )
    )


# ---------------------------------------------------------------------
# Write parquet dataset (directory)
# ---------------------------------------------------------------------
def write_parquet_dataset(
    df: DataFrame,
    out_path: str,
    repartition: int | None = None,
) -> str:
    """
    Writes a Spark parquet dataset to a directory path (no partitionBy).
    """
    out_dir = Path(out_path)
    out_dir.parent.mkdir(parents=True, exist_ok=True)

    if repartition and repartition > 0:
        df = df.repartition(repartition)

    df.write.mode("overwrite").parquet(str(out_dir))
    return str(out_dir)


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OpenAlex -> Parquet datasets (no partitioning)")
    p.add_argument("--input", default="data/raw/works.jsonl", help="Path to JSONL file/folder (wildcards OK)")
    p.add_argument("--works_path", default="data/curated/works.parquet", help="Directory path for works dataset")
    p.add_argument("--auth_path",  default="data/curated/authorships.parquet", help="Directory path for authorships dataset")
    p.add_argument("--repartition", type=int, default=0, help="Target number of part files (0 = leave as-is)")
    p.add_argument("--print_schema", action="store_true", help="Print inferred schema (debug)")
    return p.parse_args()


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    args = parse_args()
    spark = build_spark()

    df_raw = read_openalex_jsonl(spark, args.input).dropna(subset=("id",))

    if args.print_schema:
        print("\n[INFO] Inferred schema:")
        df_raw.printSchema()

    works_df = curate_works(df_raw)
    auth_df  = flatten_authorships(df_raw)

    works_out = write_parquet_dataset(works_df, args.works_path, args.repartition or None)
    auth_out  = write_parquet_dataset(auth_df,  args.auth_path,  args.repartition or None)

    print(f"Wrote works dataset      -> {works_out}  (rows: {works_df.count():,})")
    print(f"Wrote authorships dataset-> {auth_out}   (rows: {auth_df.count():,})")

    spark.stop()


if __name__ == "__main__":
    main()
