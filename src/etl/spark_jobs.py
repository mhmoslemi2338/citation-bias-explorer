# # # save as openalex_to_parquet.py
# # import json
# # import sys
# # from pathlib import Path
# # from typing import Any, Dict, Iterable, List, Union

# # import pandas as pd


# # def _brace_split(s: str) -> List[str]:
# #     """Split concatenated JSON objects by brace counting (handles embedded strings/escapes)."""
# #     objs, depth, start = [], 0, None
# #     in_str = False
# #     esc = False
# #     for i, ch in enumerate(s):
# #         if in_str:
# #             if esc:
# #                 esc = False
# #             elif ch == "\\":
# #                 esc = True
# #             elif ch == '"':
# #                 in_str = False
# #         else:
# #             if ch == '"':
# #                 in_str = True
# #             elif ch == "{":
# #                 if depth == 0:
# #                     start = i
# #                 depth += 1
# #             elif ch == "}":
# #                 depth -= 1
# #                 if depth == 0 and start is not None:
# #                     objs.append(s[start : i + 1])
# #                     start = None
# #     return objs


# # def _read_works(path: Union[str, Path]) -> List[Dict[str, Any]]:
# #     """Read OpenAlex works from:
# #        - JSON array file
# #        - single JSON object
# #        - concatenated JSON objects in a single file.
# #     """
# #     p = Path(path)
# #     raw = p.read_text(encoding="utf-8").strip()

# #     # Try JSON array or single JSON object
# #     try:
# #         data = json.loads(raw)
# #         if isinstance(data, dict):  # single work
# #             return [data]
# #         if isinstance(data, list):
# #             return data
# #     except Exception:
# #         pass

# #     # Fallback: concatenated objects
# #     objs = _brace_split(raw)
# #     if not objs:
# #         raise ValueError("Could not parse input as JSON array/object or concatenated JSON objects.")
# #     out = []
# #     for obj in objs:
# #         out.append(json.loads(obj))
# #     return out


# # def _jdumps(x: Any) -> str:
# #     return json.dumps(x, ensure_ascii=False)





# # def _flatten_work(w: Dict[str, Any]) -> Dict[str, Any]:
# #     pl = (w.get("primary_location") or {}) or {}
# #     src = (pl.get("source") or {}) or {}
# #     oa  = (w.get("open_access") or {}) or {}
# #     return {
# #         # core
# #         "work_id": w.get("id"),
# #         "title": w.get("title"),
# #         "publication_year": w.get("publication_year"),
# #         "cited_by_count": w.get("cited_by_count"),
# #         "source_id": src.get("id"),
# #         "source_display_name": src.get("display_name"),
# #         "source_type": src.get("type"),
# #         "source_issn_l": src.get("issn_l"),

# #     }



# # def _flatten_authorships(w: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
# #     auths = (w.get("authorships") or []) or []
# #     for a in auths:
# #         author = (a.get("author") or {}) or {}

# #         # Extract institution names
# #         institutions = a.get("institutions", []) or []
# #         institution_names = "; ".join([inst.get("display_name", "") for inst in institutions if inst.get("display_name")])

# #         # Extract affiliation strings
# #         affils = a.get("affiliations", []) or []
# #         affiliation_strings = "; ".join([aff.get("raw_affiliation_string", "") for aff in affils if aff.get("raw_affiliation_string")])

# #         yield {
# #             "work_id": w.get("id"),
# #             "work_title": w.get("title"),
# #             "author_id": author.get("id"),
# #             "author_display_name": author.get("display_name"),
# #             "author_position": a.get("author_position"),
# #             "is_corresponding": a.get("is_corresponding"),
# #             # "raw_author_name": a.get("raw_author_name"),
# #             # "author_orcid": author.get("orcid"),
# #             "countries": "; ".join((a.get("countries") or []) or []),
# #             # new flattened fields
# #             "institution_names": institution_names,
# #             "affiliation_strings": affiliation_strings,
# #             # keep raw JSON in case
# #             # "institutions_json": json.dumps(institutions, ensure_ascii=False),
# #             # "affiliations_json": json.dumps(affils, ensure_ascii=False),
# #         }


# # def write_parquet(df: pd.DataFrame, path: Union[str, Path]) -> None:
# #     # Try pyarrow, then fastparquet
# #     last_err = None
# #     for eng in ("pyarrow", "fastparquet"):
# #         try:
# #             df.to_parquet(path, index=False, engine=eng)
# #             return
# #         except Exception as e:
# #             last_err = e
# #     raise RuntimeError(
# #         "Failed to write Parquet. Install one of the engines:\n"
# #         "  pip install pyarrow\n"
# #         "  # or\n"
# #         "  pip install fastparquet\n"
# #         f"Last error: {last_err}"
# #     )


# # def convert(input_path: Union[str, Path],
# #             works_out: Union[str, Path] = "works.parquet",
# #             authorships_out: Union[str, Path] = "authorships.parquet") -> None:
# #     works_raw = _read_works(input_path)

# #     works_rows = [_flatten_work(w) for w in works_raw]
# #     auth_rows = [row for w in works_raw for row in _flatten_authorships(w)]

# #     works_df = pd.DataFrame(works_rows)
# #     auth_df = pd.DataFrame(auth_rows)

# #     write_parquet(works_df, works_out)
# #     write_parquet(auth_df, authorships_out)
# #     print(f"Saved: {works_out}  ({len(works_df)} rows)")
# #     print(f"Saved: {authorships_out}  ({len(auth_df)} rows)")


# # if __name__ == "__main__":
# #     inp = 'data/raw/works.jsonl'
# #     wout = sys.argv[2] if len(sys.argv) >= 3 else "data/curated/works.parquet"
# #     aout = sys.argv[3] if len(sys.argv) >= 4 else "data/curated/authorships.parquet"
# #     convert(inp, wout, aout)


# # ruff: noqa
# # mypy: ignore-errors
# """
# Spark ETL: OpenAlex works -> {works, authorships} parquet datasets.

# Usage:
#   python3.11 -m src.etl.spark_job_refine \
#     --input data/raw/works.jsonl \
#     --outdir data/curated \
#     --works_dir works \
#     --auth_dir authorships \
#     --repartition 8
# """
# from __future__ import annotations

# import argparse
# import os
# from pathlib import Path

# from pyspark.sql import SparkSession, DataFrame
# from pyspark.sql import functions as F
# from pyspark.sql import types as T


# def build_spark(app_name: str = "openalex-etl") -> SparkSession:
#     return (
#         SparkSession.builder.appName(app_name)
#         # Reasonable defaults; tweak for your cluster/local
#         .config("spark.sql.session.timeZone", "UTC")
#         .config("spark.sql.caseSensitive", "false")
#         .getOrCreate()
#     )


# def read_openalex_jsonl(spark: SparkSession, input_path: str) -> DataFrame:
#     """
#     Reads JSON Lines (one JSON object per line).
#     If you have concatenated JSON objects or arrays, pre-normalize to JSONL.
#     """
#     return (
#         spark.read
#         .option("mode", "PERMISSIVE")
#         .option("multiLine", "false")   # JSONL: one object per line
#         .json(input_path)
#     )


# from pyspark.sql import functions as F

# def flatten_works(df):
#     return df.select(
#         F.col("id").alias("work_id"),
#         F.col("title").alias("title"),
#         F.col("publication_year").cast("int").alias("publication_year"),
#         F.col("cited_by_count").cast("int").alias("cited_by_count"),

#         # Direct, null-safe nested field access (no coalesce/casts needed)
#         F.col("primary_location.source.id").alias("source_id"),
#         F.col("primary_location.source.display_name").alias("source_display_name"),
#         F.col("primary_location.source.type").alias("source_type"),
#         F.col("primary_location.source.issn_l").alias("source_issn_l"),

#         # Same here for open_access (Spark returns NULL if parent is NULL)
#         F.col("open_access.is_oa").alias("is_oa"),
#         F.col("open_access.oa_status").alias("oa_status"),
#         F.col("open_access.oa_url").alias("oa_url"),
#     )




# def _join_display_names(col: F.Column, field: str) -> F.Column:
#     """Helper: join array<struct> by a string field with '; '."""
#     return F.array_join(F.transform(col, lambda x: x.getItem(field)), "; ")



# from pyspark.sql import functions as F
# from pyspark.sql import DataFrame

# def flatten_authorships(df: DataFrame) -> DataFrame:
#     a = F.explode_outer(F.col("authorships")).alias("a")

#     inst_names = F.coalesce(
#         F.array_join(F.transform(F.col("a.institutions"), lambda x: x["display_name"]), "; "),
#         F.lit("")
#     )
#     aff_raws = F.coalesce(
#         F.array_join(F.transform(F.col("a.affiliations"), lambda x: x["raw_affiliation_string"]), "; "),
#         F.lit("")
#     )
#     countries = F.coalesce(
#         F.array_join(F.col("a.countries"), "; "),
#         F.lit("")
#     )

#     out = (
#         df.select(
#             F.col("id").alias("work_id"),
#             F.col("title").alias("work_title"),
#             a
#         )
#         .select(
#             "work_id", "work_title",
#             F.col("a.author.id").alias("author_id"),
#             F.col("a.author.display_name").alias("author_display_name"),
#             F.col("a.author_position").alias("author_position"),
#             F.col("a.is_corresponding").alias("is_corresponding"),
#             countries.alias("countries"),
#             inst_names.alias("institution_names"),
#             aff_raws.alias("affiliation_strings"),
#         )
#         # Optional: drop rows created by explode_outer when there are no authorships
#         # .where(F.col("author_id").isNotNull())
#     )
#     return out



# def write_parquet_dataset(
#     df: DataFrame,
#     outdir: str,
#     subdir: str,
#     partition_cols: list[str] | None = None,
#     repartition: int | None = None,
# ) -> str:
#     out_path = str(Path(outdir) / subdir)  # dataset directory
#     if repartition and repartition > 0:
#         df = df.repartition(repartition, *partition_cols) if partition_cols else df.repartition(repartition)
#     writer = df.write.mode("overwrite")
#     if partition_cols:
#         writer = writer.partitionBy(*partition_cols)
#     writer.parquet(out_path)
#     return out_path


# def parse_args() -> argparse.Namespace:
#     p = argparse.ArgumentParser(description="OpenAlex -> Parquet (Spark)")
#     p.add_argument("--input", default="data/raw/works.jsonl", help="Path to JSONL file or directory (wildcards OK)")
#     p.add_argument("--outdir", default="data/curated", help="Output directory (datasets will be subfolders)")
#     p.add_argument("--works_dir", default="works", help="Subdirectory for works parquet dataset")
#     p.add_argument("--auth_dir", default="authorships", help="Subdirectory for authorships parquet dataset")
#     p.add_argument("--repartition", type=int, default=0, help="Repartition count (0 = keep Spark default)")
#     return p.parse_args()


# def main():
#     args = parse_args()
#     Path(args.outdir).mkdir(parents=True, exist_ok=True)

#     spark = build_spark()
#     df = read_openalex_jsonl(spark, args.input)

#     works_df = flatten_works(df)
#     auth_df  = flatten_authorships(df)

#     works_out = write_parquet_dataset(
#         works_df,
#         args.outdir,
#         args.works_dir,
#         partition_cols=["publication_year"],
#         repartition=args.repartition or None,
#     )
#     auth_out = write_parquet_dataset(
#         auth_df,
#         args.outdir,
#         args.auth_dir,
#         partition_cols=None,
#         repartition=args.repartition or None,
#     )

#     print(f"Wrote works dataset -> {works_out}")
#     print(f"Wrote authorships dataset -> {auth_out}")
#     print(f"works rows: {works_df.count():,}  | authorships rows: {auth_df.count():,}")

#     spark.stop()


# if __name__ == "__main__":
#     main()


# src/etl/spark_job_refine.py
# ruff: noqa
# mypy: ignore-errors
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
        F.col("open_access.is_oa").alias("is_oa"),
        # extra but harmless/useful:
        F.col("primary_location.source.id").alias("source_id"),
        F.col("primary_location.source.display_name").alias("source_display_name"),
        F.col("primary_location.source.type").alias("source_type"),
        F.col("primary_location.source.issn_l").alias("source_issn_l"),
        F.col("open_access.oa_status").alias("oa_status"),
        F.col("open_access.oa_url").alias("oa_url"),
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
