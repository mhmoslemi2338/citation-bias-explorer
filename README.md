# Citation Bias Explorer

Analyze disparities in scholarly citation counts across **countries** and **institutions**, while controlling for **venue** and **year**.  
Built with **OpenAlex API**, **PySpark**, and **Streamlit**.

---

## 🚀 Overview

This project ingests metadata from the [OpenAlex](https://openalex.org/) API, processes it into clean datasets, and computes normalized citation metrics.  
The results are served in an interactive dashboard that allows filtering by publication year and venue.

**Pipeline:**
1. **Ingest**: Fetch scholarly works (journal articles, 2018–present) from OpenAlex API.  
2. **ETL**: Flatten nested authorship and institution data using PySpark.  
3. **Metrics**: Compute normalized citation ratios (venue–year baseline) and group-level indices.  
4. **Dashboard**: Explore disparities across countries and institutions via Streamlit + Plotly.

---

## 🛠️ Tech Stack

- **Python 3.11**
- **PySpark** for ETL
- **Pandas / Plotly** for analytics & visuals
- **Streamlit** for interactive app
- **Docker** for containerized deployment
- **GitHub Actions** for CI (pytest)

---
## Project Structure

```
citation-bias-explorer/
├─ README.md
├─ requirements.txt
├─ .gitignore
├─ .env.example
├─ Dockerfile
├─ .github/workflows/ci.yml
├─ data/
│  ├─ raw/        # raw JSONL (ignored by git)
│  └─ curated/    # parquet + CSVs (ignored by git)
├─ src/
│  ├─ ingest/     # OpenAlex client & fetch scripts
│  ├─ etl/        # Spark schema & ETL jobs
│  ├─ metrics/    # bias metrics & compute scripts
│  └─ app/        # Streamlit dashboard
└─ tests/ 
        # unit tests
```
## ⚙️ Project Configuration

This project uses environment variables to manage API settings.  
An example file `.env.example` is provided:

```env
OPENALEX_BASE=https://api.openalex.org
CONTACT_EMAIL= mhmoslemi2338@gmail.com
FROM_YEAR=2018
WORKS_MAX_PAGES=500
WORKS_PER_PAGE=200
CONCEPT_ID_FIELD=C154945302 # only get AI field
```



To set up, copy the example file and update values as needed:
```env
cp .env.example .env
```


<!-- ## Quickstart
1) Create env and install requirements  
2) Ingest OpenAlex works  
3) Run ETL to Parquet  
4) Launch Streamlit app

See sections below for exact commands. -->




## Usage

When you first set up the project, you’ll need to **ingest raw OpenAlex data** and then process it through the **ETL pipeline** to produce clean, analysis-ready Parquet datasets.

### Step 1: Ingest Raw OpenAlex Data

The ingestion script fetches works directly from the [OpenAlex API](https://openalex.org/). You can control how many works to download and where they are stored by editing the `.env` configuration file. Common parameters include:

```bash
# Example .env entries
FROM_YEAR=2023
WORKS_MAX_PAGES=500     
WORKS_PER_PAGE=200
CONCEPT_ID_FIELD=C154945302 # AI field
```

Then, from the **project root**, run:

```bash
python3 -m src.ingest.fetch_works
```

This command retrieves the data and stores it in `data/raw/works.jsonl`. The file may be large and contain nested JSON objects — that’s normal.

### Step 2: Run the ETL Job (Spark)

After ingestion, use the Spark ETL pipeline to **flatten** and **curate** the raw data. This step will transform the JSONL file into efficient Parquet datasets that can be queried or analyzed easily.

```bash
python3.11 -m src.etl.spark_job_refine \
  --input data/raw/works.jsonl \
  --works_path data/curated/works.parquet \
  --auth_path  data/curated/authorships.parquet \
  --repartition 8 \
  --print_schema
```

This creates two structured datasets under `data/curated/`:

* `works.parquet/` → flattened publication-level data
* `authorships.parquet/` → author and institution-level data

Each directory contains multiple part files (e.g., `part-00000-*.snappy.parquet`) optimized for distributed reading.


#### Arguments Explained

| Argument         | Description                                                                                                                                                    |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--input`        | Path to the raw OpenAlex `.jsonl` file (or directory of `.jsonl` files). Each line must be a valid JSON object.                                                |
| `--works_path`   | Output directory for the curated **works dataset** (Parquet format).                                                                                           |
| `--auth_path`    | Output directory for the curated **authorships dataset** (Parquet format).                                                                                     |
| `--repartition`  | Number of output part files per dataset. Use `--repartition 8` to create 8 Parquet parts for parallel read/write optimization. Set `0` to skip repartitioning. |
| `--print_schema` | Optional flag to display the automatically inferred schema for debugging.                                                                                      |

#### What It Does

1. **Reads** raw OpenAlex `works.jsonl` data via PySpark.
2. **Flattens** nested JSON fields into a clean tabular structure.
3. **Creates two datasets:**

   * `works` — core publication metadata (ID, title, year, venue, citation count, open access info, source info)
   * `authorships` — per-author relationships with institutions and countries
4. **Writes** both datasets in **Parquet** format under `data/curated/`:

```
data/curated/
├── works.parquet/           # directory of parquet part files
│   ├── part-00000-*.snappy.parquet
│   ├── part-00001-*.snappy.parquet
│   └── ...
└── authorships.parquet/
    ├── part-00000-*.snappy.parquet
    ├── part-00001-*.snappy.parquet
    └── ...
```

#### Output Details

##### Works Dataset

| Column                                                             | Description                                                                                                    |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------- |
| `work_id`                                                          | Unique OpenAlex work identifier.                                                                               |
| `title`                                                            | Work title.                                                                                                    |
| `year`                                                             | Publication year.                                                                                              |
| `venue`                                                            | Venue or source name (uses `host_venue.display_name` if present, else `primary_location.source.display_name`). |
| `cited_by_count`                                                   | Number of citations.                                                                                           |
| `is_oa`                                                            | Boolean for open-access availability.                                                                          |
| `source_id`, `source_display_name`, `source_type`, `source_issn_l` | Metadata about the journal or source.                                                                          |
| `oa_status`, `oa_url`                                              | Open-access status and link if available.                                                                      |

##### Authorships Dataset

| Column                | Description                                          |
| --------------------- | ---------------------------------------------------- |
| `work_id`             | Related work ID.                                     |
| `work_title`          | Work title.                                          |
| `author_id`           | OpenAlex author ID.                                  |
| `author_name`         | Author display name.                                 |
| `author_pos`          | Author position (first, middle, last).               |
| `is_corresponding`    | Boolean indicating corresponding author.             |
| `countries`           | Countries associated with the author’s affiliations. |
| `institution_names`   | Joined institution names (`;` separated).            |
| `affiliation_strings` | Joined raw affiliation strings (`;` separated).      |

<!-- ### Example Output Log

```text
[INFO] Inferred schema:
root
 |-- id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- publication_year: long (nullable = true)
 |-- cited_by_count: long (nullable = true)
 |-- open_access: struct (nullable = true)
 |    |-- is_oa: boolean (nullable = true)
 |    |-- oa_status: string (nullable = true)
 |    |-- oa_url: string (nullable = true)
 |-- primary_location: struct (...)
 |-- authorships: array (...)

Wrote works dataset      -> data/curated/works.parquet  (rows: 52,437)
Wrote authorships dataset-> data/curated/authorships.parquet (rows: 158,920)
``` -->

<!-- ### ✅ Notes

* Outputs are **directory-style parquet datasets** (not single `.parquet` files).
* You can read them later using:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
works = spark.read.parquet('data/curated/works.parquet')
auths = spark.read.parquet('data/curated/authorships.parquet')
```

* Schema automatically adapts if fields are missing (e.g., `host_venue`). -->
