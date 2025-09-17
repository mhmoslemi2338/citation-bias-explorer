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

the very first time you run the project, you need to ingest data from OpenAlex and process it into clean datasets. 
for ingesting data, you can modify the parameters in the `.env` file to control how many works to fetch.

the following commands assume you are in the root directory of the project. this is for ingesting raw and dirty data
```bash
python3 -m src.ingest.fetch_works
```

next step is to run the ETL job to flatten and curate the data. this will create parquet files in `data/curated/`
```bash
python3 -m src.etl.spark_jobs
```
