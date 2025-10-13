# # import pandas as pd
# # from .bias_metrics import add_norm_citations, country_advantage, institution_score

# # def main():
# #     works = pd.read_parquet("data/curated/works.parquet")
# #     auth  = pd.read_parquet("data/curated/authorships.parquet")

# #     # Optional filters
# #     works = works[(works["year"] >= 2018) & (works["year"] <= works["year"].max())]

# #     w_norm = add_norm_citations(works)
# #     ctry = country_advantage(auth, w_norm)
# #     inst = institution_score(auth, w_norm)

# #     ctry.to_csv("data/curated/country_advantage.csv", index=False)
# #     inst.to_csv("data/curated/institution_scores.csv", index=False)
# #     print("Saved metrics CSVs.")

# # if __name__ == "__main__":
# #     main()
# # src/analytics/compute.py
# from __future__ import annotations
# from pathlib import Path
# import argparse
# import pandas as pd

# from .bias_metrics import compute_all_metrics


# def main():
#     p = argparse.ArgumentParser(description="Compute bias metrics from parquet inputs.")
#     p.add_argument("--works", type=str, default="data/curated/works.parquet")
#     p.add_argument("--auth", type=str, default="data/curated/authorships.parquet")
#     p.add_argument("--outdir", type=str, default="data/curated/metrics")
#     p.add_argument("--min-year", type=int, default=2018)
#     args = p.parse_args()

#     works = pd.read_parquet(args.works)
#     auth = pd.read_parquet(args.auth)

#     metrics = compute_all_metrics(works, auth, min_year=args.min_year)

#     outdir = Path(args.outdir)
#     outdir.mkdir(parents=True, exist_ok=True)

#     for name, df in metrics.items():
#         if isinstance(df, pd.DataFrame):
#             df.to_csv(outdir / f"{name}.csv", index=False)

#     print(f"Wrote {len(metrics)} CSVs to {outdir.resolve()}")


# if __name__ == "__main__":
#     main()


# src/analytics/compute.py
from __future__ import annotations
from pathlib import Path
import argparse
import pandas as pd
from .bias_metrics import compute_all_metrics

def main():
    p = argparse.ArgumentParser("Compute metrics from parquet files.")
    p.add_argument("--works", default="data/curated/works.parquet")
    p.add_argument("--auth",  default="data/curated/authorships.parquet")
    p.add_argument("--outdir", default="data/curated/metrics")
    p.add_argument("--min-year", type=int, default=2018)
    args = p.parse_args()

    works = pd.read_parquet(args.works)  # expects: work_id,title,year,venue,cited_by_count,source_issn_l
    auth  = pd.read_parquet(args.auth)   # expects: schema you listed

    metrics = compute_all_metrics(works, auth, min_year=args.min_year)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    for name, df in metrics.items():
        df.to_csv(outdir / f"{name}.csv", index=False)
    print(f"Wrote {len(metrics)} CSVs to {outdir.resolve()}")

if __name__ == "__main__":
    main()
