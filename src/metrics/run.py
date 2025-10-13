# # src/analytics/run.py
# from __future__ import annotations
# from pathlib import Path
# import argparse
# import pandas as pd

# from .compute import main as compute_main
# from .bias_metrics import compute_all_metrics
# from .plots import save_all_plots


# def main():
#     p = argparse.ArgumentParser(description="End-to-end analytics: compute metrics and save plots.")
#     p.add_argument("--works", type=str, default="data/curated/works.parquet")
#     p.add_argument("--auth", type=str, default="data/curated/authorships.parquet")
#     p.add_argument("--metrics-out", type=str, default="data/curated/metrics")
#     p.add_argument("--reports-out", type=str, default="reports")
#     p.add_argument("--min-year", type=int, default=2018)
#     p.add_argument("--top-countries", type=str, nargs="*", default=None,
#                    help="Optional list like US CN GB DE FR CA etc for the trend plot")
#     args = p.parse_args()

#     metrics_out = Path(args.metrics_out)
#     reports_out = Path(args.reports_out)

#     # 1) Compute and write CSV metrics
#     works = pd.read_parquet(args.works)
#     auth = pd.read_parquet(args.auth)
#     metrics = compute_all_metrics(works, auth, min_year=args.min_year)

#     metrics_out.mkdir(parents=True, exist_ok=True)
#     for name, df in metrics.items():
#         df.to_csv(metrics_out / f"{name}.csv", index=False)

#     # 2) Render lots of plots
#     saved = save_all_plots(metrics_out, reports_out, top_countries=args.top_countries)

#     print(f"\nSaved {len(saved)} plot files to: {reports_out.resolve()}")
#     for pth in saved:
#         print(" -", pth)


# if __name__ == "__main__":
#     main()



# src/analytics/run.py
from __future__ import annotations
from pathlib import Path
import argparse
import pandas as pd
from .bias_metrics import compute_all_metrics
from .plots import save_all_plots

def main():
    ap = argparse.ArgumentParser("End-to-end analytics")
    ap.add_argument("--works", default="data/curated/works.parquet")
    ap.add_argument("--auth",  default="data/curated/authorships.parquet")
    ap.add_argument("--metrics-out", default="data/curated/metrics")
    ap.add_argument("--reports-out", default="reports")
    ap.add_argument("--min-year", type=int, default=2018)
    ap.add_argument("--top-countries", nargs="*", default=None)
    args = ap.parse_args()

    works = pd.read_parquet(args.works)
    auth  = pd.read_parquet(args.auth)

    metrics = compute_all_metrics(works, auth, min_year=args.min_year)

    mdir = Path(args.metrics_out); mdir.mkdir(parents=True, exist_ok=True)
    for name, df in metrics.items():
        df.to_csv(mdir / f"{name}.csv", index=False)

    saved = save_all_plots(mdir, Path(args.reports_out), top_countries=args.top_countries)
    print(f"Saved {len(saved)} plot files to {Path(args.reports_out).resolve()}")

if __name__ == "__main__":
    main()


