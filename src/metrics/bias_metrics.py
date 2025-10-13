# # import pandas as pd

# # def add_norm_citations(works: pd.DataFrame) -> pd.DataFrame:
# #     w = works.copy()
# #     # group mean per venue-year; add small epsilon
# #     g = w.groupby(["venue","year"])["cited_by_count"].transform(lambda s: s.mean() + 1e-6)
# #     w["norm_cites"] = w["cited_by_count"] / g
# #     return w

# # def country_advantage(auth: pd.DataFrame, works_norm: pd.DataFrame) -> pd.DataFrame:
# #     m = auth.merge(works_norm[["work_id","norm_cites"]], on="work_id", how="left")
# #     out = (m.groupby("country_code", dropna=True)["norm_cites"]
# #              .mean().reset_index()
# #              .rename(columns={"norm_cites":"country_advantage"}))
# #     return out.sort_values("country_advantage", ascending=False)

# # def institution_score(auth: pd.DataFrame, works_norm: pd.DataFrame) -> pd.DataFrame:
# #     m = auth.merge(works_norm[["work_id","norm_cites"]], on="work_id", how="left")
# #     out = (m.groupby(["institution_id","institution_name"], dropna=True)["norm_cites"]
# #              .median().reset_index()
# #              .rename(columns={"norm_cites":"institution_median_norm"}))
# #     return out.sort_values("institution_median_norm", ascending=False)

# # src/analytics/bias_metrics.py
# from __future__ import annotations
# import math
# from typing import Dict, Iterable, List, Optional, Tuple
# import numpy as np
# import pandas as pd


# # ----------------------------- helpers -----------------------------

# def _pick_first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
#     for c in candidates:
#         if c in df.columns:
#             return c
#     return None

# def _standardize_country_column(auth: pd.DataFrame) -> pd.DataFrame:
#     """Ensure there is a 'country_code' column with 2-letter codes."""
#     out = auth.copy()
#     if "country_code" not in out.columns:
#         alt = _pick_first_existing(out, ["countries", "country", "country_alpha2"])
#         if alt:
#             out = out.rename(columns={alt: "country_code"})
#         else:
#             out["country_code"] = np.nan
#     return out

# def _standardize_institution_columns(auth: pd.DataFrame) -> pd.DataFrame:
#     """Ensure there are 'institution_id' and 'institution_name' columns if possible."""
#     out = auth.copy()
#     if "institution_id" not in out.columns:
#         alt = _pick_first_existing(out, ["institution_ids", "inst_id"])
#         if alt:
#             out = out.rename(columns={alt: "institution_id"})
#     if "institution_name" not in out.columns:
#         alt = _pick_first_existing(out, ["institution_names", "affiliation", "affiliation_strings", "inst_name"])
#         if alt:
#             out = out.rename(columns={alt: "institution_name"})
#     return out

# def _winsorize(s: pd.Series, pct: float) -> pd.Series:
#     q_low, q_hi = s.quantile([pct, 1 - pct])
#     return s.clip(lower=q_low, upper=q_hi)

# def _safe_div(num: pd.Series, den: pd.Series, eps: float = 1e-9) -> pd.Series:
#     return num / (den.replace(0, np.nan) + eps)


# # ----------------------------- core transforms -----------------------------

# def add_norm_citations(
#     works: pd.DataFrame,
#     by: Iterable[str] = ("venue", "year"),
#     cite_col: str = "cited_by_count",
#     method: str = "mean",          # "mean" | "median" | "winsor_mean"
#     winsor_pct: float = 0.01,
#     min_group: int = 10,
#     out_col: str = "norm_cites",
# ) -> pd.DataFrame:
#     """
#     Normalize citations by expected value within groups (default: venue x year).
#     Produces a multiplicative index so that >1 means above group expectation.

#     If a group is too small (< min_group), it falls back to the overall-year baseline.
#     """
#     w = works.copy()
#     if cite_col not in w.columns:
#         raise KeyError(f"works is missing '{cite_col}'")

#     # overall by-year baseline for small groups
#     by_year = w.groupby("year")[cite_col].transform(lambda s: s.mean() + 1e-9)

#     # group baseline
#     def _group_stat(s: pd.Series) -> float:
#         if method == "median":
#             return float(s.median())
#         if method == "winsor_mean":
#             return float(_winsorize(s, winsor_pct).mean())
#         return float(s.mean())

#     g = w.groupby(list(by))[cite_col].transform(
#         lambda s: _group_stat(s) if len(s) >= min_group else np.nan
#     )

#     # fallback to by-year when group baseline is nan
#     baseline = g.fillna(by_year) + 1e-9
#     w[out_col] = w[cite_col] / baseline
#     return w


# # ----------------------------- metrics -----------------------------

# def country_advantage(
#     auth: pd.DataFrame,
#     works_norm: pd.DataFrame,
#     out_col: str = "country_advantage",
#     min_works: int = 50,
# ) -> pd.DataFrame:
#     """Mean normalized citations by country with counts."""
#     a = _standardize_country_column(auth)
#     m = a.merge(works_norm[["work_id", "norm_cites"]], on="work_id", how="left")
#     out = (m.groupby("country_code", dropna=True)
#              .agg(country_advantage=("norm_cites", "mean"),
#                   n_works=("work_id", "count"))
#              .reset_index())
#     out = out[out["n_works"] >= min_works]
#     return out.sort_values(out_col, ascending=False)


# def country_top_decile_share(
#     auth: pd.DataFrame,
#     works_norm: pd.DataFrame,
#     per_year: bool = True,
#     min_works: int = 50,
# ) -> pd.DataFrame:
#     """
#     Share of a country's papers that fall in top decile of normalized citations.
#     If per_year, thresholds are computed per year and then averaged.
#     """
#     a = _standardize_country_column(auth)
#     wn = works_norm.copy()

#     if per_year:
#         wn["is_top_decile"] = wn.groupby("year")["norm_cites"].transform(
#             lambda s: s >= s.quantile(0.9)
#         )
#     else:
#         thr = wn["norm_cites"].quantile(0.9)
#         wn["is_top_decile"] = wn["norm_cites"] >= thr

#     m = a.merge(wn[["work_id", "year", "is_top_decile"]], on="work_id", how="left")
#     g = (m.groupby(["country_code", "year"])["is_top_decile"]
#            .mean().reset_index(name="share_top10"))
#     out = (g.groupby("country_code")["share_top10"]
#              .mean().reset_index()
#              .rename(columns={"share_top10": "avg_share_top10"}))
#     counts = m.groupby("country_code")["work_id"].count().reset_index(name="n_works")
#     out = out.merge(counts, on="country_code", how="left")
#     out = out[out["n_works"] >= min_works]
#     return out.sort_values("avg_share_top10", ascending=False)


# # def country_trend(
# #     auth: pd.DataFrame,
# #     works_norm: pd.DataFrame,
# #     min_per_year: int = 20,
# #     keep_top_k: int = 12,
# # ) -> pd.DataFrame:
# #     """
# #     Yearly mean normalized citations per country. Filters countries that rarely appear.
# #     """
# #     a = _standardize_country_column(auth)
# #     m = a.merge(works_norm[["work_id", "year", "norm_cites"]], on="work_id", how="left")

# #     per_year = (m.groupby(["country_code", "year"])["norm_cites"]
# #                   .agg(mean_norm="mean", n=("norm_cites", "size"))
# #                   .reset_index())
# #     # keep countries that have at least min_per_year in any year
# #     keep = (per_year[per_year["n"] >= min_per_year]
# #               .groupby("country_code")["n"].sum()
# #               .nlargest(keep_top_k).index)
# #     return per_year[per_year["country_code"].isin(keep)].sort_values(["country_code", "year"])

# def country_trend(
#     auth: pd.DataFrame,
#     works_norm: pd.DataFrame,
#     min_per_year: int = 20,
#     keep_top_k: int = 12,
# ) -> pd.DataFrame:
#     """
#     Yearly mean normalized citations per country. Filters countries that rarely appear.
#     """
#     a = _standardize_country_column(auth)
#     m = a.merge(works_norm[["work_id", "year", "norm_cites"]], on="work_id", how="left")

#     per_year = (
#         m.groupby(["country_code", "year"])["norm_cites"]
#          .agg(mean_norm="mean", n="size")
#          .reset_index()
#     )

#     # keep countries that have at least min_per_year in any year (using only qualifying rows)
#     keep = (
#         per_year[per_year["n"] >= min_per_year]
#           .groupby("country_code")["n"].sum()
#           .nlargest(keep_top_k).index
#     )

#     return per_year[per_year["country_code"].isin(keep)].sort_values(["country_code", "year"])



# def institution_score(
#     auth: pd.DataFrame,
#     works_norm: pd.DataFrame,
#     min_works: int = 50
# ) -> pd.DataFrame:
#     """
#     Median normalized citations by institution with counts and p90.
#     """
#     a = _standardize_institution_columns(auth)
#     needed = _pick_first_existing(a, ["institution_name"])
#     if not needed:
#         # If there is no institution information, return empty
#         return pd.DataFrame(columns=["institution_id", "institution_name", "institution_median_norm", "p90", "n_works"])

#     m = a.merge(works_norm[["work_id", "norm_cites"]], on="work_id", how="left")
#     out = (m.groupby(["institution_id", "institution_name"], dropna=True)["norm_cites"]
#              .agg(institution_median_norm="median",
#                   p90=lambda s: s.quantile(0.90),
#                   n_works="count")
#              .reset_index())
#     out = out[out["n_works"] >= min_works]
#     return out.sort_values(["institution_median_norm", "p90"], ascending=False)


# def author_position_effect(
#     auth: pd.DataFrame,
#     works_norm: pd.DataFrame
# ) -> pd.DataFrame:
#     """
#     Mean and median normalized citations by author position if available.
#     Expected values in 'author_pos' like 'first', 'middle', 'last'.
#     """
#     if "author_pos" not in auth.columns:
#         return pd.DataFrame(columns=["author_pos", "mean_norm", "median_norm", "n"])
#     m = auth.merge(works_norm[["work_id", "norm_cites"]], on="work_id", how="left")
#     return (m.groupby("author_pos")["norm_cites"]
#               .agg(mean_norm="mean", median_norm="median", n="size")
#               .reset_index()
#               .sort_values("mean_norm", ascending=False))


# def corresponding_author_premium(
#     auth: pd.DataFrame,
#     works_norm: pd.DataFrame
# ) -> pd.DataFrame:
#     """
#     Premium for corresponding authorship, if 'is_corresponding' exists.
#     """
#     col = _pick_first_existing(auth, ["is_corresponding", "corresponding"])
#     if not col:
#         return pd.DataFrame(columns=["is_corresponding", "mean_norm", "median_norm", "n"])
#     m = auth.merge(works_norm[["work_id", "norm_cites"]], on="work_id", how="left")
#     return (m.groupby(col)["norm_cites"]
#               .agg(mean_norm="mean", median_norm="median", n="size")
#               .reset_index()
#               .rename(columns={col: "is_corresponding"})
#               .sort_values("mean_norm", ascending=False))


# def _team_stats_from_auth(auth: pd.DataFrame) -> pd.DataFrame:
#     """Return per-work team_size and n_countries."""
#     a = _standardize_country_column(auth)
#     g1 = a.groupby("work_id").size().reset_index(name="team_size")
#     g2 = (a.groupby("work_id")["country_code"]
#             .nunique(dropna=True).reset_index(name="n_countries"))
#     return g1.merge(g2, on="work_id", how="outer")


# def team_size_curve(
#     auth: pd.DataFrame,
#     works_norm: pd.DataFrame,
#     max_team: int = 50
# ) -> pd.DataFrame:
#     """
#     Median normalized citations by team size with counts.
#     """
#     ts = _team_stats_from_auth(auth)
#     m = ts.merge(works_norm[["work_id", "norm_cites"]], on="work_id", how="left")
#     m = m[m["team_size"] <= max_team]
#     return (m.groupby("team_size")["norm_cites"]
#               .agg(median_norm="median", mean_norm="mean", n="size")
#               .reset_index()
#               .sort_values("team_size"))


# def international_premium(
#     auth: pd.DataFrame,
#     works_norm: pd.DataFrame
# ) -> pd.DataFrame:
#     """
#     Compare single-country vs multi-country collaboration.
#     """
#     ts = _team_stats_from_auth(auth)
#     ts["is_international"] = ts["n_countries"].fillna(1) > 1
#     m = ts.merge(works_norm[["work_id", "norm_cites"]], on="work_id", how="left")
#     return (m.groupby("is_international")["norm_cites"]
#               .agg(mean_norm="mean", median_norm="median", n="size")
#               .reset_index()
#               .sort_values("mean_norm", ascending=False))


# def venue_rank(
#     works_norm: pd.DataFrame,
#     min_works: int = 50
# ) -> pd.DataFrame:
#     """
#     Median normalized citations by venue. Since normalization is venue x year,
#     this mainly reflects year-mix and tail behavior. Still useful for sanity checks.
#     """
#     if "venue" not in works_norm.columns:
#         return pd.DataFrame(columns=["venue", "median_norm", "p90", "n_works"])
#     out = (works_norm.groupby("venue")["norm_cites"]
#              .agg(median_norm="median", p90=lambda s: s.quantile(0.9), n_works="size")
#              .reset_index())
#     out = out[out["n_works"] >= min_works]
#     return out.sort_values(["median_norm", "p90"], ascending=False)


# def global_year_trend(works_norm: pd.DataFrame) -> pd.DataFrame:
#     """Global mean and median normalized citations by year."""
#     return (works_norm.groupby("year")["norm_cites"]
#               .agg(mean_norm="mean", median_norm="median", n="size")
#               .reset_index()
#               .sort_values("year"))


# def compute_all_metrics(
#     works: pd.DataFrame,
#     auth: pd.DataFrame,
#     min_year: Optional[int] = None
# ) -> Dict[str, pd.DataFrame]:
#     """Convenience wrapper to get everything at once."""
#     if min_year is not None and "year" in works.columns:
#         works = works[works["year"] >= int(min_year)].copy()

#     w_norm = add_norm_citations(works)

#     metrics = {
#         "country_advantage": country_advantage(auth, w_norm),
#         "country_top_decile_share": country_top_decile_share(auth, w_norm),
#         "country_trend": country_trend(auth, w_norm),
#         "institution_scores": institution_score(auth, w_norm),
#         "author_position_effect": author_position_effect(auth, w_norm),
#         "corresponding_author_premium": corresponding_author_premium(auth, w_norm),
#         "team_size_curve": team_size_curve(auth, w_norm),
#         "international_premium": international_premium(auth, w_norm),
#         "venue_rank": venue_rank(w_norm),
#         "global_year_trend": global_year_trend(w_norm),
#         "works_norm": w_norm,  # keep for distribution plots
#     }
#     return metrics


# src/analytics/bias_metrics.py
from __future__ import annotations
import numpy as np
import pandas as pd


# ---------------- helpers ----------------

def _winsorize(s: pd.Series, pct: float = 0.01) -> pd.Series:
    ql, qh = s.quantile([pct, 1 - pct])
    return s.clip(ql, qh)

def _explode_institutions(auth: pd.DataFrame) -> pd.DataFrame:
    """
    Turn 'institution_names' into one row per (work_id, institution_name).
    If there are multiple institutions separated by ';', we explode them.
    """
    a = auth.loc[:, ["work_id", "institution_names"]].copy()
    a["institution_names"] = a["institution_names"].fillna("")
    a["institution_name"] = (
        a["institution_names"]
        .astype(str)
        .str.split(";")
        .apply(lambda xs: [x.strip() for x in xs if x.strip()])
    )
    a = a.drop(columns=["institution_names"]).explode("institution_name")
    a = a.dropna(subset=["institution_name"])
    return a


# --------------- core transforms ---------------

def add_norm_citations(
    works: pd.DataFrame,
    by=("venue", "year"),
    cite_col: str = "cited_by_count",
    method: str = "mean",            # mean | median | winsor_mean
    winsor_pct: float = 0.01,
    min_group: int = 10,
    out_col: str = "norm_cites",
) -> pd.DataFrame:
    """
    Normalize citations within (venue, year). If a group is small (< min_group),
    fall back to year-only baseline.
    """
    w = works.copy()

    def _stat(s: pd.Series) -> float:
        if method == "median":
            return float(s.median())
        if method == "winsor_mean":
            return float(_winsorize(s, winsor_pct).mean())
        return float(s.mean())

    # fallback baseline: by year
    by_year = w.groupby("year")[cite_col].transform(lambda s: s.mean())

    # primary baseline: by (venue, year) where groups are large enough
    g = w.groupby(list(by))[cite_col].transform(lambda s: _stat(s) if len(s) >= min_group else np.nan)

    baseline = g.fillna(by_year) + 1e-9
    w[out_col] = w[cite_col] / baseline
    return w


# ------------------- metrics -------------------

def country_advantage(auth: pd.DataFrame, works_norm: pd.DataFrame, min_works: int = 50) -> pd.DataFrame:
    m = auth.merge(works_norm[["work_id", "norm_cites"]], on="work_id", how="left")
    out = (
        m.groupby("countries", dropna=True)["norm_cites"]
         .agg(country_advantage=("mean"))
         .reset_index()
         .rename(columns={"countries": "country_code"})
    )
    # attach counts
    counts = m.groupby("countries").size().reset_index(name="n_works").rename(columns={"countries": "country_code"})
    out = out.merge(counts, on="country_code", how="left")
    out = out[out["n_works"] >= min_works]
    return out.sort_values("country_advantage", ascending=False)


def country_top_decile_share(auth: pd.DataFrame, works_norm: pd.DataFrame, per_year: bool = True, min_works: int = 50) -> pd.DataFrame:
    wn = works_norm.copy()
    if per_year:
        wn["is_top_decile"] = wn.groupby("year")["norm_cites"].transform(lambda s: s >= s.quantile(0.9))
    else:
        thr = wn["norm_cites"].quantile(0.9)
        wn["is_top_decile"] = wn["norm_cites"] >= thr

    m = auth.merge(wn[["work_id", "year", "is_top_decile"]], on="work_id", how="left")
    gy = (
        m.groupby(["countries", "year"])["is_top_decile"]
         .mean()
         .reset_index(name="share_top10")
    )
    out = (
        gy.groupby("countries")["share_top10"]
          .mean()
          .reset_index(name="avg_share_top10")
          .rename(columns={"countries": "country_code"})
    )
    counts = m.groupby("countries").size().reset_index(name="n_works").rename(columns={"countries": "country_code"})
    out = out.merge(counts, on="country_code", how="left")
    out = out[out["n_works"] >= min_works]
    return out.sort_values("avg_share_top10", ascending=False)


def country_trend(auth: pd.DataFrame, works_norm: pd.DataFrame, min_per_year: int = 20, keep_top_k: int = 12) -> pd.DataFrame:
    m = auth.merge(works_norm[["work_id", "year", "norm_cites"]], on="work_id", how="left")
    per_year = (
        m.groupby(["countries", "year"])["norm_cites"]
         .agg(mean_norm=("mean"), n=("size"))
         .reset_index()
         .rename(columns={"countries": "country_code"})
    )
    # keep countries that have at least min_per_year in any year; then top-k by total n
    eligible = per_year[per_year["n"] >= min_per_year]
    keep = (
        eligible.groupby("country_code")["n"]
        .sum()
        .nlargest(keep_top_k)
        .index
    )
    return per_year[per_year["country_code"].isin(keep)].sort_values(["country_code", "year"])


def institution_score(auth: pd.DataFrame, works_norm: pd.DataFrame, min_works: int = 50) -> pd.DataFrame:
    inst = _explode_institutions(auth)
    if inst.empty:
        return pd.DataFrame(columns=["institution_name", "institution_median_norm", "p90", "n_works"])

    m = inst.merge(works_norm[["work_id", "norm_cites"]], on="work_id", how="left")
    out = (
        m.groupby("institution_name")["norm_cites"]
         .agg(institution_median_norm=("median"),
              p90=(lambda s: s.quantile(0.9)),
              n_works=("size"))
         .reset_index()
    )
    out = out[out["n_works"] >= min_works]
    return out.sort_values(["institution_median_norm", "p90"], ascending=False)


def author_position_effect(auth: pd.DataFrame, works_norm: pd.DataFrame) -> pd.DataFrame:
    if "author_pos" not in auth.columns:
        return pd.DataFrame(columns=["author_pos", "mean_norm", "median_norm", "n"])
    m = auth.merge(works_norm[["work_id", "norm_cites"]], on="work_id", how="left")
    return (
        m.groupby("author_pos")["norm_cites"]
         .agg(mean_norm=("mean"), median_norm=("median"), n=("size"))
         .reset_index()
         .sort_values("mean_norm", ascending=False)
    )


def corresponding_author_premium(auth: pd.DataFrame, works_norm: pd.DataFrame) -> pd.DataFrame:
    if "is_corresponding" not in auth.columns:
        return pd.DataFrame(columns=["is_corresponding", "mean_norm", "median_norm", "n"])
    m = auth.merge(works_norm[["work_id", "norm_cites"]], on="work_id", how="left")
    return (
        m.groupby("is_corresponding")["norm_cites"]
         .agg(mean_norm=("mean"), median_norm=("median"), n=("size"))
         .reset_index()
         .sort_values("mean_norm", ascending=False)
    )


def _team_stats(auth: pd.DataFrame) -> pd.DataFrame:
    g_team = auth.groupby("work_id").size().reset_index(name="team_size")
    g_cc = auth.groupby("work_id")["countries"].nunique(dropna=True).reset_index(name="n_countries")
    return g_team.merge(g_cc, on="work_id", how="outer")


def team_size_curve(auth: pd.DataFrame, works_norm: pd.DataFrame, max_team: int = 50) -> pd.DataFrame:
    ts = _team_stats(auth)
    m = ts.merge(works_norm[["work_id", "norm_cites"]], on="work_id", how="left")
    m = m[m["team_size"] <= max_team]
    return (
        m.groupby("team_size")["norm_cites"]
         .agg(median_norm=("median"), mean_norm=("mean"), n=("size"))
         .reset_index()
         .sort_values("team_size")
    )


def international_premium(auth: pd.DataFrame, works_norm: pd.DataFrame) -> pd.DataFrame:
    ts = _team_stats(auth)
    ts["is_international"] = ts["n_countries"].fillna(1) > 1
    m = ts.merge(works_norm[["work_id", "norm_cites"]], on="work_id", how="left")
    return (
        m.groupby("is_international")["norm_cites"]
         .agg(mean_norm=("mean"), median_norm=("median"), n=("size"))
         .reset_index()
         .sort_values("mean_norm", ascending=False)
    )


def venue_rank(works_norm: pd.DataFrame, min_works: int = 50) -> pd.DataFrame:
    if "venue" not in works_norm.columns:
        return pd.DataFrame(columns=["venue", "median_norm", "p90", "n_works"])
    out = (
        works_norm.groupby("venue")["norm_cites"]
         .agg(median_norm=("median"), p90=(lambda s: s.quantile(0.9)), n_works=("size"))
         .reset_index()
    )
    out = out[out["n_works"] >= min_works]
    return out.sort_values(["median_norm", "p90"], ascending=False)


def global_year_trend(works_norm: pd.DataFrame) -> pd.DataFrame:
    return (
        works_norm.groupby("year")["norm_cites"]
         .agg(mean_norm=("mean"), median_norm=("median"), n=("size"))
         .reset_index()
         .sort_values("year")
    )


def compute_all_metrics(works: pd.DataFrame, auth: pd.DataFrame, min_year: int | None = None):
    if min_year is not None:
        works = works.loc[works["year"] >= int(min_year)].copy()

    w_norm = add_norm_citations(works)

    return {
        "country_advantage": country_advantage(auth, w_norm),
        "country_top_decile_share": country_top_decile_share(auth, w_norm),
        "country_trend": country_trend(auth, w_norm),
        "institution_scores": institution_score(auth, w_norm),
        "author_position_effect": author_position_effect(auth, w_norm),
        "corresponding_author_premium": corresponding_author_premium(auth, w_norm),
        "team_size_curve": team_size_curve(auth, w_norm),
        "international_premium": international_premium(auth, w_norm),
        "venue_rank": venue_rank(w_norm),
        "global_year_trend": global_year_trend(w_norm),
        "works_norm": w_norm,
    }
