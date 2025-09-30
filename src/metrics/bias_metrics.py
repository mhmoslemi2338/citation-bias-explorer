import pandas as pd

def add_norm_citations(works: pd.DataFrame) -> pd.DataFrame:
    w = works.copy()
    # group mean per venue-year; add small epsilon
    g = w.groupby(["venue","year"])["cited_by_count"].transform(lambda s: s.mean() + 1e-6)
    w["norm_cites"] = w["cited_by_count"] / g
    return w

def country_advantage(auth: pd.DataFrame, works_norm: pd.DataFrame) -> pd.DataFrame:
    m = auth.merge(works_norm[["work_id","norm_cites"]], on="work_id", how="left")
    out = (m.groupby("country_code", dropna=True)["norm_cites"]
             .mean().reset_index()
             .rename(columns={"norm_cites":"country_advantage"}))
    return out.sort_values("country_advantage", ascending=False)

def institution_score(auth: pd.DataFrame, works_norm: pd.DataFrame) -> pd.DataFrame:
    m = auth.merge(works_norm[["work_id","norm_cites"]], on="work_id", how="left")
    out = (m.groupby(["institution_id","institution_name"], dropna=True)["norm_cites"]
             .median().reset_index()
             .rename(columns={"norm_cites":"institution_median_norm"}))
    return out.sort_values("institution_median_norm", ascending=False)
