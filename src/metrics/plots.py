# # # # # import plotly.express as px
# # # # # import pandas as pd

# # # # # def country_bar(ctry: pd.DataFrame, top_n: int = 30):
# # # # #     df = ctry.dropna().head(top_n)
# # # # #     fig = px.bar(df, x="country_code", y="country_advantage", title="Country Advantage Index")
# # # # #     return fig

# # # # # def institution_table(inst: pd.DataFrame, top_n: int = 50):
# # # # #     return inst.head(top_n)

# # # # # src/analytics/plot.py
# # # # from __future__ import annotations
# # # # from pathlib import Path
# # # # from typing import Dict, List, Optional
# # # # import numpy as np
# # # # import pandas as pd
# # # # import plotly.express as px
# # # # import plotly.graph_objects as go


# # # # def _save_fig(fig, out_html: Path, out_png: Optional[Path] = None) -> List[Path]:
# # # #     out_html.parent.mkdir(parents=True, exist_ok=True)
# # # #     fig.write_html(str(out_html), include_plotlyjs="cdn")
# # # #     saved = [out_html]
# # # #     if out_png is not None:
# # # #         try:
# # # #             # requires `pip install -U kaleido`
# # # #             fig.write_image(str(out_png), scale=2, width=1200, height=720)
# # # #             saved.append(out_png)
# # # #         except Exception:
# # # #             pass
# # # #     return saved


# # # # # ----------------------------- plotters -----------------------------

# # # # def country_bar(ctry: pd.DataFrame, top_n: int = 30):
# # # #     df = ctry.dropna().sort_values("country_advantage", ascending=False).head(top_n)
# # # #     fig = px.bar(df, x="country_code", y="country_advantage",
# # # #                  title="Country advantage (mean normalized citations)")
# # # #     return fig

# # # # def country_top_decile_bar(ctry_dec: pd.DataFrame, top_n: int = 30):
# # # #     df = ctry_dec.sort_values("avg_share_top10", ascending=False).head(top_n)
# # # #     fig = px.bar(df, x="country_code", y="avg_share_top10",
# # # #                  title="Country share of top-decile papers")
# # # #     return fig

# # # # def country_trend_lines(ctry_trend: pd.DataFrame, countries: Optional[List[str]] = None):
# # # #     df = ctry_trend.copy()
# # # #     if countries:
# # # #         df = df[df["country_code"].isin(countries)]
# # # #     fig = px.line(df, x="year", y="mean_norm", color="country_code",
# # # #                   markers=True, title="Country trend of mean normalized citations")
# # # #     return fig

# # # # def institution_table(inst: pd.DataFrame, top_n: int = 50):
# # # #     return inst.sort_values("institution_median_norm", ascending=False).head(top_n)

# # # # def norm_hist(works_norm: pd.DataFrame, max_x: float = 5.0):
# # # #     df = works_norm.copy()
# # # #     df = df[np.isfinite(df["norm_cites"])]
# # # #     df = df[df["norm_cites"] <= max_x]
# # # #     fig = px.histogram(df, x="norm_cites", nbins=80, title="Distribution of normalized citations",
# # # #                        marginal="box")
# # # #     return fig

# # # # def team_size_curve_plot(ts: pd.DataFrame):
# # # #     fig = px.line(ts, x="team_size", y="median_norm", markers=True,
# # # #                   title="Median normalized citations vs team size")
# # # #     fig.add_trace(go.Scatter(x=ts["team_size"], y=ts["mean_norm"], mode="lines+markers",
# # # #                              name="mean"))
# # # #     return fig

# # # # def international_premium_bar(df: pd.DataFrame):
# # # #     m = df.assign(group=lambda d: np.where(d["is_international"], "International", "Single-country"))
# # # #     fig = px.bar(m, x="group", y="mean_norm",
# # # #                  title="International collaboration premium (mean normalized citations)")
# # # #     return fig

# # # # def author_pos_box(auth_eff: pd.DataFrame):
# # # #     if auth_eff.empty:
# # # #         return None
# # # #     fig = px.bar(auth_eff, x="author_pos", y="mean_norm",
# # # #                  title="Author position effect (mean normalized citations)")
# # # #     return fig

# # # # def corr_author_premium_bar(df: pd.DataFrame):
# # # #     if df.empty:
# # # #         return None
# # # #     m = df.assign(group=lambda d: np.where(d["is_corresponding"], "Corresponding", "Not-corresponding"))
# # # #     fig = px.bar(m, x="group", y="mean_norm",
# # # #                  title="Corresponding author premium")
# # # #     return fig

# # # # def venue_rank_bar(vr: pd.DataFrame, top_n: int = 40):
# # # #     df = vr.sort_values("median_norm", ascending=False).head(top_n)
# # # #     fig = px.bar(df, x="venue", y="median_norm",
# # # #                  title="Venue median normalized citations (top)")
# # # #     fig.update_layout(xaxis={'categoryorder':'total descending'})
# # # #     return fig

# # # # def global_year_trend_plot(df: pd.DataFrame):
# # # #     fig = px.line(df, x="year", y=["mean_norm", "median_norm"], markers=True,
# # # #                   title="Global trend of normalized citations")
# # # #     return fig


# # # # # ----------------------------- save bundles -----------------------------

# # # # def save_all_plots(
# # # #     metrics_dir: Path,
# # # #     reports_dir: Path,
# # # #     top_countries: Optional[List[str]] = None
# # # # ) -> List[Path]:
# # # #     """
# # # #     Load CSVs from metrics_dir and write a rich set of HTML (and PNG if kaleido).
# # # #     """
# # # #     paths = []
# # # #     m = lambda name: pd.read_csv(metrics_dir / f"{name}.csv")

# # # #     cadv = m("country_advantage")
# # # #     cdec = m("country_top_decile_share")
# # # #     ctrd = m("country_trend")
# # # #     inst = m("institution_scores")
# # # #     apos = m("author_position_effect")
# # # #     corr = m("corresponding_author_premium")
# # # #     tscv = m("team_size_curve")
# # # #     intl = m("international_premium")
# # # #     vrnk = m("venue_rank")
# # # #     ytrd = m("global_year_trend")
# # # #     wnorm = m("works_norm")

# # # #     reports_dir.mkdir(parents=True, exist_ok=True)

# # # #     figs = [
# # # #         ("country_advantage_bar", country_bar(cadv)),
# # # #         ("country_top_decile_bar", country_top_decile_bar(cdec)),
# # # #         ("country_trend", country_trend_lines(ctrd, top_countries)),
# # # #         ("norm_hist", norm_hist(wnorm)),
# # # #         ("team_size_curve", team_size_curve_plot(tscv)),
# # # #         ("international_premium", international_premium_bar(intl)),
# # # #         ("venue_rank", venue_rank_bar(vrnk)),
# # # #         ("global_year_trend", global_year_trend_plot(ytrd)),
# # # #     ]

# # # #     if not apos.empty:
# # # #         figs.append(("author_position_effect", author_pos_box(apos)))
# # # #     if not corr.empty:
# # # #         figs.append(("corresponding_author_premium", corr_author_premium_bar(corr)))

# # # #     for name, fig in figs:
# # # #         if fig is None:
# # # #             continue
# # # #         paths += _save_fig(
# # # #             fig,
# # # #             out_html=reports_dir / f"{name}.html",
# # # #             out_png=reports_dir / f"{name}.png"
# # # #         )

# # # #     # also save a top institutions table for quick look
# # # #     inst.head(200).to_csv(reports_dir / "institution_top200.csv", index=False)
# # # #     return paths


# # # # src/analytics/plot.py

# # # from __future__ import annotations
# # # from pathlib import Path
# # # from typing import List, Optional
# # # import numpy as np
# # # import pandas as pd
# # # import plotly.express as px
# # # import plotly.graph_objects as go

# # # # def _save(fig, html_path: Path, png_path: Optional[Path] = None) -> List[Path]:
# # # #     html_path.parent.mkdir(parents=True, exist_ok=True)
# # # #     fig.write_html(str(html_path), include_plotlyjs="cdn")
# # # #     out = [html_path]
# # # #     if png_path is not None:
# # # #         try:
# # # #             fig.write_image(str(png_path), width=1280, height=720, scale=2)  # needs kaleido
# # # #             out.append(png_path)
# # # #         except Exception:
# # # #             pass
# # # #     return out



# # # def _save(fig, html_path: Path, png_path: Optional[Path] = None,
# # #           *, width: int = 1280, height: int = 720, scale: int = 2) -> List[Path]:
# # #     html_path.parent.mkdir(parents=True, exist_ok=True)
# # #     fig.write_html(str(html_path), include_plotlyjs="cdn")
# # #     out = [html_path]

# # #     if png_path is not None:
# # #         png_path.parent.mkdir(parents=True, exist_ok=True)
# # #         # Either approach works; both rely on kaleido under the hood.

# # #         # A) Direct writer (infers format from extension)
# # #         fig.write_image(str(png_path), width=width, height=height, scale=scale, engine="kaleido")

# # #         # B) Or via bytes:
# # #         # img_bytes = pio.to_image(fig, format="png", width=width, height=height, scale=scale, engine="kaleido")
# # #         # png_path.write_bytes(img_bytes)

# # #         out.append(png_path)

# # #     return out



# # # def country_bar(df: pd.DataFrame, top_n: int = 30):
# # #     d = df.sort_values("country_advantage", ascending=False).head(top_n)
# # #     return px.bar(d, x="country_code", y="country_advantage",
# # #                   title="Country advantage (mean normalized citations)")

# # # def country_top_decile_bar(df: pd.DataFrame, top_n: int = 30):
# # #     d = df.sort_values("avg_share_top10", ascending=False).head(top_n)
# # #     return px.bar(d, x="country_code", y="avg_share_top10",
# # #                   title="Country share of top-decile papers")

# # # def country_trend_lines(df: pd.DataFrame, countries: Optional[List[str]] = None):
# # #     d = df.copy()
# # #     if countries:
# # #         d = d[d["country_code"].isin(countries)]
# # #     return px.line(d, x="year", y="mean_norm", color="country_code", markers=True,
# # #                    title="Country trend of mean normalized citations")

# # # def institution_table(df: pd.DataFrame, top_n: int = 50):
# # #     return df.sort_values("institution_median_norm", ascending=False).head(top_n)

# # # def norm_hist(wnorm: pd.DataFrame, xmax: float = 5.0):
# # #     d = wnorm[np.isfinite(wnorm["norm_cites"])].copy()
# # #     d = d[d["norm_cites"] <= xmax]
# # #     return px.histogram(d, x="norm_cites", nbins=80, marginal="box",
# # #                         title="Distribution of normalized citations")

# # # def team_size_curve_plot(df: pd.DataFrame):
# # #     fig = px.line(df, x="team_size", y="median_norm", markers=True,
# # #                   title="Median normalized citations vs team size")
# # #     fig.add_trace(go.Scatter(x=df["team_size"], y=df["mean_norm"], mode="lines+markers", name="mean"))
# # #     return fig

# # # def international_premium_bar(df: pd.DataFrame):
# # #     d = df.assign(group=lambda x: np.where(x["is_international"], "International", "Single-country"))
# # #     return px.bar(d, x="group", y="mean_norm",
# # #                   title="International collaboration premium (mean normalized citations)")

# # # def author_pos_bar(df: pd.DataFrame):
# # #     if df.empty: return None
# # #     return px.bar(df, x="author_pos", y="mean_norm",
# # #                   title="Author position effect (mean normalized citations)")

# # # def corr_premium_bar(df: pd.DataFrame):
# # #     if df.empty: return None
# # #     d = df.assign(group=lambda x: np.where(x["is_corresponding"], "Corresponding", "Not-corresponding"))
# # #     return px.bar(d, x="group", y="mean_norm", title="Corresponding author premium")

# # # def venue_rank_bar(df: pd.DataFrame, top_n: int = 40):
# # #     d = df.sort_values("median_norm", ascending=False).head(top_n)
# # #     fig = px.bar(d, x="venue", y="median_norm", title="Venue median normalized citations (top)")
# # #     fig.update_layout(xaxis=dict(categoryorder="total descending"))
# # #     return fig

# # # def global_year_trend_plot(df: pd.DataFrame):
# # #     return px.line(df, x="year", y=["mean_norm", "median_norm"], markers=True,
# # #                    title="Global trend of normalized citations")

# # # def save_all_plots(metrics_dir: Path, reports_dir: Path, top_countries: Optional[List[str]] = None) -> List[Path]:
# # #     read = lambda name: pd.read_csv(metrics_dir / f"{name}.csv")
# # #     cadv = read("country_advantage")
# # #     cdec = read("country_top_decile_share")
# # #     ctrd = read("country_trend")
# # #     inst = read("institution_scores")
# # #     apos = read("author_position_effect")
# # #     corr = read("corresponding_author_premium")
# # #     tscv = read("team_size_curve")
# # #     intl = read("international_premium")
# # #     vrnk = read("venue_rank")
# # #     ytrd = read("global_year_trend")
# # #     wnorm = read("works_norm")

# # #     reports_dir.mkdir(parents=True, exist_ok=True)
# # #     paths: List[Path] = []

# # #     figs = [
# # #         ("country_advantage_bar", country_bar(cadv)),
# # #         ("country_top_decile_bar", country_top_decile_bar(cdec)),
# # #         ("country_trend",        country_trend_lines(ctrd, top_countries)),
# # #         ("norm_hist",            norm_hist(wnorm)),
# # #         ("team_size_curve",      team_size_curve_plot(tscv)),
# # #         ("international_premium",international_premium_bar(intl)),
# # #         ("venue_rank",           venue_rank_bar(vrnk)),
# # #         ("global_year_trend",    global_year_trend_plot(ytrd)),
# # #     ]
# # #     if not apos.empty: figs.append(("author_position_effect", author_pos_bar(apos)))
# # #     if not corr.empty: figs.append(("corresponding_author_premium", corr_premium_bar(corr)))

# # #     for name, fig in figs:
# # #         if fig is None: continue
# # #         paths += _save(fig, reports_dir / f"{name}.html", reports_dir / f"{name}.png")

# # #     # convenience export
# # #     institution_table(inst, 200).to_csv(reports_dir / "institution_top200.csv", index=False)
# # #     return paths


# # from __future__ import annotations
# # from pathlib import Path
# # from typing import List, Optional
# # import numpy as np
# # import pandas as pd
# # import plotly.express as px
# # import plotly.graph_objects as go
# # import plotly.io as pio
# # from PIL import Image
# # import io

# # def _save_png(fig, png_path: Path, *, width: int = 1280, height: int = 720, scale: int = 2) -> Path:
# #     png_path.parent.mkdir(parents=True, exist_ok=True)
# #     # Generate PNG bytes directly using Plotly's built-in image renderer
# #     img_bytes = fig.to_image(format="png", width=width, height=height, scale=scale)
# #     Image.open(io.BytesIO(img_bytes)).save(png_path, "PNG")
# #     return png_path

# # def save_all_plots(metrics_dir: Path, reports_dir: Path, top_countries: Optional[List[str]] = None) -> List[Path]:
# #     read = lambda name: pd.read_csv(metrics_dir / f"{name}.csv")
# #     cadv = read("country_advantage")
# #     cdec = read("country_top_decile_share")
# #     ctrd = read("country_trend")
# #     inst = read("institution_scores")
# #     apos = read("author_position_effect")
# #     corr = read("corresponding_author_premium")
# #     tscv = read("team_size_curve")
# #     intl = read("international_premium")
# #     vrnk = read("venue_rank")
# #     ytrd = read("global_year_trend")
# #     wnorm = read("works_norm")

# #     reports_dir.mkdir(parents=True, exist_ok=True)
# #     paths: List[Path] = []

# #     figs = [
# #         ("country_advantage_bar", px.bar(cadv.sort_values("country_advantage", ascending=False).head(30),
# #                                          x="country_code", y="country_advantage",
# #                                          title="Country advantage (mean normalized citations)")),
# #         ("country_top_decile_bar", px.bar(cdec.sort_values("avg_share_top10", ascending=False).head(30),
# #                                           x="country_code", y="avg_share_top10",
# #                                           title="Country share of top-decile papers")),
# #         ("country_trend", px.line(ctrd if top_countries is None else ctrd[ctrd["country_code"].isin(top_countries)],
# #                                   x="year", y="mean_norm", color="country_code", markers=True,
# #                                   title="Country trend of mean normalized citations")),
# #         ("norm_hist", px.histogram(wnorm[wnorm["norm_cites"].le(5.0) & np.isfinite(wnorm["norm_cites"])],
# #                                    x="norm_cites", nbins=80, marginal="box",
# #                                    title="Distribution of normalized citations")),
# #         ("team_size_curve", px.line(tscv, x="team_size", y="median_norm", markers=True,
# #                                     title="Median normalized citations vs team size")),
# #         ("international_premium", px.bar(intl.assign(
# #             group=lambda x: np.where(x["is_international"], "International", "Single-country")),
# #             x="group", y="mean_norm",
# #             title="International collaboration premium (mean normalized citations)")),
# #         ("venue_rank", px.bar(vrnk.sort_values("median_norm", ascending=False).head(40),
# #                               x="venue", y="median_norm",
# #                               title="Venue median normalized citations (top)")),
# #         ("global_year_trend", px.line(ytrd, x="year", y=["mean_norm", "median_norm"], markers=True,
# #                                       title="Global trend of normalized citations")),
# #     ]

# #     if not apos.empty:
# #         figs.append(("author_position_effect", px.bar(apos, x="author_pos", y="mean_norm",
# #                                                      title="Author position effect (mean normalized citations)")))
# #     if not corr.empty:
# #         figs.append(("corresponding_author_premium",
# #                      px.bar(corr.assign(group=lambda x: np.where(x["is_corresponding"], "Corresponding", "Not-corresponding")),
# #                             x="group", y="mean_norm", title="Corresponding author premium")))

# #     for name, fig in figs:
# #         if fig is None:
# #             continue
# #         paths.append(_save_png(fig, reports_dir / f"{name}.png"))

# #     pd.DataFrame(inst.sort_values("institution_median_norm", ascending=False).head(200))\
# #         .to_csv(reports_dir / "institution_top200.csv", index=False)

# #     return paths


# from __future__ import annotations
# from pathlib import Path
# from typing import List, Optional
# import numpy as np
# import pandas as pd
# import matplotlib.pyplot as plt

# def _save_png(fig, path: Path) -> Path:
#     path.parent.mkdir(parents=True, exist_ok=True)
#     fig.savefig(path, dpi=300, bbox_inches="tight")
#     plt.close(fig)
#     return path


# def country_bar(df: pd.DataFrame, top_n: int = 30):
#     d = df.sort_values("country_advantage", ascending=False).head(top_n)
#     fig, ax = plt.subplots(figsize=(10, 6))
#     ax.bar(d["country_code"], d["country_advantage"], color="steelblue")
#     ax.set_title("Country advantage (mean normalized citations)")
#     ax.set_xlabel("Country")
#     ax.set_ylabel("Mean normalized citations")
#     plt.xticks(rotation=45, ha="right")
#     return fig


# def country_top_decile_bar(df: pd.DataFrame, top_n: int = 30):
#     d = df.sort_values("avg_share_top10", ascending=False).head(top_n)
#     fig, ax = plt.subplots(figsize=(10, 6))
#     ax.bar(d["country_code"], d["avg_share_top10"], color="seagreen")
#     ax.set_title("Country share of top-decile papers")
#     ax.set_xlabel("Country")
#     ax.set_ylabel("Share of top 10% papers")
#     plt.xticks(rotation=45, ha="right")
#     return fig


# def country_trend_lines(df: pd.DataFrame, countries: Optional[List[str]] = None):
#     d = df.copy()
#     if countries:
#         d = d[d["country_code"].isin(countries)]
#     fig, ax = plt.subplots(figsize=(10, 6))
#     for c in d["country_code"].unique():
#         sub = d[d["country_code"] == c]
#         ax.plot(sub["year"], sub["mean_norm"], marker="o", label=c)
#     ax.set_title("Country trend of mean normalized citations")
#     ax.set_xlabel("Year")
#     ax.set_ylabel("Mean normalized citations")
#     ax.legend()
#     return fig


# def norm_hist(wnorm: pd.DataFrame, xmax: float = 5.0):
#     d = wnorm[np.isfinite(wnorm["norm_cites"])].copy()
#     d = d[d["norm_cites"] <= xmax]
#     fig, ax = plt.subplots(figsize=(10, 6))
#     ax.hist(d["norm_cites"], bins=80, color="gray", edgecolor="black")
#     ax.set_title("Distribution of normalized citations")
#     ax.set_xlabel("Normalized citations")
#     ax.set_ylabel("Count")
#     return fig


# def team_size_curve_plot(df: pd.DataFrame):
#     fig, ax = plt.subplots(figsize=(10, 6))
#     ax.plot(df["team_size"], df["median_norm"], marker="o", label="Median")
#     ax.plot(df["team_size"], df["mean_norm"], marker="s", label="Mean")
#     ax.set_title("Median normalized citations vs team size")
#     ax.set_xlabel("Team size")
#     ax.set_ylabel("Normalized citations")
#     ax.legend()
#     return fig


# def save_all_plots(metrics_dir: Path, reports_dir: Path, top_countries: Optional[List[str]] = None) -> List[Path]:
#     read = lambda name: pd.read_csv(metrics_dir / f"{name}.csv")
#     cadv = read("country_advantage")
#     cdec = read("country_top_decile_share")
#     ctrd = read("country_trend")
#     tscv = read("team_size_curve")
#     wnorm = read("works_norm")

#     reports_dir.mkdir(parents=True, exist_ok=True)
#     paths: List[Path] = []

#     figs = [
#         ("country_advantage_bar", country_bar(cadv)),
#         ("country_top_decile_bar", country_top_decile_bar(cdec)),
#         ("country_trend", country_trend_lines(ctrd, top_countries)),
#         ("norm_hist", norm_hist(wnorm)),
#         ("team_size_curve", team_size_curve_plot(tscv)),
#     ]

#     for name, fig in figs:
#         if fig is None:
#             continue
#         paths.append(_save_png(fig, reports_dir / f"{name}.png"))

#     return paths

# src/analytics/plot.py
from __future__ import annotations
from pathlib import Path
from typing import List, Optional, Callable, Tuple
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ------------------------- IO helpers -------------------------

def _save_png(fig, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return path

def _maybe_read(csv_path: Path) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(csv_path)
        if df is None or df.empty:
            return None
        return df
    except FileNotFoundError:
        return None


# --------------------------- plots ----------------------------

def country_bar(df: pd.DataFrame, top_n: int = 30):
    d = df.sort_values("country_advantage", ascending=False).head(top_n)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(d["country_code"], d["country_advantage"])
    ax.set_title("Country advantage (mean normalized citations)")
    ax.set_xlabel("Country")
    ax.set_ylabel("Mean normalized citations")
    plt.xticks(rotation=45, ha="right")
    return fig

def country_top_decile_bar(df: pd.DataFrame, top_n: int = 30):
    d = df.sort_values("avg_share_top10", ascending=False).head(top_n)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(d["country_code"], d["avg_share_top10"])
    ax.set_title("Country share of top-decile papers")
    ax.set_xlabel("Country")
    ax.set_ylabel("Share of top 10% papers")
    plt.xticks(rotation=45, ha="right")
    return fig

def country_trend_lines(df: pd.DataFrame, countries: Optional[List[str]] = None):
    d = df.copy()
    if countries:
        d = d[d["country_code"].isin(countries)]
    fig, ax = plt.subplots(figsize=(10, 6))
    for c in d["country_code"].unique():
        sub = d[d["country_code"] == c]
        ax.plot(sub["year"], sub["mean_norm"], marker="o", label=str(c))
    ax.set_title("Country trend of mean normalized citations")
    ax.set_xlabel("Year")
    ax.set_ylabel("Mean normalized citations")
    if d["country_code"].nunique() <= 20:
        ax.legend(ncol=2, fontsize=8)
    return fig

def norm_hist(wnorm: pd.DataFrame, xmax: float = 5.0):
    d = wnorm[np.isfinite(wnorm["norm_cites"])].copy()
    d = d[d["norm_cites"] <= xmax]
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(d["norm_cites"], bins=80, edgecolor="black")
    ax.set_title("Distribution of normalized citations")
    ax.set_xlabel("Normalized citations")
    ax.set_ylabel("Count")
    return fig

def team_size_curve_plot(df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(df["team_size"], df["median_norm"], marker="o", label="Median")
    ax.plot(df["team_size"], df["mean_norm"], marker="s", label="Mean")
    ax.set_title("Normalized citations vs team size")
    ax.set_xlabel("Team size")
    ax.set_ylabel("Normalized citations")
    ax.legend()
    return fig

def international_premium_bar(df: pd.DataFrame):
    d = df.copy()
    d["group"] = np.where(d["is_international"], "International", "Single-country")
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.bar(d["group"], d["mean_norm"])
    ax.set_title("International collaboration premium (mean normalized citations)")
    ax.set_xlabel("")
    ax.set_ylabel("Mean normalized citations")
    return fig

def author_pos_bar(df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(8, 5))
    order = ["first", "middle", "last"]
    # Keep provided order if column contains other labels
    d = df.copy()
    if set(order).issuperset(set(d["author_pos"].unique())):
        d["__ord__"] = d["author_pos"].map({k:i for i,k in enumerate(order)})
        d = d.sort_values("__ord__")
    ax.bar(d["author_pos"], d["mean_norm"])
    ax.set_title("Author position effect (mean normalized citations)")
    ax.set_xlabel("Author position")
    ax.set_ylabel("Mean normalized citations")
    return fig

def corr_premium_bar(df: pd.DataFrame):
    d = df.copy()
    d["group"] = np.where(d["is_corresponding"], "Corresponding", "Not-corresponding")
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.bar(d["group"], d["mean_norm"])
    ax.set_title("Corresponding author premium")
    ax.set_xlabel("")
    ax.set_ylabel("Mean normalized citations")
    return fig

def venue_rank_bar(df: pd.DataFrame, top_n: int = 40):
    d = df.sort_values("median_norm", ascending=False).head(top_n)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(d["venue"], d["median_norm"])
    ax.set_title("Venue median normalized citations (top)")
    ax.set_xlabel("Venue")
    ax.set_ylabel("Median normalized citations")
    plt.xticks(rotation=45, ha="right")
    return fig

def global_year_trend_plot(df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(df["year"], df["mean_norm"], marker="o", label="Mean")
    ax.plot(df["year"], df["median_norm"], marker="s", label="Median")
    ax.set_title("Global trend of normalized citations")
    ax.set_xlabel("Year")
    ax.set_ylabel("Normalized citations")
    ax.legend()
    return fig


# ---------------------- orchestrator ---------------------------

def save_all_plots(
    metrics_dir: Path,
    reports_dir: Path,
    top_countries: Optional[List[str]] = None
) -> List[Path]:
    reports_dir.mkdir(parents=True, exist_ok=True)
    out_paths: List[Path] = []

    # (name, csv_file, plot_func, kwargs)
    plan: List[Tuple[str, str, Callable, dict]] = [
        ("country_advantage_bar",      "country_advantage.csv",       country_bar,               {}),
        ("country_top_decile_bar",     "country_top_decile_share.csv",country_top_decile_bar,    {}),
        ("country_trend",              "country_trend.csv",           country_trend_lines,       {"countries": top_countries}),
        ("norm_hist",                  "works_norm.csv",              norm_hist,                 {}),
        ("team_size_curve",            "team_size_curve.csv",         team_size_curve_plot,      {}),
        ("international_premium",      "international_premium.csv",   international_premium_bar, {}),
        ("author_position_effect",     "author_position_effect.csv",  author_pos_bar,            {}),
        ("corresponding_author_premium","corresponding_author_premium.csv", corr_premium_bar,   {}),
        ("venue_rank",                 "venue_rank.csv",              venue_rank_bar,            {}),
        ("global_year_trend",          "global_year_trend.csv",       global_year_trend_plot,    {}),
    ]

    for name, csv_name, fn, kwargs in plan:
        df = _maybe_read(metrics_dir / csv_name)
        if df is None or df.empty:
            continue
        fig = fn(df, **kwargs) if kwargs else fn(df)
        out_paths.append(_save_png(fig, reports_dir / f"{name}.png"))

    # also export a convenient institutions table copy here if present
    inst = _maybe_read(metrics_dir / "institution_scores.csv")
    if inst is not None and not inst.empty:
        inst.sort_values("institution_median_norm", ascending=False).head(200)\
            .to_csv(reports_dir / "institution_top200.csv", index=False)

    return out_paths
