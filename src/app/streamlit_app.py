import pandas as pd
import streamlit as st
from src.metrics.bias_metrics import add_norm_citations, country_advantage, institution_score
from src.metrics.plots import country_bar, institution_table

st.set_page_config(page_title="Citation Bias Explorer", layout="wide")
st.title("Citation Bias Explorer")

@st.cache_data
def load_parquet():
    works = pd.read_parquet("data/curated/works.parquet")
    auth  = pd.read_parquet("data/curated/authorships.parquet")
    return works, auth

works, auth = load_parquet()

year_min, year_max = int(works["year"].min()), int(works["year"].max())
yr = st.slider("Publication year", min_value=year_min, max_value=year_max, value=(max(year_min, 2018), year_max))
venue_filter = st.text_input("Venue contains", "")

wf = works[(works["year"] >= yr[0]) & (works["year"] <= yr[1])].copy()
if venue_filter:
    wf = wf[wf["venue"].fillna("").str.contains(venue_filter, case=False)]

wf = add_norm_citations(wf)

c1, c2, c3 = st.columns(3)
c1.metric("Papers", len(wf))
c2.metric("Avg cites", round(wf["cited_by_count"].mean(), 2))
c3.metric("Avg normalized", round(wf["norm_cites"].mean(), 3))

ctry = country_advantage(auth, wf)
st.plotly_chart(country_bar(ctry), use_container_width=True)

inst = institution_score(auth, wf)
st.dataframe(inst.head(50))

st.download_button("Download country metrics CSV", ctry.to_csv(index=False).encode("utf-8"), "country_advantage.csv")
st.download_button("Download institution metrics CSV", inst.to_csv(index=False).encode("utf-8"), "institution_scores.csv")
