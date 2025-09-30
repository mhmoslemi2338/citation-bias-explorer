import plotly.express as px
import pandas as pd

def country_bar(ctry: pd.DataFrame, top_n: int = 30):
    df = ctry.dropna().head(top_n)
    fig = px.bar(df, x="country_code", y="country_advantage", title="Country Advantage Index")
    return fig

def institution_table(inst: pd.DataFrame, top_n: int = 50):
    return inst.head(top_n)
