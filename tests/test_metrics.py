import pandas as pd
from src.metrics.bias_metrics import add_norm_citations

def test_add_norm_citations_shapes():
    df = pd.DataFrame({
        "work_id":[1,2,3,4],
        "venue":["A","A","B","B"],
        "year":[2020,2020,2020,2020],
        "cited_by_count":[10,20,5,15]
    })
    out = add_norm_citations(df)
    assert "norm_cites" in out.columns
    assert out["norm_cites"].notna().all()
