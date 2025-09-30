import pandas as pd
from .bias_metrics import add_norm_citations, country_advantage, institution_score

def main():
    works = pd.read_parquet("data/curated/works.parquet")
    auth  = pd.read_parquet("data/curated/authorships.parquet")

    # Optional filters
    works = works[(works["year"] >= 2018) & (works["year"] <= works["year"].max())]

    w_norm = add_norm_citations(works)
    ctry = country_advantage(auth, w_norm)
    inst = institution_score(auth, w_norm)

    ctry.to_csv("data/curated/country_advantage.csv", index=False)
    inst.to_csv("data/curated/institution_scores.csv", index=False)
    print("Saved metrics CSVs.")

if __name__ == "__main__":
    main()
