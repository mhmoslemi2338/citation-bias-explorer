import os
import json
from dotenv import load_dotenv
from tqdm import tqdm
from .openalex_client import paginate

def main():
    load_dotenv()
    os.makedirs("data/raw", exist_ok=True)
    out_path = "data/raw/works.jsonl"
    from_year = os.getenv("FROM_YEAR")
    per_page = int(os.getenv("WORKS_PER_PAGE"))
    max_pages = int(os.getenv("WORKS_MAX_PAGES"))
    field = os.getenv("CONCEPT_ID_FIELD")




    params = {
        "filter": f"concepts.id:{field},from_publication_date:{from_year}-01-01,type_crossref:journal-article",
        "per_page": per_page,
        "select": ",".join([
            "id","title","authorships",
            "primary_location","cited_by_count",
            "publication_year","open_access"
        ])
    }



    count = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for obj in tqdm(paginate("works", params=params, max_pages=max_pages)):
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            count += 1
    print(f"Saved {count} works to {out_path}")

if __name__ == "__main__":
    main()
