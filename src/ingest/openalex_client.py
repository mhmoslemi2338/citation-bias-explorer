import os
import time
import requests
from typing import Dict, Iterable, Any
from dotenv import load_dotenv

load_dotenv()
BASE = os.getenv("OPENALEX_BASE", "https://api.openalex.org")
CONTACT = os.getenv("CONTACT_EMAIL", "mhmoslemi2338@gmail.com")


def paginate(endpoint: str, params: Dict[str, Any], max_pages: int) -> Iterable[Dict]:
    params = dict(params)
    if CONTACT:
        params["mailto"] = CONTACT
    url = f"{BASE}/{endpoint}"

    qp = dict(params)
    r = requests.get(url, params=qp, timeout=60)
    r.raise_for_status()
    total = r.json()["meta"]["count"]
    print(f"Total works available: {total}")


    cursor = "*"
    for _ in range(max_pages):
        qp = dict(params)
        qp["cursor"] = cursor
        r = requests.get(url, params=qp, timeout=60)
        r.raise_for_status()
        js = r.json()
        for item in js.get("results", []):
            yield item
        cursor = js.get("meta", {}).get("next_cursor")
        if not cursor:
            break
        time.sleep(0.5)  # rate limit politeness
