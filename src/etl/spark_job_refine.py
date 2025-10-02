# save as openalex_to_parquet.py
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Union

import pandas as pd


def _brace_split(s: str) -> List[str]:
    """Split concatenated JSON objects by brace counting (handles embedded strings/escapes)."""
    objs, depth, start = [], 0, None
    in_str = False
    esc = False
    for i, ch in enumerate(s):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                if depth == 0:
                    start = i
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0 and start is not None:
                    objs.append(s[start : i + 1])
                    start = None
    return objs


def _read_works(path: Union[str, Path]) -> List[Dict[str, Any]]:
    """Read OpenAlex works from:
       - JSON array file
       - single JSON object
       - concatenated JSON objects in a single file.
    """
    p = Path(path)
    raw = p.read_text(encoding="utf-8").strip()

    # Try JSON array or single JSON object
    try:
        data = json.loads(raw)
        if isinstance(data, dict):  # single work
            return [data]
        if isinstance(data, list):
            return data
    except Exception:
        pass

    # Fallback: concatenated objects
    objs = _brace_split(raw)
    if not objs:
        raise ValueError("Could not parse input as JSON array/object or concatenated JSON objects.")
    out = []
    for obj in objs:
        out.append(json.loads(obj))
    return out


def _jdumps(x: Any) -> str:
    return json.dumps(x, ensure_ascii=False)





def _flatten_work(w: Dict[str, Any]) -> Dict[str, Any]:
    pl = (w.get("primary_location") or {}) or {}
    src = (pl.get("source") or {}) or {}
    oa  = (w.get("open_access") or {}) or {}
    return {
        # core
        "work_id": w.get("id"),
        "title": w.get("title"),
        "publication_year": w.get("publication_year"),
        "cited_by_count": w.get("cited_by_count"),
        "source_id": src.get("id"),
        "source_display_name": src.get("display_name"),
        "source_type": src.get("type"),
        "source_issn_l": src.get("issn_l"),

    }



def _flatten_authorships(w: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    auths = (w.get("authorships") or []) or []
    for a in auths:
        author = (a.get("author") or {}) or {}

        # Extract institution names
        institutions = a.get("institutions", []) or []
        institution_names = "; ".join([inst.get("display_name", "") for inst in institutions if inst.get("display_name")])

        # Extract affiliation strings
        affils = a.get("affiliations", []) or []
        affiliation_strings = "; ".join([aff.get("raw_affiliation_string", "") for aff in affils if aff.get("raw_affiliation_string")])

        yield {
            "work_id": w.get("id"),
            "work_title": w.get("title"),
            "author_id": author.get("id"),
            "author_display_name": author.get("display_name"),
            "author_position": a.get("author_position"),
            "is_corresponding": a.get("is_corresponding"),
            # "raw_author_name": a.get("raw_author_name"),
            # "author_orcid": author.get("orcid"),
            "countries": "; ".join((a.get("countries") or []) or []),
            # new flattened fields
            "institution_names": institution_names,
            "affiliation_strings": affiliation_strings,
            # keep raw JSON in case
            # "institutions_json": json.dumps(institutions, ensure_ascii=False),
            # "affiliations_json": json.dumps(affils, ensure_ascii=False),
        }


def write_parquet(df: pd.DataFrame, path: Union[str, Path]) -> None:
    # Try pyarrow, then fastparquet
    last_err = None
    for eng in ("pyarrow", "fastparquet"):
        try:
            df.to_parquet(path, index=False, engine=eng)
            return
        except Exception as e:
            last_err = e
    raise RuntimeError(
        "Failed to write Parquet. Install one of the engines:\n"
        "  pip install pyarrow\n"
        "  # or\n"
        "  pip install fastparquet\n"
        f"Last error: {last_err}"
    )


def convert(input_path: Union[str, Path],
            works_out: Union[str, Path] = "works.parquet",
            authorships_out: Union[str, Path] = "authorships.parquet") -> None:
    works_raw = _read_works(input_path)

    works_rows = [_flatten_work(w) for w in works_raw]
    auth_rows = [row for w in works_raw for row in _flatten_authorships(w)]

    works_df = pd.DataFrame(works_rows)
    auth_df = pd.DataFrame(auth_rows)

    write_parquet(works_df, works_out)
    write_parquet(auth_df, authorships_out)
    print(f"Saved: {works_out}  ({len(works_df)} rows)")
    print(f"Saved: {authorships_out}  ({len(auth_df)} rows)")


if __name__ == "__main__":
    inp = '/data/raw/works.jsonl'
    wout = sys.argv[2] if len(sys.argv) >= 3 else "data/curated/works.parquet"
    aout = sys.argv[3] if len(sys.argv) >= 4 else "data/curated/authorships.parquet"
    convert(inp, wout, aout)


