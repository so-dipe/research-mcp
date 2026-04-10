import os
import asyncio
import random

import httpx
import pandas as pd

from .client import get_ngx_institutions, generate_filter, fetch_all_pages, get_doc_content
from .constants import NGX_INSTITUTIONS_LITERAL, NGX_DOCS_PARAMS, NGX_DOCS_URL, TABLES_DIR
from .parser import fetch_docs
from .utils import load_manifest, save_manifest

_ism: dict | None = None

def load_ngx_institutions(
    columns: list[NGX_INSTITUTIONS_LITERAL] | None = None
) -> pd.DataFrame:
    csv_path = TABLES_DIR / "ngx_institutions.csv"
    try:
        df = get_ngx_institutions()
    except Exception as e:
        if csv_path.exists():
            df = pd.read_csv(csv_path)
        else:
            return pd.DataFrame()

    return df[columns] if columns else df

def get_institution_symbol_map() -> dict:
    global _ism

    if _ism:
        return _ism
    
    df = load_ngx_institutions(["InternationSecIN", "Symbol"])
    if not df.empty:
        return {
            r["InternationSecIN"]: r["Symbol"]
            for _, r in df.iterrows()
        }
    return {}

async def list_docs(
    institution_code: str = "",
    doc_type: str = ""
) -> list:
    params = {**NGX_DOCS_PARAMS}
    filter_str = generate_filter(institution_code, doc_type)

    if filter_str:
        params["$filter"] = filter_str

    entries = await fetch_all_pages(NGX_DOCS_URL, params)
    docs = fetch_docs(entries)

    return docs


async def ingest_docs(docs: list) -> list:
    manifest = load_manifest()
    symbol_map = get_institution_symbol_map()

    semaphore = asyncio.Semaphore(5)

    async with httpx.AsyncClient(http2=True, timeout=60.0) as client:

        async def sem_get_doc_content(doc):
            symbol = symbol_map.get(doc['institution'], "UNKNOWN")
            async with semaphore:
                await asyncio.sleep(random.uniform(0.1, 0.5))
                return await get_doc_content(client, doc, symbol, manifest)
        
        tasks = [sem_get_doc_content(doc) for doc in docs]

        results = await asyncio.gather(*tasks)

        save_manifest(manifest)
        return results