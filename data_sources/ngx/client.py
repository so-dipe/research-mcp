import asyncio
import os

import httpx
import pandas as pd
import requests
import xmltodict

from .constants import (
    NGX_INSTITUTIONS_URL,
    RAW_DOCS_DIR, 
    SUBMISSION_FILTERS,
    TABLES_DIR
)
from .utils import (
    generate_doc_id,
    hash_content
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/atom+xml"
}

def generate_filter(
    institution_code: str,
    doc_type: str
) -> str:
    filters = []

    if institution_code:
        institution_filter = f"InternationSecIN eq '{institution_code}'"
        filters.append(institution_filter)

    if doc_type in SUBMISSION_FILTERS:
        submissions = SUBMISSION_FILTERS[doc_type]
        doc_type_filter = " or ".join({
            f"Type_of_Submission eq '{s}'"
            for s in submissions
        })
        filters.append(f"({doc_type_filter})")
    
    return " and ".join(filters)

def get_ngx_institutions() -> pd.DataFrame:
    response = requests.get(NGX_INSTITUTIONS_URL)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(data)

    TABLES_DIR.mkdir(exist_ok=True)
    df.to_csv(TABLES_DIR / "ngx_institutions.csv", index=False)
    return df

async def fetch_all_pages(url: str, params: dict | None) -> list:
    all_entries = []
    page = 0

    async with httpx.AsyncClient(http2=True, timeout=30.0) as client:
        while True:
            try:
                response = await client.get(url, params=params, headers=HEADERS)
                response.raise_for_status()

                data = xmltodict.parse(response.content)

                feed = data.get("feed", {})

                entries = feed.get("entry", [])

                if isinstance(entries, dict):
                    entries = [entries]

                all_entries.extend(entries)

                next_url = None
                links = feed.get("link", [])
                if isinstance(links, dict):
                    links = [links]

                for link in links:
                    if link.get("@rel") == "next":
                        next_url = link.get("@href", "")
                        break
                
                if not next_url:
                    break

                url = next_url
                params = None
                page += 1

            except httpx.RemoteProtocolError:
                print("Connection dropped by server. Retrying...")
                await asyncio.sleep(1)
                continue

    return all_entries


async def get_doc_content(
    client: httpx.AsyncClient,
    doc_info: dict,
    symbol: str,
    manifest: dict
) -> dict:
    doc_id = generate_doc_id(doc_info)

    existing_doc = manifest["documents"].get(doc_id)
    if existing_doc and existing_doc.get("status") == "processed":
        # Migration Code
        # manifest["documents"][doc_id]["doc_name"] = doc_info["doc_name"]
        # manifest["documents"][doc_id]["symbol"] = symbol
        # if manifest["documents"][doc_id].get("display_name"):
        #     del manifest["documents"][doc_id]["display_name"]
        return {"status": "skipped", "reason": "already processed", "doc_id": doc_id}
    
    if not doc_info.get("institution"):
        return {"status": "skipped", "reason": "institution not found."}
    
    try:
        response = await client.get(doc_info["url"], follow_redirects=True)
        response.raise_for_status()

        content_hash = hash_content(response.content)

        doc_extension = doc_info["url"].split(".")[-1].lower()
        doc_extension = doc_extension.split("?")[0]

        folder_path = RAW_DOCS_DIR / doc_info['institution']
        folder_path.mkdir(parents=True, exist_ok=True)

        doc_path = folder_path / f"{doc_id}.{doc_extension}"

        with open(doc_path, "wb") as f:
            f.write(response.content)

        manifest["documents"][doc_id] = {
            "institution": doc_info["institution"],
            "symbol": symbol,

            "date_modified": doc_info["date_modified"],
            "submission_type": doc_info["submission_type"],

            "doc_name": doc_info["doc_name"],

            "url": doc_info["url"],
            "local_path": str(doc_path),

            "content_hash": content_hash,
            "status": "processed",

            "ingested_at": pd.Timestamp.now().isoformat(),  
        }

        return {"status": "processed", "doc_id": doc_id}
    
    except httpx.HTTPStatusError as e:
        return {"status": "failed", "reason": f"HTTP Error: {e.response.status_code}"}
    except Exception as e:
        return {"status": "failed", "reason": str(e)}