import os
from pathlib import Path

import requests
import pandas as pd
import xmltodict

from pipeline_utils import get_text
from manifest import (
    load_manifest,
    save_manifest,
    generate_doc_id,
    hash_content
)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/atom+xml"
}

NGX_INSTITUTIONS_URL= "https://doclib.ngxgroup.com/REST/api/issuers/companydirectory?$orderby=CompanyName"

NGX_DOCS_URL = "https://doclib.ngxgroup.com/_api/Web/Lists/GetByTitle('XFinancial_News')/items/"

NGX_DOCS_PARAMS = {
    "$select": "URL,Modified,InternationSecIN,Type_of_Submission",
    "$orderby": "Modified desc"
}

SUBMISSION_FILTERS = {
    "Financial Statement": ["Financial Statements", "EarningsForcast"],
    "Corporate Actions": ["Corporate Actions", "Corporate Disclosures"],
    "Director Dealings": ["DirectorDealings", "Director Dealings"]
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
    return pd.DataFrame(data)

def fetch_all_pages(url: str, params: dict | None) -> list:
    all_entries = []

    while True:
        response = requests.get(url, params=params)
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

    return all_entries

def fetch_docs(entries: list) -> list:
    docs = []
    for entry in entries:
        props = entry.get("content", {}).get("m:properties", {})
        url_info = props.get("d:URL", {})

        docs.append({
            "institution": props.get("d:InternationSecIn"),
            "doc_name": get_text(url_info, "d:Description"),
            "url": url_info.get("d:Url"),
            "submission_type": get_text(props, "d:Type_of_Submission"),
            "date_modified": props.get("d:Modified", {}).get("#text")
        })

    return docs

def list_docs(
    institution_code: str = "",
    doc_type: str = ""
) -> pd.DataFrame:
    params = {**NGX_DOCS_PARAMS}
    filter_str = generate_filter(institution_code, doc_type)

    if filter_str:
        params["$filter"] = filter_str

    entries = fetch_all_pages(NGX_DOCS_URL, params)
    docs = fetch_docs(entries)

    return pd.DataFrame(docs)

def get_doc_content(doc_info: dict, manifest: dict) -> dict:
    doc_id = generate_doc_id(doc_info)

    existing_doc = manifest["documents"].get(doc_id)

    if existing_doc and existing_doc.get("status") == "processed":
        return {"status": "skipped", "reason": "already processed"}
    
    response = requests.get(doc_info["url"])
    response.raise_for_status()

    content_hash = hash_content(response.content)

    if not doc_info.get("institution"):
        return {"status": "skipped", "reason": "institution not found."}
    
    doc_extension = doc_info["url"].split(".")[-1].lower()

    folder_path = Path(__file__).resolve().parent / f"data/{doc_info['institution']}"
    os.makedirs(folder_path, exist_ok=True)

    doc_path = folder_path / f"{doc_id}.{doc_extension}"

    with open(doc_path, "wb") as f:
        f.write(response.content)

        manifest["documents"][doc_id] = {
            "doc_info": doc_info,
            "content_hash": content_hash,
            "status": "processed",

            "url": doc_info["url"],
            "institution": doc_info["institution"],
            "last_modified": doc_info["date_modified"],
            "ingested_at": pd.Timestamp.now().isoformat()
        }

    save_manifest(manifest)

    return {"status": "processed", "doc_id": doc_id}

def ingest_docs(df: pd.DataFrame) -> pd.DataFrame:
    manifest = load_manifest()

    results = []
    for _, row in df.iterrows():
        try:
            doc_info = row.to_dict()
            result = get_doc_content(doc_info, manifest)
            results.append({**doc_info, **result})
        except Exception as e:
            print(f"Error occurred while processing row: {e}")
            continue

    return pd.DataFrame(results)