from typing import Literal, Any, Hashable, Optional
from data_sources.ngx.pipeline import load_ngx_institutions as lni
from data_sources.ngx.constants import NGX_INSTITUTIONS_LITERAL
from data_sources.ngx.utils import load_manifest
from datetime import date
import pandas as pd
from pypdf import PdfReader
import os
from utils.logger import get_logger

logger = get_logger(name=__name__, log_file="mcp_tools.log")

async def say_hello(name: str) -> str:
    """A simple tool to say hello to someone."""
    return f"Hello, {name}!"

async def say_goodbye(name: str) -> str:
    """A simple tool to say goodbye to someone."""
    return f"Goodbye, {name}!"

async def get_secret_message(secret_code: Literal["hello", "goodbye"]) -> str:
    """Returns a secret message depending on the secret code provided."""
    messages = {
        "hello": "The coffee is hot! and we are ready to work on this conspiracy! -- Reporting from Area 97.",
        "goodbye": "The coffee is cold! and we are done with this conspiracy! -- Reporting from Area 97."
    }
    return messages.get(secret_code, "Unauthorized personnel!!!")

async def get_ngx_institutions(
    cols: list[NGX_INSTITUTIONS_LITERAL]
) -> list[dict[Hashable, Any]]:
    """
    Get all listed institutions on the Nigerian Exchange (NGX).
    Use this to fund the correct Symbol or ISIN for a company.
    """
    logger.info(f"Fetching NGX institutions for columns: {cols}")
    try:
        df = lni(columns=cols)
        if df.empty:
            logger.warning("NGX institution list is empty.")
            return []
    
        return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error fetching institutions: {e}")
        return []

async def search_ngx_reports(
    symbol: str | None = None,
    isin: str | None = None,
    submission_type: str | None = None,
    after: date | None = None,
    keywords: str | None = None
) -> list[dict]:
    """
    Search the local document warehouse for NGX filings using structured filters.
    
    Args:
        symbol: The stock ticker (e.g., 'ACCESSCORP', 'ZENITHBANK').
        isin: The International Securities Identification Number (e.g., 'NGACCESS0005').
        submission_type: The category of filing. Note: Use keywords like 'Financial', 'Meeting', 'Actions', or 'Forecast'.
        after: Filter for documents modified on or after this date (format: YYYY-MM-DD).
        keywords: Free-text search to find specific terms within the document title.
    """
    logger.info(
        f"Search params: symbol={symbol}, "
        f"type={submission_type}, after={after}, keywords={keywords}"
    )
    
    manifest = load_manifest()
    docs = manifest.get("documents", {})

    if not docs:
        logger.warning("Manifest is empty. No documents available to search.")
        return []
    
    df = pd.DataFrame.from_dict(docs, orient='index')
    df['doc_id'] = df.index

    logger.debug(f"Current unique submission types: {df['submission_type'].unique()}")

    if symbol:
        symbol_msk = df['symbol'].str.upper() == symbol.upper()
        df = df[symbol_msk]

    if isin:
        isin_msk = df['institution'] == isin
        df = df[isin_msk]

    if submission_type:
        submission_type_msk = df['submission_type'].str.contains(submission_type, case=False, na=False)
        df = df[submission_type_msk]

    if after:
        df['date_modified'] = pd.to_datetime(df['date_modified'])
        after_msk = df['date_modified'] >= pd.to_datetime(after)
        df = df[after_msk]

    if keywords:
        keywords_msk = df['doc_name'].str.contains(keywords, case=False, na=False)
        df = df[keywords_msk]

    if df.empty:
        return []
    
    df = df.sort_values('date_modified', ascending=False).head(10)

    output_cols = ['doc_id', 'symbol', 'doc_name', 'submission_type', 'date_modified']

    results = df[output_cols].to_dict(orient="records")
    logger.info(f"Search returned {len(results)} matches.")
    return results

async def get_doc(doc_id: str) -> str:
    """
    Extracts text from the first few pages of a specific document for analysis.
    """
    logger.info(f"Requesting content for doc_id: {doc_id}")

    manifest = load_manifest()
    docs = manifest.get("documents", {})
    
    doc = docs.get(doc_id, {})
    if not doc:
        logger.error(f"Doc ID {doc_id} not found in manifest.")
        f"Error: Document {doc_id} not found."

    local_path = doc.get("local_path")
    if not local_path or not os.path.exists(local_path):
        logger.error(f"Path missing for {doc_id}: {local_path}")
        f"Error: Document {doc_id} path is missing or unavailable on disk."

    try:
        reader = PdfReader(local_path)
        pages = reader.pages
        content = ""

        n = min(3, len(pages))
        for page in pages[:n]:
            text = page.extract_text()
            if text:
                content += text + "\n"
        
        if not content.strip():
            logger.warning(f"Doc {doc_id} has no extractable text (likely scanned image).")
            return "Document found, but no extractable text was deteced (could be a scanned image)."
        
        logger.info(f"Successfully extracted {len(content)} characters from {doc_id}.")
        return content
    
    except Exception as e:
        logger.error(f"PdfReader failed for {doc_id}: {str(e)}")
        return f"Error reading PDF: {str(e)}"
    
