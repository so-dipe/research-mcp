import re
from pathlib import Path
from pydantic import BaseModel
from typing import Any, Type, cast

import httpx
from google import genai
from google.genai import types

from config import Config
from utils.logger import get_logger

logger = get_logger(__name__, log_file="enriched.log")

OLLAMA_URL = "http://localhost:11434/api/generate"

g_client = genai.Client(api_key=Config.GEMINI_API)

async def _call_ollama_local(
    prompt: str, 
    schema: Type[BaseModel], 
    model: str="gemma4:e4b"
) -> dict[str, Any]:
    payload = {
        "model": model,
        "prompt": prompt,
        "format": "json",
        "stream": False,
        "options": {
            "think": False
        }
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(OLLAMA_URL, json=payload, timeout=60.0)
        raw = resp.json().get("response", "{}")
        return schema.model_validate_json(raw).model_dump()
    

async def _call_gemini_api(
    prompt: str, 
    schema: Type[BaseModel], 
    model: str = "gemini-2.5-flash-lite"
) -> dict[str, Any]:
    try:
        response = await g_client.aio.models.generate_content(
            model=model,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=_get_sys_prompt(),
                thinking_config=types.ThinkingConfig(thinking_level=None),
                response_mime_type="application/json",
                response_schema=schema,
                temperature=0.1
            )
        )
        if response.parsed:
            typed_output = cast(BaseModel, response.parsed)
            return typed_output.model_dump()
        else:
            return {}
    except Exception as e:
        logger.error(f"Gemini API Error: {e}")
        return {}
    
def _get_light_enrichment_prompt(header, body_snippet) -> str:
    return f"""
    Analyse this document and return a JSON object.

    DOC NAME: {header}
    CONTENT PREVIEW: {body_snippet}

    Fields:
    - "summary": 3-sentence executive summary written for a buy-side analyst.
    - "tags": exactly 5 search keywords, lowercase.
    - "signal_type": one of ["filing_delay", "dividend", "board_change", "agm_notice", 
                            "earnings_release", "regulatory_action", "rights_issue", 
                            "name_change", "suspension", "general"].
    - "signals": list of 1-5 notable facts an analyst should know, each under 15 words.
    - "data_density": integer 1-10 where 1=boilerplate/headers, 
                    4=descriptive narrative, 7=tables with metrics, 
                    10=dense financial statements.
    """

# def _get_section_enrichment_prompt(header, body_snippet) -> str:
#     return f"""
#     Analyze this document section.

#     HEADER: {header if header else "Introduction/Preamble"}
#     CONTENT PREVIEW: {body_snippet}

#     Return a JSON object:
#     1. "refined_title": Professional snake_case title.
#     2. "summary": 2-sentence executive summary.
#     3. "tags": 5 search keywords.
#     4. "data_density": Score 1-10 (1: Boilerplate/Empty; 10: Dense with information/data for financial analysis)
#     5. "contains_metrics": boolean (True if specific numbers/KPIs are discussed).
#     """

def _get_section_enrichment_prompt(header: str, body_snippet: str) -> str:
    return f"""Analyse this section from a Nigerian company filing and return a JSON object.

    SECTION HEADER: {header if header else "Preamble / No Header"}
    SECTION CONTENT:
    {body_snippet}

    Return the following fields:

    "refined_title"
    A concise human-readable title in Title Case, maximum 8 words.
    Examples: "Revenue and Gross Profit Analysis", "Directors and Senior Management"

    "summary"
    2-sentence summary written for a buy-side analyst.
    Sentence 1: what this section covers.
    Sentence 2: why it matters for investment analysis, or note if it is boilerplate.

    "tags"
    Exactly 5 lowercase search keywords relevant to this section.
    Prefer specific financial/regulatory terms over generic ones.
    Examples: ["fx exposure", "loan impairment", "tier 1 capital", "dividend policy", "cbn regulation"]

    "data_density"
    Integer 0-10 reflecting analytical value for financial research:
    0: Contains no information at all.
    1-2: Boilerplate, legal disclaimers, contact info, headers
    3-4: Descriptive narrative, no quantitative data
    5-6: Qualitative analysis with some figures or dates
    7-8: Financial tables, KPIs, quantitative analysis
    9-10: Dense financial statements, multiple metrics per line

    "contains_metrics"
    true if the section contains specific financial figures, ratios, percentages, or KPIs.
    false otherwise.

    "section_type"
    One of: ["financial_statement", "management_discussion", "audit_report", 
            "governance", "risk_factors", "notes_to_accounts", "operational_review",
            "forward_guidance", "boilerplate", "other"]
    """

# def _get_table_enrichment_prompt(context: str, table_snippet: str, enrich_level: str = "medium") -> str:
#     return f"""
#     Analyse the document context and table snippet.

#     NEARBY TEXT: {context}
#     TABLE_SNIPPET: {table_snippet}

#     Return a JSON object with:
#     1. "table_name": snake_case filename (e.g., "consolidated_balance_sheet_2024").
#     2. "tags": list of 5 key terms.
#     3. "summary": 1-sentence description.
#     """

def _get_table_enrichment_prompt(context: str, table_snippet: str) -> str:
    return f"""Analyse this table from a Nigerian company filing and return a JSON object.

    SURROUNDING TEXT (for context):
    {context if context else "No surrounding context available."}

    TABLE CONTENT:
    {table_snippet}

    Return the following fields:

    "table_name"
    Descriptive snake_case name, include year if identifiable.
    Examples: "consolidated_income_statement_2024", "loan_portfolio_by_sector_h1_2025"

    "table_type"
    One of: ["income_statement", "balance_sheet", "cash_flow_statement", 
            "statement_of_changes_in_equity", "segmental_breakdown", 
            "loan_portfolio", "capital_adequacy", "five_year_summary",
            "directors_shareholding", "operational_kpi", "other"]

    "summary"
    1-sentence description: what the table shows and the period it covers.
    Example: "Consolidated income statement for FY2024 showing revenue of ₦45.2bn and PAT of ₦6.1bn."

    "tags"
    Exactly 5 lowercase search keywords.
    Prioritise financial line items, ratios, and regulatory terms visible in the table.

    "currency"
    Detected currency if present. One of: ["NGN", "USD", "GBP", "EUR", "mixed", "none"]

    "reporting_period"
    Detected period if identifiable, e.g. "FY2024", "H1 2025", "Q3 2024". 
    null if not determinable.
    """

def _get_sys_prompt() -> str:
    return """You are a senior financial analyst at a Nigerian buy-side investment firm \
    covering equities and fixed income listed on the Nigerian Exchange (NGX). \
    You extract structured metadata from sections and tables in company filings \
    to support investment research, portfolio monitoring, and risk assessment. \
    Your outputs are consumed by downstream systems — be precise, consistent, and terse. \
    Never invent data. If something is unclear, reflect that in a lower data_density score.
    """


def get_content_snippet(content: str, limit: int=2048) -> str:
    if len(content) <= limit:
        return content
    
    half_limit = limit // 2
    head = content[:half_limit]
    tail = content[-half_limit:]

    return f"{head}\n\n[... TEXT TRUNCATED ...]\n\n{tail}"


def get_smart_section_snippet(section_path: Path, limit: int=2048) -> str:
    with open(section_path, "r") as f:
        content = f.read()
        body = "".join(content.split("\n\n")[1:])

    return get_content_snippet(body, limit)

def get_smart_table_snippet(table_path: Path, limit: int=20) -> str:
    with open(table_path, "r") as f:
        all_lines = f.readlines()

    if len(all_lines) <= limit:
        return "".join(all_lines)
    
    head = all_lines[:limit]
    tail = all_lines[-10:]

    return "".join(head) + "\n[... intermediate rows omitted ...]\n" + "".join(tail)

def extract_and_save_sections(bundle: Path, content: str) -> list[dict]:
    sections_dir = bundle / "sections"

    pattern = r'(^#{1,6}\s+.*)'
    chunks = re.split(pattern, content, flags=re.MULTILINE)

    section_list = []

    if not chunks:
        return []

    sections_dir.mkdir(exist_ok=True)

    if chunks and chunks[0].strip():
        preamble_md = "section_0.md"
        (sections_dir / preamble_md).write_text(chunks[0].strip())

        section_list.append({
            "id": 0,
            "original_header": None,
            "file": preamble_md,
            "content_preview": chunks[0][:1000],
            "type": "preamble"
        })

    for i in range(1, len(chunks), 2):
        header = chunks[i].strip()
        body = chunks[i+1].strip() if (i+1) < len(chunks) else ""

        section_id = (i // 2) + 1
        section_md = f"section_{section_id}.md"

        (sections_dir / section_md).write_text(f"{header} \n\n {body}")

        section_list.append({
            "id": section_id,
            "original_header": header,
            "file": section_md,
            "content_preview": body[:1000],
            "type": "section"
        })

    return section_list