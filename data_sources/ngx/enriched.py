import asyncio
import re
from datetime import datetime
from pathlib import Path

from .constants import PROCESSED_DIR
from .enriched_helpers import (
    _call_gemini_api, _call_ollama_local,
    _get_section_enrichment_prompt, _get_table_enrichment_prompt, _get_light_enrichment_prompt,
    get_content_snippet, get_smart_section_snippet, get_smart_table_snippet, 
    extract_and_save_sections
)
from .utils import load_bundle_artifact, save_bundle_artifact, get_enrichment_level
from .validator import SectionEnrichment, TableEnrichment, LightEnrichment
from utils.logger import get_logger

logger = get_logger(__name__, log_file="enriched.log")

SEM = asyncio.Semaphore(3)

async def get_document_suggestion(header: str, body_snippet: str, use_cloud: bool = False) -> dict:
    prompt = _get_light_enrichment_prompt(header, body_snippet)
    if use_cloud:
        return await _call_gemini_api(prompt, LightEnrichment)
    else:
        return await _call_ollama_local(prompt, LightEnrichment, model="gemma3:1b")

async def get_section_suggestion(header: str, body_snippet: str, use_cloud: bool = False) -> dict:
    prompt = _get_section_enrichment_prompt(header, body_snippet)
    if use_cloud:
        return await _call_gemini_api(prompt, SectionEnrichment)
    else:
        return await _call_ollama_local(prompt, SectionEnrichment, model="gemma3:1b")
    
async def get_table_suggestion(context: str, table_snippet: str, use_cloud: bool = False) -> dict:
    prompt = _get_table_enrichment_prompt(context, table_snippet)
    if use_cloud:
        return await _call_gemini_api(prompt, TableEnrichment)
    else:
        return await _call_ollama_local(prompt, TableEnrichment, model="gemma3:1b")

async def enrich_tables(
    tables_dir: Path, 
    content: str, 
    artifacts: dict, 
    use_cloud: bool = False
):
    async def enrich_table(table, artifacts, use_cloud=False):
        md_tables = re.findall(r'((?:\|.*\|(?:\n|$))+)', content)
        async with SEM:
            table_snippet = get_smart_table_snippet(table)
            table_idx = int(re.findall(r'\d+', table.name)[0]) - 1

            context = ""
            if 0 <= table_idx < len(md_tables):
                start_pos = content.find(md_tables[table_idx])
                context = content[max(0, start_pos-1500):start_pos]

            try:
                logger.debug("Enriching %s...", table.name)
                if use_cloud:
                    await asyncio.sleep(1)
                response = await get_table_suggestion(context, table_snippet, use_cloud)
                artifacts["tables"][table.name] = response

            except Exception as e:
                logger.error("Error processing %s: %s", table.name, e)

    
    sorted_tables = sorted(
        list(tables_dir.glob("table_*.csv")),
        key=lambda x: int(re.findall(r'\d+', x.name)[0])
    )
    if "tables" not in artifacts or isinstance(artifacts["tables"], list):
        artifacts["tables"] = {}

    tasks = [
        enrich_table(t, artifacts, use_cloud=use_cloud)
        for t in sorted_tables
    ]

    await asyncio.gather(*tasks)

async def enrich_sections(
    bundle: Path, 
    content: str, 
    doc_name: str, 
    artifacts: dict, 
    enrich_level: str = "light",
    use_cloud: bool = False
):
    async def enrich_section(section, bundle, artifacts, use_cloud):
        header = section.get("original_header", "")
        section_file = section.get("file", "")
        section_path = bundle / "sections" / section_file

        async with SEM:
            if not section_path.exists():
                return
            
            body = get_smart_section_snippet(section_path)
            try:
                logger.debug("Enriching section: %s", section_file)
                if use_cloud:
                    await asyncio.sleep(1)
                response = await get_section_suggestion(header, body, use_cloud)
                artifacts["sections"][section_file] = response
            except Exception as e:
                logger.error("Failed to enrich %s: %s", section_file, e)

    # if enrich_level == "light":
    #     snippet = get_content_snippet(content, limit=4096)
    #     if use_cloud:
    #         await asyncio.sleep(1)
    #     response = await get_document_suggestion(header=doc_name, body_snippet=snippet, use_cloud=use_cloud)
    #     artifacts["summary"] = response
    # else:
    sections_list = extract_and_save_sections(bundle, content)
    if "sections" not in artifacts:
        artifacts["sections"] = {}
    tasks = [
        enrich_section(s, bundle, artifacts, use_cloud=use_cloud)
        for s in sections_list
    ]
    await asyncio.gather(*tasks)

async def enrich_bundle(doc_id: str, doc_name: str, submission_type: str):
    bundle = PROCESSED_DIR / doc_id
    md_dir = bundle / "content.md"
    tables_dir = bundle / "tables"

    enrich_level = get_enrichment_level(submission_type)
    use_cloud = False #False if enrich_level in ["high", "medium"] else False

    if not bundle.exists():
        return None
    
    content = await asyncio.to_thread(md_dir.read_text) if md_dir.exists() else ""
    artifacts = load_bundle_artifact(bundle)

    tasks = []

    if content:
        tasks.append(enrich_sections(bundle, content, doc_name, artifacts, enrich_level, use_cloud))
        
    if tables_dir.exists():
        tasks.append(enrich_tables(tables_dir, content, artifacts, use_cloud))

    if tasks:
        await asyncio.gather(*tasks)
    
    artifacts["doc_name"] = doc_name
    artifacts["enriched_at"] = datetime.now().isoformat()
    
    await asyncio.to_thread(save_bundle_artifact, artifacts, bundle)
    
    logger.debug(f"Successfully enriched bundle: {doc_id}")

