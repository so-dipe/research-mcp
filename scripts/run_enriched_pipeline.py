import asyncio
from datetime import datetime
from glob import glob
from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn, MofNCompleteColumn
from rich.console import Console
from rich.panel import Panel

from data_sources.ngx.constants import PROCESSED_DIR
from data_sources.ngx.enriched import enrich_bundle
from data_sources.ngx.utils import load_manifest, save_manifest
from utils.helpers import is_empty_folder
from utils.logger import get_logger

logger = get_logger(__name__, log_file="run_enriched_pipeline.log")

console = Console()

async def run():
    console.print(
        Panel.fit("[bold blue]NGX Bundle Enrichment Sync[/bold blue]", subtitle="enriching uranium...shh")
    )
    try:
        manifest = load_manifest()

        processed_bundles = {Path(p).name for p in glob(str(PROCESSED_DIR / "*"))}
        filtered_bundles = {
            folder 
            for folder in processed_bundles 
            if not is_empty_folder(PROCESSED_DIR / folder)
        }
        logger.info(f"Fetched {len(processed_bundles)} Processed Folders and {len(filtered_bundles)} were not empty.")

        filtered_manifest = {
            d: info
            for d, info in manifest["documents"].items()
            # if d in filtered_bundles and not info.get("enriched_at")
            if d in filtered_bundles and info.get("date_modified") >= "2025-01-01T00:00:00"
        }
        logger.info(f"Running enrichment for {len(filtered_manifest)} bundles.")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
            transient=True
        ) as progress:
            task_id = progress.add_task("[yellow]Progress", total=len(filtered_manifest))

            count = 0
            
            for doc_id, info in filtered_manifest.items():
                doc_name = info.get("doc_name", "Unknown Document")
                submission_type = info.get("submission_type", "Unknown")

                progress.update(task_id, description=f"[yellow]Enriching {doc_id}...")

                try:
                    await enrich_bundle(doc_id, doc_name, submission_type)
                    manifest["documents"][doc_id]["enriched_at"] = datetime.now().isoformat()

                    count += 1
                    if count % 10 == 0:
                        save_manifest(manifest)
                except Exception as e:
                    logger.error(f"Failed to enrich {doc_id}: {e}")

                progress.advance(task_id)

    except Exception as e:
        logger.error(f"Global Enrichment Pipeline failed: {str(e)}")


if __name__ == '__main__':
    asyncio.run(run())