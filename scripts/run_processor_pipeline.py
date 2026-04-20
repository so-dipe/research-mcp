import asyncio
from datetime import datetime
from glob import glob
from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn, MofNCompleteColumn
from rich.console import Console

from data_sources.ngx.constants import PROCESSED_DIR
from data_sources.ngx.processor import process_document
from data_sources.ngx.utils import load_manifest, save_manifest
from utils.helpers import is_empty_folder
from utils.logger import get_logger

logger = get_logger(__name__, log_file="run_processor_pipeline.log")

console = Console()

async def run():
    logger.info("Starting Processor pipeline sync...")

    try:
        manifest = load_manifest()

        filtered_manifest = {
            d: v
            for d, v in manifest["documents"].items()
            if v.get('date_modified', "") > "2024-01-01T00:00:00"
        }

        processed_folders = {Path(p).name for p in glob(str(PROCESSED_DIR / "*"))}
        filtered_folders = {
            folder 
            for folder in processed_folders 
            if not is_empty_folder(PROCESSED_DIR / folder)
        }
        logger.info(f"Fetched {len(processed_folders)} Processed Folders and {len(filtered_folders)} were not empty.")

        unprocessed = {
            doc_id: v
            for doc_id, v in filtered_manifest.items()
            if
                not v.get("processed_at")
                or doc_id not in filtered_folders
        }

        logger.info(f"Found {len(unprocessed)} documents to process.")

        sem = asyncio.Semaphore(5)

        async def process_task(doc_id, v, progress, overall_task):

            async with sem:
                task_id = progress.add_task(f"[cyan]Processing {doc_id[:20]}...", total=None)
                try:
                    file_path_raw = v.get("local_path", "")
                    p = Path(file_path_raw)

                    if p.is_absolute() and  "research-mcp" in p.parts:
                        idx = p.parts.index("research-mcp")
                        file_path = Path.cwd() / Path(*p.parts[idx+1:])
                    else:
                        file_path = Path.cwd() / p if not p.is_absolute() else p

                    await process_document(doc_id, str(file_path))

                    manifest["documents"][doc_id]["processed_at"] = datetime.now().isoformat()

                    return True
                except Exception as e:
                    logger.error(f"Failed {doc_id}: {e}")
                    return False
                
                finally:
                    progress.remove_task(task_id)
                    progress.update(overall_task, advance=1)
                

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
            transient=True
        ) as progress:
        
            overall_task = progress.add_task("[yellow]Overall Progress", total=len(unprocessed))

            tasks = [process_task(doc_id, v, progress, overall_task) for doc_id, v in unprocessed.items()]

            completed_count = 0
            for coro in asyncio.as_completed(tasks):
                success = await coro
                completed_count += 1

                if completed_count % 10 == 0:
                    save_manifest(manifest)

        save_manifest(manifest)
        logger.info("Pipeline sync complete.")

    except Exception as e:
        logger.error(f"Global Doc Pipeline failed: {str(e)}", exc_info=True)

if __name__ == '__main__':
    asyncio.run(run())