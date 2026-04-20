import asyncio

from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    MofNCompleteColumn,
    TimeElapsedColumn
)
from rich.console import Console
from rich.panel import Panel

from data_sources.ngx.pipeline import load_ngx_institutions, list_docs, ingest_docs
from utils.logger import get_logger

logger = get_logger(__name__, "ngx_pipeline.log")

console = Console()

async def run():
    console.print(
        Panel.fit("[bold blue]NGX Corporate Disclosures Sync[/bold blue]", subtitle="Lagos, Nigeria")
    )
    try:
        with console.status("[bold green]Updating institution directory...") as status:
            institutions = load_ngx_institutions(['InternationSecIN', 'Symbol'])
            inst_list = institutions.to_dict(orient='records')
            num_inst = len(inst_list)

        total_new = 0
        total_failed = 0
        total_skipped = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=40),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
            transient=False
        ) as progress:
            main_task = progress.add_task(
                "[yellow]Syncing Institutions (New: 0 | Skip: 0 | Err: 0)", 
                total=num_inst
            )

            detail_task = progress.add_task("[cyan]Initializing...", total=None)
            for row in inst_list:
                code = row["InternationSecIN"]
                symbol = row["Symbol"]

                progress.update(
                    detail_task, 
                    description=f"[cyan]Checking {symbol}...",
                    total=1,
                    completed=0
                )

                try:
                    all_docs = await list_docs(code)

                    if not all_docs:
                        continue

                    progress.update(
                        detail_task, 
                        description=f"[bold magenta]Ingesting {len(all_docs)} docs for {symbol}...",
                        total=len(all_docs),
                        completed=0
                    )

                    def update_detail():
                        progress.update(detail_task, advance=1)

                    results = await ingest_docs(all_docs, on_progress=update_detail)

                    processed = [r for r in results if r.get("status") == "processed"]
                    skipped = [r for r in results if r.get("status") == "skipped"]
                    failed = [r for r in results if r.get("status") == "failed"]

                    total_new += len(processed)
                    total_skipped += len(skipped)
                    total_failed += len(failed)

                    progress.update(
                        main_task,
                        description=(
                            f"[yellow]Syncing Institutions "
                            f"(New: [green]{total_new}[/] | Skip: [white]{total_skipped}[/] "
                            f"| Err: [red]{total_failed}[/])"
                        )
                    )

                except Exception as e:
                    logger.error(f"Failed to process {symbol}: {str(e)}")
                    total_failed += 1
                
                finally:
                    progress.update(main_task, advance=1)

            console.print("\n")
            console.print(Panel(
                f"[bold green]Sync Complete![/bold green]\n\n"
                f"✨ [bold]New Docs:[/bold]{total_new}\n"
                f"⏭️ [bold]Skipped:[/bold] {total_skipped}\n"
                f"❌ [bold]Failed:[/bold] {total_failed})",
                title="Results",
                expand=False
            ))

    except Exception as e:
        logger.error(f"Global Pipeline failed: {str(e)}", exc_info=True)
        console.print(f"[bold red]Pipeline Crashed:[/bold red] {e}")


if __name__ == "__main__":
    asyncio.run(run())