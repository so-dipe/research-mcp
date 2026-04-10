import asyncio

from data_sources.ngx.pipeline import load_ngx_institutions, list_docs, ingest_docs
from utils.logger import get_logger

logger = get_logger(__name__, "ngx_pipeline.log")

async def run():
    logger.info("Starting NGX Pipeline Sync...")
    try:
        logger.info("Updating institution directory...")
        institutions = load_ngx_institutions(['InternationSecIN', 'Symbol'])
        inst_list = institutions.to_dict(orient='records')

        total_processed = 0
        total_failed = 0

        logger.info("Checking for new Corporate Disclosures...")
        for i, row in enumerate(inst_list, 1):
            code = row["InternationSecIN"]
            symbol = row["Symbol"]

            logger.info(f"[{i}/{len(institutions)}] Syncing {symbol} ({code})...")

            try:
                all_docs = await list_docs(institution_code=code)

                if not all_docs:
                    logger.info(f"No Documents found for {symbol}.")
                    continue

                logger.info(f"Found {len(all_docs)} docs for {symbol}. Ingesting...")

                results = await ingest_docs(all_docs)

                processed = [r for r in results if r.get("status") == "processed"]
                skipped = [r for r in results if r.get("status") == "skipped"]
                failed = [r for r in results if r.get("status") == "failed"]

                total_processed += len(processed)
                total_failed += len(failed)

                logger.info(
                    f"Result for {symbol}: {len(processed)} new, "
                    f"{len(skipped)} already exist, {len(failed)} errors."
                )

            except Exception as e:
                logger.error(f"Failed to process {symbol}: {str(e)}")
                continue

        logger.info(f"Backfill Complete. Total New: {total_processed}, Total Failed: {total_failed}")

    except Exception as e:
        logger.error(f"Global Pipeline failed: {str(e)}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(run())