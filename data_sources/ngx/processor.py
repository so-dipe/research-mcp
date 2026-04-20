import json
import asyncio
import logging
from datetime import datetime

from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.datamodel.pipeline_options import PdfPipelineOptions

from .constants import PROCESSED_DIR
from utils.logger import get_logger

logger = get_logger(__name__, log_file="docprocessor.log", level=logging.INFO)

pipeline_options = PdfPipelineOptions()
pipeline_options.do_ocr = True
pipeline_options.do_table_structure = True
pipeline_options.generate_page_images = False
pipeline_options.generate_picture_images = False

converter = DocumentConverter(
    format_options={
        InputFormat.PDF: PdfFormatOption(
            backend=PyPdfiumDocumentBackend,
            pipeline_options=pipeline_options
        )
    }
)

async def process_document(doc_id: str, raw_path: str):
    logger.debug("--- Processing: %s ---", doc_id)

    bundle_path = PROCESSED_DIR / doc_id
    bundle_path.mkdir(parents=True, exist_ok=True)

    result = None
    try:
        result = await asyncio.to_thread(converter.convert, raw_path)

        md = result.document.export_to_markdown()
        (bundle_path / "content.md").write_text(md)

        tables = result.document.tables
        table_metadata = []

        if tables:
            tables_dir = bundle_path / "tables"
            tables_dir.mkdir(exist_ok=True)

            for i, table in enumerate(tables):
                df = table.export_to_dataframe(doc=result.document)
                csv_filename = f"table_{i}.csv"
                df.to_csv(tables_dir / csv_filename, index=False)

                table_metadata.append({
                    "id": i,
                    "csv": csv_filename,
                    "status": "unprocessed",
                    "page_no": table.prov[0].page_no if table.prov else None
                })
        
        with open(bundle_path / "artifacts.json", "w") as f:
            json.dump({
                "doc_id": doc_id,
                "table_count": len(table_metadata),
                "tables": table_metadata,
                "processed_at": datetime.now().isoformat()
            }, f, indent=4)

        logger.debug("Successfully bundled doc_id: %s", doc_id)

    except Exception as e:
        logger.error("Failed to process %s: %s", doc_id, str(e))
    
    finally:
        if result and hasattr(result, "input") and result.input._backend:
            result.input._backend.unload()