from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.datamodel.pipeline_options import PdfPipelineOptions

from .constants import PROCESSED_DIR


from .utils import load_manifest
from utils.logger import get_logger
import logging

import asyncio

logger = get_logger(__name__, log_file="docprocessor.log", level=logging.DEBUG)

pipeline_options = PdfPipelineOptions()
pipeline_options.do_ocr = False
pipeline_options.do_table_structure = True
pipeline_options.generate_page_images = False
pipeline_options.generate_picture_images = False

async def process_document(
    doc_id: str,
    raw_path: str
):
    logger.debug("Starting to process the document... doc_id: %s, raw_path: %s", doc_id, raw_path)
    bundle_path = PROCESSED_DIR / doc_id
    bundle_path.mkdir(parents=True, exist_ok=True)

    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                backend=PyPdfiumDocumentBackend,
                pipeline_options=pipeline_options
            )
        }
    )
    logger.debug("Document converter initialized: %s", type(converter))
    result = converter.convert(raw_path)
    logger.debug("Document converted successfully: %s", type(result))

    (bundle_path / "content.md").write_text(
        result.document.export_to_markdown()
    )
    logger.debug("Content parsed to Markdown!!")

    tables_dir = bundle_path / "tables"
    tables_dir.mkdir(exist_ok=True)

    for i, table in enumerate(result.document.tables):
        logger.debug("starting to extract tables...")
        df = table.export_to_dataframe()
        df.to_csv(tables_dir / f"table_{i}.csv", index=False)

    logger.debug("Completed!")

if __name__ == '__main__':
    async def main():

        manifest = load_manifest()
        docs = manifest["documents"]

        i = 0

        for k, v in docs.items():
            local_path = v.get("local_path")

            if k and local_path:
                await process_document(k, local_path)
                i += 1

                if i > 5:
                    break

    asyncio.run(main())