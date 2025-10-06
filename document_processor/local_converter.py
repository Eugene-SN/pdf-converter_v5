#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Command line utility for high quality conversion of digital PDFs to Markdown.

This CLI bypasses Airflow orchestration and uses the Docling processor directly
with OCR disabled by default, which is critical for digital PDFs where OCR tends
to degrade the output quality.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

if __package__ in (None, ""):
    PACKAGE_ROOT = Path(__file__).resolve().parent.parent
    if str(PACKAGE_ROOT) not in sys.path:
        sys.path.insert(0, str(PACKAGE_ROOT))
    from docling_processor import DoclingProcessor, DoclingConfig  # type: ignore
else:
    from .docling_processor import DoclingProcessor, DoclingConfig

LOGGER = logging.getLogger("local_pdf_converter")


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        stream=sys.stderr,
    )


@dataclass
class ConversionResult:
    """Container for conversion metadata returned to the shell script."""

    input_file: Path
    output_markdown: Path
    metadata_file: Path
    raw_text_file: Optional[Path]
    assets_dir: Path
    pages: int
    sections: int
    ocr_used: bool
    processing_time: float

    def to_json(self) -> str:
        payload: Dict[str, Any] = {
            "status": "success",
            "input_file": str(self.input_file),
            "output_markdown": str(self.output_markdown),
            "metadata_file": str(self.metadata_file),
            "assets_dir": str(self.assets_dir),
            "pages": self.pages,
            "sections": self.sections,
            "ocr_used": self.ocr_used,
            "processing_time": self.processing_time,
        }
        if self.raw_text_file is not None:
            payload["raw_text_file"] = str(self.raw_text_file)
        return json.dumps(payload, ensure_ascii=False)


def _normalise_markdown(markdown: str) -> str:
    """Collapse long blank sections and trim trailing whitespace."""

    lines: List[str] = []
    previous_blank = False
    for line in markdown.splitlines():
        stripped = line.rstrip()
        if not stripped:
            if previous_blank:
                continue
            previous_blank = True
        else:
            previous_blank = False
        lines.append(stripped)
    normalised = "\n".join(lines).strip()
    return normalised + "\n" if normalised else ""


async def _convert_single(
    processor: DoclingProcessor,
    pdf_path: Path,
    output_dir: Path,
    metadata_dir: Path,
    *,
    enable_ocr: bool,
    allow_ocr_fallback: bool,
    ocr_languages: str,
    export_raw_text: bool,
    overwrite: bool,
) -> ConversionResult:
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")

    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
    if not metadata_dir.exists():
        metadata_dir.mkdir(parents=True, exist_ok=True)

    document_stem = pdf_path.stem
    markdown_path = output_dir / f"{document_stem}.md"
    metadata_path = metadata_dir / f"{document_stem}.json"
    raw_text_path = output_dir / f"{document_stem}.txt"

    if markdown_path.exists() and not overwrite:
        raise FileExistsError(
            f"Output file already exists: {markdown_path}. Use --overwrite to replace it."
        )

    # Dedicated directory for extracted tables/images
    assets_dir = output_dir / f"{document_stem}_assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Processing %s (OCR enabled: %s, fallback: %s)", pdf_path, enable_ocr, allow_ocr_fallback)

    temp_parent = Path(processor.config.temp_dir)
    temp_parent.mkdir(parents=True, exist_ok=True)

    document_structure = await processor.process_document(
        str(pdf_path),
        str(assets_dir),
        use_ocr=enable_ocr,
        ocr_languages=ocr_languages,
        allow_ocr_fallback=allow_ocr_fallback,
    )

    markdown_content = document_structure.markdown_content or ""
    markdown_content = _normalise_markdown(markdown_content)
    if not markdown_content:
        raise ValueError(
            "Docling returned an empty markdown payload. Conversion cannot continue."
        )

    markdown_path.write_text(markdown_content, encoding="utf-8")

    if export_raw_text:
        raw_text = (document_structure.raw_text or "").strip()
        if raw_text:
            raw_text_path.write_text(raw_text + "\n", encoding="utf-8")
        else:
            raw_text_path = None
    else:
        raw_text_path = None

    metadata: Dict[str, Any] = document_structure.dict()
    metadata.setdefault("metadata", {})
    metadata["metadata"].update(
        {
            "conversion_tool": "local_docling_converter",
            "ocr_requested": enable_ocr,
            "ocr_fallback_allowed": allow_ocr_fallback,
            "assets_dir": str(assets_dir),
        }
    )
    metadata["output_markdown"] = str(markdown_path)
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    stats = document_structure.processing_stats or {}
    pages = int(stats.get("total_pages") or metadata.get("metadata", {}).get("total_pages") or 0)
    sections = len(document_structure.sections)
    ocr_used = bool(stats.get("ocr_used", enable_ocr))
    processing_time = float(stats.get("processing_time_seconds") or 0.0)

    return ConversionResult(
        input_file=pdf_path,
        output_markdown=markdown_path,
        metadata_file=metadata_path,
        raw_text_file=raw_text_path,
        assets_dir=assets_dir,
        pages=pages,
        sections=sections,
        ocr_used=ocr_used,
        processing_time=processing_time,
    )


async def _run_conversion(args: argparse.Namespace) -> ConversionResult:
    processor = DoclingProcessor(DoclingConfig())
    try:
        return await _convert_single(
            processor,
            pdf_path=Path(args.input).resolve(),
            output_dir=Path(args.output_dir).resolve(),
            metadata_dir=Path(args.metadata_dir).resolve(),
            enable_ocr=args.enable_ocr,
            allow_ocr_fallback=args.allow_ocr_fallback,
            ocr_languages=args.ocr_languages,
            export_raw_text=args.export_raw_text,
            overwrite=args.overwrite,
        )
    finally:
        LOGGER.debug("Conversion finished for %s", args.input)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert a digital PDF file to Markdown without OCR degradation.",
    )
    parser.add_argument("--input", required=True, help="Path to the source PDF file")
    parser.add_argument(
        "--output-dir",
        default="output/zh",
        help="Directory for the resulting Markdown file",
    )
    parser.add_argument(
        "--metadata-dir",
        default="output/metadata",
        help="Directory where structured JSON metadata will be stored",
    )
    parser.add_argument(
        "--ocr-languages",
        default="eng",
        help="Comma separated OCR languages (used only if OCR is enabled)",
    )
    parser.add_argument(
        "--enable-ocr",
        action="store_true",
        help="Explicitly enable OCR processing (disabled by default)",
    )
    parser.add_argument(
        "--allow-ocr-fallback",
        action="store_true",
        help="Allow automatic OCR fallback if Docling returns empty content",
    )
    parser.add_argument(
        "--export-raw-text",
        action="store_true",
        help="Export plain text alongside Markdown",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing Markdown/metadata files",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)

    try:
        result = asyncio.run(_run_conversion(args))
    except Exception as exc:  # noqa: BLE001 - want to surface any processing error
        error_payload = {
            "status": "error",
            "error": str(exc),
            "input_file": args.input,
        }
        print(json.dumps(error_payload, ensure_ascii=False), file=sys.stdout)
        LOGGER.error("Conversion failed: %s", exc, exc_info=args.verbose)
        return 1

    print(result.to_json())
    LOGGER.info(
        "Markdown saved to %s (pages=%s, sections=%s, OCR used=%s)",
        result.output_markdown,
        result.pages,
        result.sections,
        result.ocr_used,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
