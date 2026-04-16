import csv
import logging
from typing import Any

from src.meta_client import extract_meta_metadata
from src.platform_utils import detect_platform, normalize_url

LOGGER = logging.getLogger(__name__)
PROGRESS_INTERVAL = 25
DESCRIPTION_MAX_LENGTH = 300
SUPPORTED_METADATA_PLATFORMS = {"youtube", "facebook", "instagram"}
METADATA_FIELDS = ["title", "description", "source_name"]


def enrich_links(input_csv_path: str, output_csv_path: str) -> None:
    _ensure_logging_config()

    with open(input_csv_path, newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        original_fieldnames = list(reader.fieldnames or [])
        output_fieldnames = original_fieldnames + [
            field for field in METADATA_FIELDS if field not in original_fieldnames
        ]

        with open(output_csv_path, "w", newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=output_fieldnames)
            writer.writeheader()

            for index, row in enumerate(reader, start=1):
                metadata = _empty_metadata()
                url = normalize_url((row.get("url") or "").strip())
                platform = _resolve_platform(row, url)

                try:
                    if not url:
                        LOGGER.warning("Row %s has no URL; leaving metadata empty", index)
                    else:
                        metadata = _extract_metadata_for_row(platform, url)
                except Exception as exc:
                    LOGGER.warning(
                        "Metadata extraction failed for row %s (%s): %s",
                        index,
                        url or "missing-url",
                        exc,
                    )
                    metadata = _empty_metadata()

                enriched_row = dict(row)
                enriched_row.update(metadata)
                writer.writerow(enriched_row)

                if index % PROGRESS_INTERVAL == 0:
                    LOGGER.info("Processed %s rows", index)

            LOGGER.info("Metadata enrichment finished. Output written to %s", output_csv_path)


def extract_youtube_metadata(url: str) -> dict[str, str | None]:
    metadata = _empty_metadata()

    try:
        from yt_dlp import YoutubeDL
    except ImportError as exc:
        LOGGER.warning("yt-dlp is not installed; cannot enrich YouTube URL %s: %s", url, exc)
        return metadata

    options = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": False,
    }

    try:
        with YoutubeDL(options) as ydl:
            info = ydl.extract_info(url, download=False) or {}
    except Exception as exc:
        LOGGER.warning("Failed to extract YouTube metadata for %s: %s", url, exc)
        return metadata

    metadata["title"] = _clean_text(info.get("title"))
    metadata["source_name"] = _clean_text(info.get("uploader"))
    metadata["description"] = _truncate_text(info.get("description"), DESCRIPTION_MAX_LENGTH)
    return metadata


def extract_social_metadata(platform: str, url: str) -> dict[str, str | None]:
    meta_result = extract_meta_metadata(url, platform, description_limit=DESCRIPTION_MAX_LENGTH)
    return {
        "title": meta_result.get("title"),
        "description": meta_result.get("description"),
        "source_name": meta_result.get("source_name"),
    }


def extract_generic_metadata(url: str) -> dict[str, str | None]:
    # Unsupported platforms are intentionally skipped to avoid network calls.
    return _empty_metadata()


def _extract_metadata_for_row(platform: str, url: str) -> dict[str, str | None]:
    if platform == "youtube":
        return extract_youtube_metadata(url)
    if platform in {"facebook", "instagram"}:
        return extract_social_metadata(platform, url)
    return extract_generic_metadata(url)


def _resolve_platform(row: dict[str, Any], url: str) -> str:
    platform = (row.get("platform") or "").strip().lower()
    if platform:
        return platform
    if not url:
        return "other"
    return detect_platform(url)


def _truncate_text(value: Any, limit: int) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    return text[:limit]


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    return text or None


def _empty_metadata(
) -> dict[str, str | None]:
    return {
        "title": None,
        "description": None,
        "source_name": None,
    }


def _ensure_logging_config() -> None:
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
