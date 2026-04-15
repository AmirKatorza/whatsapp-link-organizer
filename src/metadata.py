import csv
import logging
from typing import Any

from src.platform_utils import detect_platform, normalize_url

LOGGER = logging.getLogger(__name__)
PROGRESS_INTERVAL = 25
REQUEST_TIMEOUT = 10
DESCRIPTION_MAX_LENGTH = 300
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
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

                try:
                    if url:
                        metadata = _extract_metadata_for_row(row, url)
                    else:
                        LOGGER.warning("Row %s has no URL; leaving metadata empty", index)
                except Exception as exc:
                    LOGGER.warning(
                        "Metadata extraction failed for row %s (%s): %s",
                        index,
                        url or "missing-url",
                        exc,
                    )

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


def extract_social_metadata(url: str) -> dict[str, str | None]:
    metadata = _empty_metadata()

    soup = _fetch_soup(url)
    if soup is None:
        return metadata

    metadata["title"] = _extract_meta_content(soup, "property", "og:title")
    metadata["description"] = _extract_meta_content(soup, "property", "og:description")
    metadata["source_name"] = _extract_meta_content(soup, "property", "og:site_name")
    return metadata


def extract_generic_metadata(url: str) -> dict[str, str | None]:
    metadata = _empty_metadata()

    soup = _fetch_soup(url)
    if soup is None:
        return metadata

    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        metadata["title"] = _clean_text(title_tag.string)

    return metadata


def _extract_metadata_for_row(row: dict[str, Any], url: str) -> dict[str, str | None]:
    platform = (row.get("platform") or "").strip().lower() or detect_platform(url)

    if platform == "youtube":
        return extract_youtube_metadata(url)
    if platform in {"facebook", "instagram"}:
        return extract_social_metadata(url)
    return extract_generic_metadata(url)


def _fetch_soup(url: str):
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError as exc:
        LOGGER.warning("Missing HTML parsing dependency for %s: %s", url, exc)
        return None

    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except Exception as exc:
        LOGGER.warning("HTTP request failed for %s: %s", url, exc)
        return None

    content_type = (response.headers.get("Content-Type") or "").lower()
    if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
        LOGGER.info("Skipping non-HTML response for %s (%s)", url, content_type or "unknown")
        return None

    return BeautifulSoup(response.text, "html.parser")


def _extract_meta_content(soup, attribute_name: str, attribute_value: str) -> str | None:
    tag = soup.find("meta", attrs={attribute_name: attribute_value})
    if not tag:
        return None
    return _clean_text(tag.get("content"))


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


def _empty_metadata() -> dict[str, str | None]:
    return {
        "title": None,
        "description": None,
        "source_name": None,
    }


def _ensure_logging_config() -> None:
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
