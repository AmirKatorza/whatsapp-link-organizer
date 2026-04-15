import re
from typing import List

from src.models import ExtractedLink
from src.platform_utils import detect_platform, normalize_url


URL_PATTERN = re.compile(r"https?://[^\s]+")
WHATSAPP_LINE_PATTERN = re.compile(
    r"^(?P<timestamp>.+?) - (?P<sender>.*?): (?P<message>.*)$"
)


def extract_links_from_export(file_path: str) -> List[ExtractedLink]:
    results: List[ExtractedLink] = []

    with open(file_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue

            match = WHATSAPP_LINE_PATTERN.match(line)
            if not match:
                continue

            timestamp = match.group("timestamp").strip()
            sender = match.group("sender").strip()
            message = match.group("message").strip()

            urls = URL_PATTERN.findall(message)
            if not urls:
                continue

            for raw_url in urls:
                url = normalize_url(raw_url)
                context = message.replace(raw_url, "").strip() or None

                results.append(
                    ExtractedLink(
                        timestamp=timestamp,
                        sender=sender,
                        message_text=message,
                        url=url,
                        platform=detect_platform(url),
                        context=context,
                    )
                )

    return results