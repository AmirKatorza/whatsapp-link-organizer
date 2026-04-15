import csv
from typing import Iterable

from src.models import ExtractedLink


def write_links_to_csv(records: Iterable[ExtractedLink], output_path: str) -> None:
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "timestamp",
                "sender",
                "message_text",
                "url",
                "platform",
                "context",
            ]
        )

        for r in records:
            writer.writerow(
                [
                    r.timestamp,
                    r.sender,
                    r.message_text,
                    r.url,
                    r.platform,
                    r.context or "",
                ]
            )