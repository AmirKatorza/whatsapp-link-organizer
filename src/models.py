from dataclasses import dataclass
from typing import Optional


@dataclass
class ExtractedLink:
    timestamp: str
    sender: str
    message_text: str
    url: str
    platform: str
    context: Optional[str] = None