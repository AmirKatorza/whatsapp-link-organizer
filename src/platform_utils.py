from urllib.parse import urlparse


def normalize_url(url: str) -> str:
    return url.strip().rstrip(").,]}>\"'")


def detect_platform(url: str) -> str:
    domain = urlparse(url).netloc.lower()

    if "youtube.com" in domain or "youtu.be" in domain:
        return "youtube"
    if "facebook.com" in domain or "fb.watch" in domain:
        return "facebook"
    if "instagram.com" in domain:
        return "instagram"
    return "other"