import os
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()

META_ACCESS_TOKEN_ENV = "META_ACCESS_TOKEN"
META_GRAPH_BASE_URL_ENV = "META_GRAPH_BASE_URL"
META_ENABLE_PLAYWRIGHT_ENV = "META_ENABLE_PLAYWRIGHT"
META_PLAYWRIGHT_TIMEOUT_ENV = "META_PLAYWRIGHT_TIMEOUT_MS"
DEFAULT_REQUEST_TIMEOUT = 10
DEFAULT_PLAYWRIGHT_TIMEOUT_MS = 15000
DEFAULT_META_GRAPH_BASE_URL = "https://graph.facebook.com/v22.0"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
META_TRACKING_PARAMS = {
    "fbclid",
    "fs",
    "igshid",
    "mibextid",
    "sfnsn",
    "s",
    "utm_campaign",
    "utm_content",
    "utm_medium",
    "utm_source",
    "utm_term",
}


def extract_meta_metadata(
    url: str,
    platform: str,
    *,
    description_limit: int = 300,
    request_timeout: int = DEFAULT_REQUEST_TIMEOUT,
) -> dict[str, str | None]:
    normalized_url = normalize_meta_url(url)
    redirect_result = _resolve_meta_redirect(normalized_url, request_timeout=request_timeout)
    candidate_url = redirect_result.get("canonical_url") or normalized_url

    api_result = _extract_meta_api_metadata(
        candidate_url,
        platform,
        description_limit=description_limit,
        request_timeout=request_timeout,
    )
    if api_result["enrichment_status"] == "success":
        return api_result

    browser_result = _extract_meta_browser_metadata(
        candidate_url,
        description_limit=description_limit,
    )
    if browser_result["enrichment_status"] == "success":
        return browser_result

    return _merge_attempt_results(
        candidate_url,
        api_result,
        browser_result,
        redirect_result=redirect_result,
    )


def normalize_meta_url(url: str) -> str:
    parsed = urlparse(url.strip())

    if not parsed.scheme:
        return url

    netloc = parsed.netloc.lower()
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))

    # Some Facebook shared links go through l.facebook.com with the real target in `u`.
    if "l.facebook.com" in netloc and query.get("u"):
        return normalize_meta_url(query["u"])

    if netloc == "m.facebook.com":
        netloc = "www.facebook.com"
    elif netloc == "instagram.com":
        netloc = "www.instagram.com"

    filtered_query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in META_TRACKING_PARAMS and not key.lower().startswith("utm_")
    ]

    return urlunparse(
        (
            parsed.scheme.lower(),
            netloc,
            parsed.path.rstrip("/") or parsed.path or "/",
            parsed.params,
            urlencode(filtered_query, doseq=True),
            "",
        )
    )


def _resolve_meta_redirect(url: str, *, request_timeout: int) -> dict[str, str | None]:
    try:
        import requests
    except ImportError as exc:
        return {
            "canonical_url": url,
            "status": "dependency_missing",
            "reason": f"requests is unavailable: {exc}",
        }

    try:
        response = requests.get(
            url,
            headers=DEFAULT_HEADERS,
            timeout=request_timeout,
            allow_redirects=True,
            stream=True,
        )
        response.close()
    except Exception as exc:
        return {
            "canonical_url": url,
            "status": "redirect_unresolved",
            "reason": str(exc),
        }

    return {
        "canonical_url": normalize_meta_url(response.url or url),
        "status": "redirect_resolved",
        "reason": "Resolved redirects before metadata extraction.",
    }


def _extract_meta_api_metadata(
    url: str,
    platform: str,
    *,
    description_limit: int,
    request_timeout: int,
) -> dict[str, str | None]:
    token = os.getenv(META_ACCESS_TOKEN_ENV, "").strip()
    if not token:
        return _empty_result(
            url,
            "meta_api_token_missing",
            "META_ACCESS_TOKEN is not configured.",
        )

    try:
        import requests
    except ImportError as exc:
        return _empty_result(
            url,
            "dependency_missing",
            f"requests is unavailable: {exc}",
        )

    last_error = None
    for endpoint in _meta_oembed_endpoints(platform):
        try:
            response = requests.get(
                f"{_meta_graph_base_url()}/{endpoint}",
                params={
                    "url": url,
                    "access_token": token,
                    "omitscript": "true",
                },
                headers=DEFAULT_HEADERS,
                timeout=request_timeout,
            )
        except Exception as exc:
            last_error = str(exc)
            continue

        try:
            response.raise_for_status()
        except Exception as exc:
            last_error = str(exc)
            continue

        try:
            data = response.json()
        except ValueError as exc:
            last_error = f"Invalid JSON from Meta API: {exc}"
            continue

        result = _empty_result(
            url,
            "success",
            f"Metadata extracted from Meta {endpoint}.",
        )
        result["title"] = _clean_text(data.get("title"))
        result["description"] = _truncate_text(data.get("description"), description_limit)
        result["source_name"] = _clean_text(data.get("author_name") or data.get("provider_name"))
        result["canonical_url"] = normalize_meta_url(_clean_text(data.get("url")) or url)

        if _has_metadata(result):
            return result

        result["enrichment_status"] = "no_metadata"
        result["enrichment_reason"] = f"Meta {endpoint} returned no usable metadata."
        return result

    return _empty_result(
        url,
        "meta_api_failed",
        last_error or "Meta API did not return metadata for this URL.",
    )


def _extract_meta_browser_metadata(
    url: str,
    *,
    description_limit: int,
) -> dict[str, str | None]:
    if not _is_truthy_env(os.getenv(META_ENABLE_PLAYWRIGHT_ENV, "1")):
        return _empty_result(
            url,
            "meta_browser_disabled",
            "Playwright fallback is disabled by META_ENABLE_PLAYWRIGHT.",
        )

    try:
        from bs4 import BeautifulSoup
    except ImportError as exc:
        return _empty_result(
            url,
            "dependency_missing",
            f"beautifulsoup4 is unavailable: {exc}",
        )

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        return _empty_result(
            url,
            "meta_browser_unavailable",
            f"Playwright is unavailable: {exc}",
        )

    timeout_ms = _safe_int(
        os.getenv(META_PLAYWRIGHT_TIMEOUT_ENV),
        DEFAULT_PLAYWRIGHT_TIMEOUT_MS,
    )

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page(user_agent=DEFAULT_HEADERS["User-Agent"])
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                page.wait_for_timeout(1200)
                content = page.content()
                final_url = normalize_meta_url(page.url or url)
            finally:
                browser.close()
    except PlaywrightTimeoutError as exc:
        return _empty_result(
            url,
            "meta_browser_timeout",
            str(exc),
        )
    except Exception as exc:
        return _empty_result(
            url,
            "meta_browser_failed",
            str(exc),
        )

    lowered = content.lower()
    if _looks_like_login_wall(final_url, lowered):
        return _empty_result(
            final_url,
            "meta_browser_blocked",
            "Meta required login or blocked page access.",
        )

    soup = BeautifulSoup(content, "html.parser")
    result = _empty_result(
        final_url,
        "success",
        "Metadata extracted with Playwright.",
    )
    result["title"] = (
        _extract_meta_content(soup, "property", "og:title")
        or _extract_meta_content(soup, "name", "twitter:title")
        or _clean_text(soup.title.string if soup.title and soup.title.string else None)
    )
    result["description"] = (
        _truncate_text(_extract_meta_content(soup, "property", "og:description"), description_limit)
        or _truncate_text(_extract_meta_content(soup, "name", "description"), description_limit)
    )
    result["source_name"] = (
        _extract_meta_content(soup, "property", "og:site_name")
        or _extract_meta_content(soup, "name", "application-name")
    )
    canonical_tag = soup.find("link", attrs={"rel": "canonical"})
    if canonical_tag and canonical_tag.get("href"):
        result["canonical_url"] = normalize_meta_url(canonical_tag["href"])

    if _has_metadata(result):
        return result

    result["enrichment_status"] = "no_metadata"
    result["enrichment_reason"] = "Playwright loaded the page, but no usable metadata tags were found."
    return result


def _merge_attempt_results(
    url: str,
    api_result: dict[str, str | None],
    browser_result: dict[str, str | None],
    *,
    redirect_result: dict[str, str | None],
) -> dict[str, str | None]:
    canonical_url = (
        browser_result.get("canonical_url")
        or api_result.get("canonical_url")
        or redirect_result.get("canonical_url")
        or url
    )

    preferred = browser_result
    if browser_result["enrichment_status"] in {"meta_browser_disabled", "meta_browser_unavailable"}:
        preferred = api_result
    elif browser_result["enrichment_status"] == "no_metadata" and api_result["enrichment_status"] != "success":
        preferred = browser_result

    result = _empty_result(
        canonical_url,
        preferred["enrichment_status"] or api_result["enrichment_status"] or "meta_unresolved",
        preferred["enrichment_reason"] or api_result["enrichment_reason"],
    )
    return result


def _meta_oembed_endpoints(platform: str) -> list[str]:
    if platform == "instagram":
        return ["instagram_oembed"]
    return ["oembed_post", "oembed_video", "oembed_page"]


def _meta_graph_base_url() -> str:
    return os.getenv(META_GRAPH_BASE_URL_ENV, DEFAULT_META_GRAPH_BASE_URL).strip()


def _empty_result(
    canonical_url: str | None,
    enrichment_status: str,
    enrichment_reason: str | None,
) -> dict[str, str | None]:
    return {
        "title": None,
        "description": None,
        "source_name": None,
        "canonical_url": canonical_url,
        "enrichment_status": enrichment_status,
        "enrichment_reason": enrichment_reason,
    }


def _extract_meta_content(soup: Any, attribute_name: str, attribute_value: str) -> str | None:
    tag = soup.find("meta", attrs={attribute_name: attribute_value})
    if not tag:
        return None
    return _clean_text(tag.get("content"))


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    return text or None


def _truncate_text(value: Any, limit: int) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    return text[:limit]


def _has_metadata(metadata: dict[str, str | None]) -> bool:
    return any(metadata.get(field) for field in ("title", "description", "source_name"))


def _looks_like_login_wall(final_url: str, lowered_content: str) -> bool:
    return any(
        marker in lowered_content
        for marker in (
            "log into facebook",
            "log in to facebook",
            "login • instagram",
            "log in • instagram",
            "sign up for facebook",
        )
    ) or "/login" in final_url


def _is_truthy_env(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _safe_int(raw_value: str | None, default: int) -> int:
    try:
        return int(raw_value) if raw_value is not None else default
    except (TypeError, ValueError):
        return default
