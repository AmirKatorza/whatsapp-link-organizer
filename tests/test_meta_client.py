import os
import unittest
from unittest.mock import patch

from src import meta_client


class MetaClientTests(unittest.TestCase):
    def test_normalize_meta_url_strips_tracking_params(self) -> None:
        url = (
            "https://m.facebook.com/share/r/1HvQJfyXRm/"
            "?mibextid=xfxF2i&sfnsn=mo&fs=e&s=10&utm_source=whatsapp"
        )

        normalized = meta_client.normalize_meta_url(url)

        self.assertEqual(normalized, "https://www.facebook.com/share/r/1HvQJfyXRm")

    @patch("src.meta_client._extract_meta_browser_metadata")
    @patch("src.meta_client._extract_meta_api_metadata")
    @patch("src.meta_client._resolve_meta_redirect")
    def test_extract_meta_metadata_prefers_api_success(
        self,
        mock_redirect,
        mock_api,
        mock_browser,
    ) -> None:
        mock_redirect.return_value = {
            "canonical_url": "https://www.facebook.com/reel/123",
            "status": "redirect_resolved",
            "reason": "Resolved redirects before metadata extraction.",
        }
        mock_api.return_value = {
            "title": "Clip title",
            "description": None,
            "source_name": "Creator",
            "canonical_url": "https://www.facebook.com/reel/123",
            "enrichment_status": "success",
            "enrichment_reason": "Metadata extracted from Meta oembed_video.",
        }
        mock_browser.return_value = {
            "title": None,
            "description": None,
            "source_name": None,
            "canonical_url": "https://www.facebook.com/reel/123",
            "enrichment_status": "meta_browser_disabled",
            "enrichment_reason": "disabled",
        }

        result = meta_client.extract_meta_metadata("https://www.facebook.com/share/r/abc?s=1", "facebook")

        self.assertEqual(result["enrichment_status"], "success")
        self.assertEqual(result["title"], "Clip title")
        mock_browser.assert_not_called()

    @patch("src.meta_client._extract_meta_browser_metadata")
    @patch("src.meta_client._extract_meta_api_metadata")
    @patch("src.meta_client._resolve_meta_redirect")
    def test_extract_meta_metadata_falls_back_to_browser(
        self,
        mock_redirect,
        mock_api,
        mock_browser,
    ) -> None:
        mock_redirect.return_value = {
            "canonical_url": "https://www.instagram.com/reel/abc123",
            "status": "redirect_resolved",
            "reason": "Resolved redirects before metadata extraction.",
        }
        mock_api.return_value = {
            "title": None,
            "description": None,
            "source_name": None,
            "canonical_url": "https://www.instagram.com/reel/abc123",
            "enrichment_status": "meta_api_failed",
            "enrichment_reason": "Meta API did not return metadata for this URL.",
        }
        mock_browser.return_value = {
            "title": "Rendered reel",
            "description": "Rendered description",
            "source_name": "Instagram",
            "canonical_url": "https://www.instagram.com/reel/abc123",
            "enrichment_status": "success",
            "enrichment_reason": "Metadata extracted with Playwright.",
        }

        result = meta_client.extract_meta_metadata("https://www.instagram.com/reel/abc123/?igshid=123", "instagram")

        self.assertEqual(result["enrichment_status"], "success")
        self.assertEqual(result["source_name"], "Instagram")
        mock_browser.assert_called_once()

    def test_extract_meta_api_metadata_requires_token(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            result = meta_client._extract_meta_api_metadata(
                "https://www.instagram.com/reel/abc123",
                "instagram",
                description_limit=300,
                request_timeout=10,
            )

        self.assertEqual(result["enrichment_status"], "meta_api_token_missing")


if __name__ == "__main__":
    unittest.main()
