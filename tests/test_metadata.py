import csv
import unittest
from pathlib import Path
from unittest.mock import patch

from src.metadata import enrich_links


class MetadataPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "data" / "processed"
        self.input_path = self.temp_dir / "test_input.csv"
        self.output_path = self.temp_dir / "test_output.csv"

    def tearDown(self) -> None:
        for path in (self.input_path, self.output_path):
            if path.exists():
                path.unlink()

    def test_enrich_links_writes_meta_status_columns(self) -> None:
        with self.input_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["timestamp", "sender", "message_text", "url", "platform", "context"])
            writer.writerow(
                [
                    "2024-01-01 10:00",
                    "Amir",
                    "Check this reel",
                    "https://www.facebook.com/share/r/abc?s=1&mibextid=xfxF2i",
                    "facebook",
                    "",
                ]
            )

        with patch("src.metadata.extract_meta_metadata") as mock_meta:
            mock_meta.return_value = {
                "title": "Meta title",
                "description": None,
                "source_name": "Facebook",
            }
            enrich_links(str(self.input_path), str(self.output_path))

        with self.output_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "Meta title")
        self.assertEqual(rows[0]["source_name"], "Facebook")
        self.assertEqual(
            set(rows[0].keys()),
            {"timestamp", "sender", "message_text", "url", "platform", "context", "title", "description", "source_name"},
        )

    def test_enrich_links_marks_missing_url(self) -> None:
        with self.input_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["timestamp", "sender", "message_text", "url", "platform", "context"])
            writer.writerow(["2024-01-01 10:00", "Amir", "No link", "", "other", ""])

        enrich_links(str(self.input_path), str(self.output_path))

        with self.output_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))

        self.assertEqual(rows[0]["title"], "")
        self.assertEqual(rows[0]["description"], "")
        self.assertEqual(rows[0]["source_name"], "")

    def test_other_links_are_passed_through_without_metadata_calls(self) -> None:
        with self.input_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["timestamp", "sender", "message_text", "url", "platform", "context"])
            writer.writerow(
                [
                    "2024-01-01 10:00",
                    "Amir",
                    "Useful article",
                    "https://cert-manager.io/docs/devops-tips/syncing-secrets-across-namespaces/",
                    "other",
                    "k8s note",
                ]
            )

        with patch("src.metadata.extract_social_metadata") as mock_social, patch(
            "src.metadata.extract_youtube_metadata"
        ) as mock_youtube, patch("src.metadata.extract_generic_metadata") as mock_generic:
            mock_generic.return_value = {"title": None, "description": None, "source_name": None}
            enrich_links(str(self.input_path), str(self.output_path))

        with self.output_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))

        mock_social.assert_not_called()
        mock_youtube.assert_not_called()
        mock_generic.assert_called_once_with(
            "https://cert-manager.io/docs/devops-tips/syncing-secrets-across-namespaces/"
        )
        self.assertEqual(rows[0]["url"], "https://cert-manager.io/docs/devops-tips/syncing-secrets-across-namespaces/")
        self.assertEqual(rows[0]["context"], "k8s note")
        self.assertEqual(rows[0]["title"], "")
        self.assertEqual(rows[0]["description"], "")
        self.assertEqual(rows[0]["source_name"], "")

    def test_facebook_share_links_still_use_social_flow(self) -> None:
        with self.input_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["timestamp", "sender", "message_text", "url", "platform", "context"])
            writer.writerow(
                [
                    "2024-01-01 10:00",
                    "Amir",
                    "Check this share link",
                    "https://www.facebook.com/share/r/abc123/?mibextid=xfxF2i&s=10",
                    "facebook",
                    "",
                ]
            )

        with patch("src.metadata.extract_social_metadata") as mock_social:
            mock_social.return_value = {
                "title": "Facebook title",
                "description": "Facebook description",
                "source_name": "Facebook",
            }
            enrich_links(str(self.input_path), str(self.output_path))

        mock_social.assert_called_once_with(
            "facebook",
            "https://www.facebook.com/share/r/abc123/?mibextid=xfxF2i&s=10",
        )

    def test_missing_platform_uses_detected_other_and_skips_network(self) -> None:
        with self.input_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["timestamp", "sender", "message_text", "url", "platform", "context"])
            writer.writerow(
                [
                    "2024-01-01 10:00",
                    "Amir",
                    "Interesting article",
                    "https://example.com/post",
                    "",
                    "",
                ]
            )

        with patch("src.metadata.extract_social_metadata") as mock_social, patch(
            "src.metadata.extract_youtube_metadata"
        ) as mock_youtube, patch("src.metadata.extract_generic_metadata") as mock_generic:
            mock_generic.return_value = {"title": None, "description": None, "source_name": None}
            enrich_links(str(self.input_path), str(self.output_path))

        mock_social.assert_not_called()
        mock_youtube.assert_not_called()
        mock_generic.assert_called_once_with("https://example.com/post")


if __name__ == "__main__":
    unittest.main()
