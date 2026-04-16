import csv
import unittest
from pathlib import Path
from unittest.mock import patch

from src.categorizer import categorize_links, get_categorizer_config


class CategorizerPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "data" / "processed"
        self.input_path = self.temp_dir / "test_categorizer_input.csv"
        self.output_path = self.temp_dir / "test_categorizer_output.csv"

    def tearDown(self) -> None:
        for path in (self.input_path, self.output_path):
            if path.exists():
                path.unlink()

    def test_categorize_links_uses_rules_for_strong_match(self) -> None:
        self._write_rows(
            [
                {
                    "timestamp": "2024-01-01 10:00",
                    "sender": "Amir",
                    "message_text": "Save this recipe",
                    "url": "https://example.com/recipe",
                    "platform": "instagram",
                    "context": "meal prep idea",
                    "title": "Healthy air fryer chicken recipe",
                    "description": "Easy meal prep with simple ingredients and high protein macros.",
                    "source_name": "Healthy kitchen",
                }
            ]
        )

        with patch.dict(
            "os.environ",
            {"CATEGORIZATION_ENABLE_LLM_FALLBACK": "1", "CATEGORIZATION_RULE_CONFIDENCE_THRESHOLD": "0.55"},
            clear=False,
        ), patch("src.categorizer.classify_with_llm") as mock_llm:
            categorize_links(str(self.input_path), str(self.output_path))

        rows = self._read_rows()
        self.assertEqual(rows[0]["category"], "Healthy Cooking / Recipes")
        self.assertEqual(rows[0]["categorization_method"], "rules")
        self.assertGreaterEqual(float(rows[0]["category_confidence"]), 0.55)
        mock_llm.assert_not_called()

    def test_categorize_links_uses_llm_for_ambiguous_rows(self) -> None:
        self._write_rows(
            [
                {
                    "timestamp": "2024-01-01 10:00",
                    "sender": "Amir",
                    "message_text": "Useful clip",
                    "url": "https://example.com/video",
                    "platform": "youtube",
                    "context": "trading and mindset",
                    "title": "Trading mindset for disciplined entries",
                    "description": "Build discipline and confidence while improving your trading process.",
                    "source_name": "Creator",
                }
            ]
        )

        with patch.dict(
            "os.environ",
            {"CATEGORIZATION_ENABLE_LLM_FALLBACK": "1", "CATEGORIZATION_RULE_CONFIDENCE_THRESHOLD": "0.80"},
            clear=False,
        ), patch("src.categorizer.classify_with_llm") as mock_llm:
            mock_llm.return_value = {
                "category": "General Trading / Investing",
                "confidence": 0.84,
                "reason": "The text is mainly about trading performance and discipline.",
                "status": "ok",
            }
            categorize_links(str(self.input_path), str(self.output_path))

        rows = self._read_rows()
        self.assertEqual(rows[0]["category"], "General Trading / Investing")
        self.assertEqual(rows[0]["categorization_method"], "llm")
        self.assertEqual(rows[0]["categorization_reason"], "The text is mainly about trading performance and discipline.")
        mock_llm.assert_called_once()

    def test_categorize_links_marks_review_when_llm_fails(self) -> None:
        self._write_rows(
            [
                {
                    "timestamp": "2024-01-01 10:00",
                    "sender": "Amir",
                    "message_text": "",
                    "url": "https://example.com/unknown",
                    "platform": "facebook",
                    "context": "",
                    "title": "",
                    "description": "",
                    "source_name": "",
                }
            ]
        )

        with patch.dict(
            "os.environ",
            {"CATEGORIZATION_ENABLE_LLM_FALLBACK": "1", "CATEGORIZATION_RULE_CONFIDENCE_THRESHOLD": "0.60"},
            clear=False,
        ), patch("src.categorizer.classify_with_llm") as mock_llm:
            mock_llm.return_value = {
                "category": "Other / Review",
                "confidence": 0.0,
                "reason": "LLM returned malformed JSON.",
                "status": "error",
            }
            categorize_links(str(self.input_path), str(self.output_path))

        rows = self._read_rows()
        self.assertEqual(rows[0]["category"], "Other / Review")
        self.assertEqual(rows[0]["categorization_method"], "review")
        self.assertIn("review", rows[0]["categorization_reason"].lower())

    def test_get_categorizer_config_reads_env(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "CATEGORIZATION_RULE_CONFIDENCE_THRESHOLD": "0.77",
                "CATEGORIZATION_ENABLE_LLM_FALLBACK": "0",
                "OPENAI_MODEL": "gpt-5-mini",
            },
            clear=False,
        ):
            config = get_categorizer_config()

        self.assertEqual(config.rule_confidence_threshold, 0.77)
        self.assertFalse(config.llm_fallback_enabled)
        self.assertEqual(config.openai_model, "gpt-5-mini")

    def _write_rows(self, rows: list[dict[str, str]]) -> None:
        fieldnames = [
            "timestamp",
            "sender",
            "message_text",
            "url",
            "platform",
            "context",
            "title",
            "description",
            "source_name",
        ]
        with self.input_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def _read_rows(self) -> list[dict[str, str]]:
        with self.output_path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))


if __name__ == "__main__":
    unittest.main()
