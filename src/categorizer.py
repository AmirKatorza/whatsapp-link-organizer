import csv
import json
import logging
import os
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from urllib import error, request
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv() -> bool:
        return False

LOGGER = logging.getLogger(__name__)
PROGRESS_INTERVAL = 25
CATEGORIZATION_FIELDS = ["title", "description", "source_name", "context", "message_text"]
OUTPUT_FIELDS = [
    "category",
    "category_confidence",
    "categorization_reason",
    "categorization_method",
]
PREFERRED_CATEGORIES = [
    "SMC / Trading Concepts",
    "General Trading / Investing",
    "Daily Affirmations / Mindset",
    "Psychology / Self Development",
    "Healthy Cooking / Recipes",
    "Nutrition / Health",
    "Fitness / Training",
    "Business / Entrepreneurship",
    "Other / Review",
]
FIELD_WEIGHTS = {
    "title": 1.0,
    "description": 0.8,
    "context": 0.7,
    "message_text": 0.55,
    "source_name": 0.35,
}
CATEGORY_RULES: dict[str, dict[str, float]] = {
    "SMC / Trading Concepts": {
        "smc": 4.0,
        "smart money": 4.0,
        "order block": 3.8,
        "breaker block": 3.8,
        "fair value gap": 3.8,
        "fvg": 3.0,
        "liquidity sweep": 3.6,
        "liquidity grab": 3.6,
        "market structure": 3.4,
        "bos": 2.8,
        "choch": 3.0,
        "mitigation": 2.5,
        "displacement": 2.5,
        "premium discount": 2.8,
        "ict": 3.6,
        "kill zone": 2.8,
        "inducement": 2.8,
        "institutional": 2.2,
        "forex setup": 2.4,
    },
    "General Trading / Investing": {
        "trading": 2.2,
        "trader": 2.0,
        "investing": 2.6,
        "investment": 2.4,
        "stocks": 2.4,
        "stock market": 2.6,
        "forex": 2.8,
        "crypto": 2.4,
        "bitcoin": 2.2,
        "ethereum": 2.2,
        "nasdaq": 2.0,
        "sp500": 2.0,
        "gold": 1.8,
        "risk management": 2.6,
        "technical analysis": 2.6,
        "swing trading": 2.8,
        "day trading": 2.8,
        "entry": 1.4,
        "stop loss": 2.2,
        "take profit": 2.2,
    },
    "Daily Affirmations / Mindset": {
        "affirmation": 3.6,
        "affirmations": 3.6,
        "mindset": 2.8,
        "i am": 2.4,
        "gratitude": 2.6,
        "visualize": 2.2,
        "manifest": 2.6,
        "manifestation": 2.6,
        "abundance": 2.2,
        "positive thinking": 2.4,
        "self belief": 2.2,
        "believe in yourself": 2.6,
        "motivation": 2.0,
        "morning routine": 2.0,
    },
    "Psychology / Self Development": {
        "psychology": 3.2,
        "self development": 3.2,
        "self improvement": 3.2,
        "discipline": 2.6,
        "habits": 2.4,
        "mental health": 2.8,
        "emotional": 2.0,
        "confidence": 2.0,
        "healing": 2.0,
        "trauma": 2.4,
        "boundaries": 2.2,
        "self awareness": 2.4,
        "therapy": 2.2,
        "resilience": 2.2,
        "dopamine": 1.8,
        "inner child": 2.2,
    },
    "Healthy Cooking / Recipes": {
        "recipe": 3.6,
        "recipes": 3.6,
        "cook": 2.0,
        "cooking": 2.4,
        "meal prep": 3.0,
        "easy meal": 2.2,
        "healthy recipe": 3.2,
        "ingredients": 2.0,
        "air fryer": 2.8,
        "salad": 2.0,
        "soup": 1.8,
        "bake": 1.8,
        "oven": 1.2,
        "protein pancakes": 2.8,
        "smoothie": 2.2,
    },
    "Nutrition / Health": {
        "nutrition": 3.4,
        "healthy eating": 3.0,
        "protein": 2.2,
        "calories": 2.4,
        "macro": 2.4,
        "micronutrients": 2.8,
        "gut health": 2.8,
        "supplements": 2.4,
        "vitamin": 2.2,
        "minerals": 2.2,
        "blood sugar": 2.6,
        "hormones": 2.0,
        "diet": 2.0,
        "hydration": 2.0,
        "anti inflammatory": 2.6,
    },
    "Fitness / Training": {
        "workout": 3.2,
        "training": 2.8,
        "fitness": 3.0,
        "gym": 2.6,
        "exercise": 2.6,
        "strength": 2.2,
        "hypertrophy": 2.8,
        "cardio": 2.2,
        "running": 2.0,
        "mobility": 2.2,
        "fat loss": 2.2,
        "muscle": 2.0,
        "personal trainer": 2.8,
        "steps": 1.8,
    },
    "Business / Entrepreneurship": {
        "business": 3.0,
        "entrepreneur": 3.2,
        "entrepreneurship": 3.2,
        "startup": 2.8,
        "sales": 2.2,
        "marketing": 2.4,
        "offer": 1.8,
        "client acquisition": 3.0,
        "founder": 2.4,
        "revenue": 2.4,
        "profit": 2.2,
        "brand": 2.0,
        "agency": 2.2,
        "ecommerce": 2.6,
        "online business": 2.8,
    },
}


@dataclass(frozen=True)
class CategorizerConfig:
    rule_confidence_threshold: float
    llm_fallback_enabled: bool
    openai_model: str


@dataclass(frozen=True)
class RuleClassification:
    category: str
    confidence: float
    reason: str
    matched_terms: dict[str, list[str]]
    scores: dict[str, float]
    is_uncertain: bool


def categorize_links(input_csv_path: str, output_csv_path: str) -> None:
    load_dotenv()
    _ensure_logging_config()
    config = get_categorizer_config()
    category_counts: Counter[str] = Counter()
    method_counts: Counter[str] = Counter()

    with open(input_csv_path, newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        original_fieldnames = list(reader.fieldnames or [])
        output_fieldnames = original_fieldnames + [
            field for field in OUTPUT_FIELDS if field not in original_fieldnames
        ]

        with open(output_csv_path, "w", newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=output_fieldnames)
            writer.writeheader()

            for index, row in enumerate(reader, start=1):
                classification = _categorize_row(row, config)
                output_row = dict(row)
                output_row.update(
                    {
                        "category": classification["category"],
                        "category_confidence": f"{classification['confidence']:.2f}",
                        "categorization_reason": classification["reason"],
                        "categorization_method": classification["method"],
                    }
                )
                writer.writerow(output_row)

                category_counts[classification["category"]] += 1
                method_counts[classification["method"]] += 1

                if index % PROGRESS_INTERVAL == 0:
                    LOGGER.info(
                        "Categorized %s rows (rules=%s, llm=%s, review=%s)",
                        index,
                        method_counts.get("rules", 0),
                        method_counts.get("llm", 0),
                        method_counts.get("review", 0),
                    )

    LOGGER.info("Categorization finished. Output written to %s", output_csv_path)
    LOGGER.info("Category counts: %s", dict(category_counts))
    LOGGER.info("Categorization methods: %s", dict(method_counts))


def get_categorizer_config() -> CategorizerConfig:
    threshold_raw = os.getenv("CATEGORIZATION_RULE_CONFIDENCE_THRESHOLD", "0.62")
    try:
        threshold = float(threshold_raw)
    except ValueError:
        threshold = 0.62

    return CategorizerConfig(
        rule_confidence_threshold=max(0.0, min(1.0, threshold)),
        llm_fallback_enabled=_parse_bool(os.getenv("CATEGORIZATION_ENABLE_LLM_FALLBACK", "1")),
        openai_model=(os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip(),
    )


def classify_with_llm(text_payload: str) -> dict[str, Any]:
    load_dotenv()
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    model = (os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()

    if not api_key:
        return {
            "category": "Other / Review",
            "confidence": 0.0,
            "reason": "OPENAI_API_KEY is not configured.",
            "status": "unavailable",
        }

    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "category": {"type": "string", "enum": PREFERRED_CATEGORIES},
            "confidence": {"type": "number"},
            "reason": {"type": "string"},
        },
        "required": ["category", "confidence", "reason"],
    }
    request_body = {
        "model": model,
        "input": [
            {
                "role": "system",
                "content": (
                    "Classify short social-media link metadata into exactly one category from the provided list. "
                    "Return JSON only. Use only the evidence in the text. If the evidence is weak or mixed, "
                    "choose 'Other / Review'. Keep the reason brief and practical."
                ),
            },
            {"role": "user", "content": text_payload},
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "link_categorization",
                "schema": schema,
                "strict": True,
            }
        },
    }

    try:
        http_request = request.Request(
            "https://api.openai.com/v1/responses",
            data=json.dumps(request_body).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with request.urlopen(http_request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (error.URLError, error.HTTPError, TimeoutError, ValueError) as exc:
        LOGGER.warning("OpenAI categorization request failed: %s", exc)
        return {
            "category": "Other / Review",
            "confidence": 0.0,
            "reason": "LLM request failed.",
            "status": "error",
        }

    try:
        raw_text = _extract_openai_output_text(payload)
        parsed = json.loads(raw_text)
    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        LOGGER.warning("Malformed OpenAI categorization response: %s", exc)
        return {
            "category": "Other / Review",
            "confidence": 0.0,
            "reason": "LLM returned malformed JSON.",
            "status": "error",
        }

    category = parsed.get("category")
    reason = _clean_text(parsed.get("reason")) or "LLM response did not include a usable reason."
    try:
        confidence = float(parsed.get("confidence", 0.0))
    except (TypeError, ValueError):
        confidence = 0.0

    if category not in PREFERRED_CATEGORIES:
        return {
            "category": "Other / Review",
            "confidence": 0.0,
            "reason": "LLM returned an unsupported category.",
            "status": "error",
        }

    return {
        "category": category,
        "confidence": max(0.0, min(1.0, confidence)),
        "reason": reason[:240],
        "status": "ok",
    }


def classify_with_rules(row: dict[str, Any], config: CategorizerConfig) -> RuleClassification:
    normalized_fields = {
        field: normalize_text(row.get(field))
        for field in CATEGORIZATION_FIELDS
    }
    raw_fields = {
        field: _clean_text(row.get(field)) or ""
        for field in CATEGORIZATION_FIELDS
    }
    scores = {category: 0.0 for category in CATEGORY_RULES}
    matched_terms: dict[str, list[str]] = {category: [] for category in CATEGORY_RULES}

    for category, keyword_weights in CATEGORY_RULES.items():
        for keyword, keyword_weight in keyword_weights.items():
            normalized_keyword = normalize_text(keyword)
            if not normalized_keyword:
                continue

            for field_name, text in normalized_fields.items():
                if not text:
                    continue
                occurrences = _count_keyword_occurrences(text, normalized_keyword)
                if occurrences <= 0:
                    continue

                weighted_score = min(occurrences, 2) * keyword_weight * FIELD_WEIGHTS[field_name]
                scores[category] += weighted_score
                matched_terms[category].append(f"{keyword} ({field_name})")

    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    top_category, top_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0
    text_length = sum(len(value) for value in raw_fields.values() if value)
    populated_fields = sum(1 for value in raw_fields.values() if value)

    if top_score <= 0:
        return RuleClassification(
            category="Other / Review",
            confidence=0.0,
            reason="No rule-based category match found in the available text fields.",
            matched_terms=matched_terms,
            scores=scores,
            is_uncertain=True,
        )

    gap = top_score - second_score
    coverage_bonus = min(0.12, text_length / 800)
    confidence = min(
        0.97,
        0.28 + min(top_score / 18, 0.42) + min(gap / 10, 0.18) + coverage_bonus,
    )
    top_matches = matched_terms[top_category][:4]
    match_summary = ", ".join(top_matches) if top_matches else "general keyword evidence"
    reason = f"Rule score favored {top_category} from: {match_summary}."

    is_sparse = populated_fields < 2 or text_length < 40
    is_uncertain = (
        confidence < config.rule_confidence_threshold
        or gap < 1.2
        or (second_score > 0 and gap / max(top_score, 1.0) < 0.18)
        or is_sparse
    )
    if is_sparse:
        reason = "Available metadata is sparse, so the rule match is weak."
    elif is_uncertain and second_score > 0:
        second_category = ranked[1][0]
        reason = (
            f"Rule scores were close between {top_category} and {second_category}; "
            f"top evidence: {match_summary}."
        )

    return RuleClassification(
        category=top_category,
        confidence=max(0.0, min(1.0, confidence)),
        reason=reason[:240],
        matched_terms=matched_terms,
        scores=scores,
        is_uncertain=is_uncertain,
    )


def normalize_text(value: Any) -> str:
    cleaned = _clean_text(value)
    if not cleaned:
        return ""

    ascii_text = unicodedata.normalize("NFKD", cleaned).encode("ascii", "ignore").decode("ascii")
    ascii_text = ascii_text.lower()
    ascii_text = re.sub(r"[^a-z0-9]+", " ", ascii_text)
    return re.sub(r"\s+", " ", ascii_text).strip()


def build_text_payload(row: dict[str, Any], rule_result: RuleClassification) -> str:
    lines = [
        "Classify this link into one preferred category.",
        f"Preferred categories: {', '.join(PREFERRED_CATEGORIES)}",
        f"Rule guess: {rule_result.category}",
        f"Rule confidence: {rule_result.confidence:.2f}",
    ]

    for field in CATEGORIZATION_FIELDS:
        value = _clean_text(row.get(field))
        if value:
            lines.append(f"{field}: {value[:800]}")

    return "\n".join(lines)


def _categorize_row(row: dict[str, Any], config: CategorizerConfig) -> dict[str, Any]:
    rule_result = classify_with_rules(row, config)
    if not rule_result.is_uncertain:
        return {
            "category": rule_result.category,
            "confidence": rule_result.confidence,
            "reason": rule_result.reason,
            "method": "rules",
        }

    if not config.llm_fallback_enabled:
        return {
            "category": rule_result.category if rule_result.category in PREFERRED_CATEGORIES else "Other / Review",
            "confidence": rule_result.confidence,
            "reason": f"{rule_result.reason} LLM fallback is disabled.",
            "method": "review",
        }

    llm_result = classify_with_llm(build_text_payload(row, rule_result))
    if llm_result.get("status") == "ok":
        return {
            "category": llm_result["category"],
            "confidence": llm_result["confidence"],
            "reason": llm_result["reason"],
            "method": "llm",
        }

    fallback_category = (
        rule_result.category if rule_result.category in PREFERRED_CATEGORIES else "Other / Review"
    )
    return {
        "category": fallback_category,
        "confidence": rule_result.confidence,
        "reason": f"{rule_result.reason} LLM fallback failed, keeping row for review.",
        "method": "review",
    }


def _count_keyword_occurrences(text: str, keyword: str) -> int:
    pattern = rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])"
    return len(re.findall(pattern, text))


def _extract_openai_output_text(payload: dict[str, Any]) -> str:
    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    for output_item in payload.get("output", []):
        for content_item in output_item.get("content", []):
            if content_item.get("type") in {"output_text", "text"}:
                text = content_item.get("text")
                if isinstance(text, str) and text.strip():
                    return text

    raise KeyError("OpenAI response did not include output text.")


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _ensure_logging_config() -> None:
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
