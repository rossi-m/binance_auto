#!/usr/bin/env python3

import csv
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ai_bias_observer import CSV_FIELDS, append_csv
from ai_market_bias import news_item_score, news_source_group, normalize_bias, select_news_items


class NormalizeBiasTests(unittest.TestCase):
    def normalize(self, raw):
        return normalize_bias(raw, "ETHUSDT", 9, {"rss": 1})

    def test_underweight_without_explicit_short_is_futures_neutral(self):
        result = self.normalize(
            {
                "portfolio_stance": "underweight",
                "futures_bias": "neutral",
                "confidence": 0.7,
                "explicit_short_signal": False,
                "allow_long": True,
                "allow_short": True,
                "risk_multiplier": 0.8,
            }
        )
        self.assertEqual(result["portfolio_stance"], "underweight")
        self.assertEqual(result["futures_bias"], "neutral")
        self.assertEqual(result["bias"], "neutral")
        self.assertTrue(result["allow_long"])
        self.assertTrue(result["allow_short"])

    def test_legacy_bearish_does_not_automatically_block_long(self):
        result = self.normalize(
            {
                "bias": "bearish",
                "confidence": 0.65,
                "allow_long": False,
                "allow_short": True,
            }
        )
        self.assertEqual(result["futures_bias"], "short")
        self.assertFalse(result["explicit_short_signal"])
        self.assertTrue(result["allow_long"])
        self.assertTrue(result["allow_short"])

    def test_explicit_short_can_block_long(self):
        result = self.normalize(
            {
                "portfolio_stance": "underweight",
                "futures_bias": "short",
                "confidence": 0.75,
                "explicit_short_signal": True,
                "allow_long": False,
                "allow_short": True,
            }
        )
        self.assertFalse(result["allow_long"])
        self.assertTrue(result["allow_short"])
        self.assertEqual(result["bias"], "bearish")

    def test_explicit_short_preserves_allow_both_request(self):
        result = self.normalize(
            {
                "futures_bias": "short",
                "confidence": 0.75,
                "explicit_short_signal": True,
                "allow_long": True,
                "allow_short": True,
            }
        )
        self.assertTrue(result["allow_long"])
        self.assertTrue(result["allow_short"])

    def test_low_confidence_is_fail_open(self):
        result = self.normalize(
            {
                "futures_bias": "short",
                "confidence": 0.5,
                "explicit_short_signal": True,
                "allow_long": False,
            }
        )
        self.assertTrue(result["allow_long"])
        self.assertTrue(result["allow_short"])

    def test_risk_multiplier_keeps_legacy_alias(self):
        result = self.normalize({"risk_multiplier": 0.8})
        self.assertEqual(result["risk_multiplier"], 0.8)
        self.assertEqual(result["size_multiplier"], 0.8)
        self.assertEqual(result["time_horizon_hours"], 9)


class ObserverCsvMigrationTests(unittest.TestCase):
    def test_append_csv_migrates_legacy_header(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "observations.csv"
            path.write_text("observed_at,bias\nold,bearish\n", encoding="utf-8")

            append_csv(path, {"observed_at": "new", "bias": "neutral", "futures_bias": "neutral"})

            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fields = reader.fieldnames
                rows = list(reader)
            self.assertEqual(fields, CSV_FIELDS)
            self.assertEqual(rows[0]["bias"], "bearish")
            self.assertEqual(rows[1]["futures_bias"], "neutral")


class NewsSelectionTests(unittest.TestCase):
    NOW = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)

    def item(self, title, source, age_hours=1, summary=""):
        return {
            "title": title,
            "source": source,
            "summary": summary,
            "url": f"https://example.com/{title.replace(' ', '-')}",
            "published_at": (self.NOW - timedelta(hours=age_hours)).isoformat(),
        }

    def test_selection_reserves_space_for_each_available_source_group(self):
        items = []
        for index in range(8):
            items.append(self.item(f"Ethereum ETF update {index}", "finnhub:Reuters", index + 1))
        for index in range(3):
            items.append(self.item(f"Ethereum staking report {index}", "rss:https://coindesk.com/rss", index + 1))
            items.append(self.item(f"Ethereum regulation story {index}", "gdelt:US", index + 1))

        selected = select_news_items(items, limit=6, current_time=self.NOW)
        counts = {}
        for item in selected:
            group = news_source_group(item)
            counts[group] = counts.get(group, 0) + 1

        self.assertEqual(counts, {"finnhub": 2, "gdelt": 2, "rss": 2})

    def test_recent_eth_market_event_scores_above_old_generic_crypto_story(self):
        recent = self.item(
            "Ethereum ETF approval changes staking outlook",
            "rss:https://coindesk.com/rss",
            age_hours=2,
            summary="SEC approval and ETF inflows affect ETH staking demand.",
        )
        old = self.item("General crypto market commentary", "gdelt:US", age_hours=160)

        self.assertGreater(news_item_score(recent, self.NOW), news_item_score(old, self.NOW))

    def test_duplicate_title_keeps_higher_scoring_copy(self):
        title = "Ethereum ETF inflows accelerate"
        older = self.item(title, "gdelt:US", age_hours=100)
        newer = self.item(title, "finnhub:Reuters", age_hours=2)

        selected = select_news_items([older, newer], limit=12, current_time=self.NOW)

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["source"], "finnhub:Reuters")


if __name__ == "__main__":
    unittest.main()
