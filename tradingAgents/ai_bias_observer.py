#!/usr/bin/env python3
"""
Append AI research observations to a monthly CSV log.

This script is intentionally read-only with respect to trading. It does not
import bian_new.py, place orders, cancel orders, or change stops. It records the
latest AI bias together with current public market context for later review.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from ai_market_bias import (
    fetch_binance_summary,
    latest_tradingagents_report,
    now_exchange,
    symbol_to_binance,
)


CSV_FIELDS = [
    "observed_at",
    "symbol",
    "bias_generated_at",
    "bias_expires_at",
    "bias",
    "confidence",
    "allow_long",
    "allow_short",
    "size_multiplier",
    "reason",
    "risk_events",
    "news_used",
    "source_counts",
    "latest_price",
    "trend_1h",
    "pct_change_1h",
    "trend_4h",
    "pct_change_4h",
    "trend_1d",
    "pct_change_1d",
    "binance_errors",
    "tradingagents_available",
    "tradingagents_age_hours",
    "tradingagents_summary_path",
    "tradingagents_modified_at",
    "tradingagents_decision_path",
    "tradingagents_decision",
]


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def compact_json(value: Any) -> str:
    if value in (None, ""):
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def timeframe_value(binance: dict[str, Any], timeframe: str, key: str) -> Any:
    return binance.get("timeframes", {}).get(timeframe, {}).get(key, "")


def binance_errors(binance: dict[str, Any]) -> dict[str, str]:
    errors = {}
    for timeframe, payload in binance.get("timeframes", {}).items():
        if isinstance(payload, dict) and payload.get("error"):
            errors[timeframe] = str(payload.get("error"))
    return errors


def latest_tradingagents_decision(outputs_dir: Path, ticker: str) -> dict[str, Any]:
    slug = ticker.replace("/", "-").replace(":", "-")
    candidates = sorted(outputs_dir.glob(f"{slug}_*_decision.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        return {"available": False}
    path = candidates[0]
    try:
        decision = read_json(path)
    except Exception as exc:
        return {"available": False, "path": str(path), "error": f"{type(exc).__name__}: {exc}"}
    return {"available": True, "path": str(path), "decision": decision}


def build_observation(args: argparse.Namespace) -> dict[str, Any]:
    symbol = symbol_to_binance(args.symbol)
    research_dir = Path(args.research_dir)
    bias_path = Path(args.bias_file) if args.bias_file else research_dir / f"latest_bias_{symbol}.json"
    if not bias_path.exists():
        raise SystemExit(f"Bias file not found: {bias_path}")

    bias = read_json(bias_path)
    binance = fetch_binance_summary(symbol)
    report = latest_tradingagents_report(Path(args.outputs_dir), args.tradingagents_ticker)
    decision = latest_tradingagents_decision(Path(args.outputs_dir), args.tradingagents_ticker)

    latest_price = (
        timeframe_value(binance, "1h", "last_close")
        or timeframe_value(binance, "4h", "last_close")
        or timeframe_value(binance, "1d", "last_close")
    )

    return {
        "observed_at": now_exchange().isoformat(),
        "symbol": symbol,
        "bias_generated_at": bias.get("generated_at", ""),
        "bias_expires_at": bias.get("expires_at", ""),
        "bias": bias.get("bias", ""),
        "confidence": bias.get("confidence", ""),
        "allow_long": bias.get("allow_long", ""),
        "allow_short": bias.get("allow_short", ""),
        "size_multiplier": bias.get("size_multiplier", ""),
        "reason": bias.get("reason", ""),
        "risk_events": compact_json(bias.get("risk_events", [])),
        "news_used": compact_json(bias.get("news_used", [])),
        "source_counts": compact_json(bias.get("source_counts", {})),
        "latest_price": latest_price,
        "trend_1h": timeframe_value(binance, "1h", "trend_vs_recent_avg"),
        "pct_change_1h": timeframe_value(binance, "1h", "pct_change"),
        "trend_4h": timeframe_value(binance, "4h", "trend_vs_recent_avg"),
        "pct_change_4h": timeframe_value(binance, "4h", "pct_change"),
        "trend_1d": timeframe_value(binance, "1d", "trend_vs_recent_avg"),
        "pct_change_1d": timeframe_value(binance, "1d", "pct_change"),
        "binance_errors": compact_json(binance_errors(binance)),
        "tradingagents_available": report.get("available", False),
        "tradingagents_age_hours": report.get("age_hours", ""),
        "tradingagents_summary_path": report.get("path", ""),
        "tradingagents_modified_at": report.get("modified_at", ""),
        "tradingagents_decision_path": decision.get("path", ""),
        "tradingagents_decision": compact_json(decision.get("decision", decision.get("error", ""))),
    }


def default_log_path(research_dir: Path, observed_at: str) -> Path:
    month = datetime.fromisoformat(observed_at).strftime("%Y-%m")
    return research_dir / "logs" / f"ai_bias_{month}.csv"


def append_csv(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow({field: row.get(field, "") for field in CSV_FIELDS})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Append latest AI bias observation to CSV.")
    parser.add_argument("--symbol", default="ETHUSDT", help="Trading symbol, default ETHUSDT.")
    parser.add_argument("--tradingagents-ticker", default="ETH-USD", help="Ticker used by TradingAgents outputs.")
    parser.add_argument("--research-dir", default=".ai_research", help="Research cache/output directory.")
    parser.add_argument("--outputs-dir", default="outputs", help="TradingAgents outputs directory.")
    parser.add_argument("--bias-file", default=None, help="Optional explicit latest_bias JSON path.")
    parser.add_argument("--log-file", default=None, help="Optional explicit CSV log path.")
    parser.add_argument("--dry-run", action="store_true", help="Print observation without writing CSV.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    row = build_observation(args)
    if args.dry_run:
        print(json.dumps(row, ensure_ascii=False, indent=2))
        return

    log_path = Path(args.log_file) if args.log_file else default_log_path(Path(args.research_dir), row["observed_at"])
    append_csv(log_path, row)
    print(f"Appended observation: {log_path}")


if __name__ == "__main__":
    main()
