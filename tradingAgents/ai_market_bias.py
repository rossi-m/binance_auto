#!/usr/bin/env python3
"""
Build a low-frequency AI research snapshot and optional market-bias JSON.

This script is intentionally independent from bian_new.py. It fetches public
research inputs, caches them under .ai_research/, and can optionally call
DeepSeek to compress the inputs into latest_bias_SYMBOL.json.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import sys
from datetime import UTC, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import requests
from dotenv import load_dotenv

load_dotenv()

REQUEST_TIMEOUT = 30
DEFAULT_RESEARCH_DIR = ".ai_research"
DEFAULT_OUTPUTS_DIR = "outputs"
DEFAULT_DEEPSEEK_MODEL = "deepseek-chat"
DEFAULT_TRADINGAGENTS_PROVIDER = "deepseek"
DEFAULT_TRADINGAGENTS_DEEP = "deepseek-v4-pro"
DEFAULT_TRADINGAGENTS_QUICK = "deepseek-v4-flash"
DEFAULT_TRADINGAGENTS_LLM_TIMEOUT = 300.0
DEFAULT_TRADINGAGENTS_LLM_MAX_RETRIES = 1
DEFAULT_TRADINGAGENTS_MAX_DATA_ROWS = 60
DEFAULT_TRADINGAGENTS_MAX_NEWS_ITEMS = 12
DEFAULT_TRADINGAGENTS_MAX_NEWS_SUMMARY_CHARS = 500
NEWS_KEYWORDS = (
    "ETH", "Ethereum", "Bitcoin", "BTC", "crypto", "stablecoin", "ETF",
    "SEC", "Fed", "rates", "inflation", "risk appetite", "Binance",
)
RSS_FEEDS = (
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://decrypt.co/feed",
    "https://blog.ethereum.org/feed.xml",
)
FRED_SERIES = {
    "FEDFUNDS": "Fed Funds Rate",
    "CPIAUCSL": "CPI",
    "UNRATE": "Unemployment Rate",
    "DGS10": "10Y Treasury Yield",
    "DGS2": "2Y Treasury Yield",
}
EXCHANGE_TZ = timezone(timedelta(hours=8))


def now_utc() -> datetime:
    return datetime.now(UTC)


def now_exchange() -> datetime:
    return datetime.now(EXCHANGE_TZ)


def symbol_to_binance(symbol: str) -> str:
    compact = symbol.upper().replace("/", "").replace("-", "").replace(":", "")
    if compact.endswith("USD") and not compact.endswith("USDT"):
        return compact[:-3] + "USDT"
    if compact in {"ETH", "BTC", "SOL", "XRP", "ADA"}:
        return compact + "USDT"
    return compact


def parse_dt(value: str | None) -> str:
    if not value:
        return ""
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat()
    except Exception:
        return value


def request_json(url: str, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> Any:
    response = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def fetch_finnhub_crypto_news(hours: int, limit: int = 30) -> list[dict[str, Any]]:
    token = os.getenv("FINNHUB_API_KEY")
    if not token:
        return []
    cutoff = now_utc() - timedelta(hours=hours)
    try:
        data = request_json("https://finnhub.io/api/v1/news", {"category": "crypto", "token": token})
    except Exception as exc:
        return [
            {
                "source": "finnhub",
                "title": "DATA_UNAVAILABLE",
                "summary": f"{type(exc).__name__}: {exc}",
                "url": "",
                "published_at": "",
            }
        ]
    items = []
    for article in data or []:
        ts = article.get("datetime")
        published = datetime.fromtimestamp(int(ts), UTC) if ts else None
        if published and published < cutoff:
            continue
        text = f"{article.get('headline', '')} {article.get('summary', '')}"
        if not keyword_match(text):
            continue
        items.append(
            {
                "source": f"finnhub:{article.get('source') or 'unknown'}",
                "title": article.get("headline") or "",
                "summary": article.get("summary") or "",
                "url": article.get("url") or "",
                "published_at": published.isoformat() if published else "",
            }
        )
        if len(items) >= limit:
            break
    return items


def keyword_match(text: str) -> bool:
    upper = text.upper()
    return any(keyword.upper() in upper for keyword in NEWS_KEYWORDS)


def parse_rss_feed(xml_text: str, source_url: str, hours: int, limit: int) -> list[dict[str, Any]]:
    root = ElementTree.fromstring(xml_text)
    cutoff = now_utc() - timedelta(hours=hours)
    items = []

    def clean(text: str | None) -> str:
        return re.sub(r"\s+", " ", text or "").strip()

    nodes = root.findall(".//item")
    if not nodes:
        nodes = root.findall("{http://www.w3.org/2005/Atom}entry")

    for node in nodes:
        title = clean(node.findtext("title") or node.findtext("{http://www.w3.org/2005/Atom}title"))
        summary = clean(
            node.findtext("description")
            or node.findtext("summary")
            or node.findtext("{http://www.w3.org/2005/Atom}summary")
        )
        pub_raw = (
            node.findtext("pubDate")
            or node.findtext("published")
            or node.findtext("updated")
            or node.findtext("{http://www.w3.org/2005/Atom}published")
            or node.findtext("{http://www.w3.org/2005/Atom}updated")
        )
        published_at = parse_dt(pub_raw)
        if published_at:
            try:
                published_dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                if published_dt < cutoff:
                    continue
            except ValueError:
                pass
        link = clean(node.findtext("link"))
        if not link:
            link_node = node.find("{http://www.w3.org/2005/Atom}link")
            link = link_node.attrib.get("href", "") if link_node is not None else ""
        if not keyword_match(f"{title} {summary}"):
            continue
        items.append(
            {
                "source": f"rss:{source_url}",
                "title": title,
                "summary": summary,
                "url": link,
                "published_at": published_at,
            }
        )
        if len(items) >= limit:
            break
    return items


def fetch_rss_news(hours: int, limit_per_feed: int = 10) -> list[dict[str, Any]]:
    headers = {"User-Agent": "tradingagents-ai-research/1.0"}
    items = []
    for feed_url in RSS_FEEDS:
        try:
            response = requests.get(feed_url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            items.extend(parse_rss_feed(response.text, feed_url, hours, limit_per_feed))
        except Exception as exc:
            items.append(
                {
                    "source": f"rss:{feed_url}",
                    "title": "DATA_UNAVAILABLE",
                    "summary": f"{type(exc).__name__}: {exc}",
                    "url": feed_url,
                    "published_at": "",
                }
            )
    return items


def fetch_gdelt_news(hours: int, limit: int = 20) -> list[dict[str, Any]]:
    query = '(Ethereum OR ETH OR Bitcoin OR crypto OR "spot ETF" OR stablecoin) sourceCountry:US'
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": limit,
        "sort": "HybridRel",
        "timespan": f"{max(1, int(hours))}h",
    }
    try:
        data = request_json("https://api.gdeltproject.org/api/v2/doc/doc", params)
    except Exception as exc:
        return [
            {
                "source": "gdelt",
                "title": "DATA_UNAVAILABLE",
                "summary": f"{type(exc).__name__}: {exc}",
                "url": "",
                "published_at": "",
            }
        ]
    articles = data.get("articles", []) if isinstance(data, dict) else []
    return [
        {
            "source": f"gdelt:{article.get('sourceCountry') or article.get('domain') or 'unknown'}",
            "title": article.get("title") or "",
            "summary": article.get("seendate") or "",
            "url": article.get("url") or "",
            "published_at": article.get("seendate") or "",
        }
        for article in articles[:limit]
    ]


def dedupe_news(items: list[dict[str, Any]], limit: int = 60) -> list[dict[str, Any]]:
    seen = set()
    result = []
    for item in items:
        key = re.sub(r"\W+", "", (item.get("title") or item.get("url") or "").lower())[:120]
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(item)
        if len(result) >= limit:
            break
    return result


def fetch_fred_macro(cache_dir: Path) -> dict[str, Any]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    today = now_utc().date().isoformat()
    cache_path = cache_dir / f"fred_macro_{today}.json"
    if cache_path.exists():
        return json.loads(cache_path.read_text(encoding="utf-8"))

    api_key = os.getenv("FRED_API_KEY", "").strip()
    result = {"as_of": now_utc().isoformat(), "series": {}, "source": "fred"}
    if not api_key:
        result["error"] = "FRED_API_KEY is not set"
        return result

    for series_id, label in FRED_SERIES.items():
        try:
            data = request_json(
                "https://api.stlouisfed.org/fred/series/observations",
                {
                    "series_id": series_id,
                    "api_key": api_key,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 2,
                },
            )
            observations = [
                obs for obs in data.get("observations", [])
                if obs.get("value") not in {None, "."}
            ]
            latest = observations[0] if observations else {}
            result["series"][series_id] = {
                "label": label,
                "date": latest.get("date", ""),
                "value": latest.get("value", ""),
            }
        except Exception as exc:
            result["series"][series_id] = {"label": label, "error": f"{type(exc).__name__}: {exc}"}
    cache_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def fetch_binance_summary(symbol: str) -> dict[str, Any]:
    market = symbol_to_binance(symbol)
    result = {"symbol": market, "source": "binance_usdm", "timeframes": {}}
    for interval, limit in (("1h", 48), ("4h", 42), ("1d", 30)):
        try:
            rows = request_json(
                "https://fapi.binance.com/fapi/v1/klines",
                {"symbol": market, "interval": interval, "limit": limit},
            )
            closes = [float(row[4]) for row in rows]
            volumes = [float(row[5]) for row in rows]
            if len(closes) < 2:
                continue
            pct_change = (closes[-1] / closes[0] - 1.0) * 100.0
            recent = closes[-10:] if len(closes) >= 10 else closes
            trend = "up" if closes[-1] > sum(recent) / len(recent) else "down"
            result["timeframes"][interval] = {
                "last_close": closes[-1],
                "pct_change": round(pct_change, 3),
                "avg_volume": round(sum(volumes) / len(volumes), 3),
                "trend_vs_recent_avg": trend,
            }
        except Exception as exc:
            result["timeframes"][interval] = {"error": f"{type(exc).__name__}: {exc}"}
    return result


def latest_tradingagents_report(outputs_dir: Path, ticker: str) -> dict[str, Any]:
    slug = ticker.replace("/", "-").replace(":", "-")
    candidates = sorted(outputs_dir.glob(f"{slug}_*_summary.md"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        return {"available": False}
    path = candidates[0]
    text = path.read_text(encoding="utf-8", errors="replace")
    modified_at = datetime.fromtimestamp(path.stat().st_mtime, UTC)
    return {
        "available": True,
        "path": str(path),
        "modified_at": modified_at.isoformat(),
        "age_hours": round((now_utc() - modified_at).total_seconds() / 3600.0, 3),
        "excerpt": text[:20000],
    }


def run_tradingagents_report(args: argparse.Namespace) -> None:
    script = Path(__file__).with_name("analyze_eth_tradingagents.py")
    command = [
        sys.executable,
        str(script),
        "--ticker",
        args.tradingagents_ticker,
        "--asset-type",
        args.asset_type,
        "--provider",
        args.tradingagents_provider,
        "--deep",
        args.tradingagents_deep,
        "--quick",
        args.tradingagents_quick,
        "--out-dir",
        args.outputs_dir,
        "--llm-timeout",
        str(args.tradingagents_llm_timeout),
        "--llm-max-retries",
        str(args.tradingagents_llm_max_retries),
        "--max-data-rows",
        str(args.tradingagents_max_data_rows),
        "--max-news-items",
        str(args.tradingagents_max_news_items),
        "--max-news-summary-chars",
        str(args.tradingagents_max_news_summary_chars),
    ]
    if args.tradingagents_date:
        command.extend(["--date", args.tradingagents_date])
    if args.tradingagents_offline:
        command.append("--offline")
    print("Running TradingAgents report first:")
    print("  " + " ".join(command))
    subprocess.run(command, check=True)


def assert_tradingagents_ready(snapshot: dict[str, Any], args: argparse.Namespace) -> None:
    report = snapshot.get("tradingagents", {})
    if report.get("available") and float(report.get("age_hours", 999999)) <= args.max_report_age_hours:
        return
    if args.allow_missing_tradingagents:
        return

    if not report.get("available"):
        detail = f"No TradingAgents summary found for {args.tradingagents_ticker} in {args.outputs_dir}."
    else:
        detail = (
            f"TradingAgents summary is stale: age_hours={report.get('age_hours')} "
            f"> max_report_age_hours={args.max_report_age_hours}."
        )
    raise SystemExit(
        detail
        + "\nRun analyze_eth_tradingagents.py first, or rerun this command with --run-tradingagents. "
        + "Use --allow-missing-tradingagents only for data-source debugging."
    )


def build_research_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    research_dir = Path(args.research_dir)
    cache_dir = research_dir / "cache"
    news = dedupe_news(
        fetch_finnhub_crypto_news(args.hours)
        + fetch_rss_news(args.hours)
        + fetch_gdelt_news(args.hours)
    )
    source_counts: dict[str, int] = {}
    for item in news:
        source = str(item.get("source", "")).split(":", 1)[0] or "unknown"
        source_counts[source] = source_counts.get(source, 0) + 1

    return {
        "symbol": symbol_to_binance(args.symbol),
        "generated_at": now_utc().isoformat(),
        "hours": args.hours,
        "news": news,
        "source_counts": source_counts,
        "macro": fetch_fred_macro(cache_dir),
        "binance": fetch_binance_summary(args.symbol),
        "tradingagents": latest_tradingagents_report(Path(args.outputs_dir), args.tradingagents_ticker),
        "decision_policy": {
            "primary_research_engine": "TradingAgents multi-agent report",
            "normalizer": "DeepSeek JSON compression only",
            "external_data_role": "corroboration and freshness checks, not a replacement for TradingAgents",
            "execution_role": "research output only; not connected to bian_new.py",
        },
    }


def deepseek_prompt(snapshot: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "You convert a TradingAgents multi-agent research report into one conservative JSON object. "
                "TradingAgents is the primary research engine and includes analysts, trader, and risk teams. "
                "External news, macro, and Binance summaries are corroborating context only. "
                "Output JSON only. Do not wrap it in markdown. If evidence is weak, use neutral or mixed "
                "and allow both directions."
            ),
        },
        {
            "role": "user",
            "content": (
                "Return this schema exactly: "
                '{"bias":"bullish|bearish|neutral|mixed","confidence":0.0,'
                '"allow_long":true,"allow_short":true,"size_multiplier":1.0,'
                '"reason":"short reason","risk_events":[],"news_used":[]}.\n\n'
                "Rules: confidence < 0.55 must allow both directions. "
                "Only block a side when confidence >= 0.65 and evidence is clear. "
                "Use fail-open behavior when data is missing. Do not invent a direction when the "
                "TradingAgents report is stale, unavailable, or internally mixed; lower confidence instead. "
                "Do not treat SELL in a stock-style report as automatic futures short unless the report "
                "explicitly supports short-side risk/reward for this crypto symbol.\n\n"
                f"Research input:\n{json.dumps(snapshot, ensure_ascii=False)[:45000]}"
            ),
        },
    ]


def normalize_bias(raw: dict[str, Any], symbol: str, hours: int, source_counts: dict[str, int]) -> dict[str, Any]:
    bias = str(raw.get("bias", "neutral")).lower()
    if bias not in {"bullish", "bearish", "neutral", "mixed"}:
        bias = "neutral"
    try:
        confidence = float(raw.get("confidence", 0.0))
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    allow_long = bool(raw.get("allow_long", True))
    allow_short = bool(raw.get("allow_short", True))
    if confidence < 0.55 or bias in {"neutral", "mixed"}:
        allow_long = True
        allow_short = True
    elif bias == "bullish" and confidence >= 0.65:
        allow_long = True
        allow_short = False
    elif bias == "bearish" and confidence >= 0.65:
        allow_long = False
        allow_short = True

    try:
        size_multiplier = float(raw.get("size_multiplier", 1.0))
    except (TypeError, ValueError):
        size_multiplier = 1.0
    if not math.isfinite(size_multiplier):
        size_multiplier = 1.0
    size_multiplier = max(0.0, min(1.0, size_multiplier))

    generated = now_exchange()
    expires = generated + timedelta(hours=hours)
    return {
        "symbol": symbol,
        "generated_at": generated.isoformat(),
        "expires_at": expires.isoformat(),
        "bias": bias,
        "confidence": confidence,
        "allow_long": allow_long,
        "allow_short": allow_short,
        "size_multiplier": size_multiplier,
        "reason": str(raw.get("reason", ""))[:800],
        "risk_events": list(raw.get("risk_events", []))[:10] if isinstance(raw.get("risk_events", []), list) else [],
        "news_used": list(raw.get("news_used", []))[:20] if isinstance(raw.get("news_used", []), list) else [],
        "source_counts": source_counts,
    }


def call_deepseek(snapshot: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    api_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY is not set")
    payload = {
        "model": args.model,
        "messages": deepseek_prompt(snapshot),
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
    }
    response = requests.post(
        "https://api.deepseek.com/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    content = response.json()["choices"][0]["message"]["content"]
    return json.loads(content)


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build AI research inputs and optional bias JSON.")
    parser.add_argument("--symbol", default="ETHUSDT", help="Trading symbol, default ETHUSDT.")
    parser.add_argument("--tradingagents-ticker", default="ETH-USD", help="Ticker used by TradingAgents outputs.")
    parser.add_argument("--hours", type=int, default=9, help="News lookback and bias expiry horizon.")
    parser.add_argument("--research-dir", default=DEFAULT_RESEARCH_DIR, help="Research cache/output directory.")
    parser.add_argument("--outputs-dir", default=DEFAULT_OUTPUTS_DIR, help="TradingAgents outputs directory.")
    parser.add_argument("--model", default=DEFAULT_DEEPSEEK_MODEL, help="DeepSeek chat model.")
    parser.add_argument("--run-tradingagents", action="store_true", help="Run analyze_eth_tradingagents.py before building the bias snapshot.")
    parser.add_argument("--asset-type", default="crypto", choices=["crypto", "stock"], help="Asset type passed to TradingAgents when --run-tradingagents is set.")
    parser.add_argument("--tradingagents-provider", default=DEFAULT_TRADINGAGENTS_PROVIDER, help="LLM provider for --run-tradingagents.")
    parser.add_argument("--tradingagents-deep", default=DEFAULT_TRADINGAGENTS_DEEP, help="Deep model for --run-tradingagents.")
    parser.add_argument("--tradingagents-quick", default=DEFAULT_TRADINGAGENTS_QUICK, help="Quick model for --run-tradingagents.")
    parser.add_argument("--tradingagents-date", default=None, help="Optional YYYY-MM-DD date passed to TradingAgents.")
    parser.add_argument("--tradingagents-offline", action="store_true", help="Pass --offline to analyze_eth_tradingagents.py.")
    parser.add_argument("--tradingagents-llm-timeout", type=float, default=DEFAULT_TRADINGAGENTS_LLM_TIMEOUT, help="LLM timeout passed to analyze_eth_tradingagents.py.")
    parser.add_argument("--tradingagents-llm-max-retries", type=int, default=DEFAULT_TRADINGAGENTS_LLM_MAX_RETRIES, help="LLM retry count passed to analyze_eth_tradingagents.py.")
    parser.add_argument("--tradingagents-max-data-rows", type=int, default=DEFAULT_TRADINGAGENTS_MAX_DATA_ROWS, help="Maximum data rows passed to analyze_eth_tradingagents.py.")
    parser.add_argument("--tradingagents-max-news-items", type=int, default=DEFAULT_TRADINGAGENTS_MAX_NEWS_ITEMS, help="Maximum news items passed to analyze_eth_tradingagents.py.")
    parser.add_argument("--tradingagents-max-news-summary-chars", type=int, default=DEFAULT_TRADINGAGENTS_MAX_NEWS_SUMMARY_CHARS, help="Maximum news summary length passed to analyze_eth_tradingagents.py.")
    parser.add_argument("--max-report-age-hours", type=float, default=24.0, help="Maximum acceptable TradingAgents report age for bias generation.")
    parser.add_argument("--allow-missing-tradingagents", action="store_true", help="Allow DeepSeek normalization without a fresh TradingAgents report; intended only for data-source debugging.")
    parser.add_argument("--fetch-only", action="store_true", help="Only fetch/cache research inputs; do not call DeepSeek.")
    parser.add_argument("--dry-run", action="store_true", help="Print outputs without writing latest bias.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.run_tradingagents:
        run_tradingagents_report(args)

    research_dir = Path(args.research_dir)
    cache_dir = research_dir / "cache"
    snapshot = build_research_snapshot(args)
    stamp = now_utc().strftime("%Y%m%dT%H%M%SZ")
    snapshot_path = cache_dir / f"research_snapshot_{symbol_to_binance(args.symbol)}_{stamp}.json"
    write_json(snapshot_path, snapshot)
    print(f"Saved research snapshot: {snapshot_path}")
    print(f"Source counts: {snapshot.get('source_counts', {})}")
    report = snapshot.get("tradingagents", {})
    if report.get("available"):
        print(f"TradingAgents report: {report.get('path')} age_hours={report.get('age_hours')}")
    else:
        print("TradingAgents report: unavailable")

    if args.fetch_only:
        return

    assert_tradingagents_ready(snapshot, args)
    raw_bias = call_deepseek(snapshot, args)
    bias = normalize_bias(raw_bias, symbol_to_binance(args.symbol), args.hours, snapshot.get("source_counts", {}))
    if args.dry_run:
        print(json.dumps(bias, ensure_ascii=False, indent=2))
        return

    bias_path = research_dir / f"latest_bias_{symbol_to_binance(args.symbol)}.json"
    write_json(bias_path, bias)
    print(f"Saved bias: {bias_path}")


if __name__ == "__main__":
    main()
