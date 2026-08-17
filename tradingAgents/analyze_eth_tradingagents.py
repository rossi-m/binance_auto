#!/usr/bin/env python3
"""
Run TradingAgents against an asset and save the full analysis output.

Prerequisites:
  pip install tradingagents

Required for the default DeepSeek config:
  Put DEEPSEEK_API_KEY in .env, or export it before running.

Often useful when online data/news tools are enabled:
  export FINNHUB_API_KEY="..."

Example:
  python analyze_eth_tradingagents.py
  python analyze_eth_tradingagents.py --date 2026-07-05 --provider deepseek --deep deepseek-v4-pro --quick deepseek-v4-flash
"""

from __future__ import annotations

import argparse
import json
import os
from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv
from stockstats import wrap


def load_env_files() -> None:
    script_dir = Path(__file__).resolve().parent
    repo_dir = script_dir.parent
    legacy_env = Path("/home/ubuntu/binance/tradingAgents/.env")
    explicit_env = os.getenv("TRADINGAGENTS_ENV_FILE", "").strip()
    for env_path in (
        Path(explicit_env) if explicit_env else None,
        script_dir / ".env",
        legacy_env,
        repo_dir / ".ai_env.local",
        repo_dir / ".env.local",
        Path.cwd() / ".env",
    ):
        if env_path and env_path.exists():
            load_dotenv(env_path, override=False)


load_env_files()

REQUEST_TIMEOUT = 30
NO_YFINANCE_VENDOR = "no_yfinance"
CRYPTO_QUOTES = ("USDT", "USDC", "USD")
SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
MAX_DATA_ROWS = 90
MAX_NEWS_ITEMS = 12
MAX_NEWS_SUMMARY_CHARS = 500
MAX_INSIDER_FILINGS = 12


def import_tradingagents() -> tuple[Any, dict[str, Any]]:
    try:
        from tradingagents.default_config import DEFAULT_CONFIG
        from tradingagents.graph.trading_graph import TradingAgentsGraph
    except ModuleNotFoundError as exc:
        missing_module = exc.name or ""
        if missing_module and not missing_module.startswith("tradingagents"):
            raise SystemExit(
                "TradingAgents is installed, but one of its dependencies is missing:\n\n"
                f"  {missing_module}\n\n"
                "Install/repair dependencies, then rerun this script:\n\n"
                "  python -m pip install tradingagents\n"
            ) from exc

        raise SystemExit(
            "TradingAgents is not installed.\n"
            "Install it first, then rerun this script:\n\n"
            "  pip install tradingagents\n\n"
            "If you are using the GitHub source repo instead:\n"
            "  git clone https://github.com/TauricResearch/TradingAgents.git\n"
            "  cd TradingAgents\n"
            "  pip install -e .\n"
        ) from exc

    return TradingAgentsGraph, DEFAULT_CONFIG


def is_crypto_symbol(symbol: str) -> bool:
    raw_symbol = symbol.upper().split(":", 1)[0]
    compact = raw_symbol.replace("-", "").replace("/", "")
    for quote in CRYPTO_QUOTES:
        # 手动仓位没有币种白名单；常见加密报价币前只要有基础币就按加密资产处理。
        if compact.endswith(quote) and bool(compact[: -len(quote)]):
            return True
    return False


def crypto_base(symbol: str) -> str:
    raw_symbol = symbol.upper().split(":", 1)[0]
    compact = raw_symbol.replace("-", "").replace("/", "")
    for quote in CRYPTO_QUOTES:
        if compact.endswith(quote):
            base = compact[: -len(quote)]
            if base:
                return base
    return compact


def binance_usdt_symbol(symbol: str) -> str:
    return f"{crypto_base(symbol)}USDT"


def normalize_ohlcv_frame(df: pd.DataFrame) -> pd.DataFrame:
    rename = {
        "timestamp": "Date",
        "time": "Date",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "adjusted_close": "Adj Close",
        "volume": "Volume",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    if "Date" not in df.columns:
        first = df.columns[0]
        df = df.rename(columns={first: "Date"})
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    for col in ("Open", "High", "Low", "Close", "Adj Close", "Volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "Adj Close" in df.columns and "Close" not in df.columns:
        df["Close"] = df["Adj Close"]
    df = df.dropna(subset=["Date", "Close"]).sort_values("Date")
    keep = [c for c in ("Date", "Open", "High", "Low", "Close", "Adj Close", "Volume") if c in df.columns]
    return df[keep]


def truncate_text(value: Any, max_chars: int = MAX_NEWS_SUMMARY_CHARS) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "... [truncated]"


def fetch_alpha_vantage_ohlcv(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    from tradingagents.dataflows.alpha_vantage_common import _filter_csv_by_date_range, _make_api_request

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    outputsize = "compact" if (datetime.now() - start_dt).days < 100 else "full"
    csv_text = _make_api_request(
        "TIME_SERIES_DAILY",
        {
            "symbol": symbol.upper(),
            "outputsize": outputsize,
            "datatype": "csv",
        },
    )
    csv_text = _filter_csv_by_date_range(csv_text, start_date, end_date)
    df = pd.read_csv(StringIO(csv_text))
    return normalize_ohlcv_frame(df)


def fetch_finnhub_stock_ohlcv(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    token = os.getenv("FINNHUB_API_KEY")
    if not token:
        raise RuntimeError("FINNHUB_API_KEY is not set")

    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp())
    end_ts = int((datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).replace(tzinfo=UTC).timestamp())
    response = requests.get(
        "https://finnhub.io/api/v1/stock/candle",
        params={
            "symbol": symbol.upper(),
            "resolution": "D",
            "from": start_ts,
            "to": end_ts,
            "token": token,
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()
    if data.get("s") != "ok":
        return pd.DataFrame()
    return normalize_ohlcv_frame(
        pd.DataFrame(
            {
                "Date": pd.to_datetime(data.get("t", []), unit="s"),
                "Open": data.get("o", []),
                "High": data.get("h", []),
                "Low": data.get("l", []),
                "Close": data.get("c", []),
                "Volume": data.get("v", []),
            }
        )
    )


def fetch_yahoo_chart_stock_ohlcv(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp())
    end_ts = int((datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).replace(tzinfo=UTC).timestamp())
    last_error = None
    data = None
    for host in ("query2.finance.yahoo.com", "query1.finance.yahoo.com"):
        try:
            response = requests.get(
                f"https://{host}/v8/finance/chart/{symbol.upper()}",
                params={"period1": start_ts, "period2": end_ts, "interval": "1d", "events": "history"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()
            break
        except Exception as exc:
            last_error = exc
    if data is None:
        raise last_error or RuntimeError("Yahoo chart returned no data")
    result = (data.get("chart", {}).get("result") or [None])[0]
    if not result:
        return pd.DataFrame()
    timestamps = result.get("timestamp") or []
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    adjclose = ((result.get("indicators") or {}).get("adjclose") or [{}])[0].get("adjclose") or []
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(timestamps, unit="s"),
            "Open": quote.get("open") or [],
            "High": quote.get("high") or [],
            "Low": quote.get("low") or [],
            "Close": quote.get("close") or [],
            "Adj Close": adjclose,
            "Volume": quote.get("volume") or [],
        }
    )
    return normalize_ohlcv_frame(df)


def fetch_nasdaq_stock_ohlcv(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    response = requests.get(
        f"https://api.nasdaq.com/api/quote/{symbol.upper()}/historical",
        params={"assetclass": "stocks", "fromdate": start_date, "todate": end_date, "limit": "9999"},
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    rows = response.json().get("data", {}).get("tradesTable", {}).get("rows", [])
    if not rows:
        return pd.DataFrame()

    def clean_number(value: Any) -> Any:
        if value is None:
            return None
        return str(value).replace("$", "").replace(",", "").strip()

    df = pd.DataFrame(
        {
            "Date": [row.get("date") for row in rows],
            "Open": [clean_number(row.get("open")) for row in rows],
            "High": [clean_number(row.get("high")) for row in rows],
            "Low": [clean_number(row.get("low")) for row in rows],
            "Close": [clean_number(row.get("close")) for row in rows],
            "Volume": [clean_number(row.get("volume")) for row in rows],
        }
    )
    df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y", errors="coerce")
    return normalize_ohlcv_frame(df)


def stooq_symbol(symbol: str) -> str:
    compact = symbol.lower().replace("/", ".")
    if "." not in compact:
        return f"{compact}.us"
    return compact


def fetch_stooq_stock_ohlcv(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    response = requests.get(
        "https://stooq.com/q/d/l/",
        params={
            "s": stooq_symbol(symbol),
            "d1": start_date.replace("-", ""),
            "d2": end_date.replace("-", ""),
            "i": "d",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    text = response.text.strip()
    if not text or text.lower() == "no data" or "<html" in text[:100].lower():
        return pd.DataFrame()
    df = pd.read_csv(StringIO(text))
    return normalize_ohlcv_frame(df)


def fetch_stock_ohlcv_no_yfinance(symbol: str, start_date: str, end_date: str) -> tuple[pd.DataFrame, str]:
    errors = []
    for source, fetcher in (
        ("Finnhub stock candles", fetch_finnhub_stock_ohlcv),
        ("Nasdaq historical", fetch_nasdaq_stock_ohlcv),
        ("Yahoo chart direct", fetch_yahoo_chart_stock_ohlcv),
        ("Stooq daily CSV", fetch_stooq_stock_ohlcv),
    ):
        try:
            df = fetcher(symbol, start_date, end_date)
        except Exception as exc:
            errors.append(f"{source}: {type(exc).__name__}: {exc}")
            continue
        if not df.empty:
            return df, source
        errors.append(f"{source}: no rows")
    detail = "; ".join(errors) if errors else "no stock OHLCV source returned rows"
    raise RuntimeError(detail)


def fetch_binance_ohlcv(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    start_ms = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
    # Binance endTime is inclusive enough for daily klines; add one day so end_date is present.
    end_ms = int((datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).timestamp() * 1000)
    request_start = start_ms
    rows = []
    while request_start < end_ms:
        params = {
            "symbol": binance_usdt_symbol(symbol),
            "interval": "1d",
            "startTime": request_start,
            "endTime": end_ms,
            "limit": 1500,
        }
        response = requests.get("https://fapi.binance.com/fapi/v1/klines", params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break
        rows.extend(batch)
        last_open_time = int(batch[-1][0])
        next_start = last_open_time + 24 * 60 * 60 * 1000
        if next_start <= request_start:
            break
        request_start = next_start
    df = pd.DataFrame(
        rows,
        columns=[
            "open_time", "Open", "High", "Low", "Close", "Volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base", "taker_buy_quote", "ignore",
        ],
    )
    if df.empty:
        return df
    df["Date"] = pd.to_datetime(df["open_time"], unit="ms")
    return normalize_ohlcv_frame(df)


def load_ohlcv_no_yfinance(symbol: str, curr_date: str) -> pd.DataFrame:
    from tradingagents.dataflows.errors import NoMarketDataError

    end_dt = datetime.strptime(curr_date, "%Y-%m-%d")
    start_dt = end_dt - timedelta(days=365 * 5 + 10)
    start_date = start_dt.strftime("%Y-%m-%d")
    if is_crypto_symbol(symbol):
        df = fetch_binance_ohlcv(symbol, start_date, curr_date)
        canonical = binance_usdt_symbol(symbol)
    else:
        try:
            df, source = fetch_stock_ohlcv_no_yfinance(symbol, start_date, curr_date)
        except Exception as exc:
            raise NoMarketDataError(symbol, symbol.upper(), str(exc)) from exc
        canonical = f"{symbol.upper()} via {source}"

    if df.empty:
        raise NoMarketDataError(symbol, canonical, f"no rows through {curr_date}")
    df = df[df["Date"] <= pd.to_datetime(curr_date)].sort_values("Date")
    if df.empty:
        raise NoMarketDataError(symbol, canonical, f"no rows on or before {curr_date}")
    return df


def get_stock_data_no_yfinance(symbol: str, start_date: str, end_date: str) -> str:
    from tradingagents.dataflows.errors import NoMarketDataError

    if is_crypto_symbol(symbol):
        df = fetch_binance_ohlcv(symbol, start_date, end_date)
        label = f"{binance_usdt_symbol(symbol)} Binance USD-M futures"
    else:
        try:
            df, source = fetch_stock_ohlcv_no_yfinance(symbol, start_date, end_date)
        except Exception as exc:
            raise NoMarketDataError(symbol, symbol.upper(), str(exc)) from exc
        label = f"{symbol.upper()} {source}"
    if df.empty:
        raise NoMarketDataError(symbol, label, f"no rows between {start_date} and {end_date}")
    rendered_df = df.tail(max(1, int(MAX_DATA_ROWS)))
    omitted = len(df) - len(rendered_df)
    truncation_note = f"# Rows omitted from prompt: {omitted}\n" if omitted > 0 else ""
    return (
        f"# OHLCV data for {label} from {start_date} to {end_date}\n"
        f"# Total records: {len(df)}\n"
        f"# Records included: {len(rendered_df)} most recent rows\n"
        f"{truncation_note}"
        f"# yfinance disabled by local adapter\n\n"
        + rendered_df.to_csv(index=False)
    )


def get_indicators_no_yfinance(symbol: str, indicator: str, curr_date: str, look_back_days: int = 30) -> str:
    supported = {
        "close_50_sma", "close_200_sma", "close_10_ema",
        "macd", "macds", "macdh", "rsi", "boll", "boll_ub", "boll_lb", "atr", "vwma", "mfi",
    }
    if indicator not in supported:
        raise ValueError(f"Indicator {indicator} is not supported. Please choose from: {sorted(supported)}")

    df = load_ohlcv_no_yfinance(symbol, curr_date)
    stock_df = wrap(df.copy())
    stock_df["Date"] = pd.to_datetime(stock_df["Date"], errors="coerce")
    stock_df[indicator]
    start = pd.to_datetime(curr_date) - pd.Timedelta(days=int(look_back_days))
    window = stock_df[(stock_df["Date"] >= start) & (stock_df["Date"] <= pd.to_datetime(curr_date))]
    lines = []
    for _, row in window.tail(max(1, int(look_back_days))).iterrows():
        value = row.get(indicator)
        if pd.isna(value):
            rendered = "N/A"
        elif isinstance(value, float):
            rendered = f"{value:.4f}"
        else:
            rendered = str(value)
        lines.append(f"{row['Date'].strftime('%Y-%m-%d')}: {rendered}")
    if not lines:
        lines.append("No data available for the specified date range.")
    return f"## {indicator} values for {symbol} from {start.strftime('%Y-%m-%d')} to {curr_date}\n\n" + "\n".join(lines)


def get_crypto_fundamentals(symbol: str, curr_date: str | None = None) -> str:
    base = crypto_base(symbol)
    return (
        f"## Crypto fundamentals placeholder for {base}\n\n"
        "This asset is a cryptocurrency, not a listed company. Traditional balance sheet, "
        "cash flow, income statement, P/E, and insider transactions are not applicable. "
        "Use crypto-native fundamentals instead: network activity, staking, ETF flows, "
        "stablecoin liquidity, DeFi TVL, gas fees, protocol upgrades, regulation, and ecosystem adoption."
    )


def data_unavailable_message(dataset: str, ticker: str, source: str, exc: Exception) -> str:
    return (
        f"DATA_UNAVAILABLE: {dataset} for {ticker.upper()} could not be retrieved "
        f"from {source} ({type(exc).__name__}: {exc}). Proceed without this "
        "dataset; do not estimate or fabricate values."
    )


def sec_headers() -> dict[str, str]:
    return {
        "User-Agent": os.getenv("SEC_USER_AGENT", "tradingagents-no-yfinance/1.0 admin@example.com"),
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
    }


def parse_iso_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d")
    except (TypeError, ValueError):
        return None


@lru_cache(maxsize=1)
def fetch_sec_ticker_map() -> dict[str, dict[str, str]]:
    response = requests.get(SEC_COMPANY_TICKERS_URL, headers=sec_headers(), timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    raw = response.json()
    result = {}
    for item in raw.values():
        ticker = str(item.get("ticker", "")).upper()
        cik = str(item.get("cik_str", "")).zfill(10)
        title = str(item.get("title", ticker))
        if ticker and cik:
            result[ticker] = {"ticker": ticker, "cik": cik, "title": title}
    return result


@lru_cache(maxsize=256)
def resolve_sec_company(ticker: str) -> dict[str, str]:
    company = fetch_sec_ticker_map().get(ticker.upper())
    if not company:
        raise ValueError(f"{ticker.upper()} was not found in the SEC company ticker index")
    return company


@lru_cache(maxsize=256)
def fetch_sec_companyfacts(cik: str) -> dict[str, Any]:
    response = requests.get(SEC_COMPANYFACTS_URL.format(cik=cik), headers=sec_headers(), timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


@lru_cache(maxsize=256)
def fetch_sec_submissions(cik: str) -> dict[str, Any]:
    response = requests.get(SEC_SUBMISSIONS_URL.format(cik=cik), headers=sec_headers(), timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def sec_concept_entries(facts: dict[str, Any], concept: str) -> tuple[str, str, list[dict[str, Any]]]:
    concept_data = facts.get("facts", {}).get("us-gaap", {}).get(concept, {})
    units = concept_data.get("units", {})
    for unit in ("USD", "shares", "USD/shares", "pure"):
        entries = units.get(unit)
        if entries:
            return concept_data.get("label", concept), unit, entries
    for unit, entries in units.items():
        if entries:
            return concept_data.get("label", concept), unit, entries
    return concept_data.get("label", concept), "", []


def sec_entry_matches(entry: dict[str, Any], freq: str | None, curr_date: str | None) -> bool:
    if entry.get("val") is None:
        return False

    cutoff = parse_iso_date(curr_date)
    filed_dt = parse_iso_date(entry.get("filed"))
    end_dt = parse_iso_date(entry.get("end"))
    if cutoff and filed_dt and filed_dt > cutoff:
        return False
    if cutoff and end_dt and end_dt > cutoff:
        return False

    form = str(entry.get("form", ""))
    if freq == "annual":
        return form.startswith("10-K") or entry.get("fp") == "FY"
    if freq == "quarterly":
        return form.startswith("10-Q") or str(entry.get("fp", "")).startswith("Q")
    return form.startswith(("10-K", "10-Q")) or bool(form)


def sort_sec_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        entries,
        key=lambda item: (
            str(item.get("filed", "")),
            str(item.get("end", "")),
            int(item.get("fy") or 0),
            str(item.get("fp", "")),
        ),
        reverse=True,
    )


def latest_sec_fact(
    facts: dict[str, Any],
    concepts: list[str],
    freq: str | None = None,
    curr_date: str | None = None,
) -> dict[str, Any] | None:
    for concept in concepts:
        label, unit, entries = sec_concept_entries(facts, concept)
        filtered = [entry for entry in entries if sec_entry_matches(entry, freq, curr_date)]
        if not filtered and freq:
            filtered = [entry for entry in entries if sec_entry_matches(entry, None, curr_date)]
        if filtered:
            latest = sort_sec_entries(filtered)[0]
            return {"concept": concept, "label": label, "unit": unit, "entry": latest}
    return None


def format_sec_value(value: Any, unit: str) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)

    if unit == "USD":
        sign = "-" if number < 0 else ""
        absolute = abs(number)
        if absolute >= 1_000_000_000:
            return f"{sign}${absolute / 1_000_000_000:.2f}B"
        if absolute >= 1_000_000:
            return f"{sign}${absolute / 1_000_000:.2f}M"
        return f"{sign}${absolute:,.0f}"
    if unit == "shares":
        return f"{number:,.0f} shares"
    if unit == "USD/shares":
        return f"${number:.2f}/share"
    if unit == "pure":
        return f"{number:.4g}"
    return f"{number:,.4g} {unit}".strip()


def sec_fact_row(display: str, fact: dict[str, Any] | None) -> str:
    if not fact:
        return f"| {display} | DATA_UNAVAILABLE |  |  |  |"
    entry = fact["entry"]
    return (
        f"| {display} | {format_sec_value(entry.get('val'), fact['unit'])} | "
        f"{entry.get('end', '')} | {entry.get('filed', '')} | {fact['concept']} |"
    )


def build_sec_report(
    ticker: str,
    dataset: str,
    specs: list[tuple[str, list[str]]],
    freq: str | None = None,
    curr_date: str | None = None,
) -> str:
    try:
        company = resolve_sec_company(ticker)
        facts = fetch_sec_companyfacts(company["cik"])
    except Exception as exc:
        return data_unavailable_message(dataset, ticker, "SEC companyfacts", exc)

    lines = [
        f"## SEC {dataset} for {ticker.upper()}",
        "",
        f"- Company: {company['title']}",
        f"- CIK: {company['cik']}",
        f"- Source: SEC companyfacts API",
        f"- Filing cutoff: {curr_date or 'latest available'}",
        "",
        "| Metric | Value | Period End | Filed | SEC Concept |",
        "|---|---:|---|---|---|",
    ]
    for display, concepts in specs:
        lines.append(sec_fact_row(display, latest_sec_fact(facts, concepts, freq, curr_date)))

    if freq == "quarterly":
        lines.extend(
            [
                "",
                "Note: SEC 10-Q duration facts may be reported as period-to-date values. "
                "Use these figures as filed fundamentals, not as normalized vendor ratios.",
            ]
        )
    return "\n".join(lines)


FUNDAMENTAL_OVERVIEW_SPECS = [
    ("Revenue", ["RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues", "SalesRevenueNet"]),
    ("Net income", ["NetIncomeLoss", "ProfitLoss"]),
    ("Diluted EPS", ["EarningsPerShareDiluted"]),
    ("Assets", ["Assets"]),
    ("Liabilities", ["Liabilities"]),
    ("Stockholders equity", ["StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"]),
    ("Operating cash flow", ["NetCashProvidedByUsedInOperatingActivities"]),
]

BALANCE_SHEET_SPECS = [
    ("Cash and equivalents", ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"]),
    ("Current assets", ["AssetsCurrent"]),
    ("Total assets", ["Assets"]),
    ("Current liabilities", ["LiabilitiesCurrent"]),
    ("Total liabilities", ["Liabilities"]),
    ("Long-term debt", ["LongTermDebtNoncurrent", "LongTermDebtAndFinanceLeaseObligationsNoncurrent"]),
    ("Stockholders equity", ["StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"]),
]

CASHFLOW_SPECS = [
    ("Operating cash flow", ["NetCashProvidedByUsedInOperatingActivities"]),
    ("Capital expenditures", ["PaymentsToAcquirePropertyPlantAndEquipment"]),
    ("Investing cash flow", ["NetCashProvidedByUsedInInvestingActivities"]),
    ("Financing cash flow", ["NetCashProvidedByUsedInFinancingActivities"]),
    ("Dividends paid", ["PaymentsOfDividends", "PaymentsOfDividendsCommonStock"]),
    ("Share repurchases", ["PaymentsForRepurchaseOfCommonStock"]),
]

INCOME_STATEMENT_SPECS = [
    ("Revenue", ["RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues", "SalesRevenueNet"]),
    ("Cost of revenue", ["CostOfRevenue", "CostOfGoodsAndServicesSold"]),
    ("Gross profit", ["GrossProfit"]),
    ("Operating income", ["OperatingIncomeLoss"]),
    ("Net income", ["NetIncomeLoss", "ProfitLoss"]),
    ("Diluted EPS", ["EarningsPerShareDiluted"]),
]


def get_fundamentals_no_yfinance(ticker: str, curr_date: str = None) -> str:
    if is_crypto_symbol(ticker):
        return get_crypto_fundamentals(ticker, curr_date)

    return build_sec_report(ticker, "company overview", FUNDAMENTAL_OVERVIEW_SPECS, None, curr_date)


def get_balance_sheet_no_yfinance(ticker: str, freq: str = "quarterly", curr_date: str = None) -> str:
    if is_crypto_symbol(ticker):
        return "Balance sheet is not applicable to crypto assets."

    return build_sec_report(ticker, "balance sheet", BALANCE_SHEET_SPECS, freq, curr_date)


def get_cashflow_no_yfinance(ticker: str, freq: str = "quarterly", curr_date: str = None) -> str:
    if is_crypto_symbol(ticker):
        return "Cash flow statement is not applicable to crypto assets."

    return build_sec_report(ticker, "cash flow statement", CASHFLOW_SPECS, freq, curr_date)


def get_income_statement_no_yfinance(ticker: str, freq: str = "quarterly", curr_date: str = None) -> str:
    if is_crypto_symbol(ticker):
        return "Income statement is not applicable to crypto assets."

    return build_sec_report(ticker, "income statement", INCOME_STATEMENT_SPECS, freq, curr_date)


def format_finnhub_articles(title: str, articles: list[dict], limit: int | None = None) -> str:
    if not articles:
        return f"No Finnhub articles found for {title}."
    limit = min(limit or MAX_NEWS_ITEMS, MAX_NEWS_ITEMS)
    lines = [f"## Finnhub news for {title}", ""]
    for item in articles[:limit]:
        headline = item.get("headline") or item.get("title") or "Untitled"
        source = item.get("source") or "Unknown"
        summary = item.get("summary") or ""
        url = item.get("url") or ""
        dt = item.get("datetime")
        date_text = ""
        if dt:
            try:
                date_text = datetime.fromtimestamp(int(dt), UTC).strftime("%Y-%m-%d")
            except (TypeError, ValueError, OSError):
                date_text = str(dt)
        lines.append(f"### {headline} (source: {source}{', date: ' + date_text if date_text else ''})")
        if summary:
            lines.append(truncate_text(summary))
        if url:
            lines.append(f"Link: {url}")
        lines.append("")
    return "\n".join(lines)


def get_news_no_yfinance(ticker: str, start_date: str, end_date: str) -> str:
    token = os.getenv("FINNHUB_API_KEY")

    if is_crypto_symbol(ticker):
        try:
            from ai_market_bias import fetch_finnhub_crypto_news, fetch_gdelt_news, fetch_rss_news, select_news_items

            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            lookback_hours = max(24, int((end_dt - start_dt).total_seconds() // 3600))
            items = select_news_items(
                fetch_finnhub_crypto_news(lookback_hours, limit=MAX_NEWS_ITEMS, symbol=ticker)
                + fetch_rss_news(lookback_hours, limit_per_feed=4, symbol=ticker)
                + fetch_gdelt_news(lookback_hours, limit=MAX_NEWS_ITEMS, symbol=ticker),
                limit=MAX_NEWS_ITEMS,
                symbol=ticker,
            )
        except Exception as exc:
            return data_unavailable_message("crypto news", ticker, "Finnhub/RSS/GDELT", exc)

        if not items:
            return "DATA_UNAVAILABLE: no crypto news found from Finnhub/RSS/GDELT."
        lines = [f"## Crypto news for {ticker} from Finnhub/RSS/GDELT", ""]
        for item in items:
            lines.append(f"### {item.get('title') or 'Untitled'}")
            lines.append(f"Source: {item.get('source', 'unknown')}")
            if item.get("published_at"):
                lines.append(f"Date: {item['published_at']}")
            if item.get("summary"):
                lines.append(truncate_text(item["summary"]))
            if item.get("url"):
                lines.append(f"Link: {item['url']}")
            lines.append("")
        return "\n".join(lines)

    if not token:
        return "DATA_UNAVAILABLE: FINNHUB_API_KEY is not set; yfinance is disabled."

    response = requests.get(
        "https://finnhub.io/api/v1/company-news",
        params={"symbol": ticker.upper(), "from": start_date, "to": end_date, "token": token},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return format_finnhub_articles(ticker.upper(), response.json(), MAX_NEWS_ITEMS)


def get_global_news_no_yfinance(curr_date: str, look_back_days: int | None = None, limit: int | None = None) -> str:
    token = os.getenv("FINNHUB_API_KEY")
    articles = []
    if token:
        response = requests.get(
            "https://finnhub.io/api/v1/news",
            params={"category": "general", "token": token},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        articles = response.json()

    try:
        from ai_market_bias import fetch_gdelt_news

        gdelt_items = fetch_gdelt_news((look_back_days or 1) * 24, limit=min(limit or MAX_NEWS_ITEMS, MAX_NEWS_ITEMS))
    except Exception:
        gdelt_items = []

    if articles:
        return format_finnhub_articles("global markets", articles, min(limit or MAX_NEWS_ITEMS, MAX_NEWS_ITEMS))
    if gdelt_items:
        lines = ["## Global market news from GDELT", ""]
        for item in gdelt_items[: min(limit or MAX_NEWS_ITEMS, MAX_NEWS_ITEMS)]:
            lines.append(f"### {item.get('title') or 'Untitled'}")
            if item.get("published_at"):
                lines.append(f"Date: {item['published_at']}")
            if item.get("url"):
                lines.append(f"Link: {item['url']}")
            lines.append("")
        return "\n".join(lines)
    return "DATA_UNAVAILABLE: no global news found from Finnhub/GDELT."


def get_insider_transactions_no_yfinance(ticker: str) -> str:
    if is_crypto_symbol(ticker):
        return "Insider transactions are not applicable to crypto assets."

    try:
        company = resolve_sec_company(ticker)
        submissions = fetch_sec_submissions(company["cik"])
    except Exception as exc:
        return data_unavailable_message("insider filings", ticker, "SEC submissions", exc)

    recent = submissions.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])
    accessions = recent.get("accessionNumber", [])
    descriptions = recent.get("primaryDocDescription", [])
    primary_docs = recent.get("primaryDocument", [])

    lines = [
        f"## SEC insider filings for {ticker.upper()}",
        "",
        f"- Company: {company['title']}",
        f"- CIK: {company['cik']}",
        f"- Source: SEC submissions API",
        "",
        "| Form | Filing Date | Report Date | Description | SEC Filing URL |",
        "|---|---|---|---|---|",
    ]

    count = 0
    for idx, form in enumerate(forms):
        if form not in {"3", "3/A", "4", "4/A", "5", "5/A"}:
            continue
        accession = str(accessions[idx]) if idx < len(accessions) else ""
        primary_doc = str(primary_docs[idx]) if idx < len(primary_docs) else ""
        filing_url = ""
        if accession and primary_doc:
            accession_no_dashes = accession.replace("-", "")
            filing_url = f"https://www.sec.gov/Archives/edgar/data/{int(company['cik'])}/{accession_no_dashes}/{primary_doc}"
        filing_date = filing_dates[idx] if idx < len(filing_dates) else ""
        report_date = report_dates[idx] if idx < len(report_dates) else ""
        description = descriptions[idx] if idx < len(descriptions) else ""
        lines.append(f"| {form} | {filing_date} | {report_date} | {description} | {filing_url} |")
        count += 1
        if count >= MAX_INSIDER_FILINGS:
            break

    if count == 0:
        lines.append("| DATA_UNAVAILABLE |  |  | No recent Form 3/4/5 filings found in SEC submissions. |  |")
    return "\n".join(lines)


def install_no_yfinance_adapters(TradingAgentsGraph: Any) -> None:
    """Patch TradingAgents data routing in-process without editing site-packages."""
    from tradingagents.dataflows import interface
    from tradingagents.dataflows import market_data_validator, stockstats_utils
    from tradingagents.agents.analysts import sentiment_analyst
    from tradingagents.agents.utils.agent_utils import build_instrument_context

    if NO_YFINANCE_VENDOR not in interface.VENDOR_LIST:
        interface.VENDOR_LIST.append(NO_YFINANCE_VENDOR)
    interface.VENDOR_METHODS["get_stock_data"][NO_YFINANCE_VENDOR] = get_stock_data_no_yfinance
    interface.VENDOR_METHODS["get_indicators"][NO_YFINANCE_VENDOR] = get_indicators_no_yfinance
    interface.VENDOR_METHODS["get_fundamentals"][NO_YFINANCE_VENDOR] = get_fundamentals_no_yfinance
    interface.VENDOR_METHODS["get_balance_sheet"][NO_YFINANCE_VENDOR] = get_balance_sheet_no_yfinance
    interface.VENDOR_METHODS["get_cashflow"][NO_YFINANCE_VENDOR] = get_cashflow_no_yfinance
    interface.VENDOR_METHODS["get_income_statement"][NO_YFINANCE_VENDOR] = get_income_statement_no_yfinance
    interface.VENDOR_METHODS["get_news"][NO_YFINANCE_VENDOR] = get_news_no_yfinance
    interface.VENDOR_METHODS["get_global_news"][NO_YFINANCE_VENDOR] = get_global_news_no_yfinance
    interface.VENDOR_METHODS["get_insider_transactions"][NO_YFINANCE_VENDOR] = get_insider_transactions_no_yfinance

    stockstats_utils.load_ohlcv = load_ohlcv_no_yfinance
    market_data_validator.load_ohlcv = load_ohlcv_no_yfinance

    def resolve_context_no_yfinance(self, ticker: str, asset_type: str = "stock") -> str:
        identity = {}
        if is_crypto_symbol(ticker):
            identity = {"name": crypto_base(ticker), "exchange": "Binance USD-M Futures"}
        return build_instrument_context(ticker, asset_type, identity)

    def skip_pending_entries(self, ticker: str) -> None:
        return None

    TradingAgentsGraph.resolve_instrument_context = resolve_context_no_yfinance
    TradingAgentsGraph._resolve_pending_entries = skip_pending_entries
    TradingAgentsGraph._fetch_returns = lambda self, ticker, trade_date, holding_days=5, benchmark="SPY": (None, None, None)

    original_get_provider_kwargs = TradingAgentsGraph._get_provider_kwargs

    def get_provider_kwargs_with_timeout(self) -> dict[str, Any]:
        kwargs = original_get_provider_kwargs(self)
        llm_timeout = self.config.get("llm_timeout")
        if llm_timeout is not None and llm_timeout != "":
            kwargs["timeout"] = float(llm_timeout)
        llm_max_retries = self.config.get("llm_max_retries")
        if llm_max_retries is not None and llm_max_retries != "":
            kwargs["max_retries"] = int(llm_max_retries)
        return kwargs

    TradingAgentsGraph._get_provider_kwargs = get_provider_kwargs_with_timeout

    sentiment_analyst.fetch_stocktwits_messages = (
        lambda ticker, limit=30, timeout=10.0: "<stocktwits disabled: yfinance-free run avoids public social endpoints>"
    )
    sentiment_analyst.fetch_reddit_posts = (
        lambda ticker, *args, **kwargs: "<reddit disabled: yfinance-free run avoids public social endpoints>"
    )


def json_default(value: Any) -> str:
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "dict"):
        return value.dict()
    return str(value)


def state_get(state: Any, key: str, default: Any = "") -> Any:
    if isinstance(state, dict):
        return state.get(key, default)
    return getattr(state, key, default)


def section(title: str, content: Any) -> str:
    if not content:
        return ""
    if isinstance(content, (dict, list)):
        body = "```json\n" + json.dumps(content, ensure_ascii=False, indent=2, default=json_default) + "\n```"
    else:
        body = str(content)
    return f"\n## {title}\n\n{body}\n"


def get_env_requirements(provider: str) -> list[str]:
    provider_keys = {
        "openai": ["OPENAI_API_KEY"],
        "anthropic": ["ANTHROPIC_API_KEY"],
        "google": ["GOOGLE_API_KEY"],
        "deepseek": ["DEEPSEEK_API_KEY"],
        "qwen": ["DASHSCOPE_API_KEY"],
        "openrouter": ["OPENROUTER_API_KEY"],
    }
    return provider_keys.get(provider.lower(), [])


def warn_missing_keys(provider: str, online_tools: bool) -> None:
    required_keys = get_env_requirements(provider)
    missing = [key for key in required_keys if not os.getenv(key)]

    if missing:
        print("Missing LLM API key(s): " + ", ".join(missing))
        print("Set them before running, for example:")
        print(f'  export {missing[0]}="your_key_here"')
        raise SystemExit(2)

    if online_tools and not os.getenv("FINNHUB_API_KEY"):
        print("Warning: FINNHUB_API_KEY is not set. Some news/fundamental tools may be unavailable.")


def warn_missing_data_keys(data_vendor: str) -> None:
    if data_vendor.lower() == "alpha_vantage" and not os.getenv("ALPHA_VANTAGE_API_KEY"):
        print("Missing data API key: ALPHA_VANTAGE_API_KEY")
        print("Set it in .env before running with Alpha Vantage, for example:")
        print('  ALPHA_VANTAGE_API_KEY="your_key_here"')
        raise SystemExit(2)
    if data_vendor.lower() == NO_YFINANCE_VENDOR and not os.getenv("FINNHUB_API_KEY"):
        print("Warning: FINNHUB_API_KEY is not set. Stock candles will fall back to Stooq; news will use RSS/GDELT only.")


def build_config(args: argparse.Namespace, default_config: dict[str, Any]) -> dict[str, Any]:
    config = deepcopy(default_config)
    runtime_dir = Path(args.runtime_dir).resolve()

    config["llm_provider"] = args.provider
    if args.deep:
        config["deep_think_llm"] = args.deep
    if args.quick:
        config["quick_think_llm"] = args.quick
    config["results_dir"] = str(runtime_dir / "logs")
    config["data_cache_dir"] = str(runtime_dir / "cache")
    config["memory_log_path"] = str(runtime_dir / "memory" / "trading_memory.md")
    config["max_debate_rounds"] = args.debate_rounds
    config["max_risk_discuss_rounds"] = args.risk_rounds
    config["online_tools"] = not args.offline
    config["output_language"] = args.language
    if args.data_vendor:
        config["data_vendors"] = {
            **config.get("data_vendors", {}),
            "core_stock_apis": args.data_vendor,
            "technical_indicators": args.data_vendor,
            "fundamental_data": args.data_vendor,
            "news_data": args.data_vendor,
        }

    if args.backend_url:
        config["backend_url"] = args.backend_url
    if args.llm_timeout:
        config["llm_timeout"] = args.llm_timeout
    if args.llm_max_retries is not None:
        config["llm_max_retries"] = args.llm_max_retries

    return config


def write_outputs(
    out_dir: Path,
    ticker: str,
    analysis_date: str,
    asset_type: str,
    state: Any,
    decision: Any,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    slug = f"{ticker.replace('/', '-').replace(':', '-')}_{analysis_date}"

    state_path = out_dir / f"{slug}_state.json"
    decision_path = out_dir / f"{slug}_decision.json"
    summary_path = out_dir / f"{slug}_summary.md"
    final_trade_decision = state_get(state, "final_trade_decision")

    state_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, default=json_default),
        encoding="utf-8",
    )
    decision_path.write_text(
        json.dumps(decision, ensure_ascii=False, indent=2, default=json_default),
        encoding="utf-8",
    )
    summary = (
        "# TradingAgents Analysis\n\n"
        f"- Ticker: `{ticker}`\n"
        f"- Date: `{analysis_date}`\n\n"
        f"- Asset type: `{asset_type}`\n\n"
        "## Final Decision\n\n"
        "```json\n"
        f"{json.dumps(decision, ensure_ascii=False, indent=2, default=json_default)}\n"
        "```\n"
        + section("Final Trade Decision", final_trade_decision)
        + section("Trader Investment Plan", state_get(state, "trader_investment_plan"))
        + section("Investment Plan", state_get(state, "investment_plan"))
        + section("Market Report", state_get(state, "market_report"))
        + section("Sentiment Report", state_get(state, "sentiment_report"))
        + section("News Report", state_get(state, "news_report"))
        + section("Fundamentals Report", state_get(state, "fundamentals_report"))
        + section("Investment Debate State", state_get(state, "investment_debate_state"))
        + section("Risk Debate State", state_get(state, "risk_debate_state"))
    )
    summary_path.write_text(
        summary,
        encoding="utf-8",
    )

    print("\nSaved outputs:")
    print(f"  state:    {state_path}")
    print(f"  decision: {decision_path}")
    print(f"  summary:  {summary_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze an asset with TradingAgents.")
    parser.add_argument("--ticker", default="ETH-USD", help="Ticker/symbol to analyze. Default: ETH-USD")
    parser.add_argument("--asset-type", default="crypto", choices=["stock", "crypto"], help="TradingAgents asset pipeline.")
    parser.add_argument("--date", default=date.today().isoformat(), help="Analysis date, YYYY-MM-DD.")
    parser.add_argument("--provider", default="deepseek", help="LLM provider used by TradingAgents.")
    parser.add_argument("--deep", default="deepseek-v4-pro", help="Deep-thinking model.")
    parser.add_argument("--quick", default="deepseek-v4-flash", help="Quick-thinking model.")
    parser.add_argument("--backend-url", default=None, help="Optional OpenAI-compatible backend URL.")
    parser.add_argument("--debate-rounds", type=int, default=1, help="Bull/bear debate rounds.")
    parser.add_argument("--risk-rounds", type=int, default=1, help="Risk debate rounds.")
    parser.add_argument("--language", default="Chinese", help="Report output language passed to TradingAgents.")
    parser.add_argument(
        "--data-vendor",
        choices=[
            NO_YFINANCE_VENDOR,
            "alpha_vantage",
            "yfinance",
            "yfinance,alpha_vantage",
            "alpha_vantage,yfinance",
        ],
        default=NO_YFINANCE_VENDOR,
        help=(
            "Data vendor for prices, indicators, fundamentals, and news. "
            "Default no_yfinance uses Binance for crypto OHLCV, Finnhub/Nasdaq/Yahoo-chart/Stooq for stock OHLCV, "
            "SEC companyfacts for US stock fundamentals, and Finnhub/RSS/GDELT for news."
        ),
    )
    parser.add_argument("--offline", action="store_true", help="Disable online tools if supported.")
    parser.add_argument("--debug", action="store_true", help="Enable TradingAgents debug output.")
    parser.add_argument("--out-dir", default="outputs", help="Directory for result files.")
    parser.add_argument("--runtime-dir", default=".tradingagents", help="Directory for TradingAgents cache/log/memory files.")
    parser.add_argument("--llm-timeout", type=float, default=300.0, help="LLM request timeout in seconds for OpenAI-compatible providers.")
    parser.add_argument("--llm-max-retries", type=int, default=1, help="LLM request retry count for OpenAI-compatible providers.")
    parser.add_argument("--max-data-rows", type=int, default=MAX_DATA_ROWS, help="Maximum OHLCV rows returned to TradingAgents tool prompts.")
    parser.add_argument("--max-news-items", type=int, default=MAX_NEWS_ITEMS, help="Maximum news items returned to TradingAgents tool prompts.")
    parser.add_argument("--max-news-summary-chars", type=int, default=MAX_NEWS_SUMMARY_CHARS, help="Maximum characters per news summary returned to TradingAgents.")
    return parser.parse_args()


def main() -> None:
    global MAX_DATA_ROWS, MAX_NEWS_ITEMS, MAX_NEWS_SUMMARY_CHARS

    args = parse_args()
    MAX_DATA_ROWS = max(10, int(args.max_data_rows))
    MAX_NEWS_ITEMS = max(3, int(args.max_news_items))
    MAX_NEWS_SUMMARY_CHARS = max(120, int(args.max_news_summary_chars))

    TradingAgentsGraph, DEFAULT_CONFIG = import_tradingagents()

    warn_missing_keys(args.provider, online_tools=not args.offline)
    warn_missing_data_keys(args.data_vendor)
    if args.data_vendor == NO_YFINANCE_VENDOR:
        install_no_yfinance_adapters(TradingAgentsGraph)
    config = build_config(args, DEFAULT_CONFIG)

    print(f"Running TradingAgents for {args.ticker} on {args.date} ...")
    print(
        "Provider: "
        f"{config['llm_provider']}, deep: {config['deep_think_llm']}, "
        f"quick: {config['quick_think_llm']}"
    )
    print(f"Data vendors: {config.get('data_vendors')}")

    graph = TradingAgentsGraph(debug=args.debug, config=config)
    state, decision = graph.propagate(args.ticker, args.date, asset_type=args.asset_type)

    print("\nFinal decision:")
    print(json.dumps(decision, ensure_ascii=False, indent=2, default=json_default))

    write_outputs(Path(args.out_dir), args.ticker, args.date, args.asset_type, state, decision)


if __name__ == "__main__":
    main()
