"""
Backtest wrapper for bian_new.py.

demo.py is intentionally a thin replay shell:
- It fetches public Binance futures OHLCV only.
- It calls bian_new.py strategy functions directly for multi-timeframe context,
  entry candidates, support/resistance, and exit-rule stop tightening.
- It monkeypatches only exchange side effects such as latest price and stop-order
  refresh, so no real order can be placed during backtest.
"""
import argparse
import csv
import datetime
import hashlib
import json
import logging
import math
import os
import bisect
import time
from decimal import Decimal

import ccxt
import pandas as pd

import bian_new as strategy

logging.getLogger().setLevel(logging.WARNING)
strategy.logging.getLogger().setLevel(logging.WARNING)


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "backtest_cache")
OUTPUT_DIR = os.path.join(BASE_DIR, "backtest_outputs")
SYMBOL = "ETH/USDT"
SYMBOL_SAFE = SYMBOL.replace("/", "_").replace(":", "_")
TIMEFRAME_MS = {
    "1m": 60 * 1000,
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
}
RESAMPLE_RULE = {"5m": "5min", "15m": "15min", "1h": "1h", "4h": "4h", "1d": "1D"}
LIVE_FETCH_LIMITS = {"5m": 220, "15m": 220, "1h": 220, "4h": 220, "1d": 140}

INITIAL_EQUITY = 10_000.0
TAKER_FEE_RATE = 0.0004
SLIPPAGE_RATE = 0.0002
PRICE_TICK = 0.01

backtest_runtime = {
    "latest_price": 0.0,
    "price_tick": PRICE_TICK,
    "now_ms": 0,
    "events": [],
    "zone_cache": {},
    "full_context_cache": {},
    "trend_state_cache": {},
    "candidate_cache": {},
    "closed_ms_by_tf": {},
    "delayed_trend_entry": None,
}


def parse_args():
    parser = argparse.ArgumentParser(description="Replay bian_new.py strategy on the latest Binance public data.")
    parser.add_argument("--symbol", default=SYMBOL)
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--warmup-days", type=int, default=90)
    parser.add_argument(
        "--execution-timeframe",
        choices=("1m", "5m"),
        default="1m",
        help="Historical execution resolution. 1m is the parity default for the live management loop.",
    )
    parser.add_argument("--end-ms", type=int, default=0, help="Fixed exclusive backtest end timestamp in milliseconds.")
    parser.add_argument("--end-date", default="", help="Fixed exclusive backtest end time, e.g. 2026-06-24 00:00:00 Asia/Shanghai.")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--limit-trades", type=int, default=0)
    parser.add_argument("--output-tag", default="", help="Optional suffix for output filenames.")
    parser.add_argument(
        "--risk-per-trade",
        type=float,
        default=strategy.RISK_PER_TRADE,
        help="Risk ratio per trade, e.g. 0.02 means 2%%. Default: 0.02.",
    )
    parser.add_argument("--adx-profile", choices=("current", "old-global"), default="current")
    parser.add_argument(
        "--entry-direction-filter",
        choices=("current", "legacy"),
        default="current",
        help="Backtest-only multi-timeframe entry filter profile. legacy restores the previous soft allowances.",
    )
    parser.add_argument("--trigger-priority", choices=("current", "strong-first", "opposite-first"), default="current")
    parser.add_argument(
        "--trend-entry-buffer",
        choices=("current", "adaptive", "atr", "tick"),
        default="current",
        help="Backtest-only trend trigger buffer override. Production default is adaptive.",
    )
    parser.add_argument(
        "--trend-entry-atr-multiplier",
        type=float,
        default=None,
        help="Backtest-only multiplier for the fixed atr buffer mode, e.g. 0.5. Fixed atr default is 1.0.",
    )
    parser.add_argument("--strategy-timeframes", default="", help="Comma-separated backtest-only strategy timeframes, e.g. 4h or 4h,1h.")
    parser.add_argument("--enable-single-strong-entry", action="store_true", help="Backtest-only override for ENABLE_SINGLE_STRONG_ENTRY.")
    parser.add_argument(
        "--trend-entry-delay",
        choices=("none", "fixed-15m", "exit-timeframe"),
        default="none",
        help=(
            "Backtest-only delay before placing trend-trigger entries. fixed-15m waits 15 minutes for all trend entries; "
            "exit-timeframe waits 15m for 1h strategies and 1h for 4h strategies."
        ),
    )
    return parser.parse_args()


def format_ms(ms):
    return datetime.datetime.fromtimestamp(int(ms) / 1000, tz=datetime.timezone.utc).astimezone(strategy.EXCHANGE_TZ).strftime(strategy.BAR_TIME_FORMAT)


def dt_from_ms(ms):
    return datetime.datetime.fromtimestamp(int(ms) / 1000, tz=datetime.timezone.utc).astimezone(strategy.EXCHANGE_TZ)


def raw_ms_from_ts(value):
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize(strategy.EXCHANGE_TZ)
    return int(ts.tz_convert("UTC").timestamp() * 1000)


def closed_bar_ms_fast(timeframe, now_ms):
    timestamps = backtest_runtime["closed_ms_by_tf"].get(timeframe) or []
    if not timestamps:
        return None
    idx = bisect.bisect_right(timestamps, int(now_ms) - TIMEFRAME_MS[timeframe]) - 1
    if idx < 0:
        return None
    return timestamps[idx]


def closed_bar_time_fast(timeframe, now_ms):
    bar_ms = closed_bar_ms_fast(timeframe, now_ms)
    if bar_ms is None:
        return ""
    return format_ms(bar_ms)


def parse_end_ms(args, exchange):
    if args.end_ms:
        return int(args.end_ms // TIMEFRAME_MS["5m"] * TIMEFRAME_MS["5m"])
    if args.end_date:
        return int(raw_ms_from_ts(args.end_date) // TIMEFRAME_MS["5m"] * TIMEFRAME_MS["5m"])
    server_now = exchange.fetch_time()
    return int(server_now // TIMEFRAME_MS["5m"] * TIMEFRAME_MS["5m"])


def safe_tag(value):
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(value)).strip("_")


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def apply_backtest_profiles(args):
    strategy.RISK_PER_TRADE = strategy.parse_risk_per_trade(args.risk_per_trade)

    if args.adx_profile == "old-global":
        strategy.DEFAULT_ADX_CONFIG = {
            "length": 14,
            "range_max": 20,
            "no_trade_max": 25,
            "trend_min": 25,
            "extreme": 40,
        }
        strategy.ADX_BY_TIMEFRAME = {}
    elif args.adx_profile != "current":
        raise ValueError(f"Unsupported ADX profile: {args.adx_profile}")

    if args.entry_direction_filter == "legacy":
        strategy.ENABLE_DAILY_4H_LONG_EMA_FILTER = False
        strategy.ALLOW_BACKGROUND_WEAKENING_SOFT = True
        strategy.STRICT_BACKGROUND_SOFT_ALLOW = False
    elif args.entry_direction_filter != "current":
        raise ValueError(f"Unsupported entry direction filter: {args.entry_direction_filter}")

    if args.trigger_priority == "strong-first":
        strategy.ENTRY_TRIGGER_PRIORITY = ("strong_candle", "opposite_strong_break")
    elif args.trigger_priority == "opposite-first":
        strategy.ENTRY_TRIGGER_PRIORITY = ("opposite_strong_break", "strong_candle")
    elif args.trigger_priority != "current":
        raise ValueError(f"Unsupported trigger priority: {args.trigger_priority}")

    if args.trend_entry_buffer != "current":
        strategy.ENTRY_TRIGGER_BUFFER_MODE = args.trend_entry_buffer
    if args.trend_entry_atr_multiplier is not None:
        if args.trend_entry_atr_multiplier <= 0:
            raise ValueError("trend-entry-atr-multiplier must be greater than zero")
        strategy.ENTRY_TRIGGER_ATR_MULTIPLIER = args.trend_entry_atr_multiplier

    if args.enable_single_strong_entry:
        strategy.ENABLE_SINGLE_STRONG_ENTRY = True

    if args.strategy_timeframes:
        allowed = set(strategy.STRATEGY_BACKGROUND_TF)
        selected = tuple(tf.strip() for tf in args.strategy_timeframes.split(",") if tf.strip())
        invalid = [tf for tf in selected if tf not in allowed]
        if not selected or invalid:
            raise ValueError(f"Unsupported strategy timeframes: {args.strategy_timeframes}")
        strategy.STRATEGY_TIMEFRAMES = selected


def public_exchange():
    return ccxt.binance({
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
        "timeout": 30_000,
    })


def cache_path(symbol, kind, start_ms, end_ms, timeframe="5m"):
    safe = symbol.replace("/", "_").replace(":", "_")
    return os.path.join(CACHE_DIR, f"{safe}_{kind}_{timeframe}_{start_ms}_{end_ms}.csv")


def ohlcv_to_df(rows):
    df = pd.DataFrame(rows, columns=["timestamp_ms", "open", "high", "low", "close", "volume"])
    if df.empty:
        return df
    df = df.drop_duplicates("timestamp_ms").sort_values("timestamp_ms").reset_index(drop=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["timestamp_ms"] = pd.to_numeric(df["timestamp_ms"], errors="coerce").astype("int64")
    df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True).dt.tz_convert("Asia/Shanghai")
    return df[["timestamp_ms", "timestamp", "open", "high", "low", "close", "volume"]].dropna().reset_index(drop=True)


def fetch_candles(exchange, symbol, start_ms, end_ms, kind, timeframe="5m", refresh=False):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = cache_path(symbol, kind, start_ms, end_ms, timeframe=timeframe)
    if os.path.exists(path) and not refresh:
        print(f"读取缓存: {path}")
        df = pd.read_csv(path)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("Asia/Shanghai")
        return df

    params = {"price": "mark"} if kind == "mark" else {}
    rows = []
    cursor = start_ms
    request_count = 0
    print(f"获取 {kind} {timeframe}: {format_ms(start_ms)} -> {format_ms(end_ms)}")
    while cursor < end_ms:
        batch = exchange.fetch_ohlcv(symbol, timeframe, since=cursor, limit=1500, params=params)
        request_count += 1
        if not batch:
            break
        batch = [row for row in batch if row[0] < end_ms]
        rows.extend(batch)
        cursor_next = batch[-1][0] + TIMEFRAME_MS[timeframe]
        if cursor_next <= cursor:
            break
        cursor = cursor_next
        if request_count % 20 == 0:
            print(f"  {kind}: {len(rows)} 根，到 {format_ms(batch[-1][0])}")
        time.sleep(exchange.rateLimit / 1000)

    df = ohlcv_to_df(rows)
    df.to_csv(path, index=False)
    print(f"写入缓存: {path} ({len(df)} 根)")
    return df


def add_indicators_like_bian_new(df, timeframe):
    df = df.copy()
    df["ema20"] = strategy.ta.ema(df["close"], length=20)
    df["ema50"] = strategy.ta.ema(df["close"], length=50)
    df["atr"] = strategy.ta.atr(df["high"], df["low"], df["close"], length=14)

    adx_config = strategy.get_adx_config(timeframe)
    adx_length = int(adx_config.get("length", strategy.ADX_LENGTH))
    adx = strategy.ta.adx(df["high"], df["low"], df["close"], length=adx_length)
    if adx is not None:
        df = pd.concat([df, adx], axis=1)
        df.rename(columns={
            f"ADX_{adx_length}": "adx",
            f"DMP_{adx_length}": "plus_di",
            f"DMN_{adx_length}": "minus_di",
        }, inplace=True)
    return df


def resample_from_5m(df_5m, timeframe):
    frame = df_5m.copy()
    frame["dt"] = pd.to_datetime(frame["timestamp_ms"], unit="ms", utc=True)
    frame = frame.set_index("dt")
    out = frame.resample(RESAMPLE_RULE[timeframe], label="left", closed="left").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).dropna()
    out["timestamp_ms"] = (out.index.view("int64") // 1_000_000).astype("int64")
    out["timestamp"] = pd.to_datetime(out["timestamp_ms"], unit="ms", utc=True).tz_convert("Asia/Shanghai")
    return out[["timestamp_ms", "timestamp", "open", "high", "low", "close", "volume"]].reset_index(drop=True)


def prepare_dfs(df_5m):
    dfs = {}
    for timeframe in ("5m", "15m", "1h", "4h", "1d"):
        raw = resample_from_5m(df_5m, timeframe)
        dfs[timeframe] = add_indicators_like_bian_new(raw, timeframe)
    return dfs


def patch_strategy_side_effects():
    strategy.send_msg = lambda *_args, **_kwargs: None
    strategy.get_latest_price = lambda: float(backtest_runtime["latest_price"])
    strategy.get_price_tick = lambda _reference_price=None: float(backtest_runtime["price_tick"])
    strategy.precision_price = lambda price: round(
        round(float(price) / backtest_runtime["price_tick"]) * backtest_runtime["price_tick"],
        max(0, -Decimal(str(backtest_runtime["price_tick"])).normalize().as_tuple().exponent),
    )
    strategy.refresh_protective_stop_order = lambda stop_price: True
    strategy.reconcile_conditional_orders_for_position = lambda *_args, **_kwargs: True
    strategy.cleanup_orphan_conditional_orders_if_needed = lambda *_args, **_kwargs: True
    strategy.cancel_all_close_position_conditional_orders = lambda *_args, **_kwargs: True

    def live_window_get_closed_df(df, timeframe, now_dt=None):
        """Match the finite candle window fetched by each live strategy cycle."""
        last_idx = strategy.get_last_closed_index(df, timeframe, now_dt=now_dt)
        if last_idx is None:
            return pd.DataFrame()
        limit = LIVE_FETCH_LIMITS.get(timeframe, 220)
        first_idx = max(0, last_idx - limit + 1)
        return df.iloc[first_idx:last_idx + 1].copy()

    strategy.get_closed_df = live_window_get_closed_df

    original_build_zones = strategy.build_support_resistance_zones

    def cached_build_support_resistance_zones(df_4h, now_dt=None):
        closed_bar = strategy.get_closed_bar_time(df_4h, "4h", now_dt=now_dt)
        key = closed_bar or "no_closed_4h"
        cached = backtest_runtime["zone_cache"].get(key)
        if cached is not None:
            return cached
        zones = original_build_zones(df_4h, now_dt=now_dt)
        backtest_runtime["zone_cache"][key] = zones
        return zones

    strategy.build_support_resistance_zones = cached_build_support_resistance_zones

    def update_stop_if_tighter_for_backtest(side, new_stop, reason, curr_price, signal_bar_15m="", perf=None):
        current_stop = strategy.price_to_float(strategy.trade_state.get("stop_loss_price"))
        new_stop = strategy.precision_price(new_stop)
        tick = strategy.get_price_tick(curr_price)
        if new_stop <= 0:
            return False
        if side == "long":
            if new_stop <= current_stop or new_stop > curr_price - tick:
                return False
        else:
            if (current_stop > 0 and new_stop >= current_stop) or new_stop < curr_price + tick:
                return False

        strategy.trade_state["stop_loss_price"] = new_stop
        strategy.trade_state["stop_order_price"] = new_stop
        strategy.trade_state["stop_order_id"] = f"BT_STOP_{strategy.trade_state.get('open_order_id', '')}_{len(backtest_runtime['events'])}"
        strategy.trade_state["stop_order_refresh_fail_count"] = 0
        backtest_runtime["events"].append(make_event(
            "STOP_UPDATE",
            backtest_runtime["now_ms"],
            price=curr_price,
            stop_old=current_stop,
            stop_new=new_stop,
            reason=reason,
            source="bian_new.apply_new_exit_rules",
            details={
                "signal_bar_15m": signal_bar_15m,
                "simulated_order_action": "cancel old reduceOnly STOP_MARKET and place new reduceOnly STOP_MARKET",
                "workingType": strategy.STOP_WORKING_TYPE,
            },
        ))
        return True

    strategy.update_stop_if_tighter = update_stop_if_tighter_for_backtest


def get_full_context(dfs, now_dt):
    now_ms = raw_ms_from_ts(now_dt)
    closed_bars = {
        timeframe: closed_bar_time_fast(timeframe, now_ms)
        for timeframe in ("15m", "1h", "4h", "1d")
    }
    key = tuple((timeframe, closed_bars[timeframe]) for timeframe in ("15m", "1h", "4h", "1d"))
    cached = backtest_runtime["full_context_cache"].get(key)
    if cached is not None:
        return cached

    trend_states = {}
    for timeframe in ("15m", "1h", "4h", "1d"):
        state_key = (timeframe, closed_bars[timeframe])
        state = backtest_runtime["trend_state_cache"].get(state_key)
        if state is None:
            state = strategy.evaluate_adx_ema_context(dfs[timeframe], timeframe, now_dt=now_dt)
            backtest_runtime["trend_state_cache"][state_key] = state
        trend_states[timeframe] = state

    context = {
        "dfs": dfs,
        "now_dt": now_dt,
        "trend_states": trend_states,
        "zones": strategy.build_support_resistance_zones(dfs["4h"], now_dt=now_dt),
    }
    backtest_runtime["full_context_cache"][key] = context
    return context


def get_exit_context(dfs, now_dt):
    return {
        "dfs": dfs,
        "now_dt": now_dt,
        "trend_states": {},
        "zones": strategy.build_support_resistance_zones(dfs["4h"], now_dt=now_dt),
    }


def reset_strategy_trade_state():
    for key in list(strategy.trade_state.keys()):
        if isinstance(strategy.trade_state[key], bool):
            strategy.trade_state[key] = False
        elif isinstance(strategy.trade_state[key], (int, float)):
            strategy.trade_state[key] = 0
        else:
            strategy.trade_state[key] = ""
    strategy.trade_state.update({
        "has_position": False,
        "side": None,
        "last_exit_time": "",
        "last_entry_bar_15m": "",
        "last_exit_bar_15m": "",
        "last_processed_bar_5m": "",
        "max_seen_bar_5m": "",
        "last_processed_bar_15m": "",
        "max_seen_bar_15m": "",
    })


def json_safe(value):
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [json_safe(v) for v in value]
    if isinstance(value, pd.Timestamp):
        return strategy.format_bar_time(value)
    if isinstance(value, datetime.datetime):
        return value.strftime(strategy.BAR_TIME_FORMAT)
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except Exception:
        pass
    return value


def zone_text(zone):
    if not zone:
        return ""
    return f"{zone.get('type')} {zone.get('lower', 0):.2f}-{zone.get('upper', 0):.2f} status={zone.get('status', 'active')}"


def make_event(event_type, now_ms, **kwargs):
    return {
        "time": format_ms(now_ms),
        "timestamp_ms": int(now_ms),
        "event_type": event_type,
        "trade_id": strategy.trade_state.get("open_order_id", ""),
        "side": strategy.trade_state.get("side") or kwargs.get("side", ""),
        "price": kwargs.get("price", ""),
        "stop_old": kwargs.get("stop_old", ""),
        "stop_new": kwargs.get("stop_new", ""),
        "reason": kwargs.get("reason", ""),
        "source": kwargs.get("source", ""),
        "entry_price": strategy.trade_state.get("entry_price", ""),
        "target": strategy.trade_state.get("entry_sr_target", ""),
        "entry_initial_risk": strategy.trade_state.get("entry_initial_risk", ""),
        "strategy_tf": strategy.trade_state.get("entry_strategy_tf", ""),
        "trigger_tf": strategy.trade_state.get("entry_trigger_tf", ""),
        "exit_tf": strategy.trade_state.get("entry_exit_tf", ""),
        "amount": strategy.trade_state.get("amount", ""),
        "details_json": json.dumps(json_safe(kwargs.get("details", {})), ensure_ascii=False),
    }


def fill_price(price, side, is_entry):
    if is_entry:
        return price * (1 + SLIPPAGE_RATE) if side == "long" else price * (1 - SLIPPAGE_RATE)
    return price * (1 - SLIPPAGE_RATE) if side == "long" else price * (1 + SLIPPAGE_RATE)


def backtest_candidate_amount(candidate, entry_price, stop_price, cash_equity):
    return strategy.calculate_candidate_amount_raw(
        entry_price,
        stop_price,
        candidate=candidate,
        account_equity=cash_equity,
    )


def close_trade(now_ms, close_price, reason, cash_equity):
    side = strategy.trade_state["side"]
    entry_price = float(strategy.trade_state["entry_price"])
    amount = float(strategy.trade_state["amount"])
    open_fee = float(strategy.trade_state.get("open_fee", 0.0))
    points = close_price - entry_price if side == "long" else entry_price - close_price
    gross = points * amount
    close_fee = abs(close_price * amount) * TAKER_FEE_RATE
    net = gross - open_fee - close_fee
    cash_equity += gross - close_fee
    trade = {
        "trade_id": strategy.trade_state.get("open_order_id", ""),
        "side": side,
        "entry_time": strategy.trade_state.get("entry_time", ""),
        "exit_time": format_ms(now_ms),
        "entry_price": round(entry_price, 4),
        "exit_price": round(close_price, 4),
        "initial_stop": round(float(strategy.trade_state.get("open_initial_stop", strategy.trade_state.get("stop_loss_price", 0))), 4),
        "final_stop": round(float(strategy.trade_state.get("stop_loss_price", 0)), 4),
        "target": round(float(strategy.trade_state.get("entry_sr_target", 0)), 4),
        "entry_initial_risk": round(float(strategy.trade_state.get("entry_initial_risk", 0)), 4),
        "amount": round(amount, 8),
        "points_pnl": round(points, 4),
        "gross_pnl_usdt": round(gross, 4),
        "open_fee": round(open_fee, 4),
        "close_fee": round(close_fee, 4),
        "net_pnl_usdt": round(net, 4),
        "is_profit": net > 0,
        "close_reason": reason,
        "entry_reason": strategy.trade_state.get("entry_reason", ""),
        "strategy_tf": strategy.trade_state.get("entry_strategy_tf", ""),
        "trigger_tf": strategy.trade_state.get("entry_trigger_tf", ""),
        "exit_tf": strategy.trade_state.get("entry_exit_tf", ""),
        "holding_minutes": round((now_ms - raw_ms_from_ts(strategy.trade_state.get("entry_time"))) / 60_000, 2),
        "state_summary": strategy.trade_state.get("entry_state_summary", ""),
        "target_zone": strategy.trade_state.get("entry_target_zone", ""),
    }
    backtest_runtime["events"].append(make_event("CLOSE", now_ms, price=round(close_price, 4), reason=reason, source="backtest_close"))
    strategy.record_sr_breakout_failure_from_state(exit_time=format_ms(now_ms), net_pnl_usdt=net)
    backtest_runtime["candidate_cache"] = {}
    last_exit_time = format_ms(now_ms)
    exit_signal_bar_15m = closed_bar_time_fast("15m", now_ms) if backtest_runtime.get("dfs") else ""
    reset_strategy_trade_state()
    strategy.trade_state["last_exit_time"] = last_exit_time
    strategy.trade_state["last_exit_bar_15m"] = exit_signal_bar_15m
    return cash_equity, trade


def choose_candidate_like_bian_new(candidates, context, curr_price, signal_bar_15m):
    candidate, _code, reason = strategy.select_entry_candidate(
        candidates,
        context,
        curr_price,
        signal_bar_15m=signal_bar_15m,
    )
    return candidate, reason


def trend_entry_delay_ms(candidate, mode):
    """Return the backtest-only confirmation delay for a trend candidate."""
    if candidate.get("module") != "trend_trigger" or mode == "none":
        return 0
    if mode == "fixed-15m":
        return TIMEFRAME_MS["15m"]
    if mode == "exit-timeframe":
        exit_tf = candidate.get("exit_tf") or strategy.STRATEGY_EXIT_TF.get(candidate.get("strategy_tf"))
        if exit_tf not in ("15m", "1h"):
            raise ValueError(f"Unsupported delayed trend exit timeframe: {exit_tf}")
        return TIMEFRAME_MS[exit_tf]
    raise ValueError(f"Unsupported trend entry delay mode: {mode}")


def delayed_entry_price_crossed(candidate, curr_price):
    """判断等待结束价格是否仍越过原趋势触发价。"""
    side = candidate.get("side")
    original_entry = strategy.price_to_float(candidate.get("entry_level"))
    return (
        (side == "long" and curr_price >= original_entry)
        or (side == "short" and curr_price <= original_entry)
    )


def delayed_candidate_for_release(delayed, current_context, curr_price):
    """按当前趋势复核延迟信号；越过原触发价时用延迟K线完整重建候选。"""
    original = delayed["candidate"]
    side = original.get("side")
    strategy_tf = original.get("strategy_tf")
    background_tf = strategy.STRATEGY_BACKGROUND_TF.get(strategy_tf)
    crossed = delayed_entry_price_crossed(original, curr_price)

    allowed, deny_reason = strategy.entry_context_allows_side(
        current_context,
        strategy_tf,
        background_tf,
        side,
    )
    if not allowed:
        return None, crossed, f"延迟结束时趋势已失效: {deny_reason}"

    if not crossed:
        candidate = dict(original)
        candidate["delay_original_entry_level"] = strategy.price_to_float(original.get("entry_level"))
        candidate["delay_price_crossed"] = False
        return candidate, False, "价格未越过原触发价，继续使用原条件单"

    fresh_candidates = strategy.build_trend_trigger_candidates(current_context)
    for fresh in fresh_candidates:
        if (
            fresh.get("module") == "trend_trigger"
            and fresh.get("strategy_tf") == strategy_tf
            and fresh.get("side") == side
        ):
            candidate = dict(fresh)
            candidate["delay_original_entry_level"] = strategy.price_to_float(original.get("entry_level"))
            candidate["delay_price_crossed"] = True
            return candidate, True, "价格仍越过原触发价，已按延迟结束K线重建趋势候选"
    return None, True, "价格仍越过原触发价，但延迟结束K线无法生成新的趋势触发候选"


def stage_delayed_trend_entry(candidate, context, now_ms, trade_seq, signal_bar_15m, mode):
    delay_ms = trend_entry_delay_ms(candidate, mode)
    if delay_ms <= 0:
        return False
    backtest_runtime["delayed_trend_entry"] = {
        "candidate": candidate,
        "context": context,
        "created_ms": now_ms,
        "eligible_ms": now_ms + delay_ms,
        "trade_seq": trade_seq,
        "signal_bar_15m": signal_bar_15m,
        "mode": mode,
        "delay_ms": delay_ms,
    }
    backtest_runtime["events"].append(make_event(
        "TREND_ENTRY_WAIT",
        now_ms,
        side=candidate.get("side", ""),
        price=backtest_runtime.get("latest_price", ""),
        reason=f"趋势入场延迟确认 {delay_ms // 60_000} 分钟",
        source="backtest_trend_entry_delay",
        details={
            "candidate": candidate,
            "mode": mode,
            "delay_minutes": delay_ms // 60_000,
            "eligible_time": format_ms(now_ms + delay_ms),
            "original_entry_level": candidate.get("entry_level"),
        },
    ))
    return True


def release_delayed_trend_entry(row, now_ms, cash_equity, current_context):
    delayed = backtest_runtime.get("delayed_trend_entry")
    if not delayed or now_ms < delayed["eligible_ms"]:
        return cash_equity, None, False

    backtest_runtime["delayed_trend_entry"] = None
    curr_price = float(row["close"])
    candidate, crossed, release_reason = delayed_candidate_for_release(delayed, current_context, curr_price)
    backtest_runtime["events"].append(make_event(
        "TREND_ENTRY_RELEASE",
        now_ms,
        side=delayed["candidate"].get("side", ""),
        price=curr_price,
        reason=release_reason,
        source="backtest_trend_entry_delay",
        details={
            "mode": delayed["mode"],
            "delay_minutes": delayed["delay_ms"] // 60_000,
            "original_entry_level": delayed["candidate"].get("entry_level"),
            "released_entry_level": candidate.get("entry_level") if candidate else None,
            "released_stop": candidate.get("stop") if candidate else None,
            "price_crossed": crossed,
        },
    ))
    if not candidate:
        return cash_equity, None, True

    pending_id = place_pending_entry_for_backtest(
        candidate,
        current_context,
        now_ms,
        curr_price,
        cash_equity,
        delayed["trade_seq"],
        delayed["signal_bar_15m"],
    )
    return cash_equity, None, True


def cancel_pending_entry_for_backtest(now_ms, reason):
    if not strategy.has_pending_entry():
        return
    backtest_runtime["events"].append(make_event(
        "PENDING_CANCEL",
        now_ms,
        side=strategy.trade_state.get("pending_entry_side", ""),
        price=backtest_runtime.get("latest_price", ""),
        reason=reason,
        source="backtest_pending_entry",
        details={
            "pending_order_id": strategy.trade_state.get("pending_entry_order_id", ""),
            "stopPrice": strategy.trade_state.get("pending_entry_stop_price", ""),
            "limitPrice": strategy.trade_state.get("pending_entry_limit_price", ""),
        },
    ))
    strategy.clear_pending_entry_state(reason)


def place_pending_entry_for_backtest(candidate, context, now_ms, curr_price, cash_equity, trade_seq, signal_bar_15m=""):
    side = candidate["side"]
    pending_prices, price_reason = strategy.resolve_pending_entry_prices(candidate, curr_price)
    if not pending_prices:
        backtest_runtime["events"].append(make_event(
            "SKIP",
            now_ms,
            side=side,
            price=curr_price,
            reason=price_reason,
            source="pending_open_guard",
            details={"candidate": candidate},
        ))
        return None
    entry_level = pending_prices["entry_level"]
    stop_price = pending_prices["stop_price"]
    limit_price = pending_prices["limit_price"]
    allowed_slippage = pending_prices["allowed_slippage"]
    protective_stop = strategy.precision_price(strategy.price_to_float(candidate.get("stop")))
    target = strategy.precision_price(strategy.price_to_float(candidate.get("target")))

    safe_stop, stop_meta = strategy.ensure_stop_price_safe(entry_level, protective_stop, side, liquidation_price=None)
    if not strategy.stop_price_is_still_valid(entry_level, safe_stop, side):
        backtest_runtime["events"].append(make_event(
            "SKIP",
            now_ms,
            side=side,
            price=curr_price,
            reason="pending STOP_LIMIT 初始止损方向无效",
            source="pending_open_guard",
            details={"candidate": candidate, "stop_meta": stop_meta},
        ))
        return None

    target_ok, target_reason = strategy.target_profit_still_valid(
        side,
        entry_level,
        safe_stop,
        target,
        float(candidate.get("profit_check_atr") or 0.0),
    )
    if not target_ok:
        backtest_runtime["events"].append(make_event(
            "SKIP",
            now_ms,
            side=side,
            price=curr_price,
            reason=target_reason,
            source="pending_open_guard",
            details={"candidate": candidate},
        ))
        return None

    # 与实盘一致，只按条件触发入场价与初始止损价计算固定风险。
    amount, amount_meta = backtest_candidate_amount(candidate, entry_level, safe_stop, cash_equity)
    if amount <= 0:
        backtest_runtime["events"].append(make_event(
            "SKIP",
            now_ms,
            side=side,
            price=curr_price,
            reason="SR breakout 风险距离超过ATR分档限制" if candidate.get("module") == "sr_breakout" else "候选仓位为0",
            source="pending_open_guard",
            details={"candidate": candidate, "amount_meta": amount_meta},
        ))
        return None
    trade_id = f"BT{trade_seq:05d}"
    states = strategy.states_for_candidate(context, candidate)
    background_tf = strategy.STRATEGY_BACKGROUND_TF.get(candidate.get("strategy_tf"), "1h")
    strategy.trade_state.update({
        "pending_entry_order_id": trade_id,
        "pending_entry_client_order_id": f"{strategy.STRATEGY_ENTRY_CLIENT_PREFIX}{trade_id}",
        "pending_entry_side": side,
        "pending_entry_amount": amount,
        "pending_entry_stop_price": stop_price,
        "pending_entry_limit_price": limit_price,
        "pending_entry_protective_stop": strategy.precision_price(safe_stop),
        "pending_entry_target": target,
        "pending_entry_strategy_tf": candidate.get("strategy_tf", ""),
        "pending_entry_background_tf": background_tf,
        "pending_entry_trigger_tf": f"{candidate.get('strategy_tf')}->{candidate.get('trigger_tf')}",
        "pending_entry_exit_tf": candidate.get("exit_tf", ""),
        "pending_entry_module": candidate.get("module", ""),
        "pending_entry_sr_breakout_key": candidate.get("sr_breakout_key", ""),
        "pending_entry_created_time": format_ms(now_ms),
        "pending_entry_signal_bar": signal_bar_15m,
        "pending_entry_candidate_json": json.dumps(json_safe(candidate), ensure_ascii=False),
        "pending_entry_triggered": False,
        "pending_entry_filled_amount": 0.0,
        "pending_entry_missing_count": 0,
        "pending_entry_initial_balance": cash_equity,
        "pending_entry_reason": candidate.get("reason", ""),
        "pending_entry_cond_4h": states[0].get("summary", ""),
        "pending_entry_cond_1h": states[1].get("summary", ""),
        "pending_entry_cond_15m": states[2].get("summary", ""),
        "pending_entry_condition_details": candidate.get("state_summary", ""),
        "pending_entry_profit_check_atr": float(candidate.get("profit_check_atr") or 0.0),
        "last_entry_bar_15m": signal_bar_15m,
    })
    backtest_runtime["events"].append(make_event(
        "PENDING_ENTRY",
        now_ms,
        side=side,
        price=curr_price,
        stop_new=strategy.precision_price(safe_stop),
        reason=candidate.get("reason", ""),
        source="backtest_stop_limit_entry",
        details={
            "candidate": candidate,
            "stopPrice": stop_price,
            "limitPrice": limit_price,
            "allowed_slippage": allowed_slippage,
            "order_type": strategy.ENTRY_STOP_LIMIT_ORDER_TYPE,
            "workingType": strategy.STOP_WORKING_TYPE,
            "amount_meta": amount_meta,
            "simulated_order_action": "place STOP_LIMIT pending entry; protective STOP_MARKET waits until fill",
        },
    ))
    return trade_id


def process_pending_entry_for_backtest(row, mark, now_ms, cash_equity):
    if not strategy.has_pending_entry():
        return cash_equity, None

    side = strategy.trade_state.get("pending_entry_side")
    stop_price = float(strategy.trade_state.get("pending_entry_stop_price") or 0)
    limit_price = float(strategy.trade_state.get("pending_entry_limit_price") or 0)
    created_ms = raw_ms_from_ts(strategy.trade_state.get("pending_entry_created_time"))
    curr_price = float(row["close"])
    mark_high = float(mark.get("high", row["high"]))
    mark_low = float(mark.get("low", row["low"]))

    if now_ms - created_ms > strategy.PENDING_ENTRY_MAX_AGE_SECONDS * 1000:
        cancel_pending_entry_for_backtest(now_ms, "pending STOP_LIMIT 超时")
        return cash_equity, None

    triggered = bool(strategy.trade_state.get("pending_entry_triggered"))
    if not triggered:
        triggered = (side == "long" and mark_high >= stop_price) or (side == "short" and mark_low <= stop_price)
        if triggered:
            strategy.trade_state["pending_entry_triggered"] = True
            backtest_runtime["events"].append(make_event(
                "PENDING_TRIGGER",
                now_ms,
                side=side,
                price=curr_price,
                reason="MARK_PRICE 触发 STOP_LIMIT 入场",
                source="backtest_stop_limit_entry",
                details={
                    "stopPrice": stop_price,
                    "limitPrice": limit_price,
                    "mark_high": mark_high,
                    "mark_low": mark_low,
                },
            ))

    if not triggered:
        return cash_equity, None

    fillable = (side == "long" and float(row["low"]) <= limit_price) or (side == "short" and float(row["high"]) >= limit_price)
    if not fillable:
        if side == "long" and curr_price > limit_price:
            cancel_pending_entry_for_backtest(now_ms, "触发后价格高于多单 limitPrice，取消 pending")
        elif side == "short" and curr_price < limit_price:
            cancel_pending_entry_for_backtest(now_ms, "触发后价格低于空单 limitPrice，取消 pending")
        return cash_equity, None

    raw_entry = fill_price(stop_price, side, is_entry=True)
    entry_price = min(raw_entry, limit_price) if side == "long" else max(raw_entry, limit_price)
    protective_stop = float(strategy.trade_state.get("pending_entry_protective_stop") or 0)
    target = float(strategy.trade_state.get("pending_entry_target") or 0)
    target_ok, target_reason = strategy.target_profit_still_valid(
        side,
        entry_price,
        protective_stop,
        target,
        float(strategy.trade_state.get("pending_entry_profit_check_atr") or 0.0),
    )
    if not target_ok:
        cancel_pending_entry_for_backtest(now_ms, f"pending 成交价目标空间无效: {target_reason}")
        return cash_equity, None

    amount = float(strategy.trade_state.get("pending_entry_amount") or 0)
    open_fee = abs(entry_price * amount) * TAKER_FEE_RATE
    cash_equity -= open_fee
    trade_id = strategy.trade_state.get("pending_entry_order_id", "")
    strategy.trade_state.update({
        "has_position": True,
        "side": side,
        "entry_price": entry_price,
        "stop_loss_price": protective_stop,
        "highest_price": entry_price,
        "lowest_price": entry_price,
        "amount": amount,
        "entry_time": format_ms(now_ms),
        "initial_balance": cash_equity + open_fee,
        "open_fee": open_fee,
        "open_order_id": trade_id,
        "close_order_id": "",
        "entry_reason": strategy.trade_state.get("pending_entry_reason", ""),
        "entry_trigger_tf": strategy.trade_state.get("pending_entry_trigger_tf", ""),
        "entry_strategy_tf": strategy.trade_state.get("pending_entry_strategy_tf", ""),
        "entry_exit_tf": strategy.trade_state.get("pending_entry_exit_tf", ""),
        "entry_module": strategy.trade_state.get("pending_entry_module", ""),
        "entry_sr_breakout_key": strategy.trade_state.get("pending_entry_sr_breakout_key", ""),
        "entry_sr_target": target,
        "entry_initial_risk": abs(entry_price - protective_stop),
        "stop_order_id": f"BT_STOP_{trade_id}_0",
        "stop_order_price": protective_stop,
        "entry_signal_bar_15m": strategy.trade_state.get("pending_entry_signal_bar", ""),
        "last_entry_bar_15m": strategy.trade_state.get("pending_entry_signal_bar", ""),
        "cond_4h": strategy.trade_state.get("pending_entry_cond_4h", ""),
        "cond_1h": strategy.trade_state.get("pending_entry_cond_1h", ""),
        "cond_15m": strategy.trade_state.get("pending_entry_cond_15m", ""),
        "entry_state_summary": strategy.trade_state.get("pending_entry_condition_details", ""),
        "entry_target_zone": "",
        "open_initial_stop": protective_stop,
    })
    backtest_runtime["events"].append(make_event(
        "OPEN",
        now_ms,
        price=round(entry_price, 4),
        stop_new=protective_stop,
        reason=strategy.trade_state.get("entry_reason", ""),
        source="backtest_stop_limit_fill",
        details={
            "stopPrice": stop_price,
            "limitPrice": limit_price,
            "mark_high": mark_high,
            "mark_low": mark_low,
            "last_high": float(row["high"]),
            "last_low": float(row["low"]),
            "simulated_order_action": "STOP_LIMIT filled, then place reduceOnly STOP_MARKET protective stop",
            "workingType": strategy.STOP_WORKING_TYPE,
        },
    ))
    strategy.clear_pending_entry_state("backtest pending STOP_LIMIT filled")
    return cash_equity, trade_id


def pending_entry_cancel_reason_like_bian_new(context, curr_price):
    """Delegate pending-order invalidation to the live strategy."""
    _code, reason, _age = strategy.pending_entry_cancel_decision(context, curr_price)
    return reason


def open_trade(candidate, context, now_ms, curr_price, cash_equity, trade_seq, signal_bar_15m=""):
    side = candidate["side"]
    entry_price = fill_price(curr_price, side, is_entry=True)
    stop = float(candidate["stop"])
    if (side == "long" and stop >= entry_price) or (side == "short" and stop <= entry_price):
        backtest_runtime["events"].append(make_event("SKIP", now_ms, side=side, price=curr_price, reason="初始止损方向无效", source="open_guard", details={"candidate": candidate}))
        return None, cash_equity

    target = float(candidate.get("target") or 0.0)
    target_ok, target_reason = strategy.target_profit_still_valid(
        side,
        entry_price,
        stop,
        target,
        float(candidate.get("profit_check_atr") or 0.0),
    )
    if not target_ok:
        backtest_runtime["events"].append(make_event("SKIP", now_ms, side=side, price=curr_price, reason=target_reason, source="open_guard", details={"candidate": candidate}))
        return None, cash_equity

    notional = cash_equity * strategy.MARGIN_RATE * strategy.LEVERAGE
    amount = notional / entry_price
    open_fee = notional * TAKER_FEE_RATE
    cash_equity -= open_fee
    trade_id = f"BT{trade_seq:05d}"

    states = strategy.states_for_candidate(context, candidate)
    strategy.trade_state.update({
        "has_position": True,
        "side": side,
        "entry_price": entry_price,
        "stop_loss_price": stop,
        "highest_price": entry_price,
        "lowest_price": entry_price,
        "amount": amount,
        "entry_time": format_ms(now_ms),
        "initial_balance": cash_equity + open_fee,
        "open_fee": open_fee,
        "open_order_id": trade_id,
        "close_order_id": "",
        "entry_reason": candidate.get("reason", ""),
        "entry_trigger_tf": f"{candidate.get('strategy_tf')}->{candidate.get('trigger_tf')}",
        "entry_strategy_tf": candidate.get("strategy_tf", ""),
        "entry_exit_tf": candidate.get("exit_tf", ""),
        "entry_module": candidate.get("module", ""),
        "entry_sr_breakout_key": candidate.get("sr_breakout_key", ""),
        "entry_sr_target": target,
        "entry_initial_risk": abs(entry_price - stop),
        "stop_order_id": f"BT_STOP_{trade_id}_0",
        "stop_order_price": stop,
        "entry_signal_bar_15m": signal_bar_15m,
        "last_entry_bar_15m": signal_bar_15m,
        "cond_4h": states[0].get("summary", ""),
        "cond_1h": states[1].get("summary", ""),
        "cond_15m": states[2].get("summary", ""),
        "entry_state_summary": candidate.get("state_summary", ""),
        "entry_target_zone": zone_text(candidate.get("target_zone")),
        "open_initial_stop": stop,
    })
    backtest_runtime["events"].append(make_event(
        "OPEN",
        now_ms,
        price=round(entry_price, 4),
        stop_new=stop,
        reason=candidate.get("reason", ""),
        source=candidate.get("module", ""),
        details={
            "candidate": candidate,
            "trend_states": {tf: state.get("summary", "") for tf, state in context.get("trend_states", {}).items()},
            "support_count": len(context.get("zones", {}).get("support", [])),
            "resistance_count": len(context.get("zones", {}).get("resistance", [])),
            "simulated_order_action": "place reduceOnly STOP_MARKET protective stop",
            "workingType": strategy.STOP_WORKING_TYPE,
            "no_take_profit_limit_order": True,
            "no_half_take_profit": True,
        },
    ))
    return trade_id, cash_equity


def write_csv(path, rows, headers):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def run_backtest(args):
    backtest_runtime["events"] = []
    backtest_runtime["zone_cache"] = {}
    backtest_runtime["full_context_cache"] = {}
    backtest_runtime["trend_state_cache"] = {}
    backtest_runtime["candidate_cache"] = {}
    backtest_runtime["delayed_trend_entry"] = None
    strategy.sr_breakout_failure_cooldowns["side"] = {}
    strategy.sr_breakout_failure_cooldowns["zone"] = {}
    patch_strategy_side_effects()
    apply_backtest_profiles(args)
    reset_strategy_trade_state()
    exchange = public_exchange()
    exchange.load_markets()
    market = exchange.market(args.symbol)
    market_price_tick = float(market.get("precision", {}).get("price") or PRICE_TICK)
    if market_price_tick <= 0:
        market_price_tick = PRICE_TICK
    backtest_runtime["price_tick"] = market_price_tick
    end_ms = parse_end_ms(args, exchange)
    start_ms = end_ms - args.days * TIMEFRAME_MS["1d"]
    fetch_start_ms = start_ms - args.warmup_days * TIMEFRAME_MS["1d"]

    execution_timeframe = args.execution_timeframe
    execution_ms = TIMEFRAME_MS[execution_timeframe]
    last_execution = fetch_candles(
        exchange,
        args.symbol,
        fetch_start_ms,
        end_ms,
        "last",
        timeframe=execution_timeframe,
        refresh=args.refresh,
    )
    try:
        mark_execution = fetch_candles(
            exchange,
            args.symbol,
            fetch_start_ms,
            end_ms,
            "mark",
            timeframe=execution_timeframe,
            refresh=args.refresh,
        )
    except Exception as exc:
        print(f"获取 mark price K线失败，回退用普通K线模拟 STOP_MARKET MARK_PRICE: {exc}")
        mark_execution = last_execution.copy()
    mark_by_ms = mark_execution.set_index("timestamp_ms").to_dict("index")
    strategy_5m = resample_from_5m(last_execution, "5m")
    dfs = prepare_dfs(strategy_5m)
    backtest_runtime["dfs"] = dfs
    backtest_runtime["closed_ms_by_tf"] = {
        timeframe: [int(value) for value in df["timestamp_ms"].tolist()]
        for timeframe, df in dfs.items()
    }
    execution_rows = last_execution[last_execution["timestamp_ms"] >= start_ms].reset_index(drop=True)
    if execution_rows.empty:
        raise RuntimeError(f"没有可回测的{execution_timeframe}数据")

    cash_equity = INITIAL_EQUITY
    peak_equity = INITIAL_EQUITY
    max_drawdown = 0.0
    trades = []
    equity_rows = []
    trade_seq = 0
    next_progress = 0

    print(f"开始回测 bian_new.py 逻辑: {format_ms(start_ms)} -> {format_ms(end_ms)}")
    for idx, row in enumerate(execution_rows.itertuples(index=False)):
        bar_ms = int(row.timestamp_ms)
        now_ms = bar_ms + execution_ms
        now_dt = dt_from_ms(now_ms)
        curr_price = float(row.close)
        row_data = {
            "timestamp_ms": bar_ms,
            "open": float(row.open),
            "high": float(row.high),
            "low": float(row.low),
            "close": curr_price,
            "volume": float(row.volume),
        }
        backtest_runtime["latest_price"] = curr_price
        backtest_runtime["now_ms"] = now_ms
        delayed_active_at_bar_start = bool(backtest_runtime.get("delayed_trend_entry"))
        delayed_released_this_bar = False

        if idx >= next_progress:
            print(f"进度 {idx}/{len(execution_rows)} {format_ms(bar_ms)} trades={len(trades)} cash={cash_equity:.2f}", flush=True)
            next_progress += max(1, len(execution_rows) // 20)

        if strategy.trade_state.get("has_position"):
            side = strategy.trade_state["side"]
            strategy.trade_state["highest_price"] = max(float(strategy.trade_state.get("highest_price", 0) or 0), row_data["high"])
            low_state = float(strategy.trade_state.get("lowest_price", 0) or 0)
            strategy.trade_state["lowest_price"] = row_data["low"] if low_state <= 0 else min(low_state, row_data["low"])

            mark = mark_by_ms.get(bar_ms, row_data)
            mark_low = float(mark.get("low", row_data["low"]))
            mark_high = float(mark.get("high", row_data["high"]))
            stop = float(strategy.trade_state.get("stop_loss_price", 0) or 0)
            stop_hit = (side == "long" and mark_low <= stop) or (side == "short" and mark_high >= stop)
            if stop_hit:
                close_px = fill_price(stop, side, is_entry=False)
                cash_equity, trade = close_trade(now_ms, close_px, "MARK_PRICE触发STOP_MARKET保护止损", cash_equity)
                trades.append(trade)
                if args.limit_trades and len(trades) >= args.limit_trades:
                    break
            else:
                context = get_exit_context(dfs, now_dt)
                strategy.apply_new_exit_rules(
                    context,
                    signal_bar_15m=closed_bar_time_fast("15m", now_ms),
                    curr_price=curr_price,
                    price_ts=time.monotonic(),
                )

        if not strategy.trade_state.get("has_position") and delayed_active_at_bar_start:
            delayed_context = get_full_context(dfs, now_dt)
            cash_equity, opened_trade_id, delayed_released_this_bar = release_delayed_trend_entry(
                row_data,
                now_ms,
                cash_equity,
                delayed_context,
            )
            if opened_trade_id and args.limit_trades and len(trades) >= args.limit_trades:
                break

        pending_created_this_bar = delayed_released_this_bar and strategy.has_pending_entry()
        if (
            not strategy.trade_state.get("has_position")
            and strategy.has_pending_entry()
            and not pending_created_this_bar
        ):
            mark = mark_by_ms.get(bar_ms, row_data)
            cash_equity, opened_trade_id = process_pending_entry_for_backtest(row_data, mark, now_ms, cash_equity)
            if opened_trade_id and args.limit_trades and len(trades) >= args.limit_trades:
                break
            if strategy.has_pending_entry():
                pending_context = get_full_context(dfs, now_dt)
                pending_cancel_reason = pending_entry_cancel_reason_like_bian_new(pending_context, curr_price)
                if pending_cancel_reason:
                    cancel_pending_entry_for_backtest(now_ms, pending_cancel_reason)

        signal_bar_5m = closed_bar_time_fast("5m", now_ms)
        signal_bar_15m = closed_bar_time_fast("15m", now_ms)
        signal_guard_5m = strategy.validate_signal_bar("5m", signal_bar_5m, now_dt=now_dt)
        signal_guard_15m = strategy.validate_signal_bar("15m", signal_bar_15m, now_dt=now_dt)
        if (
            not strategy.trade_state.get("has_position")
            and not strategy.has_pending_entry()
            and not backtest_runtime.get("delayed_trend_entry")
            and not delayed_released_this_bar
            and signal_guard_5m["valid"]
            and signal_guard_5m["is_new"]
            and not strategy.is_in_post_exit_cooldown(now_dt)
        ):
            context = get_full_context(dfs, now_dt)
            candidate_key = tuple(
                (timeframe, closed_bar_time_fast(timeframe, now_ms))
                for timeframe in ("15m", "1h", "4h", "1d")
            )
            candidates = backtest_runtime["candidate_cache"].get(candidate_key)
            if candidates is None:
                candidates = strategy.build_entry_candidates(context)
                backtest_runtime["candidate_cache"][candidate_key] = candidates
            candidate, skip_reason = choose_candidate_like_bian_new(candidates, context, curr_price, signal_bar_15m)
            if candidate:
                trade_seq += 1
                candidate_signal_bar = signal_bar_15m if signal_guard_15m.get("valid") else ""
                if not stage_delayed_trend_entry(
                    candidate,
                    context,
                    now_ms,
                    trade_seq,
                    candidate_signal_bar,
                    args.trend_entry_delay,
                ):
                    place_pending_entry_for_backtest(
                        candidate,
                        context,
                        now_ms,
                        curr_price,
                        cash_equity,
                        trade_seq,
                        candidate_signal_bar,
                    )
            elif candidates:
                backtest_runtime["events"].append(make_event(
                    "SKIP",
                    now_ms,
                    price=curr_price,
                    reason=skip_reason,
                    source="candidate_selection",
                    details={"candidate_count": len(candidates)},
                ))
            strategy.trade_state["last_processed_bar_5m"] = signal_bar_5m
            if signal_guard_15m.get("valid") and signal_guard_15m.get("is_new"):
                strategy.trade_state["last_processed_bar_15m"] = signal_bar_15m

        if delayed_active_at_bar_start:
            if signal_guard_5m.get("valid") and signal_guard_5m.get("is_new"):
                strategy.trade_state["last_processed_bar_5m"] = signal_bar_5m
            if signal_guard_15m.get("valid") and signal_guard_15m.get("is_new"):
                strategy.trade_state["last_processed_bar_15m"] = signal_bar_15m

        floating = 0.0
        if strategy.trade_state.get("has_position"):
            side = strategy.trade_state["side"]
            entry = float(strategy.trade_state["entry_price"])
            amount = float(strategy.trade_state["amount"])
            floating = (curr_price - entry) * amount if side == "long" else (entry - curr_price) * amount
        equity = cash_equity + floating
        peak_equity = max(peak_equity, equity)
        if peak_equity > 0:
            max_drawdown = max(max_drawdown, (peak_equity - equity) / peak_equity)
        equity_sample_interval = max(1, TIMEFRAME_MS["1h"] // execution_ms)
        if idx % equity_sample_interval == 0:
            equity_rows.append({
                "time": format_ms(now_ms),
                "timestamp_ms": now_ms,
                "equity": round(equity, 4),
                "cash_equity": round(cash_equity, 4),
                "floating_pnl": round(floating, 4),
                "drawdown_pct": round(max_drawdown * 100, 4),
                "open_trade_id": strategy.trade_state.get("open_order_id", "") if strategy.trade_state.get("has_position") else "",
            })

    if strategy.trade_state.get("has_position"):
        last = execution_rows.iloc[-1]
        side = strategy.trade_state["side"]
        close_px = fill_price(float(last["close"]), side, is_entry=False)
        cash_equity, trade = close_trade(int(last["timestamp_ms"]) + execution_ms, close_px, "回测结束强制平仓", cash_equity)
        trades.append(trade)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start_label = dt_from_ms(start_ms).strftime("%Y%m%d")
    end_label = dt_from_ms(end_ms).strftime("%Y%m%d")
    tag = safe_tag(args.output_tag)
    tag_suffix = f"_{tag}" if tag else ""
    symbol_safe = args.symbol.replace("/", "_").replace(":", "_")
    prefix = os.path.join(OUTPUT_DIR, f"{symbol_safe}_bian_new_backtest_{start_label}_{end_label}{tag_suffix}")
    trades_path = f"{prefix}_trades.csv"
    events_path = f"{prefix}_events.csv"
    stops_path = f"{prefix}_stop_updates.csv"
    equity_path = f"{prefix}_equity.csv"
    summary_path = f"{prefix}_summary.json"

    trade_headers = [
        "trade_id", "side", "entry_time", "exit_time", "entry_price", "exit_price",
        "initial_stop", "final_stop", "target", "entry_initial_risk", "amount",
        "points_pnl", "gross_pnl_usdt", "open_fee", "close_fee", "net_pnl_usdt",
        "is_profit", "close_reason", "entry_reason", "strategy_tf", "trigger_tf",
        "exit_tf", "holding_minutes", "state_summary", "target_zone",
    ]
    event_headers = [
        "time", "timestamp_ms", "event_type", "trade_id", "side", "price",
        "stop_old", "stop_new", "reason", "source", "entry_price", "target",
        "entry_initial_risk", "strategy_tf", "trigger_tf", "exit_tf", "amount",
        "details_json",
    ]
    equity_headers = ["time", "timestamp_ms", "equity", "cash_equity", "floating_pnl", "drawdown_pct", "open_trade_id"]
    write_csv(trades_path, trades, trade_headers)
    write_csv(events_path, backtest_runtime["events"], event_headers)
    write_csv(stops_path, [e for e in backtest_runtime["events"] if e["event_type"] == "STOP_UPDATE"], event_headers)
    write_csv(equity_path, equity_rows, equity_headers)

    wins = [t for t in trades if float(t["net_pnl_usdt"]) > 0]
    losses = [t for t in trades if float(t["net_pnl_usdt"]) <= 0]
    gross_profit = sum(float(t["net_pnl_usdt"]) for t in wins)
    gross_loss = abs(sum(float(t["net_pnl_usdt"]) for t in losses))
    summary = {
        "symbol": args.symbol,
        "strategy_source": os.path.join(BASE_DIR, "bian_new.py"),
        "strategy_source_sha256": file_sha256(os.path.join(BASE_DIR, "bian_new.py")),
        "backtest_source_sha256": file_sha256(os.path.abspath(__file__)),
        "start": format_ms(start_ms),
        "end": format_ms(end_ms),
        "available_data_start": format_ms(int(execution_rows.iloc[0]["timestamp_ms"])),
        "available_data_end": format_ms(int(execution_rows.iloc[-1]["timestamp_ms"]) + execution_ms),
        "days": args.days,
        "warmup_days": args.warmup_days,
        "limit_trades": args.limit_trades,
        "execution_timeframe": execution_timeframe,
        "market_id": market.get("id", ""),
        "market_price_tick": market_price_tick,
        "market_amount_step": market.get("precision", {}).get("amount"),
        "live_fetch_limits": LIVE_FETCH_LIMITS,
        "adx_profile": args.adx_profile,
        "entry_direction_filter": args.entry_direction_filter,
        "daily_4h_long_ema_filter": strategy.ENABLE_DAILY_4H_LONG_EMA_FILTER,
        "allow_background_weakening_soft": strategy.ALLOW_BACKGROUND_WEAKENING_SOFT,
        "strict_background_soft_allow": strategy.STRICT_BACKGROUND_SOFT_ALLOW,
        "background_soft_allow_min_di_gap": strategy.BACKGROUND_SOFT_ALLOW_MIN_DI_GAP,
        "adx_config": {
            "default": strategy.DEFAULT_ADX_CONFIG,
            "by_timeframe": strategy.ADX_BY_TIMEFRAME,
        },
        "trigger_priority": args.trigger_priority,
        "entry_trigger_priority": list(strategy.ENTRY_TRIGGER_PRIORITY),
        "trend_entry_buffer_mode": strategy.ENTRY_TRIGGER_BUFFER_MODE,
        "trend_entry_atr_multiplier": strategy.ENTRY_TRIGGER_ATR_MULTIPLIER,
        "adaptive_trend_entry_buffer": {
            "clean_di_gap": strategy.ADAPTIVE_ENTRY_CLEAN_DI_GAP,
            "clean_atr_multiplier": strategy.ADAPTIVE_ENTRY_CLEAN_ATR_MULTIPLIER,
            "extreme_di_gap": strategy.ADAPTIVE_ENTRY_EXTREME_DI_GAP,
            "extreme_buffer": "1_tick",
            "volatility_lookback": strategy.ADAPTIVE_ENTRY_VOLATILITY_LOOKBACK,
            "high_volatility_percentile": strategy.ADAPTIVE_ENTRY_HIGH_VOLATILITY_PERCENTILE,
            "volatility_min_samples": strategy.ADAPTIVE_ENTRY_VOLATILITY_MIN_SAMPLES,
            "chop_lookback": strategy.ADAPTIVE_ENTRY_CHOP_LOOKBACK,
            "chop_min_di_flips": strategy.ADAPTIVE_ENTRY_CHOP_MIN_DI_FLIPS,
            "defensive_atr_multiplier": strategy.ADAPTIVE_ENTRY_DEFENSIVE_ATR_MULTIPLIER,
        },
        "strategy_timeframes": list(strategy.STRATEGY_TIMEFRAMES),
        "trend_entry_delay": args.trend_entry_delay,
        "enable_single_strong_entry": strategy.ENABLE_SINGLE_STRONG_ENTRY,
        "enable_synthetic_strong_entry": strategy.ENABLE_SYNTHETIC_STRONG_ENTRY,
        "ai_research_mode": strategy.AI_RESEARCH_CONFIG.mode,
        "initial_equity": INITIAL_EQUITY,
        "risk_per_trade": strategy.RISK_PER_TRADE,
        "final_equity": round(cash_equity, 4),
        "net_profit": round(cash_equity - INITIAL_EQUITY, 4),
        "return_pct": round((cash_equity / INITIAL_EQUITY - 1) * 100, 4),
        "trade_count": len(trades),
        "win_rate": round(len(wins) / len(trades), 4) if trades else 0.0,
        "gross_profit": round(gross_profit, 4),
        "gross_loss": round(gross_loss, 4),
        "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
        "max_drawdown_pct": round(max_drawdown * 100, 4),
        "stop_update_count": len([e for e in backtest_runtime["events"] if e["event_type"] == "STOP_UPDATE"]),
        "pending_entry_count": len([e for e in backtest_runtime["events"] if e["event_type"] == "PENDING_ENTRY"]),
        "pending_trigger_count": len([e for e in backtest_runtime["events"] if e["event_type"] == "PENDING_TRIGGER"]),
        "pending_cancel_count": len([e for e in backtest_runtime["events"] if e["event_type"] == "PENDING_CANCEL"]),
        "trend_entry_wait_count": len([e for e in backtest_runtime["events"] if e["event_type"] == "TREND_ENTRY_WAIT"]),
        "trend_entry_release_count": len([e for e in backtest_runtime["events"] if e["event_type"] == "TREND_ENTRY_RELEASE"]),
        "open_count": len([e for e in backtest_runtime["events"] if e["event_type"] == "OPEN"]),
        "close_count": len([e for e in backtest_runtime["events"] if e["event_type"] == "CLOSE"]),
        "assumptions": [
            "Signals and exit rules call bian_new.py directly.",
            "Indicator and support/resistance inputs use the same finite candle counts fetched by each live cycle.",
            f"Position management replays every {execution_timeframe} historical bar; live trading runs every successful main-loop iteration.",
            "Only public OHLCV is fetched; private order APIs are monkeypatched out.",
            "STOP_LIMIT pending entry is simulated with MARK_PRICE candles for trigger and last-price candles for limit fill.",
            f"If trigger and fill happen within the same {execution_timeframe} candle, intrabar order is approximated from OHLC range.",
            f"STOP_MARKET workingType=MARK_PRICE is simulated with {execution_timeframe} mark-price candles when available.",
            "No fixed take-profit limit order and no half take-profit are simulated.",
            "Trend-entry delay applies only to trend_trigger candidates; SR rebound and breakout candidates are unchanged.",
            "At delay expiry, current trend filters are revalidated. A still-crossed original trigger is rebuilt from the delayed candle; otherwise the original trigger remains pending.",
            "Every simulated STOP_LIMIT trigger is placed on the not-yet-triggered side of the observable close.",
        ],
        "outputs": {
            "trades": trades_path,
            "events": events_path,
            "stop_updates": stops_path,
            "equity": equity_path,
        },
    }
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"输出文件:\n- {trades_path}\n- {events_path}\n- {stops_path}\n- {equity_path}\n- {summary_path}")


if __name__ == "__main__":
    run_backtest(parse_args())
