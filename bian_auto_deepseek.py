"""
环境：
python version>=3.12
说明：
该脚本是 ETH/USDT U 本位合约自动交易脚本。
当前版本不再使用本地策略信号做开平仓判断，所有交易决策都交给 DeepSeek：
- 是否开仓
- 多空方向
- 是否平仓或反手
- 止损
- 止盈
- 最大利润回撤控制

本地代码只负责：
- 获取行情和账户信息
- 调用 AI
- 执行交易
- 强制约束：只交易 ETH/USDT、保证金占比最多 60%、杠杆最多 20x
- 服务端止损单和基础状态同步
"""

import ccxt
import concurrent.futures
import csv
import datetime
import json
import logging
import os
import smtplib
import time
import traceback

import pandas as pd
import pandas_ta as ta
import requests

from email.header import Header
from email.mime.text import MIMEText


BAR_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIMEFRAME_SECONDS = {
    "15m": 15 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
}
EXCHANGE_TZ = datetime.timezone(datetime.timedelta(hours=8))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_ENV_PATH = os.path.join(BASE_DIR, ".env.local")


def load_local_env(env_path=LOCAL_ENV_PATH):
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def require_env(name):
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"缺少环境变量: {name}")
    return value


def read_bool_env(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on", "y")


load_local_env()


API_KEY = require_env("BINANCE_API_KEY")
SECRET_KEY = require_env("BINANCE_SECRET_KEY")

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.qq.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
EMAIL_SENDER = os.getenv("EMAIL_SENDER", "").strip()
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "").strip()
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER", EMAIL_SENDER).strip()

SYMBOL = "ETH/USDT"
MAX_LEVERAGE = 20
DEFAULT_LEVERAGE = 20
MAX_MARGIN_RATE = 0.6
DEFAULT_MARGIN_RATE = 0.6

STOP_WORKING_TYPE = "MARK_PRICE"
POSITION_AMT_EPSILON = 1e-8
LIQUIDATION_SAFE_BUFFER_RATIO = 0.003
ESTIMATED_LIQUIDATION_GUARD_RATIO = 0.8
EXTERNAL_CLOSE_CONFIRM_MISS_COUNT = 3
STOP_ORDER_CANCEL_CONFIRM_RETRIES = 5
STOP_ORDER_CANCEL_CONFIRM_SLEEP_SECONDS = 0.2
STOP_ORDER_POST_CANCEL_DELAY_SECONDS = 0.15
STOP_ORDER_REFRESH_RETRY_DELAYS_SECONDS = (0.3, 0.8, 1.5)
EXCHANGE_HTTP_TIMEOUT_MS = 10000
FETCH_DF_TASK_TIMEOUT_SECONDS = 15
FETCH_DF_SLOW_LOG_SECONDS = 5
MAIN_LOOP_SLEEP_SECONDS = 1
MAIN_LOOP_ERROR_SLEEP_SECONDS = 5
HEARTBEAT_INTERVAL_SECONDS = 15 * 60

DEEPSEEK_AI_ENABLED = read_bool_env("DEEPSEEK_AI_ENABLED", True)
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "").strip()
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com").strip().rstrip("/")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-v4-pro").strip() or "deepseek-v4-pro"
DEEPSEEK_THINKING_ENABLED = read_bool_env("DEEPSEEK_THINKING_ENABLED", True)
DEEPSEEK_TIMEOUT_SECONDS = float(os.getenv("DEEPSEEK_TIMEOUT_SECONDS", "18"))
DEEPSEEK_MAX_TOKENS = int(os.getenv("DEEPSEEK_MAX_TOKENS", "900"))
DEEPSEEK_BAR_LIMIT = max(2, int(os.getenv("DEEPSEEK_BAR_LIMIT", "6")))


trade_state = {
    "has_position": False,
    "side": None,
    "entry_price": 0.0,
    "stop_loss_price": 0.0,
    "take_profit_price": 0.0,
    "max_drawdown_ratio": 0.0,
    "highest_price": 0.0,
    "lowest_price": 0.0,
    "amount": 0.0,
    "entry_time": "",
    "initial_balance": 0.0,
    "open_fee": 0.0,
    "open_order_id": "",
    "close_order_id": "",
    "entry_reason": "",
    "entry_trigger_tf": "",
    "selected_leverage": 0,
    "selected_margin_rate": 0.0,
    "ai_plan_summary": "",
    "last_ai_action": "",
    "liquidation_price": 0.0,
    "stop_order_id": "",
    "stop_order_price": 0.0,
    "entry_signal_bar_15m": "",
    "last_entry_bar_15m": "",
    "last_exit_bar_15m": "",
    "last_processed_bar_15m": "",
    "position_miss_count": 0,
    "close_cond_4h": "",
    "close_cond_1h": "",
    "close_cond_15m": "",
}

runtime_state = {
    "last_heartbeat_ts": 0.0,
    "deepseek_config_warned": False,
}


exchange = ccxt.binance({
    "apiKey": API_KEY,
    "secret": SECRET_KEY,
    "options": {"defaultType": "future"},
    "enableRateLimit": True,
    "timeout": EXCHANGE_HTTP_TIMEOUT_MS,
})
exchange.enable_demo_trading(True)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


TRADE_CSV_HEADERS = [
    "建仓时间", "方向", "4H摘要", "1H摘要", "15M摘要", "入场原因",
    "平仓时间", "平仓原因", "点数盈亏", "手续费", "净利润(USDT)", "是否盈利",
    "入场15M信号时间", "平仓15M信号时间", "平仓触发来源", "持仓秒数",
    "开仓订单ID", "平仓订单ID",
]


def send_msg(subject, content):
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        logging.warning("未配置邮件通知环境变量，已跳过邮件发送。")
        return
    try:
        message = MIMEText(content, "plain", "utf-8")
        message["From"] = EMAIL_SENDER
        message["To"] = EMAIL_RECEIVER
        message["Subject"] = Header(subject, "utf-8")
        smtp_obj = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT)
        smtp_obj.login(EMAIL_SENDER, EMAIL_PASSWORD)
        smtp_obj.sendmail(EMAIL_SENDER, [EMAIL_RECEIVER], message.as_string())
        smtp_obj.quit()
    except Exception as e:
        logging.error(f"邮件发送失败: {e}")


def extract_order_id(order):
    if not isinstance(order, dict):
        return ""
    info = order.get("info", {})
    candidates = [
        order.get("id"),
        order.get("orderId"),
        order.get("clientOrderId"),
    ]
    if isinstance(info, dict):
        candidates.extend([
            info.get("orderId"),
            info.get("id"),
            info.get("clientOrderId"),
        ])
    for candidate in candidates:
        if candidate not in (None, ""):
            return str(candidate)
    return ""


def format_order_id_lines(open_order_id="", close_order_id="", stop_order_id=""):
    lines = []
    if open_order_id:
        lines.append(f"开仓订单ID: {open_order_id}")
    if close_order_id:
        lines.append(f"平仓订单ID: {close_order_id}")
    if stop_order_id:
        lines.append(f"止损订单ID: {stop_order_id}")
    return "\n".join(lines)


def format_bar_time(ts):
    if ts is None or pd.isna(ts):
        return ""
    if isinstance(ts, pd.Timestamp):
        return ts.to_pydatetime().strftime(BAR_TIME_FORMAT)
    if isinstance(ts, datetime.datetime):
        return ts.strftime(BAR_TIME_FORMAT)
    return str(ts)


def normalize_mail_value(value):
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def format_mail_scalar(value):
    value = normalize_mail_value(value)
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if pd.isna(value):
            return "NaN"
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return str(value)


def safe_float(value, default=None):
    try:
        if value in (None, ""):
            return default
        result = float(value)
        if pd.isna(result):
            return default
        return result
    except Exception:
        return default


def safe_int(value, default=None):
    parsed = safe_float(value, None)
    if parsed is None:
        return default
    try:
        return int(round(parsed))
    except Exception:
        return default


def to_plain_value(value):
    if isinstance(value, dict):
        return {str(k): to_plain_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_plain_value(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.strftime(BAR_TIME_FORMAT)
    if isinstance(value, datetime.datetime):
        return value.strftime(BAR_TIME_FORMAT)
    if isinstance(value, datetime.date):
        return value.isoformat()
    if value is None:
        return None
    if isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return None if pd.isna(value) else value
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def get_server_time_str():
    server_time_ms = exchange.fetch_time()
    return datetime.datetime.fromtimestamp(
        server_time_ms / 1000.0,
        tz=EXCHANGE_TZ,
    ).strftime(BAR_TIME_FORMAT)


def get_server_now_dt():
    server_time_ms = exchange.fetch_time()
    return datetime.datetime.fromtimestamp(server_time_ms / 1000.0, tz=EXCHANGE_TZ)


def maybe_log_heartbeat():
    now_ts = time.monotonic()
    if now_ts - runtime_state.get("last_heartbeat_ts", 0.0) < HEARTBEAT_INTERVAL_SECONDS:
        return
    runtime_state["last_heartbeat_ts"] = now_ts
    logging.info(
        "心跳: has_position=%s, side=%s, last_processed_15m=%s",
        trade_state.get("has_position"),
        trade_state.get("side"),
        trade_state.get("last_processed_bar_15m", ""),
    )


def fetch_df(symbol, timeframe, limit=100):
    start_ts = time.monotonic()
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["timestamp"] = df["timestamp"].dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai")
        df["ema20"] = ta.ema(df["close"], length=20)
        df["ema50"] = ta.ema(df["close"], length=50)
        boll = ta.bbands(df["close"], length=20, std=2)
        df = pd.concat([df, boll], axis=1)
        df.rename(columns={
            "BBL_20_2.0_2.0": "boll_dn",
            "BBM_20_2.0_2.0": "boll_mid",
            "BBU_20_2.0_2.0": "boll_up",
        }, inplace=True)
        df["rsi"] = ta.rsi(df["close"], length=14)
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=14)
        macd = ta.macd(df["close"])
        df = pd.concat([df, macd], axis=1)
        df.rename(columns={
            "MACD_12_26_9": "macd",
            "MACDs_12_26_9": "macd_signal",
            "MACDh_12_26_9": "macd_hist",
        }, inplace=True)
        elapsed = time.monotonic() - start_ts
        if elapsed >= FETCH_DF_SLOW_LOG_SECONDS:
            logging.warning(f"获取数据较慢({timeframe}): {elapsed:.2f}s")
        return df
    except Exception as e:
        logging.error(f"获取数据失败({timeframe}): {e}")
        return None


def get_last_closed_index(df, timeframe, now_dt=None):
    if df is None or len(df) == 0:
        return None
    tf_seconds = TIMEFRAME_SECONDS.get(timeframe)
    if tf_seconds is None:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    if now_dt is None:
        now_dt = get_server_now_dt()
    elif now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=EXCHANGE_TZ)
    else:
        now_dt = now_dt.astimezone(EXCHANGE_TZ)
    ts_series = df["timestamp"]
    if getattr(ts_series.dt, "tz", None) is None:
        ts_series = ts_series.dt.tz_localize(EXCHANGE_TZ)
    else:
        ts_series = ts_series.dt.tz_convert(EXCHANGE_TZ)
    closed_mask = (ts_series + pd.to_timedelta(tf_seconds, unit="s")) <= now_dt
    closed_positions = closed_mask.to_numpy().nonzero()[0]
    if len(closed_positions) == 0:
        return None
    return int(closed_positions[-1])


def get_closed_bar_time(df, timeframe, now_dt=None):
    closed_idx = get_last_closed_index(df, timeframe, now_dt=now_dt)
    if closed_idx is None:
        return ""
    return format_bar_time(df.iloc[closed_idx]["timestamp"])


def get_latest_price():
    ticker = exchange.fetch_ticker(SYMBOL)
    return float(ticker["last"])


def get_futures_usdt_balance_snapshot():
    balance = exchange.fetch_balance({"type": "future"})
    total_usdt = float(balance.get("total", {}).get("USDT", 0) or 0)
    free_usdt = float(balance.get("free", {}).get("USDT", total_usdt) or total_usdt)
    used_usdt = float(balance.get("used", {}).get("USDT", max(total_usdt - free_usdt, 0)) or 0)
    return {
        "balance": balance,
        "total_usdt": total_usdt,
        "free_usdt": free_usdt,
        "used_usdt": used_usdt,
    }


def calculate_amount(price, margin_rate=None, leverage=None, balance_snapshot=None):
    try:
        leverage = int(max(1, min(MAX_LEVERAGE, leverage or DEFAULT_LEVERAGE)))
        margin_rate = max(0.0, min(MAX_MARGIN_RATE, float(margin_rate if margin_rate is not None else DEFAULT_MARGIN_RATE)))
        if balance_snapshot is None:
            balance_snapshot = get_futures_usdt_balance_snapshot()
        usdt_free = float(balance_snapshot.get("free_usdt", 0) or 0)
        if usdt_free <= 0 or price <= 0:
            return 0
        position_value = usdt_free * margin_rate * leverage
        amount = position_value / price
        return exchange.amount_to_precision(SYMBOL, amount)
    except Exception as e:
        logging.error(f"计算下单数量失败: {e}")
        return 0


def get_trading_fee_rate():
    try:
        fee_info = exchange.fetch_trading_fee(SYMBOL)
        return float(fee_info.get("taker", 0) or 0)
    except Exception as e:
        logging.error(f"获取 {SYMBOL} 手续费率失败: {e}，使用默认 0.04%")
        return 0.0004


def estimate_liquidation_price(entry_price, side):
    guard_ratio = ESTIMATED_LIQUIDATION_GUARD_RATIO / MAX_LEVERAGE
    if side == "long":
        return entry_price * (1 - guard_ratio)
    return entry_price * (1 + guard_ratio)


def normalize_liquidation_price(liquidation_price):
    try:
        liq = float(liquidation_price)
        return liq if liq > 0 else None
    except Exception:
        return None


def infer_position_side(position_amt, info=None, pos=None):
    info = info or {}
    pos = pos or {}
    raw_side = str(info.get("positionSide") or pos.get("side") or "").strip().upper()
    if raw_side in ("LONG", "SHORT"):
        return raw_side.lower()
    return "long" if float(position_amt) > 0 else "short"


def get_position_risk(side=None):
    try:
        positions = exchange.fetch_positions_risk([SYMBOL])
        for pos in positions:
            info = pos.get("info", {})
            position_amt = float(info.get("positionAmt", pos.get("contracts", 0)) or 0)
            if abs(position_amt) <= POSITION_AMT_EPSILON:
                continue
            pos_side = infer_position_side(position_amt, info=info, pos=pos)
            if side and pos_side != side:
                continue
            return {
                "side": pos_side,
                "position_amt": position_amt,
                "liquidation_price": normalize_liquidation_price(info.get("liquidationPrice", 0)),
                "mark_price": normalize_liquidation_price(info.get("markPrice", 0)),
                "info": info,
            }
    except Exception as e:
        logging.warning(f"获取仓位风险信息失败: {e}")
        return {"fetch_failed": True, "error": str(e)}
    return None


def has_open_position_on_exchange(side=None):
    try:
        positions = exchange.fetch_positions([SYMBOL])
        for pos in positions:
            info = pos.get("info", {})
            position_amt = float(info.get("positionAmt", pos.get("contracts", 0)) or 0)
            if abs(position_amt) <= POSITION_AMT_EPSILON:
                continue
            pos_side = infer_position_side(position_amt, info=info, pos=pos)
            if side and pos_side != side:
                continue
            return {"has_position": True, "fetch_failed": False, "side": pos_side, "position_amt": position_amt}
        return {"has_position": False, "fetch_failed": False}
    except Exception as e:
        logging.warning(f"二次确认仓位失败: {e}")
        return {"has_position": False, "fetch_failed": True, "error": str(e)}


def ensure_stop_price_safe(entry_price, stop_price, side, liquidation_price=None):
    if stop_price is None or pd.isna(stop_price):
        return stop_price, {"adjusted": False, "source": "none", "liquidation_price": None}
    liq = normalize_liquidation_price(liquidation_price)
    source = "actual"
    if liq is None:
        liq = estimate_liquidation_price(entry_price, side)
        source = "estimated"
    safe_buffer = entry_price * LIQUIDATION_SAFE_BUFFER_RATIO
    adjusted_stop = float(stop_price)
    if side == "long":
        adjusted_stop = max(adjusted_stop, liq + safe_buffer)
    else:
        adjusted_stop = min(adjusted_stop, liq - safe_buffer)
    return adjusted_stop, {
        "adjusted": abs(adjusted_stop - float(stop_price)) > 1e-12,
        "source": source,
        "liquidation_price": liq,
    }


def stop_price_is_still_valid(entry_price, stop_price, side):
    if stop_price is None or pd.isna(stop_price):
        return False
    return stop_price < entry_price if side == "long" else stop_price > entry_price


def place_protective_stop_order(side, stop_price):
    stop_side = "sell" if side == "long" else "buy"
    params = {
        "stopPrice": stop_price,
        "closePosition": True,
        "workingType": STOP_WORKING_TYPE,
    }
    return exchange.create_order(SYMBOL, "STOP_MARKET", stop_side, None, None, params)


def normalize_exchange_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ("true", "1", "yes")


def is_close_position_conditional_order(order, side=None):
    if not isinstance(order, dict):
        return False
    info = order.get("info", {})
    if not isinstance(info, dict):
        info = {}
    close_position = normalize_exchange_bool(info.get("closePosition", order.get("closePosition")))
    if not close_position:
        return False
    order_type = str(order.get("type") or info.get("type") or "").strip().upper()
    if order_type not in ("STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET"):
        return False
    if side:
        expected_side = "SELL" if side == "long" else "BUY"
        order_side = str(order.get("side") or info.get("side") or "").strip().upper()
        if order_side != expected_side:
            return False
    return True


def fetch_open_close_position_orders(side=None):
    try:
        open_orders = exchange.fetch_open_orders(SYMBOL)
    except Exception as e:
        logging.warning(f"查询未成交条件单失败: {e}")
        return None
    return [order for order in open_orders if is_close_position_conditional_order(order, side=side)]


def wait_until_stop_order_disappears(stop_order_id, side):
    if not stop_order_id:
        return True
    target_order_id = str(stop_order_id)
    for attempt in range(1, STOP_ORDER_CANCEL_CONFIRM_RETRIES + 1):
        matched_orders = fetch_open_close_position_orders(side=side)
        if matched_orders is None:
            time.sleep(STOP_ORDER_CANCEL_CONFIRM_SLEEP_SECONDS)
            continue
        if not any(extract_order_id(order) == target_order_id for order in matched_orders):
            return True
        logging.info(
            "等待旧服务端止损单从交易所消失: order_id=%s, attempt=%s/%s",
            target_order_id,
            attempt,
            STOP_ORDER_CANCEL_CONFIRM_RETRIES,
        )
        time.sleep(STOP_ORDER_CANCEL_CONFIRM_SLEEP_SECONDS)
    return False


def is_close_position_conflict_error(error):
    error_text = str(error)
    return 'code":-4130' in error_text or "closePosition in the direction is existing" in error_text


def cancel_protective_stop_order(silent=False):
    stop_order_id = trade_state.get("stop_order_id", "")
    if not stop_order_id:
        return True
    try:
        exchange.cancel_order(stop_order_id, SYMBOL)
        trade_state["stop_order_id"] = ""
        trade_state["stop_order_price"] = 0.0
        if not silent:
            logging.info(f"已撤销旧服务端止损单: {stop_order_id}")
        return True
    except Exception as e:
        if not silent:
            logging.warning(f"撤销服务端止损单失败({stop_order_id}): {e}")
        return False


def refresh_protective_stop_order(stop_price):
    if not trade_state.get("has_position") or not trade_state.get("side"):
        return True
    side = trade_state["side"]
    previous_stop_order_id = trade_state.get("stop_order_id", "")
    if previous_stop_order_id:
        if not cancel_protective_stop_order(silent=True):
            return False
        if not wait_until_stop_order_disappears(previous_stop_order_id, side=side):
            return False
        time.sleep(STOP_ORDER_POST_CANCEL_DELAY_SECONDS)
    retry_delays = (0.0,) + STOP_ORDER_REFRESH_RETRY_DELAYS_SECONDS
    for attempt, retry_delay in enumerate(retry_delays, start=1):
        if retry_delay > 0:
            time.sleep(retry_delay)
        try:
            stop_order = place_protective_stop_order(side, stop_price)
            trade_state["stop_order_id"] = extract_order_id(stop_order)
            trade_state["stop_order_price"] = float(stop_price)
            logging.info(
                "已更新服务端止损单: id=%s, stop=%s, attempt=%s",
                trade_state["stop_order_id"],
                stop_price,
                attempt,
            )
            return True
        except Exception as e:
            if is_close_position_conflict_error(e) and attempt < len(retry_delays):
                logging.warning("服务端止损单冲突，准备重试: %s", e)
                continue
            logging.error(f"重挂服务端止损单失败: {e}")
            return False
    return False


def compute_holding_seconds(entry_time, exit_time):
    try:
        entry_dt = datetime.datetime.strptime(entry_time, BAR_TIME_FORMAT)
        exit_dt = datetime.datetime.strptime(exit_time, BAR_TIME_FORMAT)
        return int((exit_dt - entry_dt).total_seconds())
    except Exception:
        return 0


def log_trade_to_csv(entry_time, side, cond_4h, cond_1h, cond_15m, entry_reason, exit_time, close_reason, pnl_points, fee_cost, net_pnl_usdt, is_profit, entry_signal_bar_15m="", exit_signal_bar_15m="", exit_trigger="", holding_seconds=0, open_order_id="", close_order_id=""):
    month_str = exit_time[:7]
    filename = os.path.join(BASE_DIR, f"trades_log_{month_str}.csv")
    file_exists = os.path.isfile(filename)
    with open(filename, mode="a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(TRADE_CSV_HEADERS)
        writer.writerow([
            entry_time, side, cond_4h, cond_1h, cond_15m, entry_reason,
            exit_time, close_reason, pnl_points, fee_cost, net_pnl_usdt, is_profit,
            entry_signal_bar_15m, exit_signal_bar_15m, exit_trigger, holding_seconds,
            open_order_id, close_order_id,
        ])


def get_recent_bar_snapshot(df, timeframe, count=DEEPSEEK_BAR_LIMIT, now_dt=None):
    if df is None or len(df) == 0:
        return []
    last_closed_idx = get_last_closed_index(df, timeframe, now_dt=now_dt)
    if last_closed_idx is None:
        return []
    start_idx = max(0, last_closed_idx - count + 1)
    columns = [
        "timestamp", "open", "high", "low", "close", "volume",
        "ema20", "ema50", "boll_up", "boll_mid", "boll_dn",
        "rsi", "atr", "macd", "macd_signal", "macd_hist",
    ]
    bars = []
    for _, row in df.iloc[start_idx:last_closed_idx + 1].iterrows():
        item = {}
        for column in columns:
            if column not in row.index:
                continue
            value = row[column]
            if column == "timestamp":
                item[column] = format_bar_time(value)
            elif value is None or pd.isna(value):
                item[column] = None
            else:
                item[column] = round(float(value), 6)
        bars.append(item)
    return bars


def summarize_timeframe_for_ai(df, timeframe, now_dt=None):
    if df is None or len(df) == 0:
        return {"timeframe": timeframe.upper(), "missing": True}
    last_closed_idx = get_last_closed_index(df, timeframe, now_dt=now_dt)
    if last_closed_idx is None:
        return {"timeframe": timeframe.upper(), "missing": True}
    last = df.iloc[last_closed_idx]
    prev = df.iloc[last_closed_idx - 1] if last_closed_idx > 0 else last
    close_price = safe_float(last.get("close"), 0.0) or 0.0
    prev_close = safe_float(prev.get("close"), close_price) or close_price
    volume = safe_float(last.get("volume"), 0.0) or 0.0
    prev_volume = safe_float(prev.get("volume"), volume) or volume
    pct_change = 0.0 if prev_close == 0 else ((close_price - prev_close) / prev_close) * 100.0
    volume_change = 0.0 if prev_volume == 0 else ((volume - prev_volume) / prev_volume) * 100.0
    trend_bias = "flat"
    ema20 = safe_float(last.get("ema20"))
    ema50 = safe_float(last.get("ema50"))
    if ema20 is not None and ema50 is not None:
        if close_price > ema20 >= ema50:
            trend_bias = "bullish"
        elif close_price < ema20 <= ema50:
            trend_bias = "bearish"
    return {
        "timeframe": timeframe.upper(),
        "signal_bar_time": format_bar_time(last.get("timestamp")),
        "close": round(close_price, 6),
        "pct_change": round(pct_change, 4),
        "volume_change_pct": round(volume_change, 4),
        "rsi": to_plain_value(last.get("rsi")),
        "atr": to_plain_value(last.get("atr")),
        "macd": to_plain_value(last.get("macd")),
        "macd_signal": to_plain_value(last.get("macd_signal")),
        "macd_hist": to_plain_value(last.get("macd_hist")),
        "ema20": to_plain_value(ema20),
        "ema50": to_plain_value(ema50),
        "boll_up": to_plain_value(last.get("boll_up")),
        "boll_mid": to_plain_value(last.get("boll_mid")),
        "boll_dn": to_plain_value(last.get("boll_dn")),
        "trend_bias": trend_bias,
        "mail_snapshot": (
            f"{timeframe.upper()} time={format_bar_time(last.get('timestamp'))}, "
            f"close={close_price:.4f}, change={pct_change:.2f}%, volume_change={volume_change:.2f}%, "
            f"rsi={format_mail_scalar(last.get('rsi'))}, atr={format_mail_scalar(last.get('atr'))}, trend_bias={trend_bias}"
        ),
    }


def format_market_summary_for_mail(summary):
    if not isinstance(summary, dict):
        return "无行情摘要"
    return str(summary.get("mail_snapshot") or "无行情摘要")


def get_position_snapshot_for_ai(curr_price=None):
    if not trade_state.get("has_position"):
        return {"has_position": False}
    entry_price = float(trade_state.get("entry_price", 0) or 0)
    side = trade_state.get("side")
    highest_price = float(trade_state.get("highest_price", entry_price) or entry_price)
    lowest_price = float(trade_state.get("lowest_price", entry_price) or entry_price)
    max_favorable_points = 0.0
    current_drawdown_ratio = 0.0
    unrealized_points = None
    if curr_price is not None:
        if side == "long":
            unrealized_points = round(curr_price - entry_price, 6)
            max_favorable_points = max(0.0, highest_price - entry_price)
            if max_favorable_points > 0:
                current_drawdown_ratio = max(0.0, highest_price - curr_price) / max_favorable_points
        else:
            unrealized_points = round(entry_price - curr_price, 6)
            max_favorable_points = max(0.0, entry_price - lowest_price)
            if max_favorable_points > 0:
                current_drawdown_ratio = max(0.0, curr_price - lowest_price) / max_favorable_points
    return {
        "has_position": True,
        "side": side,
        "entry_price": to_plain_value(entry_price),
        "stop_loss_price": to_plain_value(trade_state.get("stop_loss_price")),
        "take_profit_price": to_plain_value(trade_state.get("take_profit_price")),
        "max_drawdown_ratio": to_plain_value(trade_state.get("max_drawdown_ratio")),
        "amount": to_plain_value(trade_state.get("amount")),
        "entry_time": trade_state.get("entry_time", ""),
        "entry_reason": trade_state.get("entry_reason", ""),
        "entry_trigger_tf": trade_state.get("entry_trigger_tf", ""),
        "selected_leverage": to_plain_value(trade_state.get("selected_leverage")),
        "selected_margin_rate": to_plain_value(trade_state.get("selected_margin_rate")),
        "ai_plan_summary": trade_state.get("ai_plan_summary", ""),
        "liquidation_price": to_plain_value(trade_state.get("liquidation_price")),
        "highest_price": to_plain_value(highest_price),
        "lowest_price": to_plain_value(lowest_price),
        "current_price": to_plain_value(curr_price),
        "unrealized_points": unrealized_points,
        "max_favorable_points": round(max_favorable_points, 6),
        "current_drawdown_ratio": round(current_drawdown_ratio, 6),
    }


def build_market_snapshot_for_ai(df_4h, df_1h, df_15m, signal_bar_15m, server_now_dt=None):
    if server_now_dt is None:
        server_now_dt = get_server_now_dt()
    return {
        "symbol": SYMBOL,
        "signal_bar_15m": signal_bar_15m,
        "server_time": format_bar_time(server_now_dt),
        "timeframes": {
            "4h": {
                "summary": summarize_timeframe_for_ai(df_4h, "4h", now_dt=server_now_dt),
                "bars": get_recent_bar_snapshot(df_4h, "4h", now_dt=server_now_dt),
            },
            "1h": {
                "summary": summarize_timeframe_for_ai(df_1h, "1h", now_dt=server_now_dt),
                "bars": get_recent_bar_snapshot(df_1h, "1h", now_dt=server_now_dt),
            },
            "15m": {
                "summary": summarize_timeframe_for_ai(df_15m, "15m", now_dt=server_now_dt),
                "bars": get_recent_bar_snapshot(df_15m, "15m", now_dt=server_now_dt),
            },
        },
    }


def get_account_snapshot_for_ai():
    balance_snapshot = get_futures_usdt_balance_snapshot()
    return {
        "total_usdt": round(balance_snapshot.get("total_usdt", 0.0), 6),
        "free_usdt": round(balance_snapshot.get("free_usdt", 0.0), 6),
        "used_usdt": round(balance_snapshot.get("used_usdt", 0.0), 6),
        "max_new_position_margin_rate": MAX_MARGIN_RATE,
        "max_leverage": MAX_LEVERAGE,
        "symbol": SYMBOL,
        "market_type": "USDT_PERPETUAL",
    }


def log_deepseek_config_warning_once(message):
    if runtime_state.get("deepseek_config_warned"):
        return
    runtime_state["deepseek_config_warned"] = True
    logging.warning(message)


def call_deepseek_json(system_prompt, user_payload):
    if not DEEPSEEK_AI_ENABLED:
        return {"action": "hold", "confidence": 0.0, "reason": "DeepSeek AI 已关闭"}
    if not DEEPSEEK_API_KEY:
        raise RuntimeError("缺少环境变量: DEEPSEEK_API_KEY")
    url = f"{DEEPSEEK_BASE_URL}/chat/completions"
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
        "response_format": {"type": "json_object"},
        "max_tokens": DEEPSEEK_MAX_TOKENS,
    }
    if DEEPSEEK_THINKING_ENABLED:
        payload["thinking"] = {"type": "enabled"}
    else:
        payload["temperature"] = 0.1
    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=DEEPSEEK_TIMEOUT_SECONDS,
    )
    try:
        response.raise_for_status()
    except Exception as exc:
        raise RuntimeError(f"DeepSeek HTTP错误: status={response.status_code}, body={response.text[:300]}") from exc
    body = response.json()
    choices = body.get("choices") or []
    if not choices:
        raise RuntimeError(f"DeepSeek响应缺少choices: {body}")
    message = choices[0].get("message") or {}
    content = (message.get("content") or "").strip()
    if not content:
        raise RuntimeError(f"DeepSeek响应内容为空: {body}")
    if content.startswith("```"):
        content = content.strip("`")
        if content.startswith("json"):
            content = content[4:].strip()
    result = json.loads(content)
    if not isinstance(result, dict):
        raise RuntimeError(f"DeepSeek返回的JSON不是对象: {result}")
    return result


def format_ai_review_text(ai_review):
    if not isinstance(ai_review, dict):
        return ""
    decision = ai_review.get("action") or ai_review.get("decision", "")
    confidence = ai_review.get("confidence")
    reason = ai_review.get("reason") or ai_review.get("close_reason") or ai_review.get("entry_reason", "")
    risk_flags = ai_review.get("risk_flags") or []
    confidence_text = f", confidence={confidence:.2f}" if isinstance(confidence, (int, float)) else ""
    risk_text = f", risk_flags={';'.join(str(x) for x in risk_flags[:3])}" if risk_flags else ""
    return f"AI decision={decision}{confidence_text}, reason={reason}{risk_text}"


def build_trade_constraints_for_ai():
    return {
        "symbol": SYMBOL,
        "market_type": "USDT_PERPETUAL",
        "max_leverage": MAX_LEVERAGE,
        "max_new_position_margin_rate": MAX_MARGIN_RATE,
        "allow_sides": ["long", "short"],
        "allowed_actions_when_flat": ["hold", "open_long", "open_short"],
        "allowed_actions_when_in_position": ["hold", "close", "update_risk", "reverse_to_long", "reverse_to_short"],
    }


def normalize_ai_trade_decision(raw_decision, has_position):
    normalized = {
        "action": "hold",
        "reason": "",
        "entry_reason": "",
        "close_reason": "",
        "confidence": 0.0,
        "risk_flags": [],
        "stop_loss": None,
        "take_profit": None,
        "max_drawdown_ratio": None,
        "leverage": DEFAULT_LEVERAGE,
        "margin_ratio": DEFAULT_MARGIN_RATE,
        "notes": "",
    }
    if not isinstance(raw_decision, dict):
        normalized["reason"] = "AI 返回不是 JSON 对象"
        normalized["risk_flags"].append("invalid_json")
        return normalized
    allowed_actions = (
        build_trade_constraints_for_ai()["allowed_actions_when_in_position"]
        if has_position else
        build_trade_constraints_for_ai()["allowed_actions_when_flat"]
    )
    action = str(raw_decision.get("action", "hold")).strip().lower()
    if action not in allowed_actions:
        normalized["risk_flags"].append("invalid_action")
        action = "hold"
    normalized["action"] = action
    normalized["reason"] = str(raw_decision.get("reason", "")).strip()
    normalized["entry_reason"] = str(raw_decision.get("entry_reason", "")).strip()
    normalized["close_reason"] = str(raw_decision.get("close_reason", "")).strip()
    normalized["confidence"] = max(0.0, min(1.0, safe_float(raw_decision.get("confidence"), 0.0) or 0.0))
    normalized["risk_flags"].extend([str(item) for item in (raw_decision.get("risk_flags") or [])][:5])
    normalized["stop_loss"] = safe_float(raw_decision.get("stop_loss"))
    normalized["take_profit"] = safe_float(raw_decision.get("take_profit"))
    drawdown_ratio = safe_float(raw_decision.get("max_drawdown_ratio"))
    normalized["max_drawdown_ratio"] = None if drawdown_ratio is None else max(0.0, min(1.0, drawdown_ratio))
    normalized["leverage"] = max(1, min(MAX_LEVERAGE, safe_int(raw_decision.get("leverage"), DEFAULT_LEVERAGE) or DEFAULT_LEVERAGE))
    normalized["margin_ratio"] = max(0.0, min(MAX_MARGIN_RATE, safe_float(raw_decision.get("margin_ratio"), DEFAULT_MARGIN_RATE) or DEFAULT_MARGIN_RATE))
    normalized["notes"] = str(raw_decision.get("notes", "")).strip()
    return normalized


def validate_ai_price_plan(side, current_price, stop_loss=None, take_profit=None):
    errors = []
    if stop_loss is not None:
        if side == "long" and stop_loss >= current_price:
            errors.append("long_stop_not_below_price")
        if side == "short" and stop_loss <= current_price:
            errors.append("short_stop_not_above_price")
    if take_profit is not None:
        if side == "long" and take_profit <= current_price:
            errors.append("long_take_profit_not_above_price")
        if side == "short" and take_profit >= current_price:
            errors.append("short_take_profit_not_below_price")
    return errors


def request_trade_decision_with_ai(curr_price, market_snapshot, signal_bar_15m):
    context = {
        "task": "full_trade_control",
        "signal_bar_15m": signal_bar_15m,
        "current_price": curr_price,
        "market": market_snapshot,
        "account": get_account_snapshot_for_ai(),
        "position": get_position_snapshot_for_ai(curr_price),
        "constraints": build_trade_constraints_for_ai(),
    }
    system_prompt = (
        "你是 ETH/USDT U 本位永续合约自动交易系统的唯一决策核心。"
        "你必须负责策略判断、开仓、平仓、反手、止损、止盈和利润回撤控制。"
        "禁止交易任何非 ETH/USDT 品种。新开仓的 margin_ratio 不得超过 0.6，leverage 不得超过 20。"
        "无仓位时只允许返回 hold/open_long/open_short。"
        "有仓位时只允许返回 hold/close/update_risk/reverse_to_long/reverse_to_short。"
        "所有开仓和反手动作必须给出 stop_loss，可选给出 take_profit 和 max_drawdown_ratio。"
        "不确定时返回 hold。"
        "必须只返回 JSON 对象，字段限定为 action, reason, confidence, entry_reason, close_reason, "
        "stop_loss, take_profit, max_drawdown_ratio, leverage, margin_ratio, risk_flags, notes。"
    )
    result = call_deepseek_json(system_prompt, context)
    return normalize_ai_trade_decision(result, has_position=trade_state.get("has_position", False))


def guarded_ai_trade_decision(curr_price, market_snapshot, signal_bar_15m):
    try:
        decision = request_trade_decision_with_ai(curr_price, market_snapshot, signal_bar_15m)
        logging.info("AI全权决策: %s", format_ai_review_text(decision))
        return decision
    except Exception as e:
        log_deepseek_config_warning_once(f"DeepSeek 全权决策不可用: {e}")
        fallback = {
            "action": "hold",
            "reason": f"AI决策异常: {e}",
            "confidence": 0.0,
            "entry_reason": "",
            "close_reason": "",
            "stop_loss": None,
            "take_profit": None,
            "max_drawdown_ratio": None,
            "leverage": DEFAULT_LEVERAGE,
            "margin_ratio": DEFAULT_MARGIN_RATE,
            "risk_flags": ["ai_error"],
        }
        logging.warning("AI全权决策失败，fallback=%s", format_ai_review_text(fallback))
        return fallback


def reset_trade_state_after_external_close(signal_bar_15m="", reason="检测到交易所仓位已关闭", external_context=None):
    external_context = external_context or {}
    exit_signal_bar_15m = signal_bar_15m or trade_state.get("last_processed_bar_15m", "")
    detected_time = get_server_time_str()
    stop_order_id_for_notify = external_context.get("stop_order_id_before_cancel") or trade_state.get("stop_order_id", "")
    stop_order_price_for_notify = external_context.get("stop_order_price_before_cancel", trade_state.get("stop_order_price", 0.0))
    estimated_close_price = safe_float(stop_order_price_for_notify)
    if estimated_close_price is None:
        estimated_close_price = safe_float(get_latest_price(), 0.0) or 0.0
    entry_price = float(trade_state.get("entry_price", 0.0) or 0.0)
    amount = float(trade_state.get("amount", 0.0) or 0.0)
    pnl_points = 0.0
    if trade_state.get("side") == "long":
        pnl_points = estimated_close_price - entry_price
    elif trade_state.get("side") == "short":
        pnl_points = entry_price - estimated_close_price
    fee_cost = float(trade_state.get("open_fee", 0.0) or 0.0)
    try:
        fee_cost += estimated_close_price * amount * get_trading_fee_rate()
    except Exception:
        pass
    final_usdt = None
    net_pnl_usdt = 0.0
    try:
        balance_after = get_futures_usdt_balance_snapshot()
        final_usdt = float(balance_after["total_usdt"])
        net_pnl_usdt = final_usdt - float(trade_state.get("initial_balance", 0.0) or 0.0)
    except Exception:
        pass
    if trade_state.get("entry_time"):
        log_trade_to_csv(
            trade_state.get("entry_time", ""),
            trade_state.get("side", ""),
            trade_state.get("close_cond_4h", ""),
            trade_state.get("close_cond_1h", ""),
            trade_state.get("close_cond_15m", ""),
            trade_state.get("entry_reason", ""),
            detected_time,
            f"{reason}（外部平仓检测）",
            round(pnl_points, 4),
            round(fee_cost, 4),
            round(net_pnl_usdt, 4),
            net_pnl_usdt > 0,
            entry_signal_bar_15m=trade_state.get("entry_signal_bar_15m", ""),
            exit_signal_bar_15m=exit_signal_bar_15m,
            exit_trigger="external_close_detected",
            holding_seconds=compute_holding_seconds(trade_state.get("entry_time", ""), detected_time),
            open_order_id=trade_state.get("open_order_id", ""),
            close_order_id=str(external_context.get("external_close_order_id", "")),
        )
    order_id_suffix = format_order_id_lines(
        open_order_id=trade_state.get("open_order_id", ""),
        stop_order_id=stop_order_id_for_notify,
    )
    send_msg(
        "ETH交易: ⚠️检测到外部平仓",
        f"原因: {reason}\n"
        f"检测时间: {detected_time}\n"
        f"方向: {trade_state.get('side')}\n"
        f"入场价: {trade_state.get('entry_price', 0)}\n"
        f"估算出场价: {estimated_close_price:.4f}\n"
        f"点数盈亏: {pnl_points:.2f}\n"
        f"净利润(USDT): {net_pnl_usdt:.2f}\n"
        f"服务端止损价: {stop_order_price_for_notify}\n"
        f"{order_id_suffix}"
    )
    trade_state.update({
        "has_position": False,
        "side": None,
        "entry_price": 0.0,
        "stop_loss_price": 0.0,
        "take_profit_price": 0.0,
        "max_drawdown_ratio": 0.0,
        "highest_price": 0.0,
        "lowest_price": 0.0,
        "amount": 0.0,
        "entry_time": "",
        "initial_balance": 0.0,
        "open_fee": 0.0,
        "open_order_id": "",
        "close_order_id": "",
        "entry_reason": "",
        "entry_trigger_tf": "",
        "selected_leverage": 0,
        "selected_margin_rate": 0.0,
        "ai_plan_summary": "",
        "last_ai_action": "",
        "liquidation_price": 0.0,
        "stop_order_id": "",
        "stop_order_price": 0.0,
        "entry_signal_bar_15m": "",
        "last_exit_bar_15m": exit_signal_bar_15m,
        "last_processed_bar_15m": exit_signal_bar_15m or trade_state.get("last_processed_bar_15m", ""),
        "position_miss_count": 0,
        "close_cond_4h": "",
        "close_cond_1h": "",
        "close_cond_15m": "",
    })


def open_order(side, price, summary_4h, summary_1h, summary_15m, signal_bar_15m, entry_reason, ai_review):
    global trade_state
    leverage = int(max(1, min(MAX_LEVERAGE, ai_review.get("leverage") or DEFAULT_LEVERAGE)))
    margin_rate = max(0.0, min(MAX_MARGIN_RATE, float(ai_review.get("margin_ratio") if ai_review.get("margin_ratio") is not None else DEFAULT_MARGIN_RATE)))
    stop_loss = safe_float(ai_review.get("stop_loss"))
    take_profit = safe_float(ai_review.get("take_profit"))
    max_drawdown_ratio = safe_float(ai_review.get("max_drawdown_ratio"))
    if take_profit is not None and take_profit <= 0:
        take_profit = None
    if max_drawdown_ratio is not None:
        max_drawdown_ratio = max(0.0, min(1.0, max_drawdown_ratio))
    if stop_loss is None:
        logging.warning("AI 请求开仓但未提供止损。")
        return False
    if validate_ai_price_plan(side, price, stop_loss=stop_loss, take_profit=take_profit):
        logging.warning("AI 开仓风控参数无效: %s", format_ai_review_text(ai_review))
        return False
    balance_snapshot = get_futures_usdt_balance_snapshot()
    amount = float(calculate_amount(price, margin_rate=margin_rate, leverage=leverage, balance_snapshot=balance_snapshot) or 0)
    if amount <= 0:
        return False
    estimated_safe_stop, estimated_stop_meta = ensure_stop_price_safe(price, stop_loss, side, liquidation_price=None)
    if not stop_price_is_still_valid(price, estimated_safe_stop, side):
        logging.warning(f"拒绝开仓：止损无效，meta={estimated_stop_meta}")
        return False
    open_order_id = ""
    try:
        initial_usdt = float(balance_snapshot.get("total_usdt", 0) or 0)
        exchange.set_leverage(leverage, SYMBOL)
        order_side = "buy" if side == "long" else "sell"
        order = exchange.create_market_order(SYMBOL, order_side, amount)
        open_order_id = extract_order_id(order)
        actual_price = safe_float(order.get("average"), price) or price
        position_risk = get_position_risk(side=side)
        actual_liquidation_price = None if not position_risk or position_risk.get("fetch_failed") else position_risk.get("liquidation_price")
        actual_safe_stop, _ = ensure_stop_price_safe(actual_price, estimated_safe_stop, side, liquidation_price=actual_liquidation_price)
        if not stop_price_is_still_valid(actual_price, actual_safe_stop, side):
            panic_side = "sell" if side == "long" else "buy"
            panic_close = exchange.create_market_order(SYMBOL, panic_side, amount)
            send_msg("ETH交易: ⚠️开仓后立即撤退", f"真实强平价过近\n{format_order_id_lines(open_order_id, extract_order_id(panic_close))}")
            return False
        open_fee = actual_price * amount * get_trading_fee_rate()
        cond_4h_str = f"原因:{entry_reason} | {format_market_summary_for_mail(summary_4h)} | {format_ai_review_text(ai_review)}"
        cond_1h_str = f"原因:{entry_reason} | {format_market_summary_for_mail(summary_1h)} | {format_ai_review_text(ai_review)}"
        cond_15m_str = f"原因:{entry_reason} | {format_market_summary_for_mail(summary_15m)} | {format_ai_review_text(ai_review)}"
        trade_state.update({
            "has_position": True,
            "side": side,
            "entry_price": actual_price,
            "stop_loss_price": actual_safe_stop,
            "take_profit_price": take_profit or 0.0,
            "max_drawdown_ratio": max_drawdown_ratio or 0.0,
            "highest_price": actual_price,
            "lowest_price": actual_price,
            "amount": amount,
            "entry_time": get_server_time_str(),
            "initial_balance": initial_usdt,
            "open_fee": open_fee,
            "open_order_id": open_order_id,
            "close_order_id": "",
            "entry_reason": entry_reason,
            "entry_trigger_tf": "AI",
            "selected_leverage": leverage,
            "selected_margin_rate": margin_rate,
            "ai_plan_summary": format_ai_review_text(ai_review),
            "last_ai_action": f"open_{side}",
            "liquidation_price": actual_liquidation_price or 0.0,
            "stop_order_id": "",
            "stop_order_price": 0.0,
            "entry_signal_bar_15m": signal_bar_15m,
            "last_entry_bar_15m": signal_bar_15m,
            "position_miss_count": 0,
            "close_cond_4h": "",
            "close_cond_1h": "",
            "close_cond_15m": "",
            "cond_4h": cond_4h_str,
            "cond_1h": cond_1h_str,
            "cond_15m": cond_15m_str,
        })
        if not refresh_protective_stop_order(actual_safe_stop):
            close_position("服务端止损挂单失败，主动平仓", curr_price=actual_price, signal_bar_15m=signal_bar_15m, trigger_label="服务端止损挂单失败")
            return False
        send_msg(
            f"ETH交易: 开仓 {side}",
            f"方向: {side}\n"
            f"入场价: {actual_price}\n"
            f"止损价: {actual_safe_stop}\n"
            f"止盈价: {take_profit if take_profit is not None else '未设置'}\n"
            f"最大回撤比例: {f'{max_drawdown_ratio:.2%}' if max_drawdown_ratio is not None else '未设置'}\n"
            f"杠杆: {leverage}x\n"
            f"保证金占比: {margin_rate:.0%}\n"
            f"AI: {format_ai_review_text(ai_review)}\n"
            f"{format_order_id_lines(open_order_id=open_order_id, stop_order_id=trade_state.get('stop_order_id', ''))}"
        )
        logging.info("开仓成功: %s", format_ai_review_text(ai_review))
        return True
    except Exception as e:
        logging.error(f"开仓失败: {e}")
        logging.error(traceback.format_exc())
        send_msg("ETH交易: ⚠️开仓失败警告", f"开仓失败: {e}\n{format_order_id_lines(open_order_id=open_order_id)}")
        return False


def close_position(reason, curr_price=None, signal_bar_15m="", trigger_label="", ai_review=None):
    global trade_state
    side = "sell" if trade_state["side"] == "long" else "buy"
    close_order_id = ""
    try:
        if curr_price is None:
            curr_price = get_latest_price()
        cancel_protective_stop_order(silent=True)
        order = exchange.create_market_order(SYMBOL, side, trade_state["amount"])
        close_order_id = extract_order_id(order)
        trade_state["close_order_id"] = close_order_id
        time.sleep(1)
        final_usdt = float(get_futures_usdt_balance_snapshot()["total_usdt"])
        net_pnl_usdt = final_usdt - trade_state["initial_balance"]
        actual_close_price = safe_float(order.get("average"), curr_price) or curr_price
        close_fee = actual_close_price * trade_state["amount"] * get_trading_fee_rate()
        fee_cost = trade_state["open_fee"] + close_fee
        pnl_points = (
            actual_close_price - trade_state["entry_price"]
            if trade_state["side"] == "long"
            else trade_state["entry_price"] - actual_close_price
        )
        exit_time = get_server_time_str()
        holding_seconds = compute_holding_seconds(trade_state["entry_time"], exit_time)
        exit_signal_bar_15m = signal_bar_15m
        entry_signal_bar_15m = trade_state.get("entry_signal_bar_15m", "")
        log_trade_to_csv(
            trade_state["entry_time"],
            trade_state["side"],
            trade_state.get("cond_4h", ""),
            trade_state.get("cond_1h", ""),
            trade_state.get("cond_15m", ""),
            trade_state.get("entry_reason", ""),
            exit_time,
            reason,
            round(pnl_points, 4),
            round(fee_cost, 4),
            round(net_pnl_usdt, 4),
            net_pnl_usdt > 0,
            entry_signal_bar_15m=entry_signal_bar_15m,
            exit_signal_bar_15m=exit_signal_bar_15m,
            exit_trigger=trigger_label or reason,
            holding_seconds=holding_seconds,
            open_order_id=trade_state.get("open_order_id", ""),
            close_order_id=close_order_id,
        )
        send_msg(
            "ETH交易: 平仓通知",
            f"原因: {reason}\n"
            f"入场价: {trade_state['entry_price']}\n"
            f"出场价: {actual_close_price}\n"
            f"点数盈亏: {pnl_points:.2f}\n"
            f"净利润(USDT): {net_pnl_usdt:.2f}\n"
            f"AI: {format_ai_review_text(ai_review)}\n"
            f"{format_order_id_lines(trade_state.get('open_order_id', ''), close_order_id)}"
        )
        trade_state.update({
            "has_position": False,
            "side": None,
            "entry_price": 0.0,
            "stop_loss_price": 0.0,
            "take_profit_price": 0.0,
            "max_drawdown_ratio": 0.0,
            "highest_price": 0.0,
            "lowest_price": 0.0,
            "amount": 0.0,
            "entry_time": "",
            "initial_balance": 0.0,
            "open_fee": 0.0,
            "open_order_id": "",
            "close_order_id": "",
            "entry_reason": "",
            "entry_trigger_tf": "",
            "selected_leverage": 0,
            "selected_margin_rate": 0.0,
            "ai_plan_summary": "",
            "last_ai_action": "",
            "liquidation_price": 0.0,
            "stop_order_id": "",
            "stop_order_price": 0.0,
            "entry_signal_bar_15m": "",
            "last_exit_bar_15m": exit_signal_bar_15m,
            "last_processed_bar_15m": exit_signal_bar_15m or trade_state.get("last_processed_bar_15m", ""),
            "position_miss_count": 0,
            "close_cond_4h": "",
            "close_cond_1h": "",
            "close_cond_15m": "",
            "cond_4h": "",
            "cond_1h": "",
            "cond_15m": "",
        })
        logging.info("平仓成功: %s", reason)
        return True
    except Exception as e:
        logging.error(f"清仓失败: {e}")
        return False


def sync_trade_state_with_exchange(signal_bar_15m=""):
    if not trade_state.get("has_position"):
        return False
    position_risk = get_position_risk(side=trade_state["side"])
    if position_risk and position_risk.get("fetch_failed"):
        logging.warning("本轮无法获取交易所仓位风险信息，先保留本地仓位状态")
        return True
    if position_risk is None:
        trade_state["position_miss_count"] = int(trade_state.get("position_miss_count", 0) or 0) + 1
        if trade_state["position_miss_count"] < EXTERNAL_CLOSE_CONFIRM_MISS_COUNT:
            return True
        fallback_check = has_open_position_on_exchange(side=trade_state["side"])
        if fallback_check.get("fetch_failed"):
            return True
        if fallback_check.get("has_position"):
            trade_state["position_miss_count"] = 0
            return True
        external_close_context = {
            "stop_order_id_before_cancel": trade_state.get("stop_order_id", ""),
            "stop_order_price_before_cancel": trade_state.get("stop_order_price", 0.0),
        }
        cancel_protective_stop_order(silent=True)
        reset_trade_state_after_external_close(
            signal_bar_15m=signal_bar_15m,
            reason="检测到交易所实际已无仓位，本地状态已重置",
            external_context=external_close_context,
        )
        return False
    trade_state["position_miss_count"] = 0
    trade_state["liquidation_price"] = position_risk.get("liquidation_price") or 0.0
    return True


def apply_ai_position_updates(side, curr_price, decision, signal_bar_15m=""):
    if not trade_state.get("has_position"):
        return True
    trade_state["ai_plan_summary"] = format_ai_review_text(decision) or trade_state.get("ai_plan_summary", "")
    trade_state["last_ai_action"] = decision.get("action", "hold")
    stop_loss = safe_float(decision.get("stop_loss"))
    if stop_loss is not None:
        entry_price = float(trade_state.get("entry_price", 0) or 0)
        safe_stop, _ = ensure_stop_price_safe(entry_price, stop_loss, side, liquidation_price=trade_state.get("liquidation_price"))
        if not validate_ai_price_plan(side, curr_price, stop_loss=safe_stop):
            if abs(float(trade_state.get("stop_loss_price", 0) or 0) - safe_stop) > 1e-9:
                if refresh_protective_stop_order(safe_stop):
                    trade_state["stop_loss_price"] = safe_stop
                    trade_state["stop_order_price"] = safe_stop
                    logging.info("AI更新止损成功: side=%s, stop=%.4f", side, safe_stop)
    take_profit = safe_float(decision.get("take_profit"))
    if take_profit is not None:
        if take_profit <= 0:
            trade_state["take_profit_price"] = 0.0
        elif not validate_ai_price_plan(side, curr_price, take_profit=take_profit):
            trade_state["take_profit_price"] = take_profit
    drawdown_ratio = safe_float(decision.get("max_drawdown_ratio"))
    if drawdown_ratio is not None:
        trade_state["max_drawdown_ratio"] = max(0.0, min(1.0, drawdown_ratio))
    return True


def evaluate_ai_risk_guards(curr_price, signal_bar_15m=""):
    if not trade_state.get("has_position"):
        return False
    side = trade_state.get("side")
    if side == "long":
        trade_state["highest_price"] = max(float(trade_state.get("highest_price", curr_price) or curr_price), curr_price)
    else:
        trade_state["lowest_price"] = min(float(trade_state.get("lowest_price", curr_price) or curr_price), curr_price)
    stop_loss_price = safe_float(trade_state.get("stop_loss_price"))
    if stop_loss_price is not None:
        if side == "long" and curr_price <= stop_loss_price:
            close_position("AI止损触发", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="AI止损")
            return True
        if side == "short" and curr_price >= stop_loss_price:
            close_position("AI止损触发", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="AI止损")
            return True
    take_profit_price = safe_float(trade_state.get("take_profit_price"))
    if take_profit_price is not None and take_profit_price > 0:
        if side == "long" and curr_price >= take_profit_price:
            close_position("AI止盈触发", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="AI止盈")
            return True
        if side == "short" and curr_price <= take_profit_price:
            close_position("AI止盈触发", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="AI止盈")
            return True
    max_drawdown_ratio = safe_float(trade_state.get("max_drawdown_ratio"))
    entry_price = float(trade_state.get("entry_price", 0) or 0)
    if max_drawdown_ratio is not None and max_drawdown_ratio > 0 and entry_price > 0:
        if side == "long":
            max_profit = max(0.0, float(trade_state.get("highest_price", entry_price) or entry_price) - entry_price)
            retrace_ratio = 0.0 if max_profit <= 0 else max(0.0, float(trade_state.get("highest_price", entry_price) or entry_price) - curr_price) / max_profit
        else:
            max_profit = max(0.0, entry_price - float(trade_state.get("lowest_price", entry_price) or entry_price))
            retrace_ratio = 0.0 if max_profit <= 0 else max(0.0, curr_price - float(trade_state.get("lowest_price", entry_price) or entry_price)) / max_profit
        if max_profit > 0 and retrace_ratio >= max_drawdown_ratio:
            close_position(
                f"AI回撤控制触发(阈值:{max_drawdown_ratio:.0%}, 当前:{retrace_ratio:.0%})",
                curr_price,
                signal_bar_15m=signal_bar_15m,
                trigger_label="AI回撤控制",
            )
            return True
    return False


def execute_ai_trade_decision(decision, curr_price, summary_4h, summary_1h, summary_15m, signal_bar_15m):
    action = decision.get("action", "hold")
    has_position = trade_state.get("has_position", False)
    if has_position:
        trade_state["close_cond_4h"] = format_market_summary_for_mail(summary_4h)
        trade_state["close_cond_1h"] = format_market_summary_for_mail(summary_1h)
        trade_state["close_cond_15m"] = format_market_summary_for_mail(summary_15m)
    if not has_position:
        if action not in ("open_long", "open_short"):
            return False
        side = "long" if action == "open_long" else "short"
        return open_order(
            side,
            curr_price,
            summary_4h,
            summary_1h,
            summary_15m,
            signal_bar_15m,
            decision.get("entry_reason") or decision.get("reason") or f"AI_{action}",
            decision,
        )
    side = trade_state.get("side")
    if action in ("hold", "update_risk"):
        return apply_ai_position_updates(side, curr_price, decision, signal_bar_15m=signal_bar_15m)
    if action == "close":
        close_reason = decision.get("close_reason") or decision.get("reason") or "AI主动平仓"
        trade_state["ai_plan_summary"] = format_ai_review_text(decision)
        trade_state["last_ai_action"] = action
        return close_position(close_reason, curr_price, signal_bar_15m=signal_bar_15m, trigger_label="AI主动平仓", ai_review=decision)
    if action in ("reverse_to_long", "reverse_to_short"):
        target_side = "long" if action == "reverse_to_long" else "short"
        stop_loss = safe_float(decision.get("stop_loss"))
        take_profit = safe_float(decision.get("take_profit"))
        if take_profit is not None and take_profit <= 0:
            take_profit = None
        if stop_loss is None or validate_ai_price_plan(target_side, curr_price, stop_loss=stop_loss, take_profit=take_profit):
            logging.warning("AI 反手参数无效，已拒绝执行: %s", format_ai_review_text(decision))
            return False
        close_reason = decision.get("close_reason") or decision.get("reason") or f"AI反手到{target_side}"
        if not close_position(close_reason, curr_price, signal_bar_15m=signal_bar_15m, trigger_label=f"AI反手到{target_side}", ai_review=decision):
            return False
        reverse_price = get_latest_price()
        if validate_ai_price_plan(target_side, reverse_price, stop_loss=stop_loss, take_profit=take_profit):
            logging.warning("AI 反手后的价格已变化，原止损方向失效，本轮仅完成平仓")
            return False
        return open_order(
            target_side,
            reverse_price,
            summary_4h,
            summary_1h,
            summary_15m,
            signal_bar_15m,
            decision.get("entry_reason") or decision.get("reason") or f"AI_{action}",
            decision,
        )
    return False


def run_strategy():
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
    try:
        future_4h = executor.submit(fetch_df, SYMBOL, "4h", 100)
        future_1h = executor.submit(fetch_df, SYMBOL, "1h", 100)
        future_15m = executor.submit(fetch_df, SYMBOL, "15m", 100)
        df_4h = future_4h.result(timeout=FETCH_DF_TASK_TIMEOUT_SECONDS)
        df_1h = future_1h.result(timeout=FETCH_DF_TASK_TIMEOUT_SECONDS)
        df_15m = future_15m.result(timeout=FETCH_DF_TASK_TIMEOUT_SECONDS)
    except concurrent.futures.TimeoutError:
        logging.error(f"抓取K线任务超时，已跳过本轮: timeout={FETCH_DF_TASK_TIMEOUT_SECONDS}s")
        executor.shutdown(wait=False, cancel_futures=True)
        return
    except Exception:
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)
    if df_4h is None or df_1h is None or df_15m is None:
        return
    server_now_dt = get_server_now_dt()
    summary_4h = summarize_timeframe_for_ai(df_4h, "4h", now_dt=server_now_dt)
    summary_1h = summarize_timeframe_for_ai(df_1h, "1h", now_dt=server_now_dt)
    summary_15m = summarize_timeframe_for_ai(df_15m, "15m", now_dt=server_now_dt)
    signal_bar_15m = get_closed_bar_time(df_15m, "15m", now_dt=server_now_dt)
    if not signal_bar_15m:
        return
    market_snapshot = build_market_snapshot_for_ai(df_4h, df_1h, df_15m, signal_bar_15m, server_now_dt=server_now_dt)
    curr_price = get_latest_price()
    if trade_state.get("has_position"):
        if not sync_trade_state_with_exchange(signal_bar_15m=signal_bar_15m):
            return
        if evaluate_ai_risk_guards(curr_price, signal_bar_15m=signal_bar_15m):
            return
    if signal_bar_15m == trade_state.get("last_processed_bar_15m", ""):
        return
    decision = guarded_ai_trade_decision(curr_price, market_snapshot, signal_bar_15m)
    execute_ai_trade_decision(decision, curr_price, summary_4h, summary_1h, summary_15m, signal_bar_15m)
    trade_state["last_processed_bar_15m"] = signal_bar_15m


if __name__ == "__main__":
    try:
        current_time_str = get_server_time_str()
        print("网络连通成功！服务器时间:", current_time_str)
        final_usdt = float(get_futures_usdt_balance_snapshot()["total_usdt"])
        logging.info(f"🚀 自动化交易系统启动，初始金额：{final_usdt}")
        logging.info(
            "DeepSeek AI 全权决策层: enabled=%s, model=%s, thinking=%s, base_url=%s",
            DEEPSEEK_AI_ENABLED,
            DEEPSEEK_MODEL,
            DEEPSEEK_THINKING_ENABLED,
            DEEPSEEK_BASE_URL,
        )
        if DEEPSEEK_AI_ENABLED and not DEEPSEEK_API_KEY:
            log_deepseek_config_warning_once(
                "DeepSeek AI 已启用但缺少 DEEPSEEK_API_KEY；系统不会执行主动交易决策。"
            )
    except Exception as e:
        logging.error(f"启动自检失败，但主循环会继续尝试运行: {e}")

    while True:
        try:
            maybe_log_heartbeat()
            run_strategy()
        except Exception as e:
            print(traceback.format_exc())
            logging.error(f"系统运行报错: {e}")
            logging.error("主循环将继续运行，休眠后自动进入下一轮")
            time.sleep(MAIN_LOOP_ERROR_SLEEP_SECONDS)
            continue
        time.sleep(MAIN_LOOP_SLEEP_SECONDS)
