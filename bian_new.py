"""
环境：
python version>=3.12 必须的
该脚本在东京服务器linux跑的,在国内挂了vpn也会有一堆问题的。
当前主要库版本：
python==3.12.9
ccxt==4.5.33
pandas_ta==0.4.71b0
说明:
该脚本是币安U本位合约交易自动脚本（只交易ETH）

策略：
新版策略使用 ADX/DI + EMA20 背景趋势、强阳/强阴K线、合成强K、
EMA20连续背景、4H支撑阻力区、15m/1h/4h 多周期权重进行交易。
"""
import os
# pandas_ta 会触发 numba 缓存；部分服务器/打包环境没有可用locator，会导致启动即失败。
os.environ.setdefault('NUMBA_DISABLE_JIT', '1')

import ccxt
import pandas as pd
import pandas_ta as ta
import time
import logging
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import sys
import concurrent.futures
import csv
import datetime
import traceback
import sqlite3

BAR_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'  # 定义统一的K线时间格式字符串，用于日志记录和CSV输出
TIMEFRAME_SECONDS = {  # 定义各时间周期对应的秒数，用于计算K线是否已收盘
    '5m': 5 * 60,      # 5分钟 = 300秒
    '15m': 15 * 60,    # 15分钟 = 900秒
    '1h': 60 * 60,     # 1小时 = 3600秒
    '4h': 4 * 60 * 60, # 4小时 = 14400秒
    '1d': 24 * 60 * 60 # 1天 = 86400秒
}
EXCHANGE_TZ = datetime.timezone(datetime.timedelta(hours=8))  # 定义交易所时区为东八区（北京时间），用于时间戳转换
# 当前脚本所在目录，用来定位同目录下的 .env.local 和统计数据库文件。
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 本地环境变量文件路径，API密钥和邮件密码优先从这里读取，避免写死在代码里。
LOCAL_ENV_PATH = os.path.join(BASE_DIR, '.env.local')
# 交易统计 SQLite 数据库路径，用于持久化复盘/统计数据。
STATS_DB_PATH = os.path.join(BASE_DIR, 'trade_stats.db')


def load_local_env(env_path=LOCAL_ENV_PATH):
    """从本地 .env.local 读取环境变量，避免把密钥写进代码仓库。"""
    if not os.path.exists(env_path):
        # 服务器可直接通过系统环境变量注入密钥；没有本地文件时不阻断启动。
        return

    with open(env_path, 'r', encoding='utf-8') as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue

            key, value = line.split('=', 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


def require_env(name):
    """读取必须存在的环境变量。"""
    value = os.getenv(name, '').strip()
    if not value:
        raise RuntimeError(f'缺少环境变量: {name}')
    return value


load_local_env()

# ==========================================
# 1. 配置参数 (请务必填写你的真实信息)
# ==========================================

# --- 币安 API 配置 ---
API_KEY = require_env('BINANCE_API_KEY')  # 从环境变量读取币安 API 公钥
SECRET_KEY = require_env('BINANCE_SECRET_KEY')  # 从环境变量读取币安 API 私钥

# --- 邮件通知配置 (以QQ邮箱为例) ---
SMTP_HOST = os.getenv('SMTP_HOST', 'smtp.qq.com')  # 设置QQ邮箱的SMTP服务器地址
SMTP_PORT = int(os.getenv('SMTP_PORT', '465'))  # 设置SMTP服务器的SSL端口号（QQ邮箱默认为465）
EMAIL_SENDER = os.getenv('EMAIL_SENDER', '').strip()  # 从环境变量读取发件人的邮箱地址
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD', '').strip()  # 从环境变量读取发件人邮箱授权码
EMAIL_RECEIVER = os.getenv('EMAIL_RECEIVER', EMAIL_SENDER).strip()  # 默认把通知发给自己

# --- 交易策略参数 ---
SYMBOL = 'ETH/USDT'  # 设置交易对为ETH/USDT
LEVERAGE = 20  # 设置合约的杠杆倍数为20倍
MARGIN_RATE = 0.6  # 设置每次开仓使用的保证金比例，使用余额的60%
STOP_WORKING_TYPE = 'MARK_PRICE'  # 服务端条件止损按标记价格触发，避免只看最新成交价带来的偏差
STOP_ORDER_CANCEL_CONFIRM_RETRIES = 5  # 撤掉旧条件单后，最多确认 5 次交易所侧是否真的消失
STOP_ORDER_CANCEL_CONFIRM_SLEEP_SECONDS = 0.2  # 每次确认旧条件单状态之间的等待时间
STOP_ORDER_POST_CANCEL_DELAY_SECONDS = 0.15  # 确认旧单消失后，再额外等一小会儿给交易所状态同步
STOP_ORDER_REFRESH_RETRY_DELAYS_SECONDS = (0.3, 0.8, 1.5)  # 遇到 -4130 时的重试退避时间
STOP_ORDER_REFRESH_FAILURE_CLOSE_THRESHOLD = 5  # 服务端止损更新连续失败 5 次后，才执行保护性平仓
LIQUIDATION_SAFE_BUFFER_RATIO = 0.003  # 止损价和强平价之间至少保留 0.3% 的价格缓冲
ESTIMATED_LIQUIDATION_GUARD_RATIO = 0.8  # 用于开仓前估算强平距离，取 0.8 / 杠杆，故意保守一点
POSITION_AMT_EPSILON = 1e-8  # 持仓数量小于该阈值视为0，避免浮点噪音误判
EXTERNAL_CLOSE_CONFIRM_MISS_COUNT = 3  # 连续3轮查不到仓位才触发外部平仓重置，降低瞬时接口波动误判
EXCHANGE_HTTP_TIMEOUT_MS = 10000  # 单次交易所HTTP请求最多等待10秒，避免底层请求长时间挂起
FETCH_DF_TASK_TIMEOUT_SECONDS = 15  # 单个周期抓取任务最多等待15秒，超过就跳过本轮
FETCH_DF_SLOW_LOG_SECONDS = 5  # 单次抓K线+算指标超过5秒就记慢查询日志
MAIN_LOOP_SLEEP_SECONDS = 1  # 主循环正常节奏
MAIN_LOOP_ERROR_SLEEP_SECONDS = 5  # 主循环遇到顶层异常后，先休息5秒再继续
HEARTBEAT_INTERVAL_SECONDS = 15 * 60  # 每15分钟输出一次心跳日志，方便判断进程是否还活着
MAX_SIGNAL_BAR_STALENESS_SECONDS = 45 * 60  # 15M信号K线最多允许落后45分钟，防止交易所/测试网返回旧K线

# --- 新版策略参数 ---
# 策略会参与打分和选信号的主周期；越靠前代表越高周期、权重通常越重。
STRATEGY_TIMEFRAMES = ('4h', '1h', '15m')
# 每个主周期对应的上级背景周期，用来判断大趋势环境。
STRATEGY_BACKGROUND_TF = {'15m': '1h', '1h': '4h', '4h': '1d'}
# 每个主周期对应的低级别触发周期，用来找更细的入场触发K线。
STRATEGY_TRIGGER_TF = {'15m': '5m', '1h': '15m', '4h': '1h'}
# 持仓后用于观察平仓、缩紧止损和止盈管理的周期。
STRATEGY_EXIT_TF = {'15m': '5m', '1h': '15m', '4h': '1h'}
# ADX/DI 指标计算周期，数值越大越平滑，信号越慢。
ADX_LENGTH = 14
# ADX 低于该值时认为趋势强度不足，倾向于不交易。
ADX_NO_TRADE_MAX = 25
# ADX 达到该值后才认为市场进入可交易的趋势状态。
ADX_TREND_MIN = 25
# ADX 高于该值视为极强趋势，止损缓冲会按极端趋势逻辑处理。
ADX_EXTREME = 40
# 背景趋势使用的 EMA 周期，当前是 EMA20。
EMA_TREND_LENGTH = 20
# 判断 EMA 斜率时向前比较的K线数量，用来确认均线是否持续上/下行。
EMA_SLOPE_LOOKBACK = 3
# 判断价格是否刚穿越 EMA 时回看的K线数量。
EMA_CROSS_LOOKBACK = 4
# 判断价格站上/跌破 EMA 是否持续有效时要求连续观察的K线数量。
EMA_PERSISTENCE_BARS = 20
# 强K实体至少要达到 ATR 的比例，过滤实体太小的K线。
STRONG_BODY_ATR_RATIO = 0.55
# 强阳/强阴要求收盘价靠近高点/低点的区域比例，数值越小要求越强。
STRONG_CLOSE_ZONE_RATIO = 0.27
# 强K实体占整根K线振幅的最低比例，过滤长影线假突破。
STRONG_BODY_RANGE_RATIO = 0.60
# 单根强K振幅超过该 ATR 倍数时视为过大，避免追在异常大K后面。
LARGE_STRONG_RANGE_ATR_RATIO = 2.25
# 合成强K最少合并的K线数量，用来识别连续推进但单根不够强的走势。
SYNTHETIC_STRONG_MIN_BARS = 2
# 合成强K最多合并的K线数量，防止把太长一段震荡误判为强K。
SYNTHETIC_STRONG_MAX_BARS = 3
# 回看多少根K线检查是否出现过反向强K，用来过滤方向冲突。
OPPOSITE_STRONG_LOOKBACK = 24
# 检查最近几根强K是否多空来回切换，避免在剧烈震荡里开仓。
STRONG_CHOP_LOOKBACK_BARS = 4
# 初始止损距离使用 ATR 的倍数，决定入场后第一版保护止损有多宽。
ENTRY_ATR_STOP_MULTIPLIER = 1.0
# 极强趋势下止损额外缓冲的 ATR 倍数，避免止损离强平价或关键位太近。
EXTREME_ADX_STOP_BUFFER_ATR = 0.1
# 普通趋势下止损额外缓冲的 ATR 倍数。
NORMAL_STOP_BUFFER_ATR = 0.2
# 预期利润至少要达到 ATR 的这个比例，否则认为空间太小不值得进场。
MIN_EXPECTED_PROFIT_ATR = 0.25
# 预期利润至少要覆盖手续费的倍数，防止收益空间被手续费吃掉。
MIN_EXPECTED_PROFIT_FEE_MULTIPLIER = 3.0
# 平仓后冷却时间，防止刚出场又在同一段短周期波动里立刻重开。
POST_EXIT_COOLDOWN_SECONDS = 4 * TIMEFRAME_SECONDS['5m']
# 识别4H支撑/阻力摆动高低点时，左右各观察的K线窗口。
SUP_RES_SWING_WINDOW = 5
# 支撑/阻力价位相距小于该 ATR 倍数时会合并成同一个区域。
SUP_RES_MERGE_ATR = 0.5
# 支撑/阻力区域上下边界额外扩展的 ATR 倍数。
SUP_RES_ZONE_BUFFER_ATR = 0.1
# 一个支撑/阻力区至少被触碰多少次才算有效区域。
SUP_RES_VALID_TOUCHES = 2
# 根据支撑/阻力设置止损时，止损放在区域外侧的 ATR 缓冲。
SUP_RES_STOP_BUFFER_ATR = 0.2
# 当前价格距离支撑/阻力小于该 ATR 倍数时，认为已经靠近关键区域。
SUP_RES_NEAR_ATR = 0.25
# 计算4H支撑/阻力时向前回看的K线数量。
SUP_RES_LOOKBACK_4H = 220
# 用1H数据确认支撑/阻力有效性时的回看K线数量。
SR_CONFIRM_LOOKBACK_1H = 18
# 用15M数据寻找支撑/阻力入场触发时的回看K线数量。
SR_ENTRY_LOOKBACK_15M = 24

# --- 全局运行状态记录 ---
trade_state = {
    'has_position': False,  # 记录当前是否持有仓位，初始为False
    'side': None,  # 记录持仓方向：'long' (多单) 或 'short' (空单)，初始为空
    'entry_price': 0,  # 记录开仓时的入场价格，初始为0
    'stop_loss_price': 0,  # 记录当前的止损价格，初始为0
    'highest_price': 0,  # 记录持有多单时的最高价格，用于计算利润回撤，初始为0
    'lowest_price': 0,  # 记录持有空单时的最低价格，用于计算利润回撤，初始为0
    'amount': 0,  # 记录当前持仓的数量，初始为0
    'entry_time': '',  # 记录建仓时间
    'cond_4h': '',     # 记录开仓时的 4H 具体条件信息
    'cond_1h': '',     # 记录开仓时的 1H 具体条件信息
    'cond_15m': '',    # 记录开仓时的 15M 具体条件信息
    'close_cond_4h': '',  # 记录平仓触发时的 4H 条件快照信息
    'close_cond_1h': '',  # 记录平仓触发时的 1H 条件快照信息
    'close_cond_15m': '',  # 记录平仓触发时的 15M 条件快照信息
    'initial_balance': 0.0, # 记录开仓前的账户USDT总余额
    'open_fee': 0.0,   # 记录开仓时产生的手续费
    'open_order_id': '',  # 记录开仓市价单的订单ID
    'close_order_id': '',  # 记录最近一次平仓市价单的订单ID
    'entry_reason': '', # 记录本次开仓来源
    'entry_trigger_tf': '', # 记录本次开仓真正触发的周期，如 1H / 4H / 4H+1H+15M
    'entry_strategy_tf': '', # 记录策略判断周期：15m / 1h / 4h
    'entry_exit_tf': '', # 记录持仓后用于平仓/缩紧止盈的周期
    'entry_sr_target': 0.0, # 支撑阻力模块给出的第一目标位
    'entry_initial_risk': 0.0, # 入场价到初始止损的距离，用于1:2备用目标
    'partial_taken': False, # 是否已经执行过第一目标半仓止盈
    'last_exit_time': '', # 最近一次平仓时间，用于5分钟触发策略的冷却
    'liquidation_price': 0.0,  # 记录当前仓位从交易所返回的真实强平价
    'stop_order_id': '',  # 记录服务端 STOP_MARKET 止损单的订单ID，便于后续撤单和替换
    'stop_order_price': 0.0,  # 记录当前服务端止损单对应的触发价格
    'stop_order_refresh_fail_count': 0,  # 连续几次更新服务端止损单失败，达到阈值后才保护性平仓
    'last_stop_order_refresh_error': '',  # 最近一次服务端止损更新失败的原始错误文本
    'entry_signal_bar_15m': '',   # 记录入场所对应的15M已收盘信号K线时间
    'last_entry_bar_15m': '',     # 最近一次入场所对应的15M已收盘信号K线时间（防止同根K线重复开仓）
    'last_exit_bar_15m': '',      # 最近一次平仓所对应的15M已收盘信号K线时间（防止同根K线平仓后立即重开）
    'last_processed_bar_5m': '',  # 最近一次已处理过的5M已收盘信号K线时间
    'max_seen_bar_5m': '',        # 运行期间见过的最大5M信号时间
    'last_processed_bar_15m': '', # 最近一次已处理过的15M已收盘信号K线时间（核心去重字段，防止同一根K线重复执行策略逻辑）
    'max_seen_bar_15m': '',       # 运行期间见过的最大15M信号时间，防止接口回跳旧K线后被当成新信号
    'position_miss_count': 0  # 连续几轮未在交易所查到仓位，用于避免误判“外部平仓”
}

# 初始化币安合约 API
exchange = ccxt.binance({
    'apiKey': API_KEY,
    'secret': SECRET_KEY,
    'options': {'defaultType': 'future'},  # 设置默认交易类型为U本位合约 (future)
    'enableRateLimit': True,  # 开启内置的速率限制功能，防止请求频率过高被封IP
    'timeout': EXCHANGE_HTTP_TIMEOUT_MS,  # 给交易所HTTP请求设置硬超时，避免网络卡死时无限等待
})
exchange.enable_demo_trading(True)  # 开启模拟交易模式（测试网），不会产生真实交易

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 记录程序层面的临时运行状态，不属于某一笔交易。
runtime_state = {
    # 上一次打印心跳日志的单调时间戳，用来控制心跳输出频率。
    'last_heartbeat_ts': 0.0
}


# ==========================================
# 2. 功能模块
# ==========================================

def send_msg(subject, content):
    """发送邮件通知"""
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        logging.warning("未配置邮件通知环境变量，已跳过邮件发送。")
        # 邮件只是告警通道，配置缺失时不能影响交易主循环。
        return

    try:
        message = MIMEText(content, 'plain', 'utf-8')
        message['From'] = EMAIL_SENDER
        message['To'] = EMAIL_RECEIVER
        message['Subject'] = Header(subject, 'utf-8')
        smtp_obj = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT)
        smtp_obj.login(EMAIL_SENDER, EMAIL_PASSWORD)
        smtp_obj.sendmail(EMAIL_SENDER, [EMAIL_RECEIVER], message.as_string())
        smtp_obj.quit()
    except Exception as e:
        logging.error(f"邮件发送失败: {e}")


# 当前版本交易记录 CSV 的表头，新增字段时要同步兼容旧文件升级逻辑。
TRADE_CSV_HEADERS = [
    '建仓时间', '趋势方向', '4H条件', '1H条件', '15M条件', '入场原因',
    '平仓时间', '平仓原因', '点数盈亏', '手续费', '净利润(USDT)', '是否盈利',
    '入场15M信号时间', '平仓15M信号时间', '平仓触发周期', '持仓秒数',
    '开仓订单ID', '平仓订单ID'
]
# 旧版 CSV 表头缺少订单ID字段，用它识别历史文件并自动补齐新列。
LEGACY_TRADE_CSV_HEADERS = TRADE_CSV_HEADERS[:-2]


def extract_order_id(order):
    """从 CCXT 订单结果中尽量稳定地提取订单ID"""
    if not isinstance(order, dict):
        return ''

    info = order.get('info', {})
    candidates = [
        order.get('id'),
        order.get('orderId'),
        order.get('clientOrderId')
    ]
    if isinstance(info, dict):
        candidates.extend([
            info.get('orderId'),
            info.get('id'),
            info.get('clientOrderId')
        ])

    for candidate in candidates:
        if candidate not in (None, ''):
            return str(candidate)
    return ''


def format_exception_message(error):
    """尽量把异常格式化成稳定可读的字符串。"""
    if error is None:
        return ''
    text = str(error).strip()
    if text:
        return text
    return repr(error)


def clear_local_stop_order_state():
    """清空本地缓存的服务端止损单状态。"""
    trade_state['stop_order_id'] = ''
    trade_state['stop_order_price'] = 0.0


def extract_order_timestamp_ms(order):
    """尽量提取订单时间戳，便于在多张条件单里选最新的一张。"""
    if not isinstance(order, dict):
        return 0

    info = order.get('info', {})
    if not isinstance(info, dict):
        info = {}

    candidates = [
        order.get('timestamp'),
        order.get('lastTradeTimestamp'),
        info.get('updateTime'),
        info.get('workingTime'),
        info.get('time')
    ]
    for candidate in candidates:
        try:
            if candidate not in (None, ''):
                return int(candidate)
        except Exception:
            continue
    return 0


def extract_order_stop_price(order):
    """从交易所订单对象里提取条件触发价。"""
    if not isinstance(order, dict):
        return None

    info = order.get('info', {})
    if not isinstance(info, dict):
        info = {}

    candidates = [
        order.get('stopPrice'),
        order.get('triggerPrice'),
        info.get('stopPrice'),
        info.get('triggerPrice'),
        info.get('activatePrice')
    ]
    for candidate in candidates:
        try:
            if candidate not in (None, ''):
                return float(candidate)
        except Exception:
            continue
    return None


def format_order_id_lines(open_order_id='', close_order_id='', stop_order_id=''):
    """把可用的订单ID格式化成邮件/日志可直接复用的多行文本"""
    lines = []
    if open_order_id:
        lines.append(f"开仓订单ID: {open_order_id}")
    if close_order_id:
        lines.append(f"平仓订单ID: {close_order_id}")
    if stop_order_id:
        lines.append(f"止损订单ID: {stop_order_id}")
    return '\n'.join(lines)


def ensure_trade_csv_schema(filename):
    """兼容老版CSV表头，必要时补齐订单ID列，避免新旧列数不一致"""
    if not os.path.isfile(filename):
        return True

    try:
        with open(filename, mode='r', newline='', encoding='utf-8-sig') as f:
            rows = list(csv.reader(f))
    except Exception as e:
        logging.error(f"读取CSV表头失败: {filename}, error={e}")
        return False

    if not rows:
        return True

    header = rows[0]
    if header == TRADE_CSV_HEADERS:
        return True

    if header != LEGACY_TRADE_CSV_HEADERS:
        logging.warning(f"CSV表头不是预期格式，跳过自动升级: {filename}")
        return True

    upgraded_rows = [TRADE_CSV_HEADERS]
    legacy_len = len(LEGACY_TRADE_CSV_HEADERS)
    for row in rows[1:]:
        normalized_row = list(row[:legacy_len])
        if len(normalized_row) < legacy_len:
            normalized_row.extend([''] * (legacy_len - len(normalized_row)))
        normalized_row.extend(['', ''])
        upgraded_rows.append(normalized_row)

    try:
        with open(filename, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerows(upgraded_rows)
        logging.info(f"已自动升级CSV表头，补充订单ID列: {filename}")
        return True
    except Exception as e:
        logging.error(f"升级CSV表头失败: {filename}, error={e}")
        return False


def fetch_df(symbol, timeframe, limit=100):
    """获取K线并计算技术指标"""
    start_ts = time.monotonic()
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        # 统一转成北京时间，后续信号去重、CSV 和日志都按同一时区比较。
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['timestamp'] = df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai')
       
        
        # 交易所只给原始K线；策略用到的趋势/波动指标在本地补齐。
        df['ema20'] = ta.ema(df['close'], length=20)
        df['ema50'] = ta.ema(df['close'], length=50)

        df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)

        # ADX / DI 14，用于新版背景趋势判断。
        adx = ta.adx(df['high'], df['low'], df['close'], length=ADX_LENGTH)
        if adx is not None:
            df = pd.concat([df, adx], axis=1)
            df.rename(columns={
                f'ADX_{ADX_LENGTH}': 'adx',
                f'DMP_{ADX_LENGTH}': 'plus_di',
                f'DMN_{ADX_LENGTH}': 'minus_di'
            }, inplace=True)
        elapsed = time.monotonic() - start_ts
        if elapsed >= FETCH_DF_SLOW_LOG_SECONDS:
            logging.warning(f"获取数据较慢 ({timeframe}): {elapsed:.2f}s")
        return df
    except Exception as e:
        logging.error(f"获取数据失败 ({timeframe}): {e}")
        return None


def maybe_log_heartbeat():
    """定期输出心跳日志，确认主循环仍然存活且没有卡死"""
    now_ts = time.monotonic()
    last_heartbeat_ts = runtime_state.get('last_heartbeat_ts', 0.0)
    if now_ts - last_heartbeat_ts < HEARTBEAT_INTERVAL_SECONDS:
        # 主循环每秒跑一次；未到间隔时不刷心跳，避免日志被无效信息淹没。
        return

    runtime_state['last_heartbeat_ts'] = now_ts
    logging.info(
        "心跳: has_position=%s, side=%s, last_processed_15m=%s, last_entry_15m=%s, last_exit_15m=%s",
        trade_state.get('has_position'),
        trade_state.get('side'),
        trade_state.get('last_processed_bar_15m', ''),
        trade_state.get('last_entry_bar_15m', ''),
        trade_state.get('last_exit_bar_15m', '')
    )


def calculate_amount(price):
    """根据余额计算下单数量"""
    try:
        balance = exchange.fetch_balance({'type': 'future'})
        # 当前按账户 USDT 总额估算目标仓位，最后交给交易所精度规则截断数量。
        usdt_free = float(balance['total']['USDT'])
        position_value = usdt_free * MARGIN_RATE * LEVERAGE
        amount = position_value / price
        return exchange.amount_to_precision(SYMBOL, amount)
    except Exception as e:
        logging.error(f"计算下单数量失败: {e}")
        return 0


def get_trading_fee_rate():
    """获取指定交易对的交易手续费率"""
    try:
        # 使用CCXT标准方法获取指定交易对的费率
        fee_info = exchange.fetch_trading_fee(SYMBOL)
       
        taker_fee_rate = fee_info.get('taker', 0)
        return taker_fee_rate
    except Exception as e:
        logging.error(f"获取 {SYMBOL} 手续费率失败: {e}，使用默认费率 Maker 0.02%, Taker 0.04%")
        return  0.0004


def get_server_time_str():
    """获取交易所服务器时间并格式化输出"""
    server_time_ms = exchange.fetch_time()
    return datetime.datetime.fromtimestamp(
        server_time_ms / 1000.0,
        tz=EXCHANGE_TZ
    ).strftime(BAR_TIME_FORMAT)


def format_bar_time(ts):
    """统一格式化K线时间戳，便于状态去重和CSV记录"""
    if ts is None or pd.isna(ts):
        return ''
    if isinstance(ts, pd.Timestamp):
        return ts.to_pydatetime().strftime(BAR_TIME_FORMAT)
    if isinstance(ts, datetime.datetime):
        return ts.strftime(BAR_TIME_FORMAT)
    return str(ts)


def get_server_now_dt():
    """获取交易所服务器时间(datetime)"""
    server_time_ms = exchange.fetch_time()
    return datetime.datetime.fromtimestamp(server_time_ms / 1000.0, tz=EXCHANGE_TZ)


def get_last_closed_index(df, timeframe, now_dt=None):
    """按timeframe定位最近一根已收盘K线的iloc索引"""
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

    ts_series = df['timestamp']
    # 判断时间戳是否包含时区信息，如果包含则进行转换，否则进行本地化
    if getattr(ts_series.dt, 'tz', None) is None:
        ts_series = ts_series.dt.tz_localize(EXCHANGE_TZ)
    else:
        ts_series = ts_series.dt.tz_convert(EXCHANGE_TZ)

    closed_mask = (ts_series + pd.to_timedelta(tf_seconds, unit='s')) <= now_dt
    closed_positions = closed_mask.to_numpy().nonzero()[0]
    if len(closed_positions) == 0:
        return None
    return int(closed_positions[-1])


def get_closed_bar_time(df, timeframe, now_dt=None):
    """获取最近一根已收盘K线的时间"""
    closed_idx = get_last_closed_index(df, timeframe, now_dt=now_dt)
    if closed_idx is None:
        return ''
    return format_bar_time(df.iloc[closed_idx]['timestamp'])


def parse_bar_time(value):
    """把CSV/状态里的K线时间字符串解析成东八区datetime。"""
    if not value:
        return None
    try:
        parsed = datetime.datetime.strptime(str(value), BAR_TIME_FORMAT)
        return parsed.replace(tzinfo=EXCHANGE_TZ)
    except Exception:
        return None


def validate_signal_bar_15m(signal_bar_15m, now_dt=None):
    """校验15M信号K线是否新鲜且相对历史最大值单调向前。"""
    if not signal_bar_15m:
        return {'valid': False, 'is_new': False, 'reason': 'empty'}

    signal_dt = parse_bar_time(signal_bar_15m)
    if signal_dt is None:
        return {'valid': False, 'is_new': False, 'reason': 'parse_failed'}

    if now_dt is None:
        now_dt = get_server_now_dt()
    elif now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=EXCHANGE_TZ)
    else:
        now_dt = now_dt.astimezone(EXCHANGE_TZ)

    signal_close_dt = signal_dt + datetime.timedelta(seconds=TIMEFRAME_SECONDS['15m'])
    stale_seconds = (now_dt - signal_close_dt).total_seconds()
    if stale_seconds < -5:
        return {'valid': False, 'is_new': False, 'reason': 'future_bar', 'stale_seconds': stale_seconds}
    if stale_seconds > MAX_SIGNAL_BAR_STALENESS_SECONDS:
        return {'valid': False, 'is_new': False, 'reason': 'stale_bar', 'stale_seconds': stale_seconds}

    max_seen_dt = parse_bar_time(trade_state.get('max_seen_bar_15m', ''))
    if max_seen_dt is not None and signal_dt < max_seen_dt:
        return {
            'valid': False,
            'is_new': False,
            'reason': 'bar_time_rollback',
            'max_seen_bar_15m': trade_state.get('max_seen_bar_15m', ''),
            'stale_seconds': stale_seconds
        }

    last_processed_dt = parse_bar_time(trade_state.get('last_processed_bar_15m', ''))
    is_new = last_processed_dt is None or signal_dt > last_processed_dt
    if max_seen_dt is None or signal_dt > max_seen_dt:
        trade_state['max_seen_bar_15m'] = signal_bar_15m

    return {'valid': True, 'is_new': is_new, 'reason': 'ok', 'stale_seconds': stale_seconds}


def get_latest_price():
    """获取最新成交价，用于真实下单和风控"""
    ticker = exchange.fetch_ticker(SYMBOL)
    return float(ticker['last'])


def estimate_liquidation_price(entry_price, side):
    """按杠杆做一个保守的强平价预估，只用于开仓前风险过滤"""
    # 这里故意用 0.8 / 杠杆 作为估算比例，比理想情况下更保守，宁可少开也不把止损放到强平外面
    guard_ratio = ESTIMATED_LIQUIDATION_GUARD_RATIO / LEVERAGE
    if side == 'long':
        return entry_price * (1 - guard_ratio)
    return entry_price * (1 + guard_ratio)


def normalize_liquidation_price(liquidation_price):
    """把交易所返回的强平价转换成可用数值，0 或负数都视为无效"""
    try:
        liq = float(liquidation_price)
        if liq <= 0:
            return None
        return liq
    except Exception:
        return None


def infer_position_side(position_amt, info=None, pos=None):
    """优先用 positionSide 判断方向，兼容对冲模式下 SHORT 仓位数量为正数的情况。"""
    info = info or {}
    pos = pos or {}
    raw_side = str(info.get('positionSide') or pos.get('side') or '').strip().upper()
    if raw_side in ('LONG', 'SHORT'):
        return raw_side.lower()
    return 'long' if float(position_amt) > 0 else 'short'


def get_position_risk(side=None):
    """获取当前合约仓位风险信息，包括强平价和标记价格"""
    try:
        positions = exchange.fetch_positions_risk([SYMBOL])
        for pos in positions:
            info = pos.get('info', {})

            position_amt = float(info.get('positionAmt', pos.get('contracts', 0)) or 0)
            if abs(position_amt) <= POSITION_AMT_EPSILON:
                continue
            pos_side = infer_position_side(position_amt, info=info, pos=pos)
            if side and pos_side != side:
                continue
            #liquidationPrice：强平价
            liquidation_price = normalize_liquidation_price(info.get('liquidationPrice', 0))
            #markPrice：标记价格
            mark_price = normalize_liquidation_price(info.get('markPrice', 0))
            return {
                'side': pos_side,
                'position_amt': position_amt,
                'liquidation_price': liquidation_price,
                'mark_price': mark_price,
                'info': info
            }
    except Exception as e:
        logging.warning(f"获取仓位风险信息失败: {e}")
        return {'fetch_failed': True, 'error': str(e)}
    return None


def has_open_position_on_exchange(side=None):
    """使用标准仓位接口做二次确认，避免仓位接口瞬时返回空导致误判。"""
    try:
        positions = exchange.fetch_positions([SYMBOL])
        for pos in positions:
            info = pos.get('info', {})
            position_amt = float(info.get('positionAmt', pos.get('contracts', 0)) or 0)
            if abs(position_amt) <= POSITION_AMT_EPSILON:
                continue
            pos_side = infer_position_side(position_amt, info=info, pos=pos)
            if side and pos_side != side:
                continue
            return {'has_position': True, 'fetch_failed': False, 'side': pos_side, 'position_amt': position_amt}
        return {'has_position': False, 'fetch_failed': False}
    except Exception as e:
        logging.warning(f"二次确认仓位失败(fetch_positions): {e}")
        return {'has_position': False, 'fetch_failed': True, 'error': str(e)}


def ensure_stop_price_safe(entry_price, stop_price, side, liquidation_price=None):
    """确保止损价在强平价的安全一侧；如果太危险，则自动往安全方向推回去"""
    if stop_price is None or pd.isna(stop_price):
        return stop_price, {'adjusted': False, 'source': 'none', 'liquidation_price': None, 'safe_buffer': 0.0}

    liq = normalize_liquidation_price(liquidation_price)
    source = 'actual'
    if liq is None:
        liq = estimate_liquidation_price(entry_price, side)
        source = 'estimated'

    safe_buffer = entry_price * LIQUIDATION_SAFE_BUFFER_RATIO
    adjusted_stop = float(stop_price)
    if side == 'long':
        min_safe_stop = liq + safe_buffer
        adjusted_stop = max(adjusted_stop, min_safe_stop)
    else:
        max_safe_stop = liq - safe_buffer
        adjusted_stop = min(adjusted_stop, max_safe_stop)

    return adjusted_stop, {
        'adjusted': abs(adjusted_stop - float(stop_price)) > 1e-12,
        'source': source,
        'liquidation_price': liq,
        'safe_buffer': safe_buffer
    }


def stop_price_is_still_valid(entry_price, stop_price, side):
    """检查止损价是否仍处在合理方向，避免止损价比入场价还离谱"""
    if stop_price is None or pd.isna(stop_price):
        return False
    if side == 'long':
        return stop_price < entry_price
    return stop_price > entry_price


def place_protective_stop_order(side, stop_price):
    """在交易所挂一个服务端 STOP_MARKET 止损单，避免本地轮询来不及止损"""
    stop_side = 'sell' if side == 'long' else 'buy'
    amount = float(trade_state.get('amount', 0.0) or 0.0)
    if amount <= 0:
        raise RuntimeError("缺少有效持仓数量，无法挂 reduceOnly 服务端止损单")

    # 使用显式数量 + reduceOnly，避免 closePosition 条件单在 demo/futures 环境里查不到、
    # 撤不掉，进而导致后续重挂持续 -4130 冲突。
    params = {
        'stopPrice': stop_price,
        'reduceOnly': True,
        'workingType': STOP_WORKING_TYPE
    }
    return exchange.create_order(SYMBOL, 'STOP_MARKET', stop_side, amount, None, params)


def normalize_exchange_bool(value):
    """把交易所返回的真假值统一转成 Python bool。"""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ('true', '1', 'yes')


def is_close_position_conditional_order(order, side=None):
    """判断某个 open order 是否是当前方向的全平仓条件单。"""
    if not isinstance(order, dict):
        return False

    info = order.get('info', {})
    if not isinstance(info, dict):
        info = {}

    close_position = normalize_exchange_bool(
        info.get('closePosition', order.get('closePosition'))
    )
    reduce_only = normalize_exchange_bool(
        info.get('reduceOnly', order.get('reduceOnly'))
    )
    if not close_position and not reduce_only:
        return False

    order_type = str(order.get('type') or info.get('type') or '').strip().upper()
    if order_type not in ('STOP', 'STOP_MARKET', 'TAKE_PROFIT', 'TAKE_PROFIT_MARKET'):
        return False

    if side:
        expected_side = 'SELL' if side == 'long' else 'BUY'
        order_side = str(order.get('side') or info.get('side') or '').strip().upper()
        if order_side != expected_side:
            return False

    return True


def ensure_stats_db():
    with sqlite3.connect(STATS_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_pnl (
                trade_day TEXT PRIMARY KEY,
                pnl REAL NOT NULL DEFAULT 0,
                trade_count INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
            """
        )


def update_daily_pnl_stats(exit_time, net_pnl_usdt):
    """按平仓日期更新 SQLite 日收益汇总。"""
    if not exit_time or len(exit_time) < 10:
        return

    trade_day = exit_time[:10]
    updated_at = datetime.datetime.now().isoformat(timespec='seconds')

    try:
        ensure_stats_db()
        with sqlite3.connect(STATS_DB_PATH) as conn:
            conn.execute(
                """
                INSERT INTO daily_pnl (trade_day, pnl, trade_count, updated_at)
                VALUES (?, ?, 1, ?)
                ON CONFLICT(trade_day) DO UPDATE SET
                    pnl = daily_pnl.pnl + excluded.pnl,
                    trade_count = daily_pnl.trade_count + 1,
                    updated_at = excluded.updated_at
                """,
                (trade_day, float(net_pnl_usdt), updated_at)
            )
    except Exception as e:
        logging.warning(f"写入 SQLite 日收益失败: {e}")


def get_exchange_symbol_id():
    """获取交易所原生symbol，未load_markets时用ETHUSDT兜底。"""
    try:
        market = exchange.market(SYMBOL)
        market_id = market.get('id')
        if market_id:
            return market_id
    except Exception:
        pass
    return SYMBOL.replace('/', '').replace(':USDT', '')


def normalize_raw_open_order(raw_order):
    """把交易所原始未成交单包装成可复用的CCXT-like结构。"""
    if not isinstance(raw_order, dict):
        return raw_order
    return {
        'id': raw_order.get('orderId') or raw_order.get('id') or raw_order.get('clientOrderId'),
        'type': raw_order.get('type'),
        'side': raw_order.get('side'),
        'timestamp': raw_order.get('time') or raw_order.get('updateTime') or raw_order.get('workingTime'),
        'stopPrice': raw_order.get('stopPrice') or raw_order.get('triggerPrice') or raw_order.get('activatePrice'),
        'closePosition': raw_order.get('closePosition'),
        'reduceOnly': raw_order.get('reduceOnly'),
        'info': raw_order
    }


def fetch_raw_future_open_orders():
    """直接调用币安U本位未成交单接口，补齐CCXT统一接口漏掉的条件单。"""
    method = getattr(exchange, 'fapiPrivateGetOpenOrders', None)
    if method is None:
        return []
    try:
        raw_orders = method({'symbol': get_exchange_symbol_id()})
    except Exception as e:
        logging.warning(f"原始接口查询U本位未成交单失败: {e}")
        return None
    if not isinstance(raw_orders, list):
        return []
    return [normalize_raw_open_order(order) for order in raw_orders]


def fetch_open_close_position_orders(side=None):
    """读取当前仓位方向上的全平仓条件单，便于做撤单确认和冲突排查。"""
    open_orders = []
    unified_fetch_failed = False
    try:
        open_orders = exchange.fetch_open_orders(SYMBOL)
    except Exception as e:
        logging.warning(f"查询未成交条件单失败: {e}")
        unified_fetch_failed = True
        open_orders = []

    raw_open_orders = fetch_raw_future_open_orders()
    if raw_open_orders is None:
        if unified_fetch_failed:
            return None
        raw_open_orders = []
    if raw_open_orders:
        known_order_ids = {extract_order_id(order) for order in open_orders if extract_order_id(order)}
        for raw_order in raw_open_orders:
            raw_order_id = extract_order_id(raw_order)
            if raw_order_id and raw_order_id in known_order_ids:
                continue
            open_orders.append(raw_order)

    matched_orders = []
    for order in open_orders:
        if is_close_position_conditional_order(order, side=side):
            matched_orders.append(order)
    return matched_orders


def fetch_open_protective_stop_orders(side=None):
    """读取当前方向的 STOP / STOP_MARKET 全平仓条件单。"""
    matched_orders = fetch_open_close_position_orders(side=side)
    if matched_orders is None:
        return None

    stop_orders = []
    for order in matched_orders:
        order_type = str(order.get('type') or order.get('info', {}).get('type') or '').strip().upper()
        if order_type in ('STOP', 'STOP_MARKET'):
            stop_orders.append(order)
    return stop_orders


def cancel_all_open_orders_for_symbol(silent=False):
    """撤销当前交易对的所有未成交单，用作 closePosition 隐藏冲突的最后恢复手段。"""
    cancel_succeeded = False
    error_messages = []

    try:
        exchange.cancel_all_orders(SYMBOL, {'type': 'future'})
        cancel_succeeded = True
        if not silent:
            logging.warning(f"已通过 cancel_all_orders 撤销 {SYMBOL} 全部未成交单")
    except Exception as unified_error:
        error_messages.append(f"cancel_all_orders failed: {format_exception_message(unified_error)}")

    raw_method = getattr(exchange, 'fapiPrivateDeleteAllOpenOrders', None)
    if raw_method is not None:
        try:
            raw_method({'symbol': get_exchange_symbol_id()})
            cancel_succeeded = True
            if not silent:
                logging.warning(f"已通过原始接口撤销 {SYMBOL} 全部未成交单")
        except Exception as raw_error:
            error_messages.append(f"raw cancel all failed: {format_exception_message(raw_error)}")
    else:
        error_messages.append("raw cancel all failed: fapiPrivateDeleteAllOpenOrders unavailable")

    if cancel_succeeded:
        clear_local_stop_order_state()
        return True

    trade_state['last_stop_order_refresh_error'] = '; '.join(error_messages)
    if not silent:
        logging.warning(f"批量撤销 {SYMBOL} 未成交单失败: {trade_state['last_stop_order_refresh_error']}")
    return False


def pick_active_protective_stop_order(orders, preferred_order_id=''):
    """在多张候选止损单里优先选本地记录对应的，否则选最新的一张。"""
    if not orders:
        return None

    preferred_order_id = str(preferred_order_id or '')
    if preferred_order_id:
        for order in orders:
            if extract_order_id(order) == preferred_order_id:
                return order

    return max(orders, key=extract_order_timestamp_ms)


def sync_protective_stop_order_state(side=None, silent=False):
    """用交易所未成交条件单刷新本地 stop_order 缓存。"""
    side = side or trade_state.get('side', '')
    if not side:
        return None

    stop_orders = fetch_open_protective_stop_orders(side=side)
    if stop_orders is None:
        return None

    local_order_id = str(trade_state.get('stop_order_id', '') or '')
    local_stop_price = float(trade_state.get('stop_order_price', 0.0) or 0.0)

    if not stop_orders:
        if local_order_id or local_stop_price:
            if not silent:
                logging.warning(
                    f"交易所未找到当前方向的服务端止损单，已清空本地缓存: "
                    f"side={side}, local_order_id={local_order_id}, local_stop={local_stop_price}"
                )
            clear_local_stop_order_state()
        return {'orders': [], 'active_order': None, 'active_order_id': '', 'active_stop_price': 0.0}

    active_order = pick_active_protective_stop_order(stop_orders, preferred_order_id=local_order_id)
    active_order_id = extract_order_id(active_order)
    active_stop_price = extract_order_stop_price(active_order)
    if active_stop_price is None:
        active_stop_price = local_stop_price

    if len(stop_orders) > 1 and not silent:
        order_ids = [extract_order_id(order) or 'unknown' for order in stop_orders]
        logging.warning(f"检测到同方向存在多张服务端止损单: side={side}, order_ids={order_ids}，将优先处理 {active_order_id}")

    if (local_order_id != active_order_id) or (
        active_stop_price and abs(local_stop_price - active_stop_price) > 1e-12
    ):
        if not silent:
            logging.info(
                f"已按交易所状态刷新服务端止损缓存: side={side}, "
                f"local_order_id={local_order_id}, exchange_order_id={active_order_id}, "
                f"local_stop={local_stop_price}, exchange_stop={active_stop_price}"
            )
        trade_state['stop_order_id'] = active_order_id
        trade_state['stop_order_price'] = float(active_stop_price or 0.0)

    return {
        'orders': stop_orders,
        'active_order': active_order,
        'active_order_id': active_order_id,
        'active_stop_price': float(active_stop_price or 0.0)
    }


def wait_until_stop_order_disappears(stop_order_ids, side, retries=STOP_ORDER_CANCEL_CONFIRM_RETRIES, sleep_seconds=STOP_ORDER_CANCEL_CONFIRM_SLEEP_SECONDS):
    """轮询确认指定止损单已经不在交易所未成交列表里。"""
    if not stop_order_ids:
        return True

    if isinstance(stop_order_ids, (list, tuple, set)):
        target_order_ids = {str(order_id) for order_id in stop_order_ids if order_id}
    else:
        target_order_ids = {str(stop_order_ids)}
    if not target_order_ids:
        return True

    for attempt in range(1, retries + 1):
        matched_orders = fetch_open_protective_stop_orders(side=side)
        if matched_orders is None:
            time.sleep(sleep_seconds)
            continue

        remaining_ids = [
            extract_order_id(order) for order in matched_orders
            if extract_order_id(order) in target_order_ids
        ]
        if not remaining_ids:
            return True

        logging.info(
            f"等待旧服务端止损单从交易所消失: order_ids={remaining_ids}, "
            f"attempt={attempt}/{retries}"
        )
        time.sleep(sleep_seconds)

    return False


def is_close_position_conflict_error(error):
    """识别 Binance 同方向 closePosition 条件单冲突(-4130)。"""
    error_text = format_exception_message(error)
    return 'code":-4130' in error_text or 'closePosition in the direction is existing' in error_text


def is_order_already_absent_error(error):
    """识别撤单时常见的“订单已不存在”类错误。"""
    error_text = format_exception_message(error).lower()
    absent_markers = (
        'code":-2011',
        'unknown order',
        'order does not exist',
        'order not found',
        'cancel rejected'
    )
    return any(marker in error_text for marker in absent_markers)


def reset_stop_order_refresh_failure_state():
    """服务端止损单一旦成功更新，就把连续失败计数清零。"""
    trade_state['stop_order_refresh_fail_count'] = 0
    trade_state['last_stop_order_refresh_error'] = ''


def handle_stop_order_refresh_failure(close_reason, curr_price, signal_bar_15m='', trigger_label=''):
    """服务端止损单更新失败时累计次数，连续达到阈值后才保护性平仓。"""
    fail_count = int(trade_state.get('stop_order_refresh_fail_count', 0) or 0) + 1
    trade_state['stop_order_refresh_fail_count'] = fail_count
    error_text = trade_state.get('last_stop_order_refresh_error', '')
    logging.warning(
        f"服务端止损更新失败，第{fail_count}/{STOP_ORDER_REFRESH_FAILURE_CLOSE_THRESHOLD}次；"
        f"本轮先不平仓，等待后续重试。error={error_text}"
    )
    if fail_count < STOP_ORDER_REFRESH_FAILURE_CLOSE_THRESHOLD:
        return False

    close_position(close_reason, curr_price, signal_bar_15m=signal_bar_15m, trigger_label=trigger_label)
    return True


def cancel_protective_stop_order(silent=False):
    """撤销当前记录的服务端止损单，平仓或替换止损时要先撤旧单"""
    side = trade_state.get('side', '')
    local_stop_order_id_before_sync = str(trade_state.get('stop_order_id', '') or '')
    sync_result = sync_protective_stop_order_state(side=side, silent=True) if side else None
    stop_order_ids = []
    if sync_result is not None:
        stop_order_ids = [extract_order_id(order) for order in sync_result['orders'] if extract_order_id(order)]

    local_stop_order_id = str(trade_state.get('stop_order_id', '') or '') or local_stop_order_id_before_sync
    if local_stop_order_id and local_stop_order_id not in stop_order_ids:
        stop_order_ids.append(local_stop_order_id)

    stop_order_ids = list(dict.fromkeys([order_id for order_id in stop_order_ids if order_id]))
    if not stop_order_ids:
        clear_local_stop_order_state()
        return True

    cancel_failed = False
    for stop_order_id in stop_order_ids:
        try:
            exchange.cancel_order(stop_order_id, SYMBOL)
            if not silent:
                logging.info(f"已撤销旧服务端止损单: {stop_order_id}")
        except Exception as e:
            error_text = format_exception_message(e)
            trade_state['last_stop_order_refresh_error'] = error_text

            if is_order_already_absent_error(e):
                if not silent:
                    logging.warning(f"撤销服务端止损单时提示已不存在，按成功处理({stop_order_id}): {error_text}")
                continue

            matched_orders = fetch_open_protective_stop_orders(side=side or None)
            if matched_orders is not None:
                still_exists = any(extract_order_id(order) == str(stop_order_id) for order in matched_orders)
                if not still_exists:
                    if not silent:
                        logging.warning(f"撤单接口报错，但旧服务端止损单已不在交易所未成交列表中，按成功处理({stop_order_id}): {error_text}")
                    continue

            cancel_failed = True
            if not silent:
                logging.warning(f"撤销服务端止损单失败({stop_order_id}): {error_text}")

    if cancel_failed:
        return False

    clear_local_stop_order_state()
    return True


def refresh_protective_stop_order(stop_price):
    """更新服务端止损单：先撤旧单，再按新的止损价重挂"""
    if not trade_state.get('has_position') or not trade_state.get('side'):
        return True

    side = trade_state['side']
    local_stop_order_id_before_sync = str(trade_state.get('stop_order_id', '') or '')
    sync_result = sync_protective_stop_order_state(side=side, silent=True)
    previous_stop_order_ids = []
    if sync_result is not None:
        previous_stop_order_ids = [extract_order_id(order) for order in sync_result['orders'] if extract_order_id(order)]
    local_stop_order_id = str(trade_state.get('stop_order_id', '') or '') or local_stop_order_id_before_sync
    if local_stop_order_id and local_stop_order_id not in previous_stop_order_ids:
        previous_stop_order_ids.append(local_stop_order_id)

    if previous_stop_order_ids:
        if not cancel_protective_stop_order(silent=True):
            trade_state['last_stop_order_refresh_error'] = (
                trade_state.get('last_stop_order_refresh_error', '') or
                f"cancel stop order failed: order_ids={previous_stop_order_ids}"
            )
            logging.error(f"撤销旧服务端止损单失败，已取消重挂: order_ids={previous_stop_order_ids}")
            return False

        if not wait_until_stop_order_disappears(previous_stop_order_ids, side=side):
            trade_state['last_stop_order_refresh_error'] = (
                f"old stop order still visible after cancel confirm retries: order_ids={previous_stop_order_ids}"
            )
            logging.error(
                f"旧服务端止损单撤销后仍未从交易所消失，已取消重挂: order_ids={previous_stop_order_ids}"
            )
            return False

        time.sleep(STOP_ORDER_POST_CANCEL_DELAY_SECONDS)

    retry_delays = (0.0,) + STOP_ORDER_REFRESH_RETRY_DELAYS_SECONDS
    for attempt, retry_delay in enumerate(retry_delays, start=1):
        if retry_delay > 0:
            time.sleep(retry_delay)

        try:
            stop_order = place_protective_stop_order(side, stop_price)
            trade_state['stop_order_id'] = extract_order_id(stop_order)
            trade_state['stop_order_price'] = float(stop_price)
            reset_stop_order_refresh_failure_state()
            logging.info(
                f"已更新服务端止损单: id={trade_state['stop_order_id']}, stop={stop_price}, attempt={attempt}"
            )
            return True
        except Exception as e:
            error_text = format_exception_message(e)
            trade_state['last_stop_order_refresh_error'] = error_text
            if is_close_position_conflict_error(e) and attempt < len(retry_delays):
                matched_orders = fetch_open_close_position_orders(side=side)
                matched_order_ids = []
                if matched_orders is not None:
                    matched_order_ids = [extract_order_id(order) or 'unknown' for order in matched_orders]
                    if matched_order_ids:
                        logging.warning(
                            f"发现导致 closePosition 冲突的未成交条件单，准备撤销后重试: order_ids={matched_order_ids}"
                        )
                        if not cancel_protective_stop_order(silent=True):
                            logging.error(
                                f"closePosition 冲突恢复失败：撤销旧条件单失败，order_ids={matched_order_ids}"
                            )
                            return False
                        if not wait_until_stop_order_disappears(matched_order_ids, side=side):
                            logging.warning(
                                f"closePosition 冲突恢复时旧条件单仍可见，准备继续重试: order_ids={matched_order_ids}"
                            )
                    else:
                        logging.warning("closePosition 冲突但未查询到具体条件单，准备批量撤销当前交易对未成交单后重试")
                        if not cancel_all_open_orders_for_symbol(silent=True):
                            logging.error(
                                f"closePosition 冲突恢复失败：批量撤销未成交单失败，error={trade_state.get('last_stop_order_refresh_error', '')}"
                            )
                            return False
                        time.sleep(max(STOP_ORDER_POST_CANCEL_DELAY_SECONDS, 0.5))
                logging.warning(
                    f"重挂服务端止损单遇到 closePosition 冲突，准备重试: attempt={attempt}, "
                    f"stop={stop_price}, open_close_position_orders={matched_order_ids}, error={error_text}"
                )
                continue

            logging.error(f"重挂服务端止损单失败: {error_text}")
            return False

    return False


def reset_trade_state_after_external_close(signal_bar_15m='', reason='检测到交易所仓位已关闭', external_context=None):
    """当服务端止损或人工操作已把仓位关掉时，重置本地状态，避免下个循环误操作"""
    external_context = external_context or {}

    stop_order_id_for_notify = external_context.get('stop_order_id_before_cancel') or trade_state.get('stop_order_id', '')
    stop_order_price_for_notify = external_context.get('stop_order_price_before_cancel', trade_state.get('stop_order_price', 0.0))
    exit_signal_bar_15m = signal_bar_15m or trade_state.get('last_processed_bar_15m', '')
    detected_time = get_server_time_str()

    # 外部平仓（服务端止损/人工）不会经过 close_position，因此在这里补写 CSV，避免漏单。
    side = trade_state.get('side')
    entry_time = trade_state.get('entry_time', '')
    has_trade_snapshot = bool(side) and bool(entry_time)
    estimated_close_price = None
    try:
        estimated_close_price = float(stop_order_price_for_notify)
    except Exception:
        estimated_close_price = None
    if estimated_close_price is None:
        try:
            estimated_close_price = float(get_latest_price())
        except Exception as e:
            logging.warning(f"外部平仓估算平仓价失败，将按0点数记录: {e}")

    net_pnl_usdt = 0.0
    final_usdt = None
    balance_pnl_available = False
    is_profit = False
    open_fee = float(trade_state.get('open_fee', 0.0) or 0.0)
    fee_cost = open_fee
    if has_trade_snapshot:
        try:
            balance_after = exchange.fetch_balance({'type': 'future'})
            final_usdt = float(balance_after['total']['USDT'])
            initial_balance = float(trade_state.get('initial_balance', 0.0) or 0.0)
            net_pnl_usdt = final_usdt - initial_balance
            balance_pnl_available = initial_balance > 0
            is_profit = net_pnl_usdt > 0
        except Exception as e:
            logging.warning(f"外部平仓读取账户余额失败，将回退到价格估算盈亏: {e}")

    pnl_points = 0.0
    entry_price = float(trade_state.get('entry_price', 0.0) or 0.0)
    amount = float(trade_state.get('amount', 0.0) or 0.0)
    if estimated_close_price is not None and entry_price > 0 and side in ('long', 'short'):
        if side == 'long':
            pnl_points = estimated_close_price - entry_price
        else:
            pnl_points = entry_price - estimated_close_price
        try:
            taker_fee_rate = get_trading_fee_rate()
            close_fee_est = estimated_close_price * amount * taker_fee_rate
            fee_cost = open_fee + close_fee_est
        except Exception as e:
            logging.warning(f"外部平仓估算手续费失败，改为仅记录开仓手续费: {e}")

    if not balance_pnl_available:
        is_profit = pnl_points > 0

    if has_trade_snapshot:
        close_reason = f"{reason}（外部平仓检测）"
        log_trade_to_csv(
            entry_time,
            side,
            trade_state.get('cond_4h', ''),
            trade_state.get('cond_1h', ''),
            trade_state.get('cond_15m', ''),
            trade_state.get('entry_reason', ''),
            detected_time,
            close_reason,
            round(pnl_points, 4),
            round(fee_cost, 4),
            round(net_pnl_usdt, 4),
            is_profit,
            entry_signal_bar_15m=trade_state.get('entry_signal_bar_15m', ''),
            exit_signal_bar_15m=exit_signal_bar_15m,
            exit_trigger='external_close_detected',
            holding_seconds=compute_holding_seconds(entry_time, detected_time),
            open_order_id=trade_state.get('open_order_id', ''),
            close_order_id=str(external_context.get('external_close_order_id', ''))
        )

    order_id_lines = format_order_id_lines(
        open_order_id=trade_state.get('open_order_id', ''),
        stop_order_id=stop_order_id_for_notify
    )
    order_id_suffix = f"\n{order_id_lines}" if order_id_lines else ''
    estimated_close_price_text = (
        f"{estimated_close_price:.4f}" if estimated_close_price is not None else "未知"
    )
    final_usdt_text = f"{final_usdt:.4f}" if final_usdt is not None else "未知"
    pnl_points_text = f"{pnl_points:.2f}"
    fee_cost_text = f"{fee_cost:.4f}"
    net_pnl_text = f"{net_pnl_usdt:.2f}"
    send_msg(
        "ETH交易: ⚠️检测到外部平仓",
        f"原因: {reason}\n"
        f"检测时间: {detected_time}\n"
        f"方向: {trade_state.get('side')}\n"
        f"入场时间: {trade_state.get('entry_time', '')}\n"
        f"入场价: {trade_state.get('entry_price', 0)}\n"
        f"估算出场价: {estimated_close_price_text}\n"
        f"点数盈亏: {pnl_points_text}\n"
        f"手续费: {fee_cost_text}\n"
        f"净利润(USDT): {net_pnl_text}\n"
        f"平仓后账户资金(USDT): {final_usdt_text}\n"
        f"触发周期: {trade_state.get('entry_trigger_tf', '')}\n"
        f"本地止损价: {trade_state.get('stop_loss_price', 0)}\n"
        f"服务端止损价: {stop_order_price_for_notify}\n"
        f"持仓数量: {trade_state.get('amount', 0)}\n"
        f"开仓原因: {trade_state.get('entry_reason', '')}\n"
        f"15M信号时间: {exit_signal_bar_15m}\n"
        f"说明: 已检测到交易所无仓位，可能是服务端止损成交或人工平仓。"
        f"{order_id_suffix}"
    )

    post_exit_processed_bar_15m = exit_signal_bar_15m or trade_state.get('last_processed_bar_15m', '')
    trade_state.update({
        'has_position': False,
        'side': None,
        'entry_price': 0,
        'stop_loss_price': 0,
        'highest_price': 0,
        'lowest_price': 0,
        'amount': 0,
        'entry_time': '',
        'cond_4h': '',
        'cond_1h': '',
        'cond_15m': '',
        'close_cond_4h': '',
        'close_cond_1h': '',
        'close_cond_15m': '',
        'entry_reason': '',
        'entry_trigger_tf': '',
        'entry_strategy_tf': '',
        'entry_exit_tf': '',
        'entry_sr_target': 0.0,
        'entry_initial_risk': 0.0,
        'partial_taken': False,
        'last_exit_time': detected_time,
        'initial_balance': 0.0,
        'open_fee': 0.0,
        'open_order_id': '',
        'close_order_id': '',
        'liquidation_price': 0.0,
        'stop_order_id': '',
        'stop_order_price': 0.0,
        'stop_order_refresh_fail_count': 0,
        'last_stop_order_refresh_error': '',
        'entry_signal_bar_15m': '',
        'last_exit_bar_15m': exit_signal_bar_15m,
        'last_processed_bar_15m': post_exit_processed_bar_15m,
        'position_miss_count': 0
    })
    logging.warning(reason)


def compute_holding_seconds(entry_time, exit_time):
    """计算持仓秒数，便于复盘"""
    try:
        entry_dt = datetime.datetime.strptime(entry_time, BAR_TIME_FORMAT)
        exit_dt = datetime.datetime.strptime(exit_time, BAR_TIME_FORMAT)
        return int((exit_dt - entry_dt).total_seconds())
    except Exception:
        return 0


def normalize_mail_value(value):
    """把 numpy 标量转成普通 Python 值，避免邮件里出现 np.True_ 这类文本。"""
    if hasattr(value, 'item'):
        try:
            return value.item()
        except Exception:
            return value
    return value


def format_mail_bool(value):
    """把布尔值压缩成更易读的中文。"""
    return '是' if bool(normalize_mail_value(value)) else '否'


def format_mail_scalar(value):
    """把数字和值格式化成适合邮件阅读的短文本。"""
    value = normalize_mail_value(value)
    if isinstance(value, bool):
        return format_mail_bool(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.4f}".rstrip('0').rstrip('.')
    return str(value)


def format_mail_checks(checks, label_map=None, default_labels=None):
    """把 dict/list 形式的条件检查压缩成一行短文本。"""
    label_map = label_map or {}

    if isinstance(checks, dict):
        normalized = {}
        for key, value in checks.items():
            value = normalize_mail_value(value)
            if isinstance(value, dict):
                continue
            normalized[key] = value

        parts = []
        score = normalized.get('score')
        threshold = normalized.get('threshold')
        if score is not None and threshold is not None:
            parts.append(f"得分={format_mail_scalar(score)}/{format_mail_scalar(threshold)}")

        for key, value in normalized.items():
            if key in ('score', 'threshold'):
                continue
            label = label_map.get(key, key)
            parts.append(f"{label}={format_mail_scalar(value)}")
        return '，'.join(parts) if parts else '无'

    if isinstance(checks, (list, tuple)):
        labels = default_labels or [f'条件{i + 1}' for i in range(len(checks))]
        parts = []
        for idx, value in enumerate(checks):
            label = labels[idx] if idx < len(labels) else f'条件{idx + 1}'
            parts.append(f"{label}={format_mail_scalar(value)}")
        return '，'.join(parts) if parts else '无'

    if checks in (None, ''):
        return '无'
    return format_mail_scalar(checks)


def format_entry_condition_for_mail(state, side_dir, entry_reason=''):
    """新版策略只写入 summary，避免旧策略条件混入复盘。"""
    if not isinstance(state, dict):
        return '无状态数据'
    if state.get('summary'):
        return str(state.get('summary'))
    return state.get('status', '') or ''


def format_condition_snapshot_for_mail(timeframe, state):
    """把新版趋势状态压缩成简洁快照，便于写入通知和复盘。"""
    if not isinstance(state, dict):
        return f"{timeframe}: 无状态数据"
    if state.get('summary'):
        return f"{timeframe}: {state.get('summary')}"
    return f"{timeframe}信号时间={state.get('signal_bar_time', '')}"


def log_trade_to_csv(entry_time, side, cond_4h, cond_1h, cond_15m, entry_reason, exit_time, close_reason, pnl_points, fee_cost, net_pnl_usdt, is_profit, entry_signal_bar_15m='', exit_signal_bar_15m='', exit_trigger='', holding_seconds=0, open_order_id='', close_order_id=''):
    """将交易记录写入CSV文件，按月分表"""
    # 根据平仓时间生成当月的 CSV 文件名，例如 trades_log_2024-05.csv
    month_str = exit_time[:7]  # 提取 'YYYY-MM' 部分
    filename = os.path.join(BASE_DIR, f'trades_log_{month_str}.csv')
    
    file_exists = os.path.isfile(filename)
    if file_exists and not ensure_trade_csv_schema(filename):
        return
    try:
        with open(filename, mode='a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            if not file_exists:
                # 写入表头
                # 这里特意新增“入场原因”一列，方便后面复盘开仓来源
                writer.writerow(TRADE_CSV_HEADERS)
            # 写入具体数据
            writer.writerow([
                entry_time, side, cond_4h, cond_1h, cond_15m, entry_reason,
                exit_time, close_reason, pnl_points, fee_cost, net_pnl_usdt, is_profit,
                entry_signal_bar_15m, exit_signal_bar_15m, exit_trigger, holding_seconds,
                open_order_id, close_order_id
            ])
        update_daily_pnl_stats(exit_time, net_pnl_usdt)
    except Exception as e:
        logging.error(f"写入CSV失败: {e}")


# ==========================================
# 3. 核心逻辑：趋势、入场、监控
# ==========================================

def open_order(side, price, sl_price, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='trend', entry_trigger_tf='', entry_meta=None):
    """执行开仓指令"""
    global trade_state
    entry_meta = entry_meta or {}
    amount = calculate_amount(price)
    amount = float(amount)
    if amount == 0:
        return False
    open_order_id = ''

    # 开仓前先用“保守估算强平价”预校验止损，防止止损本来就落到强平外面
    estimated_safe_stop, estimated_stop_meta = ensure_stop_price_safe(price, sl_price, side, liquidation_price=None)
    if not stop_price_is_still_valid(price, estimated_safe_stop, side):
        logging.warning(
            f"拒绝开仓：预估强平价过近，止损无效 side={side}, "
            f"entry={price}, stop={sl_price}, adjusted_stop={estimated_safe_stop}, meta={estimated_stop_meta}"
        )
        send_msg(
            "ETH交易: ⚠️开仓已拒绝",
            f"原因: 止损价可能落在强平价外侧\n方向: {side}\n原始止损: {sl_price}\n"
            f"修正后止损: {estimated_safe_stop}\n估算强平价: {estimated_stop_meta.get('liquidation_price')}"
        )
        return False

    try:
        # 获取开仓前的可用余额，用于后续平仓时计算精确净利润
        balance_before = exchange.fetch_balance({'type': 'future'})
        initial_usdt = float(balance_before['total']['USDT'])
        
        # 设置杠杆
        exchange.set_leverage(LEVERAGE, SYMBOL)

        # 市价单开仓
        order_side = 'buy' if side == 'long' else 'sell'
        order = exchange.create_market_order(SYMBOL, order_side, amount)
        open_order_id = extract_order_id(order)

        # 记录实际成交均价 (如有滑点以实际为准)
        actual_price = order.get('average', price)
        if actual_price is None or actual_price == 0:
            actual_price = price

        # 成交后再拿交易所真实仓位风险信息，获取真正的强平价
        position_risk = get_position_risk(side=side)
        actual_liquidation_price = None
        if position_risk and not position_risk.get('fetch_failed'):
            actual_liquidation_price = position_risk['liquidation_price']
        actual_safe_stop, actual_stop_meta = ensure_stop_price_safe(actual_price, estimated_safe_stop, side, liquidation_price=actual_liquidation_price)
        if not stop_price_is_still_valid(actual_price, actual_safe_stop, side):
            logging.error(
                f"开仓后发现真实强平价过近，立即撤退 side={side}, entry={actual_price}, "
                f"stop={actual_safe_stop}, liq={actual_liquidation_price}, meta={actual_stop_meta}"
            )
            # 先反向市价平掉刚开的仓位，避免把仓位裸露在强平附近
            panic_side = 'sell' if side == 'long' else 'buy'
            panic_close_order = exchange.create_market_order(SYMBOL, panic_side, amount)
            panic_close_order_id = extract_order_id(panic_close_order)
            order_id_lines = format_order_id_lines(
                open_order_id=open_order_id,
                close_order_id=panic_close_order_id
            )
            order_id_suffix = f"\n{order_id_lines}" if order_id_lines else ''
            send_msg(
                "ETH交易: ⚠️开仓后立即撤退",
                f"原因: 真实强平价过近，止损无安全空间\n方向: {side}\n"
                f"入场价: {actual_price}\n真实强平价: {actual_liquidation_price}\n修正后止损: {actual_safe_stop}"
                f"{order_id_suffix}"
            )
            return False

        # 计算开仓手续费 (参考 test.py 方式)
        taker_fee_rate = get_trading_fee_rate()
        open_fee = actual_price * amount * taker_fee_rate
        print("openfree:",open_fee)

        # 提取各个级别的具体成立条件字符串
        def format_cond(state, side_dir):
            return format_entry_condition_for_mail(state, side_dir, entry_reason)

        def build_cond_str(state, side_dir):
            condensed = format_cond(state, side_dir)
            return f"原因:{entry_reason} | {condensed}" if condensed else f"原因:{entry_reason}"
                
        # 这三段字符串后面会一起写进 trade_state 和 CSV，方便复盘每次进场的上下文
        cond_4h_str = build_cond_str(state_4h, side)
        cond_1h_str = build_cond_str(state_1h, side)
        cond_15m_str = build_cond_str(state_15m, side)
        entry_trigger_tf_display = entry_trigger_tf or '4H+1H+15M'

        open_condition_lines = []
        if format_cond(state_4h, side):
            open_condition_lines.append(f"4H({state_4h.get('signal_bar_time', '')}): {cond_4h_str}")
        if format_cond(state_1h, side):
            open_condition_lines.append(f"1H({state_1h.get('signal_bar_time', '')}): {cond_1h_str}")
        if format_cond(state_15m, side):
            open_condition_lines.append(f"15M({state_15m.get('signal_bar_time', '')}): {cond_15m_str}")
        open_condition_details = '\n'.join(open_condition_lines) if open_condition_lines else f"原因:{entry_reason}"
        
        entry_time = get_server_time_str()

        trade_state.update({
            'has_position': True,
            'side': side,
            'entry_price': actual_price,
            'stop_loss_price': actual_safe_stop,
            'highest_price': actual_price,
            'lowest_price': actual_price,
            'amount': amount,
            'entry_time': entry_time,
            'cond_4h': cond_4h_str,
            'cond_1h': cond_1h_str,
            'cond_15m': cond_15m_str,
            'close_cond_4h': '',
            'close_cond_1h': '',
            'close_cond_15m': '',
            'entry_reason': entry_reason,
            'entry_trigger_tf': entry_trigger_tf_display,
            'entry_strategy_tf': entry_meta.get('strategy_tf', ''),
            'entry_exit_tf': entry_meta.get('exit_tf', ''),
            'entry_sr_target': float(entry_meta.get('sr_target', 0.0) or 0.0),
            'entry_initial_risk': abs(float(actual_price) - float(actual_safe_stop)),
            'partial_taken': False,
            'initial_balance': initial_usdt,
            'open_fee': open_fee,
            'open_order_id': open_order_id,
            'close_order_id': '',
            'liquidation_price': actual_liquidation_price or 0.0,
            'stop_order_id': '',
            'stop_order_price': 0.0,
            'stop_order_refresh_fail_count': 0,
            'last_stop_order_refresh_error': '',
            'entry_signal_bar_15m': signal_bar_15m,
            'last_entry_bar_15m': signal_bar_15m,
            'position_miss_count': 0
        })

        # 开仓成功后立刻把服务端止损单挂上，真正的触发由交易所负责，不依赖本地轮询
        if not refresh_protective_stop_order(actual_safe_stop):
            logging.error("服务端止损单挂单失败，立即主动平仓避免裸奔风险")
            close_position("服务端止损挂单失败，主动平仓", curr_price=actual_price, signal_bar_15m=signal_bar_15m, trigger_label="服务端止损挂单失败")
            return False

        order_id_lines = format_order_id_lines(
            open_order_id=open_order_id,
            stop_order_id=trade_state.get('stop_order_id', '')
        )
        order_id_suffix = f"\n{order_id_lines}" if order_id_lines else ''
        msg = (f"🚀 【已开仓】\n方向: {side}\n入场价: {actual_price}\n"
               f"止损价: {actual_safe_stop}\n强平价: {actual_liquidation_price}\n数量: {amount}\n"
               f"杠杆: {LEVERAGE}x\n开仓前账户资金(USDT): {initial_usdt:.4f}\n入场原因: {entry_reason}\n触发周期: {entry_trigger_tf_display}\n15M信号时间: {signal_bar_15m}\n"
               f"开仓条件明细:\n{open_condition_details}"
               f"{order_id_suffix}")
        send_msg(f"ETH交易: 开仓 {side}", msg)
        logging.info(
            f"开仓成功: {side} at {actual_price}, SL: {actual_safe_stop}, "
            f"liq={actual_liquidation_price}, reason={entry_reason}, "
            f"open_order_id={open_order_id}, stop_order_id={trade_state.get('stop_order_id', '')}, stop_meta={actual_stop_meta}"
        )
        return True

    except Exception as e:
        error_msg = f"开仓失败: {e}"
        order_id_lines = format_order_id_lines(open_order_id=open_order_id)
        if order_id_lines:
            error_msg = f"{error_msg}\n{order_id_lines}"
        logging.error(error_msg)
        logging.error(traceback.format_exc())
        # 可以加上邮件通知
        send_msg("ETH交易: ⚠️开仓失败警告", error_msg)
        return False


def close_position(reason, curr_price=None, signal_bar_15m='', trigger_label=''):
    """执行平仓指令"""
    global trade_state
    # 平仓方向与持仓方向相反
    side = 'sell' if trade_state['side'] == 'long' else 'buy'
    close_order_id = ''
    try:  
        if curr_price is None:
            curr_price = get_latest_price()
        # 在主动市价平仓前先撤掉旧的服务端止损单，避免平仓后残留条件单
        cancel_protective_stop_order(silent=True)

        order = exchange.create_market_order(SYMBOL, side, trade_state['amount'])
        close_order_id = extract_order_id(order)
        trade_state['close_order_id'] = close_order_id
        
        # 为了获取绝对准确的净利润，平仓后再次查询账户余额
        # (稍微休眠一下等待交易所结算完成)
        time.sleep(1)
        balance_after = exchange.fetch_balance({'type': 'future'})
        final_usdt = float(balance_after['total']['USDT'])
        
        # 净利润 = 平仓后的总余额 - 开仓前的总余额
        net_pnl_usdt = final_usdt - trade_state['initial_balance']
        
        # 从订单结果中获取实际平仓均价和手续费
        actual_close_price = order.get('average', curr_price)
        if actual_close_price is None or actual_close_price == 0:
            actual_close_price = curr_price
            
        # 计算平仓手续费 (参考 test.py 方式)
        taker_fee_rate = get_trading_fee_rate()
        close_fee = actual_close_price * trade_state['amount'] * taker_fee_rate
        
        # 总手续费 = 开仓手续费 + 平仓手续费
        fee_cost = trade_state['open_fee'] + close_fee
        
        # 计算点数盈亏 (用于展示和记录)
        if trade_state['side'] == 'long':
            pnl_points = actual_close_price - trade_state['entry_price']
        else:
            pnl_points = trade_state['entry_price'] - actual_close_price

        is_profit = net_pnl_usdt > 0
        exit_time = get_server_time_str()
        holding_seconds = compute_holding_seconds(trade_state['entry_time'], exit_time)
        exit_signal_bar_15m = signal_bar_15m
        if not exit_signal_bar_15m:
            logging.warning("平仓时未获取到当前15M信号时间，CSV将写空值")

        entry_signal_bar_15m = trade_state.get('entry_signal_bar_15m', '')
        if exit_signal_bar_15m and entry_signal_bar_15m:
            try:
                exit_signal_dt = datetime.datetime.strptime(exit_signal_bar_15m, BAR_TIME_FORMAT)
                entry_signal_dt = datetime.datetime.strptime(entry_signal_bar_15m, BAR_TIME_FORMAT)
                if exit_signal_dt < entry_signal_dt:
                    logging.warning(
                        f"检测到平仓信号时间倒退: exit={exit_signal_bar_15m}, "
                        f"entry={entry_signal_bar_15m}，已提升到入场信号时间"
                    )
                    exit_signal_bar_15m = entry_signal_bar_15m
            except Exception as e:
                logging.warning(f"平仓信号时间比较失败: {e}")

        open_order_id = trade_state.get('open_order_id', '')
        # 记录到 CSV
        log_trade_to_csv(
            trade_state['entry_time'],
            trade_state['side'],
            trade_state['cond_4h'],
            trade_state['cond_1h'],
            trade_state['cond_15m'],
            trade_state.get('entry_reason', ''),
            exit_time,
            reason,
            round(pnl_points, 4),
            round(fee_cost, 4),
            round(net_pnl_usdt, 4),
            is_profit,
            entry_signal_bar_15m=entry_signal_bar_15m,
            exit_signal_bar_15m=exit_signal_bar_15m,
            exit_trigger=trigger_label or reason,
            holding_seconds=holding_seconds,
            open_order_id=open_order_id,
            close_order_id=close_order_id
        )

        order_id_lines = format_order_id_lines(
            open_order_id=open_order_id,
            close_order_id=close_order_id
        )
        close_cond_4h = trade_state.get('close_cond_4h') or "4H: 无可用平仓条件快照"
        close_cond_1h = trade_state.get('close_cond_1h') or "1H: 无可用平仓条件快照"
        close_cond_15m = trade_state.get('close_cond_15m') or "15M: 无可用平仓条件快照"
        close_condition_lines = []
        for close_cond in (close_cond_4h, close_cond_1h, close_cond_15m):
            if close_cond and '无关键平仓条件' not in close_cond:
                close_condition_lines.append(close_cond)
        close_condition_details = '\n'.join(close_condition_lines) if close_condition_lines else f"触发来源: {trigger_label or reason}"
        order_id_suffix = f"\n{order_id_lines}" if order_id_lines else ''
        msg = (f"🏁 【已平仓】\n原因: {reason}\n入场价: {trade_state['entry_price']}\n"
               f"出场价: {actual_close_price}\n点数盈亏: {pnl_points:.2f}\n手续费: {fee_cost:.4f}\n净利润(USDT): {net_pnl_usdt:.2f}\n"
               f"平仓后账户资金(USDT): {final_usdt:.4f}\n15M信号时间: {exit_signal_bar_15m}\n触发来源: {trigger_label or reason}\n"
               f"平仓条件明细:\n{close_condition_details}"
               f"{order_id_suffix}")
        send_msg(f"ETH交易: 平仓通知", msg)

        post_exit_processed_bar_15m = exit_signal_bar_15m or trade_state.get('last_processed_bar_15m', '')
        trade_state.update({
            'has_position': False,
            'side': None,
            'entry_price': 0,
            'stop_loss_price': 0,
            'highest_price': 0,
            'lowest_price': 0,
            'amount': 0,
            'entry_time': '',
            'cond_4h': '',
            'cond_1h': '',
            'cond_15m': '',
            'close_cond_4h': '',
            'close_cond_1h': '',
            'close_cond_15m': '',
            'entry_reason': '',
            'entry_trigger_tf': '',
            'entry_strategy_tf': '',
            'entry_exit_tf': '',
            'entry_sr_target': 0.0,
            'entry_initial_risk': 0.0,
            'partial_taken': False,
            'last_exit_time': exit_time,
            'initial_balance': 0.0,
            'open_fee': 0.0,
            'open_order_id': '',
            'close_order_id': '',
            'liquidation_price': 0.0,
            'stop_order_id': '',
            'stop_order_price': 0.0,
            'stop_order_refresh_fail_count': 0,
            'last_stop_order_refresh_error': '',
            'entry_signal_bar_15m': '',
            'last_exit_bar_15m': exit_signal_bar_15m,
            'last_processed_bar_15m': post_exit_processed_bar_15m,
            'position_miss_count': 0
        })
        logging.info(
            f"清仓成功: {reason}, PnL: {pnl_points:.2f}, Net USDT: {net_pnl_usdt:.2f}, "
            f"open_order_id={open_order_id}, close_order_id={close_order_id}"
        )
        return True
    except Exception as e:
        logging.error(f"清仓失败: {e}")
        return False


def tf_state_key(prefix, timeframe):
    return f"{prefix}_{timeframe}"


def validate_signal_bar(timeframe, signal_bar, now_dt=None):
    """通用K线新鲜度/去重校验，新版策略以5M为主循环节拍。"""
    if not signal_bar:
        return {'valid': False, 'is_new': False, 'reason': 'empty'}

    signal_dt = parse_bar_time(signal_bar)
    if signal_dt is None:
        return {'valid': False, 'is_new': False, 'reason': 'parse_failed'}

    if now_dt is None:
        now_dt = get_server_now_dt()
    elif now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=EXCHANGE_TZ)
    else:
        now_dt = now_dt.astimezone(EXCHANGE_TZ)

    tf_seconds = TIMEFRAME_SECONDS.get(timeframe)
    if tf_seconds is None:
        return {'valid': False, 'is_new': False, 'reason': f'bad_timeframe:{timeframe}'}

    signal_close_dt = signal_dt + datetime.timedelta(seconds=tf_seconds)
    stale_seconds = (now_dt - signal_close_dt).total_seconds()
    max_stale_seconds = max(3 * tf_seconds, 15 * 60)
    if stale_seconds < -5:
        return {'valid': False, 'is_new': False, 'reason': 'future_bar', 'stale_seconds': stale_seconds}
    if stale_seconds > max_stale_seconds:
        return {'valid': False, 'is_new': False, 'reason': 'stale_bar', 'stale_seconds': stale_seconds}

    max_seen_key = tf_state_key('max_seen_bar', timeframe)
    processed_key = tf_state_key('last_processed_bar', timeframe)
    max_seen_dt = parse_bar_time(trade_state.get(max_seen_key, ''))
    if max_seen_dt is not None and signal_dt < max_seen_dt:
        return {'valid': False, 'is_new': False, 'reason': 'bar_time_rollback', 'stale_seconds': stale_seconds}

    last_processed_dt = parse_bar_time(trade_state.get(processed_key, ''))
    is_new = last_processed_dt is None or signal_dt > last_processed_dt
    if max_seen_dt is None or signal_dt > max_seen_dt:
        trade_state[max_seen_key] = signal_bar

    return {'valid': True, 'is_new': is_new, 'reason': 'ok', 'stale_seconds': stale_seconds}


def get_closed_df(df, timeframe, now_dt=None):
    last_idx = get_last_closed_index(df, timeframe, now_dt=now_dt)
    if last_idx is None:
        return pd.DataFrame()
    return df.iloc[:last_idx + 1].copy()


def is_number(value):
    try:
        return value is not None and not pd.isna(value)
    except Exception:
        return False


def price_to_float(value):
    try:
        return float(value)
    except Exception:
        return 0.0


def precision_price(price):
    try:
        return float(exchange.price_to_precision(SYMBOL, price))
    except Exception:
        return float(price)


def get_price_tick(reference_price=None):
    try:
        if not getattr(exchange, 'markets', None):
            exchange.load_markets()
        market = exchange.market(SYMBOL)
        precision = market.get('precision', {}).get('price')
        if isinstance(precision, int):
            return 10 ** (-precision)
        if isinstance(precision, float) and precision > 0:
            return precision
    except Exception:
        pass
    if reference_price:
        return max(float(reference_price) * 0.00001, 0.01)
    return 0.01


def candle_parts(row):
    open_price = price_to_float(row.get('open'))
    high = price_to_float(row.get('high'))
    low = price_to_float(row.get('low'))
    close = price_to_float(row.get('close'))
    candle_range = max(high - low, 0.0)
    body = abs(close - open_price)
    return open_price, high, low, close, candle_range, body


def strong_candle_check(open_price, high, low, close, atr, direction, body_override=None):
    if not is_number(atr) or atr <= 0:
        return {'ok': False, 'reason': 'bad_atr'}
    candle_range = high - low
    if candle_range <= 0:
        return {'ok': False, 'reason': 'zero_range'}

    raw_body = close - open_price if direction == 'long' else open_price - close
    body = body_override if body_override is not None else raw_body
    if direction == 'long':
        direction_ok = close > open_price
        close_zone_ok = close >= high - candle_range * STRONG_CLOSE_ZONE_RATIO
    else:
        direction_ok = close < open_price
        close_zone_ok = close <= low + candle_range * STRONG_CLOSE_ZONE_RATIO

    body_atr_ok = body >= STRONG_BODY_ATR_RATIO * atr
    body_range_ratio = body / candle_range
    body_range_ok = body_range_ratio >= STRONG_BODY_RANGE_RATIO
    ok = direction_ok and body_atr_ok and close_zone_ok and body_range_ok
    return {
        'ok': ok,
        'direction_ok': direction_ok,
        'body': body,
        'body_atr_ratio': body / atr,
        'close_zone_ok': close_zone_ok,
        'body_range_ratio': body_range_ratio,
        'body_atr_ok': body_atr_ok,
        'body_range_ok': body_range_ok
    }


def is_large_strong_candle(open_price, high, low, close, atr, direction, body_override=None):
    if not is_number(atr) or atr <= 0:
        return False
    candle_range = high - low
    if candle_range < LARGE_STRONG_RANGE_ATR_RATIO * atr:
        return False
    check = strong_candle_check(open_price, high, low, close, atr, direction, body_override=body_override)
    return bool(check.get('direction_ok') and check.get('body_range_ok'))


def simple_strong_candle(row, direction):
    open_price, high, low, close, _, _ = candle_parts(row)
    atr = price_to_float(row.get('atr'))
    large = is_large_strong_candle(open_price, high, low, close, atr, direction)
    check = strong_candle_check(open_price, high, low, close, atr, direction)
    if not check.get('ok') or large:
        return None
    return {
        'kind': 'single',
        'direction': direction,
        'open': open_price,
        'high': high,
        'low': low,
        'close': close,
        'atr': atr,
        'body': check.get('body', 0.0),
        'large': False,
        'bar_time': format_bar_time(row.get('timestamp')),
        'details': check
    }


def latest_large_strong(df_closed):
    if df_closed is None or len(df_closed) == 0:
        return None
    row = df_closed.iloc[-1]
    open_price, high, low, close, _, _ = candle_parts(row)
    atr = price_to_float(row.get('atr'))
    for direction in ('long', 'short'):
        if is_large_strong_candle(open_price, high, low, close, atr, direction):
            return {
                'direction': direction,
                'high': high,
                'low': low,
                'close': close,
                'atr': atr,
                'bar_time': format_bar_time(row.get('timestamp'))
            }
    return None


def standalone_strong_candle_kind(row):
    """单根已经是强K或大强K时，不允许再参与合成强K。"""
    open_price, high, low, close, _, _ = candle_parts(row)
    atr = price_to_float(row.get('atr'))
    for direction in ('long', 'short'):
        if is_large_strong_candle(open_price, high, low, close, atr, direction):
            return {'kind': 'large', 'direction': direction}
        check = strong_candle_check(open_price, high, low, close, atr, direction)
        if check.get('ok'):
            return {'kind': 'single', 'direction': direction}
    return None


def composite_strong_candle(df_closed, end_idx, bars_count, direction):
    if end_idx - bars_count + 1 < 0:
        return None
    segment = df_closed.iloc[end_idx - bars_count + 1:end_idx + 1]
    if len(segment) != bars_count:
        return None
    for _, row in segment.iterrows():
        if standalone_strong_candle_kind(row):
            return None

    if direction == 'long':
        if not all(segment['close'] > segment['open']):
            return None
        first = segment.iloc[0]
        last = segment.iloc[-1]
        open_price = price_to_float(first['open'])
        close = price_to_float(last['close'])
        low = price_to_float(first['low'])
        high = price_to_float(last['high'])
        body = float((segment['close'] - segment['open']).sum())
    else:
        if not all(segment['close'] < segment['open']):
            return None
        first = segment.iloc[0]
        last = segment.iloc[-1]
        open_price = price_to_float(first['open'])
        close = price_to_float(last['close'])
        high = price_to_float(first['high'])
        low = price_to_float(last['low'])
        body = float((segment['open'] - segment['close']).sum())

    if high <= low:
        high = float(segment['high'].max())
        low = float(segment['low'].min())
    atr = price_to_float(segment.iloc[-1].get('atr'))
    if (not is_number(atr) or atr <= 0) and 'atr' in segment.columns and segment['atr'].notna().any():
        atr = price_to_float(segment['atr'].dropna().mean())

    large = is_large_strong_candle(open_price, high, low, close, atr, direction, body_override=body)
    check = strong_candle_check(open_price, high, low, close, atr, direction, body_override=body)
    if not check.get('ok') or large:
        return None
    return {
        'kind': f'synthetic_{bars_count}',
        'direction': direction,
        'open': open_price,
        'high': high,
        'low': low,
        'close': close,
        'atr': atr,
        'body': body,
        'large': False,
        'bar_time': format_bar_time(last.get('timestamp')),
        'details': check
    }


def latest_effective_strong(df_closed, direction):
    if df_closed is None or len(df_closed) == 0:
        return None
    end_idx = len(df_closed) - 1
    single = simple_strong_candle(df_closed.iloc[end_idx], direction)
    if single:
        return single
    for bars_count in range(SYNTHETIC_STRONG_MIN_BARS, SYNTHETIC_STRONG_MAX_BARS + 1):
        synthetic = composite_strong_candle(df_closed, end_idx, bars_count, direction)
        if synthetic:
            return synthetic
    return None


def historical_effective_strong(df_closed, end_idx, direction, include_synthetic=True):
    single = simple_strong_candle(df_closed.iloc[end_idx], direction)
    if single:
        return single
    if not include_synthetic:
        return None
    for bars_count in range(SYNTHETIC_STRONG_MIN_BARS, SYNTHETIC_STRONG_MAX_BARS + 1):
        synthetic = composite_strong_candle(df_closed, end_idx, bars_count, direction)
        if synthetic:
            return synthetic
    return None


def find_previous_opposite_strong(df_closed, side, lookback=OPPOSITE_STRONG_LOOKBACK):
    if df_closed is None or len(df_closed) < 3:
        return None
    opposite = 'short' if side == 'long' else 'long'
    end_idx = len(df_closed) - 2
    start_idx = max(0, end_idx - lookback + 1)
    for idx in range(end_idx, start_idx - 1, -1):
        strong = historical_effective_strong(df_closed, idx, opposite)
        if strong:
            return strong
    return None


def count_ema_crosses(df_closed):
    recent = df_closed.tail(EMA_CROSS_LOOKBACK)
    up_cross = 0
    down_cross = 0
    for _, row in recent.iterrows():
        open_val = price_to_float(row.get('open'))
        close_val = price_to_float(row.get('close'))
        ema20_val = price_to_float(row.get('ema20'))
        if not is_number(open_val) or not is_number(close_val) or not is_number(ema20_val):
            continue
        if open_val < ema20_val and close_val > ema20_val:
            up_cross += 1
        elif open_val > ema20_val and close_val < ema20_val:
            down_cross += 1
    return up_cross, down_cross


def recent_persistent_trend_break(df_closed, persistent_direction):
    if df_closed is None or len(df_closed) < 4:
        return False
    recent = df_closed.tail(3)
    if persistent_direction == 'short':
        bullish = recent[recent['close'] > recent['open']]
        if len(bullish) >= 2 and bool((bullish['close'] > bullish['ema20']).any()):
            return True
        for offset in range(3, 1, -1):
            idx = len(df_closed) - offset
            if idx < 0 or idx + 2 >= len(df_closed):
                continue
            strong = historical_effective_strong(df_closed, idx, 'long')
            if strong and strong['close'] > price_to_float(df_closed.iloc[idx]['ema20']):
                follows = df_closed.iloc[idx + 1:idx + 3]
                if len(follows) == 2 and bool((follows['close'] > follows['ema20']).all()):
                    return True
    else:
        bearish = recent[recent['close'] < recent['open']]
        if len(bearish) >= 2 and bool((bearish['close'] < bearish['ema20']).any()):
            return True
        for offset in range(3, 1, -1):
            idx = len(df_closed) - offset
            if idx < 0 or idx + 2 >= len(df_closed):
                continue
            strong = historical_effective_strong(df_closed, idx, 'short')
            if strong and strong['close'] < price_to_float(df_closed.iloc[idx]['ema20']):
                follows = df_closed.iloc[idx + 1:idx + 3]
                if len(follows) == 2 and bool((follows['close'] < follows['ema20']).all()):
                    return True
    return False


def recent_ema_persistence(df_closed):
    if df_closed is None or len(df_closed) < EMA_PERSISTENCE_BARS + 3:
        return {'direction': None, 'first_break': False, 'broken': False}

    prior = df_closed.iloc[-EMA_PERSISTENCE_BARS - 1:-1]
    last = df_closed.iloc[-1]
    if len(prior) < EMA_PERSISTENCE_BARS or prior['ema20'].isna().any():
        return {'direction': None, 'first_break': False, 'broken': False}

    above_20 = bool((prior['low'] >= prior['ema20']).all())
    below_20 = bool((prior['high'] <= prior['ema20']).all())
    if above_20:
        first_break = is_number(last.get('ema20')) and last['low'] < last['ema20']
        trend_broken = recent_persistent_trend_break(df_closed, 'long')
        return {'direction': 'long', 'first_break': bool(first_break), 'broken': trend_broken}
    if below_20:
        first_break = is_number(last.get('ema20')) and last['high'] > last['ema20']
        trend_broken = recent_persistent_trend_break(df_closed, 'short')
        return {'direction': 'short', 'first_break': bool(first_break), 'broken': trend_broken}
    return {'direction': None, 'first_break': False, 'broken': False}


def detect_strong_chop(df_closed):
    if df_closed is None or len(df_closed) < STRONG_CHOP_LOOKBACK_BARS:
        return {'is_chop': False}

    def count_strong_hits(start_idx, end_idx):
        long_hits = 0
        short_hits = 0
        for idx in range(start_idx, end_idx):
            if historical_effective_strong(df_closed, idx, 'long', include_synthetic=False):
                long_hits += 1
            if historical_effective_strong(df_closed, idx, 'short', include_synthetic=False):
                short_hits += 1
        return long_hits, short_hits

    def make_result(is_chop, zone_window, long_hits, short_hits, resolved=False, breakout_side='none', zone_source='recent'):
        return {
            'is_chop': is_chop,
            'resolved': resolved,
            'zone_high': float(zone_window['high'].max()),
            'zone_low': float(zone_window['low'].min()),
            'long_hits': long_hits,
            'short_hits': short_hits,
            'breakout_side': breakout_side,
            'zone_source': zone_source
        }

    last_idx = len(df_closed) - 1
    last_close = float(df_closed.iloc[last_idx]['close'])

    # 先用上一段强K震荡区间判断当前收盘是否突破，避免当前K的高低点把突破条件卡死。
    if len(df_closed) > STRONG_CHOP_LOOKBACK_BARS:
        previous_start_idx = len(df_closed) - STRONG_CHOP_LOOKBACK_BARS - 1
        previous_end_idx = len(df_closed) - 1
        previous = df_closed.iloc[previous_start_idx:previous_end_idx]
        long_hits, short_hits = count_strong_hits(previous_start_idx, previous_end_idx)
        if long_hits > 0 and short_hits > 0:
            zone_high = float(previous['high'].max())
            zone_low = float(previous['low'].min())
            if last_close > zone_high:
                return make_result(
                    False, previous, long_hits, short_hits,
                    resolved=True, breakout_side='up', zone_source='previous'
                )
            if last_close < zone_low:
                return make_result(
                    False, previous, long_hits, short_hits,
                    resolved=True, breakout_side='down', zone_source='previous'
                )
            return make_result(True, previous, long_hits, short_hits, zone_source='previous')

    recent = df_closed.tail(STRONG_CHOP_LOOKBACK_BARS)
    start_idx = len(df_closed) - len(recent)
    long_hits, short_hits = count_strong_hits(start_idx, len(df_closed))
    if long_hits <= 0 or short_hits <= 0:
        return {'is_chop': False}
    return make_result(True, recent, long_hits, short_hits, zone_source='recent')


def evaluate_adx_ema_context(df, timeframe, now_dt=None):
    df_closed = get_closed_df(df, timeframe, now_dt=now_dt)
    base = {
        'timeframe': timeframe,
        'signal_bar_time': '',
        'direction': None,
        'status': 'no_data',
        'can_open_long': False,
        'can_open_short': False,
        'long_trend': False,
        'short_trend': False,
        'pullback_long': False,
        'pullback_short': False,
        'close_long': False,
        'close_short': False,
        'is_oscillation': True,
        'summary': '',
        'details': {}
    }
    min_len = max(ADX_LENGTH + 2, EMA_TREND_LENGTH + EMA_SLOPE_LOOKBACK + 2, EMA_CROSS_LOOKBACK + 2)
    if len(df_closed) < min_len:
        base['summary'] = f"{timeframe}: 数据不足"
        return base

    last = df_closed.iloc[-1]
    prev = df_closed.iloc[-2]
    ema_ref = df_closed.iloc[-1 - EMA_SLOPE_LOOKBACK]
    base['signal_bar_time'] = format_bar_time(last.get('timestamp'))

    adx = price_to_float(last.get('adx'))
    prev_adx = price_to_float(prev.get('adx'))
    plus_di = price_to_float(last.get('plus_di'))
    minus_di = price_to_float(last.get('minus_di'))
    ema20 = price_to_float(last.get('ema20'))
    ema20_ref = price_to_float(ema_ref.get('ema20'))
    up_cross, down_cross = count_ema_crosses(df_closed)
    ema_clean = up_cross <= 1 and down_cross <= 1
    ema_up = ema20 > ema20_ref
    ema_down = ema20 < ema20_ref
    adx_rising = adx > prev_adx
    adx_falling = adx < prev_adx
    extreme = adx > ADX_EXTREME
    chop = detect_strong_chop(df_closed)
    persistence = recent_ema_persistence(df_closed) if timeframe in ('15m', '1h', '4h') else {'direction': None}

    base['details'] = {
        'adx': adx,
        'prev_adx': prev_adx,
        'plus_di': plus_di,
        'minus_di': minus_di,
        'ema20': ema20,
        'ema20_ref': ema20_ref,
        'up_cross': up_cross,
        'down_cross': down_cross,
        'ema_clean': ema_clean,
        'ema_persistence': persistence,
        'strong_chop': chop,
        'extreme_adx': extreme
    }

    if chop.get('is_chop'):
        base['status'] = 'strong_chop'
        base['summary'] = f"{timeframe}: 强K交叉震荡 zone={chop.get('zone_low'):.4f}-{chop.get('zone_high'):.4f}"
        return base
    if adx <= 20:
        base['status'] = 'range'
        base['summary'] = f"{timeframe}: ADX={adx:.2f} 震荡，禁止开仓"
        return base
    if adx <= ADX_NO_TRADE_MAX:
        base['status'] = 'transition'
        base['summary'] = f"{timeframe}: ADX={adx:.2f} 过渡，等待方向确认"
        return base

    direction = None
    status = 'unclear'
    can_open_long = False
    can_open_short = False
    if plus_di > minus_di and ema_up and ema_clean:
        direction = 'long'
        if adx_rising:
            status = 'long'
            can_open_long = True
        elif adx_falling:
            status = 'long_weakening'
        else:
            status = 'long_flat_adx'
    elif minus_di > plus_di and ema_down and ema_clean:
        direction = 'short'
        if adx_rising:
            status = 'short'
            can_open_short = True
        elif adx_falling:
            status = 'short_weakening'
        else:
            status = 'short_flat_adx'

    if direction is None and persistence.get('direction') and not persistence.get('broken'):
        direction = persistence['direction']
        status = f"ema20_{direction}_persistence"
        if direction == 'long':
            can_open_long = True
        else:
            can_open_short = True

    base['direction'] = direction
    base['status'] = status
    base['can_open_long'] = can_open_long
    base['can_open_short'] = can_open_short
    base['long_trend'] = direction == 'long' and can_open_long
    base['short_trend'] = direction == 'short' and can_open_short
    base['pullback_long'] = direction == 'long' and status in ('long_weakening', 'long_flat_adx')
    base['pullback_short'] = direction == 'short' and status in ('short_weakening', 'short_flat_adx')
    base['is_oscillation'] = direction is None
    base['summary'] = (
        f"{timeframe}: status={status}, dir={direction}, ADX={adx:.2f}, "
        f"+DI={plus_di:.2f}, -DI={minus_di:.2f}, EMA clean={ema_clean}, "
        f"cross={up_cross}/{down_cross}, extreme={extreme}"
    )
    return base


def context_allows_side(local_state, background_state, side):
    if not local_state or not background_state:
        return False, '缺少趋势状态'
    opposite = 'short' if side == 'long' else 'long'
    if local_state.get('is_oscillation'):
        return False, f"{local_state.get('timeframe')} 本级别震荡/趋势不明"
    if local_state.get('direction') == opposite:
        return False, f"{local_state.get('timeframe')} 本级别方向相反"
    if background_state.get('direction') == opposite:
        return False, f"{background_state.get('timeframe')} 背景趋势相反"
    if background_state.get('status') in ('range', 'transition', 'strong_chop', 'unclear', 'no_data'):
        return False, f"{background_state.get('timeframe')} 背景趋势不允许开仓: {background_state.get('status')}"

    local_open_key = 'can_open_long' if side == 'long' else 'can_open_short'
    if not local_state.get(local_open_key) and local_state.get('direction') != side:
        return False, f"{local_state.get('timeframe')} 本级别未给出{side}方向"

    bg_same_side = background_state.get('direction') == side
    bg_open_key = 'can_open_long' if side == 'long' else 'can_open_short'
    bg_soft_allow = background_state.get('status') in (
        f'{side}_weakening',
        f'{side}_flat_adx',
        f'ema20_{side}_persistence'
    )
    if not bg_same_side or (not background_state.get(bg_open_key) and not bg_soft_allow):
        return False, f"{background_state.get('timeframe')} 背景未确认{side}"
    return True, 'ok'


def detect_entry_trigger(df, timeframe, side, now_dt=None):
    df_closed = get_closed_df(df, timeframe, now_dt=now_dt)
    if len(df_closed) < max(ADX_LENGTH + 3, SYNTHETIC_STRONG_MAX_BARS + 2):
        return None

    last = df_closed.iloc[-1]
    tick = get_price_tick(last.get('close'))
    large = latest_large_strong(df_closed)
    if large:
        return {
            'blocked': True,
            'reason': f"最新{timeframe}为大强{large['direction']}线，过滤不开仓",
            'large': large
        }

    strong = latest_effective_strong(df_closed, side)
    if strong:
        if side == 'long':
            entry_level = strong['high'] + tick
            stop = strong['low'] - tick
        else:
            entry_level = strong['low'] - tick
            stop = strong['high'] + tick
        return {
            'blocked': False,
            'type': 'strong_candle',
            'side': side,
            'timeframe': timeframe,
            'entry_level': precision_price(entry_level),
            'stop': precision_price(stop),
            'trigger': strong,
            'reason': f"{timeframe}最新{strong['kind']}强{'阳' if side == 'long' else '阴'}线"
        }

    previous = find_previous_opposite_strong(df_closed, side)
    if previous is None:
        return None
    atr = price_to_float(last.get('atr'))
    close = price_to_float(last.get('close'))
    high = price_to_float(last.get('high'))
    low = price_to_float(last.get('low'))
    if side == 'long':
        level = previous['high']
        broke = close > level and high > level
        stop = close - ENTRY_ATR_STOP_MULTIPLIER * atr
        entry_level = level + tick
    else:
        level = previous['low']
        broke = close < level and low < level
        stop = close + ENTRY_ATR_STOP_MULTIPLIER * atr
        entry_level = level - tick
    if not broke:
        return None
    return {
        'blocked': False,
        'type': 'opposite_strong_break',
        'side': side,
        'timeframe': timeframe,
        'entry_level': precision_price(entry_level),
        'stop': precision_price(stop),
        'trigger': previous,
        'reason': f"{timeframe}收盘突破前强{'阴' if side == 'long' else '阳'}线关键位"
    }


def zone_distance(zone, price):
    if zone['lower'] <= price <= zone['upper']:
        return 0.0
    return min(abs(price - zone['lower']), abs(price - zone['upper']))


def add_or_merge_zone(zones, zone_type, price, atr, source_idx):
    merge_distance = SUP_RES_MERGE_ATR * atr
    buffer = SUP_RES_ZONE_BUFFER_ATR * atr
    matched = None
    for zone in [z for z in zones if z['type'] == zone_type]:
        if zone_distance(zone, price) <= merge_distance:
            matched = zone
            break
    if matched:
        matched['lower'] = min(matched['lower'], price) - buffer
        matched['upper'] = max(matched['upper'], price) + buffer
        matched['touches'] = 0
        matched['created_idx'] = source_idx
        matched['valid'] = True
        return matched

    zone = {
        'type': zone_type,
        'lower': price - buffer,
        'upper': price + buffer,
        'touches': 0,
        'created_idx': source_idx,
        'valid': True
    }
    zones.append(zone)
    return zone


def is_swing_low(df_closed, idx, window):
    current = df_closed.iloc[idx]['low']
    for offset in range(1, window + 1):
        if current >= df_closed.iloc[idx - offset]['low'] or current >= df_closed.iloc[idx + offset]['low']:
            return False
    return True


def is_swing_high(df_closed, idx, window):
    current = df_closed.iloc[idx]['high']
    for offset in range(1, window + 1):
        if current <= df_closed.iloc[idx - offset]['high'] or current <= df_closed.iloc[idx + offset]['high']:
            return False
    return True


def rebuild_zone_touches(zones, df_closed):
    active = []
    for zone in zones:
        zone = dict(zone)
        zone['touches'] = 0
        zone['valid'] = True
        start_idx = max(0, int(zone.get('created_idx', 0)))
        for idx in range(start_idx, len(df_closed)):
            row = df_closed.iloc[idx]
            close = price_to_float(row['close'])
            if zone['type'] == 'support':
                if close < zone['lower']:
                    zone['valid'] = False
                    break
                if row['low'] <= zone['upper'] and close >= zone['lower']:
                    zone['touches'] += 1
            else:
                if close > zone['upper']:
                    zone['valid'] = False
                    break
                if row['high'] >= zone['lower'] and close <= zone['upper']:
                    zone['touches'] += 1
        if zone['valid'] and zone['touches'] >= SUP_RES_VALID_TOUCHES:
            active.append(zone)
    return active


def recently_broken_zones(zones, df_closed):
    broken = {'broken_support': [], 'broken_resistance': []}
    for raw_zone in zones:
        zone = dict(raw_zone)
        touches = 0
        invalid_idx = None
        start_idx = max(0, int(zone.get('created_idx', 0)))
        for idx in range(start_idx, len(df_closed)):
            row = df_closed.iloc[idx]
            close = price_to_float(row['close'])
            if zone['type'] == 'support':
                if close < zone['lower']:
                    invalid_idx = idx
                    break
                if row['low'] <= zone['upper'] and close >= zone['lower']:
                    touches += 1
            else:
                if close > zone['upper']:
                    invalid_idx = idx
                    break
                if row['high'] >= zone['lower'] and close <= zone['upper']:
                    touches += 1
        if touches < SUP_RES_VALID_TOUCHES or invalid_idx is None:
            continue
        if invalid_idx < len(df_closed) - 3:
            continue
        zone['touches'] = touches
        zone['invalidated_idx'] = invalid_idx
        if zone['type'] == 'support':
            broken['broken_support'].append(zone)
        else:
            broken['broken_resistance'].append(zone)
    return broken


def build_support_resistance_zones(df_4h, now_dt=None):
    df_closed = get_closed_df(df_4h, '4h', now_dt=now_dt)
    if len(df_closed) > SUP_RES_LOOKBACK_4H:
        df_closed = df_closed.tail(SUP_RES_LOOKBACK_4H).reset_index(drop=True)
    if len(df_closed) < SUP_RES_SWING_WINDOW * 2 + 20:
        return {'support': [], 'resistance': []}

    zones = []
    for idx in range(SUP_RES_SWING_WINDOW, len(df_closed) - SUP_RES_SWING_WINDOW):
        atr = price_to_float(df_closed.iloc[idx].get('atr'))
        if atr <= 0:
            continue
        if is_swing_low(df_closed, idx, SUP_RES_SWING_WINDOW):
            add_or_merge_zone(zones, 'support', price_to_float(df_closed.iloc[idx]['low']), atr, idx)
        if is_swing_high(df_closed, idx, SUP_RES_SWING_WINDOW):
            add_or_merge_zone(zones, 'resistance', price_to_float(df_closed.iloc[idx]['high']), atr, idx)

    active = rebuild_zone_touches(zones, df_closed)
    broken = recently_broken_zones(zones, df_closed)
    support = sorted([z for z in active if z['type'] == 'support'], key=lambda z: z['upper'], reverse=True)
    resistance = sorted([z for z in active if z['type'] == 'resistance'], key=lambda z: z['lower'])
    return {
        'support': support,
        'resistance': resistance,
        'broken_support': broken['broken_support'],
        'broken_resistance': broken['broken_resistance']
    }


def nearest_opposite_zone(zones, side, price):
    if side == 'long':
        candidates = [z for z in zones.get('resistance', []) if z['lower'] > price]
        return min(candidates, key=lambda z: z['lower']) if candidates else None
    candidates = [z for z in zones.get('support', []) if z['upper'] < price]
    return max(candidates, key=lambda z: z['upper']) if candidates else None


def target_from_zones_or_rr(zones, side, entry, stop):
    opposite = nearest_opposite_zone(zones, side, entry)
    if opposite:
        return opposite['lower'] if side == 'long' else opposite['upper'], opposite
    risk = abs(entry - stop)
    if side == 'long':
        return entry + 2 * risk, None
    return entry - 2 * risk, None


def expected_profit_ok(side, entry, stop, target, atr):
    if entry <= 0 or stop <= 0 or target <= 0:
        return False, '价格数据无效'
    if side == 'long':
        profit_distance = target - entry
        risk = entry - stop
    else:
        profit_distance = entry - target
        risk = stop - entry
    if profit_distance <= 0 or risk <= 0:
        return False, '盈亏方向无效'
    fee_rate = 0.0004
    min_fee_price = entry * fee_rate * MIN_EXPECTED_PROFIT_FEE_MULTIPLIER
    min_atr_price = atr * MIN_EXPECTED_PROFIT_ATR if atr and atr > 0 else 0
    min_required = max(min_fee_price, min_atr_price)
    if profit_distance < min_required:
        return False, f"预期空间过小 profit={profit_distance:.4f}, required={min_required:.4f}"
    return True, 'ok'


def find_1h_sr_confirmation(df_1h, zone, side, now_dt=None):
    df_closed = get_closed_df(df_1h, '1h', now_dt=now_dt)
    if len(df_closed) < 3:
        return None
    recent = df_closed.tail(SR_CONFIRM_LOOKBACK_1H)
    touched = False
    for _, row in recent.iterrows():
        open_price, high, low, close, _, _ = candle_parts(row)
        body = abs(close - open_price)
        if side == 'long':
            if low <= zone['upper'] and close >= zone['lower']:
                touched = True
            lower_wick = min(open_price, close) - low
            confirm = touched and close > open_price and close > zone['upper'] and lower_wick <= body * 0.5
            if confirm:
                return {'high': high, 'low': low, 'timestamp': row.get('timestamp'), 'bar_time': format_bar_time(row.get('timestamp')), 'zone': zone}
        else:
            if high >= zone['lower'] and close <= zone['upper']:
                touched = True
            upper_wick = high - max(open_price, close)
            confirm = touched and close < open_price and close < zone['lower'] and upper_wick <= body * 0.5
            if confirm:
                return {'high': high, 'low': low, 'timestamp': row.get('timestamp'), 'bar_time': format_bar_time(row.get('timestamp')), 'zone': zone}
    return None


def sr_15m_entry_ready(df_15m, confirm, side, now_dt=None):
    df_closed = get_closed_df(df_15m, '15m', now_dt=now_dt)
    if len(df_closed) == 0 or confirm is None:
        return False
    observed = df_closed[df_closed['timestamp'] >= confirm['timestamp']].tail(SR_ENTRY_LOOKBACK_15M)
    if len(observed) == 0:
        return False
    if side == 'long':
        if bool((observed['low'] < confirm['low']).any()):
            return False
        return price_to_float(observed.iloc[-1]['close']) > confirm['high']
    if bool((observed['high'] > confirm['high']).any()):
        return False
    return price_to_float(observed.iloc[-1]['close']) < confirm['low']


def build_sr_rebound_candidates(context, side):
    zones = context['zones']
    df_4h_closed = get_closed_df(context['dfs']['4h'], '4h', now_dt=context['now_dt'])
    if len(df_4h_closed) == 0:
        return []
    atr_4h = price_to_float(df_4h_closed.iloc[-1].get('atr'))
    if atr_4h <= 0:
        return []

    zone_list = zones.get('support' if side == 'long' else 'resistance', [])
    candidates = []
    for zone in zone_list[:5]:
        confirm = find_1h_sr_confirmation(context['dfs']['1h'], zone, side, now_dt=context['now_dt'])
        if not sr_15m_entry_ready(context['dfs']['15m'], confirm, side, now_dt=context['now_dt']):
            continue
        entry_ref = price_to_float(get_closed_df(context['dfs']['15m'], '15m', context['now_dt']).iloc[-1]['close'])
        tick = get_price_tick(entry_ref)
        if side == 'long':
            stop = min(zone['lower'] - SUP_RES_STOP_BUFFER_ATR * atr_4h, confirm['low'] - tick)
        else:
            stop = max(zone['upper'] + SUP_RES_STOP_BUFFER_ATR * atr_4h, confirm['high'] + tick)
        target, target_zone = target_from_zones_or_rr(zones, side, entry_ref, stop)
        ok, reason = expected_profit_ok(side, entry_ref, stop, target, atr_4h)
        if not ok:
            logging.info(f"跳过SR反弹{side}: {reason}")
            continue
        candidates.append({
            'module': 'sr_rebound',
            'strategy_tf': '15m',
            'trigger_tf': '15m',
            'exit_tf': '5m',
            'side': side,
            'entry_level': entry_ref,
            'stop': precision_price(stop),
            'target': precision_price(target),
            'target_zone': target_zone,
            'reason': f"4H有效{'支撑' if side == 'long' else '阻力'} + 1H确认 + 15M入场",
            'confirm': confirm,
            'state_summary': f"SR {side}: zone={zone['lower']:.4f}-{zone['upper']:.4f}, confirm={confirm['bar_time']}"
        })
    return candidates


def build_sr_breakout_candidates(context, side):
    zones = context['zones']
    df_4h_closed = get_closed_df(context['dfs']['4h'], '4h', now_dt=context['now_dt'])
    if len(df_4h_closed) < 3:
        return []
    atr_4h = price_to_float(df_4h_closed.iloc[-1].get('atr'))
    if atr_4h <= 0:
        return []
    recent3 = df_4h_closed.tail(3)
    candidates = []
    if side == 'long':
        for zone in zones.get('broken_resistance', [])[:5]:
            if recent3.iloc[0]['close'] > zone['upper'] and bool((recent3.iloc[1:]['close'] >= zone['upper']).all()):
                entry_ref = price_to_float(recent3.iloc[-1]['close'])
                stop = zone['upper'] - SUP_RES_STOP_BUFFER_ATR * atr_4h
                target, target_zone = target_from_zones_or_rr(zones, side, entry_ref, stop)
                candidates.append({'module': 'sr_breakout', 'strategy_tf': '4h', 'trigger_tf': '4h', 'exit_tf': '1h', 'side': side, 'entry_level': entry_ref, 'stop': precision_price(stop), 'target': precision_price(target), 'target_zone': target_zone, 'reason': "4H有效阻力突破并连续2根收盘守住", 'state_summary': f"SR breakout long: previous_res={zone['lower']:.4f}-{zone['upper']:.4f}"})
    else:
        for zone in zones.get('broken_support', [])[:5]:
            if recent3.iloc[0]['close'] < zone['lower'] and bool((recent3.iloc[1:]['close'] <= zone['lower']).all()):
                entry_ref = price_to_float(recent3.iloc[-1]['close'])
                stop = zone['lower'] + SUP_RES_STOP_BUFFER_ATR * atr_4h
                target, target_zone = target_from_zones_or_rr(zones, side, entry_ref, stop)
                candidates.append({'module': 'sr_breakout', 'strategy_tf': '4h', 'trigger_tf': '4h', 'exit_tf': '1h', 'side': side, 'entry_level': entry_ref, 'stop': precision_price(stop), 'target': precision_price(target), 'target_zone': target_zone, 'reason': "4H有效支撑跌破并连续2根收盘压住", 'state_summary': f"SR breakout short: previous_sup={zone['lower']:.4f}-{zone['upper']:.4f}"})
    return candidates


def make_empty_state(timeframe, summary=''):
    return {'timeframe': timeframe, 'signal_bar_time': '', 'long_trend': False, 'short_trend': False, 'pullback_long': False, 'pullback_short': False, 'close_long': False, 'close_short': False, 'summary': summary, 'details': {}}


def make_state_summary(context, timeframe, side, summary):
    state = dict(context['trend_states'].get(timeframe, make_empty_state(timeframe)))
    state['summary'] = summary or state.get('summary', '')
    if side == 'long':
        state['long_trend'] = True
    else:
        state['short_trend'] = True
    return state


def candidate_priority(candidate):
    tf_rank = {'4h': 0, '1h': 1, '15m': 2}
    module_rank = {'sr_breakout': 0, 'sr_rebound': 1, 'trend_trigger': 2}
    return (tf_rank.get(candidate.get('strategy_tf'), 9), module_rank.get(candidate.get('module'), 9))


def candidate_entry_price_still_valid(candidate, curr_price):
    side = candidate.get('side')
    entry_level = price_to_float(candidate.get('entry_level'))
    if entry_level <= 0:
        return False, f"候选入场价无效: module={candidate.get('module')}, entry_level={candidate.get('entry_level')}"
    if side == 'long' and curr_price < entry_level:
        return False, f"多单触发价已失效: module={candidate.get('module')}, current={curr_price}, entry_level={entry_level}"
    if side == 'short' and curr_price > entry_level:
        return False, f"空单触发价已失效: module={candidate.get('module')}, current={curr_price}, entry_level={entry_level}"
    return True, 'ok'


def build_trend_trigger_candidates(context):
    candidates = []
    zones = context['zones']
    for strategy_tf in STRATEGY_TIMEFRAMES:
        local_state = context['trend_states'].get(strategy_tf)
        background_tf = STRATEGY_BACKGROUND_TF[strategy_tf]
        background_state = context['trend_states'].get(background_tf)
        trigger_tf = STRATEGY_TRIGGER_TF[strategy_tf]
        trigger_df = context['dfs'][trigger_tf]

        for side in ('long', 'short'):
            allowed, deny_reason = context_allows_side(local_state, background_state, side)
            if not allowed:
                logging.info(f"跳过{strategy_tf}->{trigger_tf} {side}: {deny_reason}")
                continue
            if strategy_tf == '15m':
                trigger_chop = detect_strong_chop(get_closed_df(trigger_df, trigger_tf, context['now_dt']))
                if trigger_chop.get('is_chop'):
                    logging.info(f"跳过15m策略{side}: 5m强K交叉震荡未突破")
                    continue

            trigger = detect_entry_trigger(trigger_df, trigger_tf, side, now_dt=context['now_dt'])
            if not trigger:
                continue
            if trigger.get('blocked'):
                logging.info(trigger.get('reason'))
                continue

            entry_ref = price_to_float(get_closed_df(trigger_df, trigger_tf, context['now_dt']).iloc[-1]['close'])
            stop = price_to_float(trigger['stop'])
            if background_state.get('details', {}).get('extreme_adx'):
                bg_closed = get_closed_df(context['dfs'][background_tf], background_tf, context['now_dt'])
                atr = price_to_float(bg_closed.iloc[-1].get('atr')) if len(bg_closed) else 0.0
                if atr > 0:
                    if side == 'long':
                        stop = max(stop, entry_ref - EXTREME_ADX_STOP_BUFFER_ATR * atr)
                    else:
                        stop = min(stop, entry_ref + EXTREME_ADX_STOP_BUFFER_ATR * atr)
            target, target_zone = target_from_zones_or_rr(zones, side, entry_ref, stop)
            trigger_atr = price_to_float(get_closed_df(trigger_df, trigger_tf, context['now_dt']).iloc[-1].get('atr'))
            ok, reason = expected_profit_ok(side, entry_ref, stop, target, trigger_atr)
            if not ok:
                logging.info(f"跳过趋势触发{strategy_tf}->{trigger_tf} {side}: {reason}")
                continue

            candidates.append({
                'module': 'trend_trigger',
                'strategy_tf': strategy_tf,
                'trigger_tf': trigger_tf,
                'exit_tf': STRATEGY_EXIT_TF[strategy_tf],
                'side': side,
                'entry_level': trigger['entry_level'],
                'stop': precision_price(stop),
                'target': precision_price(target),
                'target_zone': target_zone,
                'reason': f"{strategy_tf}趋势 + {trigger['reason']}",
                'state_summary': f"{strategy_tf}/{background_tf}/{trigger_tf}: {trigger['reason']}; bg={background_state.get('status')}",
                'trigger': trigger
            })
    return candidates


def build_entry_candidates(context):
    candidates = build_trend_trigger_candidates(context)
    for side in ('long', 'short'):
        allowed, deny_reason = context_allows_side(context['trend_states'].get('15m'), context['trend_states'].get('1h'), side)
        if allowed:
            candidates.extend(build_sr_rebound_candidates(context, side))
        else:
            logging.info(f"跳过SR反弹{side}: {deny_reason}")

        allowed, deny_reason = context_allows_side(context['trend_states'].get('4h'), context['trend_states'].get('1d'), side)
        if allowed:
            candidates.extend(build_sr_breakout_candidates(context, side))
        else:
            logging.info(f"跳过SR突破{side}: {deny_reason}")

    candidates.sort(key=candidate_priority)
    return candidates


def build_context(dfs, now_dt):
    trend_states = {
        timeframe: evaluate_adx_ema_context(df, timeframe, now_dt=now_dt)
        for timeframe, df in dfs.items()
        if timeframe in ('15m', '1h', '4h', '1d')
    }
    return {'dfs': dfs, 'now_dt': now_dt, 'trend_states': trend_states, 'zones': build_support_resistance_zones(dfs['4h'], now_dt=now_dt)}


def is_in_post_exit_cooldown(now_dt):
    last_exit_dt = parse_bar_time(trade_state.get('last_exit_time', ''))
    if last_exit_dt is None:
        return False
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=EXCHANGE_TZ)
    else:
        now_dt = now_dt.astimezone(EXCHANGE_TZ)
    return (now_dt - last_exit_dt).total_seconds() < POST_EXIT_COOLDOWN_SECONDS


def update_stop_if_tighter(side, new_stop, reason, curr_price, signal_bar_15m=''):
    current_stop = price_to_float(trade_state.get('stop_loss_price'))
    if new_stop <= 0:
        return False
    if side == 'long':
        if new_stop <= current_stop or new_stop >= curr_price:
            return False
    else:
        if (current_stop > 0 and new_stop >= current_stop) or new_stop <= curr_price:
            return False
    new_stop = precision_price(new_stop)
    if refresh_protective_stop_order(new_stop):
        trade_state['stop_loss_price'] = new_stop
        logging.info(f"{reason}: 已收紧服务端止损到 {new_stop}")
        return True
    handle_stop_order_refresh_failure(reason, curr_price, signal_bar_15m=signal_bar_15m, trigger_label=reason)
    return False


def close_partial_position(reason, ratio=0.5, curr_price=None, signal_bar_15m=''):
    """第一目标位半仓止盈，并将剩余仓位止损推到成本附近。"""
    if trade_state.get('partial_taken') or not trade_state.get('has_position'):
        return False
    side = trade_state.get('side')
    total_amount = price_to_float(trade_state.get('amount'))
    if total_amount <= 0:
        return False
    if curr_price is None:
        curr_price = get_latest_price()

    close_amount = total_amount * ratio
    try:
        close_amount = float(exchange.amount_to_precision(SYMBOL, close_amount))
    except Exception:
        close_amount = float(close_amount)
    if close_amount <= 0 or close_amount >= total_amount:
        return False

    close_side = 'sell' if side == 'long' else 'buy'
    try:
        cancel_protective_stop_order(silent=True)
        order = exchange.create_market_order(SYMBOL, close_side, close_amount)
        actual_close_price = order.get('average', curr_price) or curr_price
        taker_fee_rate = get_trading_fee_rate()
        partial_fee = float(actual_close_price) * close_amount * taker_fee_rate
        updated_open_fee = price_to_float(trade_state.get('open_fee')) + partial_fee
        remaining_amount_raw = max(total_amount - close_amount, 0.0)
        remaining_amount = remaining_amount_raw
        try:
            remaining_amount = float(exchange.amount_to_precision(SYMBOL, remaining_amount))
        except Exception:
            remaining_amount = float(remaining_amount)

        position_after_partial = get_position_risk(side=side)
        if position_after_partial and not position_after_partial.get('fetch_failed'):
            exchange_remaining = abs(price_to_float(position_after_partial.get('position_amt')))
            if exchange_remaining > POSITION_AMT_EPSILON:
                remaining_amount = exchange_remaining
        elif position_after_partial is None and remaining_amount <= POSITION_AMT_EPSILON:
            trade_state['amount'] = 0
            trade_state['open_fee'] = updated_open_fee
            trade_state['partial_taken'] = True
            reset_trade_state_after_external_close(
                signal_bar_15m=signal_bar_15m,
                reason="半仓止盈后交易所已无剩余有效仓位",
                external_context={'external_close_order_id': extract_order_id(order)}
            )
            return True
        elif remaining_amount <= POSITION_AMT_EPSILON and remaining_amount_raw > POSITION_AMT_EPSILON:
            logging.warning(
                f"半仓后剩余数量被交易所精度压成0，改用原始剩余数量: "
                f"raw={remaining_amount_raw}, close_amount={close_amount}, total={total_amount}"
            )
            remaining_amount = remaining_amount_raw

        if remaining_amount <= POSITION_AMT_EPSILON:
            trade_state['amount'] = 0
            trade_state['open_fee'] = updated_open_fee
            trade_state['partial_taken'] = True
            reset_trade_state_after_external_close(
                signal_bar_15m=signal_bar_15m,
                reason="半仓止盈后剩余数量低于有效持仓阈值",
                external_context={'external_close_order_id': extract_order_id(order)}
            )
            return True
        trade_state['amount'] = remaining_amount
        trade_state['open_fee'] = updated_open_fee
        trade_state['partial_taken'] = True

        entry = price_to_float(trade_state.get('entry_price'))
        breakeven_stop = entry
        if side == 'long':
            breakeven_stop = min(entry, curr_price - get_price_tick(curr_price))
        else:
            breakeven_stop = max(entry, curr_price + get_price_tick(curr_price))
        breakeven_stop = precision_price(breakeven_stop)
        if stop_price_is_still_valid(curr_price, breakeven_stop, side) and refresh_protective_stop_order(breakeven_stop):
            trade_state['stop_loss_price'] = breakeven_stop
        else:
            refresh_protective_stop_order(trade_state['stop_loss_price'])

        send_msg(
            "ETH交易: 第一目标半仓止盈",
            f"原因: {reason}\n方向: {side}\n平仓数量: {close_amount}\n剩余数量: {remaining_amount}\n"
            f"平仓价: {actual_close_price}\n剩余止损: {trade_state.get('stop_loss_price')}\n15M信号时间: {signal_bar_15m}"
        )
        logging.info(
            f"半仓止盈完成: reason={reason}, close_amount={close_amount}, "
            f"remaining={remaining_amount}, stop={trade_state.get('stop_loss_price')}"
        )
        return True
    except Exception as e:
        logging.error(f"半仓止盈失败: {e}")
        logging.error(traceback.format_exc())
        refresh_protective_stop_order(trade_state.get('stop_loss_price'))
        return False


def apply_new_exit_rules(context, signal_bar_15m=''):
    if not trade_state.get('has_position'):
        return
    side = trade_state.get('side')
    curr_price = get_latest_price()
    exit_tf = trade_state.get('entry_exit_tf') or STRATEGY_EXIT_TF.get(trade_state.get('entry_strategy_tf'), '15m')
    df_exit = context['dfs'].get(exit_tf)
    if df_exit is not None:
        df_exit_closed = get_closed_df(df_exit, exit_tf, context['now_dt'])
        large = latest_large_strong(df_exit_closed)
        if large:
            if side == 'long':
                proposed = large['low'] if large['direction'] == 'long' else max(large['low'], curr_price - 0.3 * large['atr'])
            else:
                proposed = large['high'] if large['direction'] == 'short' else min(large['high'], curr_price + 0.3 * large['atr'])
            update_stop_if_tighter(side, proposed, f"{exit_tf}出现大强线，缩紧平仓价", curr_price, signal_bar_15m=signal_bar_15m)
            return

        opposite = 'short' if side == 'long' else 'long'
        strong_opposite = latest_effective_strong(df_exit_closed, opposite)
        if strong_opposite:
            proposed = strong_opposite['low'] if side == 'long' else strong_opposite['high']
            update_stop_if_tighter(side, proposed, f"{exit_tf}出现反向强K，缩紧止盈", curr_price, signal_bar_15m=signal_bar_15m)

    target = price_to_float(trade_state.get('entry_sr_target'))
    entry = price_to_float(trade_state.get('entry_price'))
    risk = price_to_float(trade_state.get('entry_initial_risk'))
    if target <= 0 and entry > 0 and risk > 0:
        target = entry + 2 * risk if side == 'long' else entry - 2 * risk
    if target > 0:
        if side == 'long' and curr_price >= target:
            if close_partial_position("达到第一目标位，平仓50%并推保护止损", curr_price=curr_price, signal_bar_15m=signal_bar_15m):
                return
            update_stop_if_tighter(side, max(entry, target - 0.25 * risk), "达到第一目标位，剩余仓位止损推至成本/目标附近", curr_price, signal_bar_15m=signal_bar_15m)
        elif side == 'short' and curr_price <= target:
            if close_partial_position("达到第一目标位，平仓50%并推保护止损", curr_price=curr_price, signal_bar_15m=signal_bar_15m):
                return
            update_stop_if_tighter(side, min(entry, target + 0.25 * risk), "达到第一目标位，剩余仓位止损推至成本/目标附近", curr_price, signal_bar_15m=signal_bar_15m)

    df_4h_closed = get_closed_df(context['dfs']['4h'], '4h', context['now_dt'])
    if len(df_4h_closed) == 0:
        return
    atr_4h = price_to_float(df_4h_closed.iloc[-1].get('atr'))
    near_buffer = SUP_RES_NEAR_ATR * atr_4h
    zones = context.get('zones', {})
    if side == 'long':
        resistance = nearest_opposite_zone(zones, side, curr_price - near_buffer)
        if resistance and curr_price >= resistance['lower'] - near_buffer:
            update_stop_if_tighter(side, max(trade_state['stop_loss_price'], resistance['lower'] - 0.1 * atr_4h), "靠近有效阻力区，缩紧止盈", curr_price, signal_bar_15m=signal_bar_15m)
    else:
        support = nearest_opposite_zone(zones, side, curr_price + near_buffer)
        if support and curr_price <= support['upper'] + near_buffer:
            update_stop_if_tighter(side, min(trade_state['stop_loss_price'], support['upper'] + 0.1 * atr_4h), "靠近有效支撑区，缩紧止盈", curr_price, signal_bar_15m=signal_bar_15m)


def monitor_position_new(context, signal_bar_15m='', allow_strategy_close=False):
    """新版持仓管理：只保留仓位同步、保护止损和新策略出场规则。"""
    try:
        side = trade_state.get('side')
        if side not in ('long', 'short'):
            return

        position_risk = get_position_risk(side=side)
        if position_risk and position_risk.get('fetch_failed'):
            logging.warning("本轮无法获取交易所仓位风险信息，跳过持仓管理，避免误判")
            return
        if position_risk is None:
            trade_state['position_miss_count'] = int(trade_state.get('position_miss_count', 0) or 0) + 1
            if trade_state['position_miss_count'] < EXTERNAL_CLOSE_CONFIRM_MISS_COUNT:
                logging.warning(
                    f"本轮未查到交易所持仓，先不重置（第{trade_state['position_miss_count']}/{EXTERNAL_CLOSE_CONFIRM_MISS_COUNT}次）"
                )
                return

            fallback_check = has_open_position_on_exchange(side=side)
            if fallback_check.get('fetch_failed'):
                logging.warning("二次确认仓位失败，本轮不重置本地状态，避免误判")
                return
            if fallback_check.get('has_position'):
                trade_state['position_miss_count'] = 0
                logging.warning("fetch_positions_risk 返回空，但 fetch_positions 仍有仓位，本轮忽略外部平仓判定")
                return

            external_close_context = {
                'stop_order_id_before_cancel': trade_state.get('stop_order_id', ''),
                'stop_order_price_before_cancel': trade_state.get('stop_order_price', 0.0)
            }
            cancel_protective_stop_order(silent=True)
            reset_trade_state_after_external_close(
                signal_bar_15m=signal_bar_15m,
                reason="检测到交易所实际已无仓位，可能是服务端止损或人工操作已成交，本地状态已重置",
                external_context=external_close_context
            )
            return

        trade_state['position_miss_count'] = 0
        trade_state['liquidation_price'] = position_risk.get('liquidation_price') or 0.0
        trade_state['close_cond_4h'] = context['trend_states'].get('4h', {}).get('summary', '')
        trade_state['close_cond_1h'] = context['trend_states'].get('1h', {}).get('summary', '')
        trade_state['close_cond_15m'] = context['trend_states'].get('15m', {}).get('summary', '')

        curr_price = get_latest_price()
        stop_loss = price_to_float(trade_state.get('stop_loss_price'))
        if side == 'long':
            if curr_price > price_to_float(trade_state.get('highest_price')):
                trade_state['highest_price'] = curr_price
            if stop_loss > 0 and curr_price <= stop_loss:
                close_position("触发保护止损", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="保护止损")
                return
        else:
            if curr_price < price_to_float(trade_state.get('lowest_price')) or price_to_float(trade_state.get('lowest_price')) <= 0:
                trade_state['lowest_price'] = curr_price
            if stop_loss > 0 and curr_price >= stop_loss:
                close_position("触发保护止损", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="保护止损")
                return

        apply_new_exit_rules(context, signal_bar_15m=signal_bar_15m)
    except Exception as e:
        logging.error(f"新版持仓管理异常: {e}")
        logging.error(traceback.format_exc())


def states_for_candidate(context, candidate):
    state_4h = make_empty_state('4h')
    state_1h = make_empty_state('1h')
    state_15m = make_empty_state('15m')
    side = candidate['side']
    strategy_tf = candidate['strategy_tf']
    summary = candidate.get('state_summary') or candidate.get('reason', '')
    if strategy_tf == '4h':
        state_4h = make_state_summary(context, '4h', side, summary)
        state_1h = make_state_summary(context, '1h', side, f"触发周期: {candidate.get('trigger_tf')}")
    elif strategy_tf == '1h':
        state_4h = make_state_summary(context, '4h', side, f"背景: {context['trend_states'].get('4h', {}).get('summary', '')}")
        state_1h = make_state_summary(context, '1h', side, summary)
        state_15m = make_state_summary(context, '15m', side, f"触发周期: {candidate.get('trigger_tf')}")
    else:
        state_1h = make_state_summary(context, '1h', side, f"背景: {context['trend_states'].get('1h', {}).get('summary', '')}")
        state_15m = make_state_summary(context, '15m', side, summary)
    return state_4h, state_1h, state_15m


def run_strategy():
    """新版策略主循环：5M节拍扫描，15m/1h/4h按权重产生候选信号。"""
    global trade_state

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
    try:
        futures = {
            '5m': executor.submit(fetch_df, SYMBOL, '5m', 220),
            '15m': executor.submit(fetch_df, SYMBOL, '15m', 220),
            '1h': executor.submit(fetch_df, SYMBOL, '1h', 220),
            '4h': executor.submit(fetch_df, SYMBOL, '4h', SUP_RES_LOOKBACK_4H),
            '1d': executor.submit(fetch_df, SYMBOL, '1d', 140),
        }
        dfs = {
            timeframe: future.result(timeout=FETCH_DF_TASK_TIMEOUT_SECONDS)
            for timeframe, future in futures.items()
        }
    except concurrent.futures.TimeoutError:
        logging.error(f"抓取K线任务超时，已跳过本轮策略计算: timeout={FETCH_DF_TASK_TIMEOUT_SECONDS}s")
        executor.shutdown(wait=False, cancel_futures=True)
        return
    except Exception:
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)

    if any(df is None for df in dfs.values()):
        # 多周期数据必须齐全；缺一个周期就放弃本轮，避免用残缺背景做交易决策。
        return

    server_now_dt = get_server_now_dt()
    signal_bar_5m = get_closed_bar_time(dfs['5m'], '5m', now_dt=server_now_dt)
    signal_bar_15m = get_closed_bar_time(dfs['15m'], '15m', now_dt=server_now_dt)
    signal_guard_5m = validate_signal_bar('5m', signal_bar_5m, now_dt=server_now_dt)
    signal_guard_15m = validate_signal_bar('15m', signal_bar_15m, now_dt=server_now_dt)

    if not signal_guard_5m['valid']:
        logging.warning(f"跳过异常5M信号K线: signal={signal_bar_5m}, reason={signal_guard_5m.get('reason')}")
        return

    is_new_signal_5m = bool(signal_guard_5m['is_new'])
    is_new_signal_15m = bool(signal_guard_15m.get('valid') and signal_guard_15m.get('is_new'))
    context = build_context(dfs, server_now_dt)

    if trade_state['has_position']:
        monitor_position_new(
            context,
            signal_bar_15m=signal_bar_15m if signal_guard_15m.get('valid') else '',
            allow_strategy_close=is_new_signal_5m
        )
        if is_new_signal_5m:
            trade_state['last_processed_bar_5m'] = signal_bar_5m
        if is_new_signal_15m:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        # 持仓期间只做仓位管理，不在同一轮继续寻找新开仓。
        return

    if not is_new_signal_5m:
        # 同一根 5M K 线只处理一次，避免轮询频率高导致重复开仓。
        return

    if is_in_post_exit_cooldown(server_now_dt):
        logging.info("平仓后冷却中：至少等待4根5M K线后再开新仓")
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        if is_new_signal_15m:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        return

    candidates = build_entry_candidates(context)
    if not candidates:
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        if is_new_signal_15m:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        return

    best_priority = candidate_priority(candidates[0])
    top_candidates = [candidate for candidate in candidates if candidate_priority(candidate) == best_priority]
    top_sides = {candidate['side'] for candidate in top_candidates}
    if len(top_sides) > 1:
        logging.info(f"同权重候选多空冲突，跳过本轮: {top_candidates}")
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        if is_new_signal_15m:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        return

    candidate = top_candidates[0]
    if candidate['strategy_tf'] == '15m' and signal_bar_15m in (trade_state.get('last_entry_bar_15m'), trade_state.get('last_exit_bar_15m')):
        logging.info(f"同一15M K线内已交易/刚平仓，跳过15M策略开仓: {signal_bar_15m}")
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        if is_new_signal_15m:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        return

    curr_price = get_latest_price()
    entry_still_valid, entry_invalid_reason = candidate_entry_price_still_valid(candidate, curr_price)
    if not entry_still_valid:
        logging.info(entry_invalid_reason)
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        if is_new_signal_15m:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        return

    state_4h, state_1h, state_15m = states_for_candidate(context, candidate)
    entry_meta = {
        'strategy_tf': candidate.get('strategy_tf'),
        'exit_tf': candidate.get('exit_tf'),
        'sr_target': candidate.get('target', 0.0)
    }
    try:
        opened = open_order(
            candidate['side'],
            curr_price,
            candidate['stop'],
            state_4h,
            state_1h,
            state_15m,
            signal_bar_15m if signal_guard_15m.get('valid') else '',
            entry_reason=candidate.get('reason', candidate.get('module', 'new_strategy')),
            entry_trigger_tf=f"{candidate.get('strategy_tf')}->{candidate.get('trigger_tf')}",
            entry_meta=entry_meta
        )
        if opened:
            logging.info(
                f"新版策略开仓成功: side={candidate['side']}, module={candidate['module']}, "
                f"strategy_tf={candidate['strategy_tf']}, trigger_tf={candidate['trigger_tf']}, "
                f"target={candidate.get('target')}"
            )
        else:
            logging.warning(f"新版策略开仓未成功: {candidate}")
    except Exception:
        logging.error(f"新版策略开仓失败:\n{traceback.format_exc()}")

    trade_state['last_processed_bar_5m'] = signal_bar_5m
    if is_new_signal_15m:
        trade_state['last_processed_bar_15m'] = signal_bar_15m


# ==========================================
# 4. 程序入口
# ==========================================
if __name__ == '__main__':
    try:
        current_time_str = get_server_time_str()
        print("网络连通成功！服务器时间:", current_time_str)
        balance_after = exchange.fetch_balance({'type': 'future'})
        final_usdt = float(balance_after['total']['USDT'])
        logging.info(f"🚀 自动化交易策略系统启动，初始金额：{final_usdt}")
    except Exception as e:
        logging.error(f"启动自检失败，但主循环会继续尝试运行: {e}")
    #exchange.load_markets()
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
