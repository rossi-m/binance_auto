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
import copy
import json
import uuid
import subprocess

from tradingAgents.ai_research_guard import (
    append_ai_audit_event,
    config_for_log,
    evaluate_entry_candidate,
    evaluate_position_reduction,
    load_ai_research_config,
    load_ai_research_guard,
)

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
LEVERAGE = 10  # 设置合约的杠杆倍数为10倍
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
CONDITIONAL_ORDER_CLEANUP_INTERVAL_SECONDS = 60  # 无本地仓位时最多每60秒清理一次残留条件委托
EXCHANGE_HTTP_TIMEOUT_MS = 10000  # 单次交易所HTTP请求最多等待10秒，避免底层请求长时间挂起
FETCH_DF_TASK_TIMEOUT_SECONDS = 15  # 单个周期抓取任务最多等待15秒，超过就跳过本轮
FETCH_DF_SLOW_LOG_SECONDS = 5  # 单次抓K线+算指标超过5秒就记慢查询日志
LIGHTWEIGHT_5M_GATE_LIMIT = 5  # 空仓轻量gate只取最近5根5M K线，不计算指标
EXIT_RULE_PRICE_MAX_AGE_MS = 500  # 持仓管理同一轮内复用最新价的最大年龄
MAIN_LOOP_SLEEP_SECONDS = 1  # 主循环正常节奏
MAIN_LOOP_ERROR_SLEEP_SECONDS = 5  # 主循环遇到顶层异常后，先休息5秒再继续
HEARTBEAT_INTERVAL_SECONDS = 15 * 60  # 每15分钟输出一次心跳日志，方便判断进程是否还活着
MAX_SIGNAL_BAR_STALENESS_SECONDS = 45 * 60  # 15M信号K线最多允许落后45分钟，防止交易所/测试网返回旧K线
DRY_RUN_OPEN_ORDER = False  # 运行时 dry-run 开关；开启后只走信号流程，不实际创建开仓订单。
# 脚本创建的保护单都带客户端ID前缀，持仓存在期间只允许修改脚本自己的订单。
STRATEGY_STOP_CLIENT_PREFIX = 'TAS_'
MANUAL_STOP_CLIENT_PREFIX = 'TAM_'
# 手动仓位同时评估两套管理周期，选中后在该笔持仓生命周期内保持不变。
MANUAL_POSITION_PROFILES = (
    {'name': '4h_1h', 'background_tf': '4h', 'exit_tf': '1h'},
    {'name': '1h_15m', 'background_tf': '1h', 'exit_tf': '15m'},
)
MANUAL_STOP_SWING_LOOKBACK = 6
MANUAL_STOP_STRUCTURE_ATR_BUFFER = 0.20
MANUAL_STOP_MIN_DISTANCE_ATR = 0.25
MANUAL_POSITION_MISS_CONFIRM_COUNT = 3
# --- TradingAgents AI research guard ---
# 默认 off，且减仓/全平/反手都有独立关闭开关。配置错误会阻止程序启动，避免误用宽松默认值。
AI_RESEARCH_CONFIG = load_ai_research_config()
# 所有持仓的逐交易对 TradingAgents 默认跟随 AI_RESEARCH_MODE；保留原环境变量名以兼容现有配置。
MANUAL_AI_RESEARCH_ENABLED = os.getenv(
    'MANUAL_AI_RESEARCH_ENABLED',
    '1' if AI_RESEARCH_CONFIG.mode != 'off' else '0',
).strip().lower() in ('1', 'true', 'yes', 'on')
MANUAL_AI_RESEARCH_HOURS = max(1, int(os.getenv('MANUAL_AI_RESEARCH_HOURS', '9')))
MANUAL_AI_RESEARCH_RETRY_SECONDS = max(60, int(os.getenv('MANUAL_AI_RESEARCH_RETRY_SECONDS', '1800')))
# 有效 bias 不按“生成后每 N 小时”滚动刷新，而是回到北京时间每天三个固定研究批次。
AI_RESEARCH_SCHEDULE_TIMES = ((5, 50), (13, 50), (20, 50))
MANUAL_AI_RUNNER_PATH = os.path.join(BASE_DIR, 'tradingAgents', 'run_symbol_research.py')
MANUAL_AI_RESEARCH_DIR = os.path.dirname(os.path.abspath(AI_RESEARCH_CONFIG.bias_file))
MANUAL_AI_OUTPUTS_DIR = os.path.join(BASE_DIR, 'tradingAgents', 'outputs')

# --- 新版策略参数 ---
# 策略会参与打分和选信号的主周期；越靠前代表越高周期、权重通常越重。
STRATEGY_TIMEFRAMES = ('4h', '1h')
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
# 按周期覆盖 ADX/DI 计算长度和趋势强度阈值。
# 默认值保留原先全局 14/20/25/40 逻辑，未单独验证的周期统一走兜底配置。
# 配置字段说明：
# - length: ADX/DI 计算周期；越大越平滑，趋势确认越慢。
# - range_max: ADX 小于等于该值时直接视为震荡，不按趋势开仓。
# - no_trade_max: ADX 小于等于该值时视为过渡区，等待方向确认。
# - trend_min: ADX 达到该值后，才允许进入 DI + EMA 方向判断。
# - extreme: ADX 超过该值时视为极强趋势，用于后续止损缓冲逻辑。
# 这些值来自 2026-06-20 的多周期联合过滤验证；1d 暂无充分回测，暂不单独配置。
DEFAULT_ADX_CONFIG = {
    'length': ADX_LENGTH, #14
    'range_max': 20, 
    'no_trade_max': ADX_NO_TRADE_MAX, #25
    'trend_min': ADX_TREND_MIN, #25
    'extreme': ADX_EXTREME #40
}
ADX_BY_TIMEFRAME = {
    # 15m 是触发周期，保持 ADX14，避免过度平滑导致入场太慢。
    # trend_min 先用 25；后续完整交易回测里再比较 25/28。
    '15m': {
        'length': 14,
        'range_max': 20,
        'no_trade_max': 25,
        'trend_min': 25,
        'extreme': 45
    },
    # 1h 是核心背景周期，联合过滤验证支持用 ADX21 提高背景质量。
    '1h': {
        'length': 21,
        'range_max': 20,
        'no_trade_max': 35,
        'trend_min': 35,
        'extreme': 45
    },
    # 4h 作为宽背景过滤；联合验证里 25 比 35 更适合保留有效趋势段。
    '4h': {
        'length': 14,
        'range_max': 20,
        'no_trade_max': 25,
        'trend_min': 25,
        'extreme': 45
    }
}
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
# 是否允许普通单根强K作为开仓触发。最新回测仅在反向强K突破优先时打开该分支。
ENABLE_SINGLE_STRONG_ENTRY = True
# 是否允许 2-3 根合成强K作为开仓触发。回测中合成强K止损偏宽，默认关闭；仍保留强K用于持仓止损收紧。
ENABLE_SYNTHETIC_STRONG_ENTRY = False
# 同方向强K触发和反向强K突破触发的检查顺序。大强K突破始终优先于这两个分支。
ENTRY_TRIGGER_PRIORITY = ('opposite_strong_break', 'strong_candle')
# 回看多少根K线检查是否出现过反向强K，用来过滤方向冲突。
OPPOSITE_STRONG_LOOKBACK = 24
# 检查最近几根强K是否多空来回切换，避免在剧烈震荡里开仓。
STRONG_CHOP_LOOKBACK_BARS = 4
# 初始止损距离使用 ATR 的倍数，决定入场后第一版保护止损有多宽。
ENTRY_ATR_STOP_MULTIPLIER = 1.0
# 极强趋势下把原始止损距离缩到该比例，0.5 表示止损距离减半。
EXTREME_ADX_STOP_RISK_RATIO = 0.5
# 趋势触发开仓的初始风险距离至少要达到策略周期 ATR 的比例，过滤贴脸止损。
MIN_TREND_TRIGGER_RISK_ATR_BY_TF = {
    '4h': 0.3,
    '1h': 0.6,
}
# 普通趋势下止损额外缓冲的 ATR 倍数。
NORMAL_STOP_BUFFER_ATR = 0.2
# 预期利润至少要达到 ATR 的这个比例，否则认为空间太小不值得进场。
MIN_EXPECTED_PROFIT_ATR = 0.25
# 预期利润至少要覆盖手续费的倍数，防止收益空间被手续费吃掉。
MIN_EXPECTED_PROFIT_FEE_MULTIPLIER = 3.0
# 预期利润相对初始风险的最低倍数，避免目标空间够绝对值但盈亏比太差。
MIN_EXPECTED_REWARD_RISK = 1.2
# 市价入场最大追价比例，按候选预期利润空间计算，避免把大部分利润让给入场滑点。
ENTRY_MAX_SLIPPAGE_PROFIT_RATIO = 0.15
# 币安U本位 STOP_LIMIT 条件限价入场在 CCXT futures 中使用 STOP 类型 + stopPrice + limit price。
ENTRY_STOP_LIMIT_ORDER_TYPE = 'STOP'
# pending 入场单最多保留 45 分钟；超过后取消，避免旧信号长期挂在交易所。
PENDING_ENTRY_MAX_AGE_SECONDS = 45 * 60
# pending 入场单/仓位查询连续缺失次数，达到阈值才清本地状态，降低接口瞬时不一致误判。
PENDING_ENTRY_MISSING_CONFIRM_COUNT = 3
# 最高浮盈达到多少 ATR 后，至少保留多少比例的最高浮盈。
PROFIT_ATR_LOCK_TIERS = (
    (4.0, 0.90),
    (3.0, 0.70),
    (2.0, 0.50),
    (1.0, 0.30),
)
# 平仓后冷却时间，防止刚出场又在同一段短周期波动里立刻重开。
POST_EXIT_COOLDOWN_SECONDS = 4 * TIMEFRAME_SECONDS['5m']
# 识别4H支撑/阻力摆动高低点时，左右各观察的K线窗口。
SUP_RES_SWING_WINDOW = 5
# 支撑/阻力价位相距小于该 ATR 倍数时会合并成同一个区域。
SUP_RES_MERGE_ATR = 0.5
# 支撑/阻力区域上下边界额外扩展的 ATR 倍数。
SUP_RES_ZONE_BUFFER_ATR = 0.1
# 支撑/阻力区域最大宽度，超过该 ATR 倍数后不再继续扩大区域边界。
SUP_RES_MAX_ZONE_WIDTH_ATR = 1.0
# 一个支撑/阻力区至少被触碰多少次才算有效区域。
SUP_RES_VALID_TOUCHES = 2
# 最近多少根4H内被突破/跌破的区域，会进入 broken_support/broken_resistance。
SUP_RES_RECENT_BREAK_LOOKBACK = 6
# 根据支撑/阻力设置止损时，止损放在区域外侧的 ATR 缓冲。
SUP_RES_STOP_BUFFER_ATR = 0.2
# SR breakout 单独过滤初始止损距离；超过该 4H ATR 倍数时跳过，其它仍用正常仓位。
SR_BREAKOUT_RISK_ATR_MAX = 1.5
# SR breakout 失败后，同区间长冷却；同方向短冷却用于避免刚止损后立刻追同一段假突破。
SR_BREAKOUT_FAILURE_ZONE_COOLDOWN_SECONDS = 24 * 60 * 60
SR_BREAKOUT_FAILURE_SIDE_COOLDOWN_SECONDS = 4 * 60 * 60
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
    'entry_trigger_price': 0.0,  # 记录条件委托入场触发价；市价开仓时等于计划入场价
    'stop_loss_price': 0,  # 记录当前的止损价格，初始为0
    'highest_price': 0,  # 记录持有多单时的最高价格，用于计算利润回撤，初始为0
    'lowest_price': 0,  # 记录持有空单时的最低价格，用于计算利润回撤，初始为0
    'amount': 0,  # 记录当前持仓的数量，初始为0
    'entry_initial_amount': 0.0,  # 该笔交易最初实际成交数量，AI目标仓位以此为基准
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
    'entry_module': '', # 记录入场模块：trend_trigger / sr_breakout / sr_rebound
    'entry_sr_breakout_key': '', # SR breakout 对应的支撑/阻力区间key，用于失败冷却
    'entry_sr_target': 0.0, # 支撑阻力模块给出的第一目标位
    'entry_initial_risk': 0.0, # 入场价到初始止损的距离，用于1:2备用目标
    'last_exit_time': '', # 最近一次平仓时间，用于5分钟触发策略的冷却
    'liquidation_price': 0.0,  # 记录当前仓位从交易所返回的真实强平价
    'stop_order_id': '',  # 记录服务端 STOP_MARKET 止损单的订单ID，便于后续撤单和替换
    'stop_order_price': 0.0,  # 记录当前服务端止损单对应的触发价格
    'stop_order_refresh_fail_count': 0,  # 连续几次更新服务端止损单失败，达到阈值后才保护性平仓
    'last_stop_order_refresh_error': '',  # 最近一次服务端止损更新失败的原始错误文本
    'pending_entry_order_id': '',  # 等待成交的 STOP_LIMIT 入场单ID
    'pending_entry_side': '',  # pending 入场方向：long / short
    'pending_entry_amount': 0.0,  # 计划开仓数量
    'pending_entry_stop_price': 0.0,  # STOP_LIMIT 触发价
    'pending_entry_limit_price': 0.0,  # STOP_LIMIT 限价
    'pending_entry_protective_stop': 0.0,  # 成交后要挂的保护止损
    'pending_entry_target': 0.0,  # 候选目标价
    'pending_entry_strategy_tf': '',  # 策略周期
    'pending_entry_background_tf': '',  # 背景周期
    'pending_entry_trigger_tf': '',  # 触发周期
    'pending_entry_exit_tf': '',  # 持仓后出场观察周期
    'pending_entry_module': '',  # 候选模块
    'pending_entry_sr_breakout_key': '',  # SR breakout 区间key
    'pending_entry_created_time': '',  # pending 单创建时间
    'pending_entry_signal_bar': '',  # 入场信号K线时间
    'pending_entry_candidate_json': '',  # 候选信号快照
    'pending_entry_triggered': False,  # 条件单是否已触发/部分成交
    'pending_entry_filled_amount': 0.0,  # 已成交数量
    'pending_entry_missing_count': 0,  # 连续未能在交易所确认到订单/仓位的次数
    'pending_entry_initial_balance': 0.0,  # 挂单前账户资金，用于成交后统计
    'pending_entry_reason': '',  # 入场原因
    'pending_entry_cond_4h': '',  # pending 创建时的4H条件快照
    'pending_entry_cond_1h': '',  # pending 创建时的1H条件快照
    'pending_entry_cond_15m': '',  # pending 创建时的15M条件快照
    'pending_entry_condition_details': '',  # pending 创建时的条件明细
    'pending_entry_profit_check_atr': 0.0,  # 成交后复核利润空间使用的ATR
    'pending_entry_ai_snapshot_json': '',  # 挂单时的有效AI快照，仅用于审计和成交后追溯
    'entry_signal_bar_15m': '',   # 记录入场所对应的15M已收盘信号K线时间
    'last_entry_bar_15m': '',     # 最近一次入场所对应的15M已收盘信号K线时间（防止同根K线重复开仓）
    'last_exit_bar_15m': '',      # 最近一次平仓所对应的15M已收盘信号K线时间（防止同根K线平仓后立即重开）
    'last_processed_bar_5m': '',  # 最近一次已处理过的5M已收盘信号K线时间
    'max_seen_bar_5m': '',        # 运行期间见过的最大5M信号时间
    'last_processed_bar_15m': '', # 最近一次已处理过的15M已收盘信号K线时间（核心去重字段，防止同一根K线重复执行策略逻辑）
    'max_seen_bar_15m': '',       # 运行期间见过的最大15M信号时间，防止接口回跳旧K线后被当成新信号
    'position_miss_count': 0,  # 连续几轮未在交易所查到仓位，用于避免误判“外部平仓”
    'ai_snapshot_json': '',  # 最近一次与该仓位关联的AI快照
    'ai_last_reduce_generated_at': '',       # 上一次成功 AI 减仓对应的 bias generated_at，用于去重（同一条 bias 不重复触发）
    'ai_last_reduce_target_ratio': 1.0,      # 上一次 AI 建议的目标仓位比例（1.0=不减仓）
    'ai_partial_reduce_count': 0,            # 本次持仓期间已执行的 AI 减仓次数
    'ai_partial_reduce_amount': 0.0,         # 累计已减仓数量
    'ai_partial_reduce_realized_pnl': 0.0,   # 累计减仓产生的平仓盈亏点数
    'ai_partial_reduce_fee': 0.0,            # 累计减仓手续费（USDT）
    'ai_reduce_reasons': []                  # 每次减仓的原因记录列表（AI 反向信号/风险系数等）
}
sr_breakout_failure_cooldowns = {
    'side': {},
    'zone': {},
}
last_conditional_order_cleanup_monotonic = 0.0

# 初始化币安合约 API
exchange = ccxt.binance({
    'apiKey': API_KEY,
    'secret': SECRET_KEY,
    'options': {'defaultType': 'future'},  # 设置默认交易类型为U本位合约 (future)
    'enableRateLimit': True,  # 开启内置的速率限制功能，防止请求频率过高被封IP
    'timeout': EXCHANGE_HTTP_TIMEOUT_MS,  # 给交易所HTTP请求设置硬超时，避免网络卡死时无限等待
})
exchange.enable_demo_trading(True)

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.info("Binance Demo U本位合约 API 已初始化")
logging.info("AI research guard 配置: %s", config_for_log(AI_RESEARCH_CONFIG))
logging.info(
    "逐交易对持仓 TradingAgents 配置: enabled=%s, hours=%s, retry_seconds=%s",
    MANUAL_AI_RESEARCH_ENABLED,
    MANUAL_AI_RESEARCH_HOURS,
    MANUAL_AI_RESEARCH_RETRY_SECONDS,
)

# 记录程序层面的临时运行状态，不属于某一笔交易。
runtime_state = {
    # 上一次打印心跳日志的单调时间戳，用来控制心跳输出频率。
    'last_heartbeat_ts': 0.0,
    # 4H 支撑阻力区只随已收盘 4H K 线变化；同一根 4H 内复用计算结果。
    'support_resistance_cache': {
        'bar_time': '',
        'zones': None
    },
    # 背景周期K线只在对应已收盘K线变化后才需要重新抓取和计算指标。
    'kline_df_cache': {},
    # AI审计事件幂等key，避免持仓轮询每秒重复写同一结论。
    'ai_audit_keys': {},
    # 每个持仓交易对最多运行一个 TradingAgents 后台任务；ETH、BTC 等不同交易对可以并行。
    'manual_ai_jobs': {},
}

# 手动仓位不复用单一 ETH trade_state；每个 symbol + positionSide 独立管理。
manual_position_states = {}


# ==========================================
# 2. 功能模块
# ==========================================

AI_GUARD_SNAPSHOT_FIELDS = (
    'valid', 'fail_open', 'error', 'symbol', 'generated_at', 'expires_at',
    'portfolio_stance', 'futures_bias', 'time_horizon_hours',
    'explicit_long_signal', 'explicit_short_signal', 'confidence',
    'allow_long', 'allow_short', 'risk_multiplier', 'reason'
)


def compact_usdt_symbol(symbol):
    """把 CCXT 交易对转换成 bias 文件使用的 Binance 紧凑符号。"""
    return normalize_futures_symbol(symbol).replace('/', '').replace(':USDT', '')


def manual_ai_bias_file(symbol):
    return os.path.join(MANUAL_AI_RESEARCH_DIR, f"latest_bias_{compact_usdt_symbol(symbol)}.json")


def get_ai_guard_for_symbol(symbol, bias_file=None):
    """按实际交易对加载独立AI结论；异常和过期结果继续按 fail-open 处理。"""
    compact_symbol = compact_usdt_symbol(symbol)
    target_bias_file = bias_file or manual_ai_bias_file(symbol)
    return load_ai_research_guard(
        AI_RESEARCH_CONFIG,
        expected_symbol=compact_symbol,
        now=datetime.datetime.now(EXCHANGE_TZ),
        bias_file=target_bias_file,
    )


def get_current_ai_guard():
    """保持原ETH策略入口兼容。"""
    return get_ai_guard_for_symbol(SYMBOL, bias_file=AI_RESEARCH_CONFIG.bias_file)


def ai_guard_snapshot(guard):
    return {field: guard.get(field) for field in AI_GUARD_SNAPSHOT_FIELDS}


def parse_ai_schedule_datetime(value):
    """把持仓/研报时间统一转换为北京时间；无时区的交易时间按北京时间解释。"""
    if not value:
        return None
    try:
        parsed = datetime.datetime.fromisoformat(str(value).strip().replace('Z', '+00:00'))
    except Exception:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=EXCHANGE_TZ)
    return parsed.astimezone(EXCHANGE_TZ)


def latest_ai_research_schedule_slot(now_dt=None):
    """返回当前时刻已经到达的最近一个固定批次：北京时间05:50、13:50或20:50。"""
    now_dt = now_dt or datetime.datetime.now(EXCHANGE_TZ)
    if now_dt.tzinfo is None or now_dt.utcoffset() is None:
        now_dt = now_dt.replace(tzinfo=EXCHANGE_TZ)
    else:
        now_dt = now_dt.astimezone(EXCHANGE_TZ)
    slots = [
        now_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
        for hour, minute in AI_RESEARCH_SCHEDULE_TIMES
    ]
    reached = [slot for slot in slots if slot <= now_dt]
    if reached:
        return reached[-1]
    yesterday = now_dt - datetime.timedelta(days=1)
    hour, minute = AI_RESEARCH_SCHEDULE_TIMES[-1]
    return yesterday.replace(hour=hour, minute=minute, second=0, microsecond=0)


def ai_research_refresh_reason(guard, state=None, now_dt=None):
    """无有效bias立即补跑；有效bias只补持仓期间已经到点但尚未完成的固定批次。"""
    if not guard.get('valid'):
        return 'invalid_bias_immediate'
    generated_at = parse_ai_schedule_datetime(guard.get('generated_at'))
    if generated_at is None:
        return 'invalid_bias_immediate'
    latest_slot = latest_ai_research_schedule_slot(now_dt=now_dt)
    state = state if isinstance(state, dict) else {}
    position_started_at = parse_ai_schedule_datetime(
        state.get('entry_time') or state.get('detected_time')
    )
    # 新仓建立前已经错过的固定批次不补跑；等待该仓位建立后的下一个固定点。
    if position_started_at is not None and latest_slot < position_started_at:
        return ''
    if generated_at < latest_slot:
        return f"scheduled_{latest_slot:%Y%m%d_%H%M}"
    return ''


def reap_manual_ai_jobs():
    """回收已结束的逐交易对研究任务，不在交易主循环里等待子进程。"""
    jobs = runtime_state.setdefault('manual_ai_jobs', {})
    for symbol, job in list(jobs.items()):
        process = job.get('process')
        if process is None:
            continue
        return_code = process.poll()
        if return_code is None:
            continue
        log_handle = job.pop('log_handle', None)
        if log_handle is not None:
            log_handle.close()
        job['process'] = None
        job['return_code'] = return_code
        job['completed_at'] = time.monotonic()
        logging.info(f"持仓 TradingAgents 后台任务结束: symbol={symbol}, return_code={return_code}")


def ensure_manual_ai_research_job(symbol, state=None):
    """必要时为持仓启动逐symbol研究；同一symbol防重，不同symbol允许并行。"""
    state = state if isinstance(state, dict) else {}
    guard = get_ai_guard_for_symbol(symbol)
    state['ai_guard_snapshot'] = ai_guard_snapshot(guard)
    state['ai_bias_file'] = manual_ai_bias_file(symbol)
    if not MANUAL_AI_RESEARCH_ENABLED:
        state['ai_research_status'] = 'disabled'
        return guard
    refresh_reason = ai_research_refresh_reason(guard, state=state)
    state['ai_research_trigger'] = refresh_reason
    if not refresh_reason:
        state['ai_research_status'] = 'ready'
        return guard

    reap_manual_ai_jobs()
    compact_symbol = compact_usdt_symbol(symbol)
    jobs = runtime_state.setdefault('manual_ai_jobs', {})
    job = jobs.get(compact_symbol, {})
    process = job.get('process')
    if process is not None and process.poll() is None:
        state['ai_research_status'] = 'running'
        return guard
    last_attempt = float(job.get('last_attempt', 0.0) or 0.0)
    # last_attempt=0 代表从未启动过；不能因服务器启动时间短于重试窗口而跳过首个任务。
    if last_attempt > 0 and time.monotonic() - last_attempt < MANUAL_AI_RESEARCH_RETRY_SECONDS:
        state['ai_research_status'] = 'retry_wait'
        return guard

    log_dir = os.path.join(MANUAL_AI_RESEARCH_DIR, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"manual_ai_{compact_symbol}.log")
    log_handle = open(log_path, 'ab', buffering=0)
    command = [
        sys.executable,
        MANUAL_AI_RUNNER_PATH,
        '--symbol', compact_symbol,
        '--hours', str(MANUAL_AI_RESEARCH_HOURS),
        '--research-dir', MANUAL_AI_RESEARCH_DIR,
        '--outputs-dir', MANUAL_AI_OUTPUTS_DIR,
    ]
    try:
        process = subprocess.Popen(
            command,
            cwd=os.path.dirname(MANUAL_AI_RUNNER_PATH),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except Exception as e:
        log_handle.close()
        jobs[compact_symbol] = {'process': None, 'last_attempt': time.monotonic(), 'error': str(e)}
        state['ai_research_status'] = 'start_failed'
        logging.warning(f"启动持仓 TradingAgents 失败: symbol={compact_symbol}, error={e}")
        return guard

    jobs[compact_symbol] = {
        'process': process,
        'log_handle': log_handle,
        'last_attempt': time.monotonic(),
        'log_path': log_path,
        'trigger': refresh_reason,
    }
    state['ai_research_status'] = 'running'
    logging.warning(
        "已后台启动持仓 TradingAgents: symbol=%s pid=%s trigger=%s log=%s",
        compact_symbol, process.pid, refresh_reason, log_path,
    )
    return guard


def write_ai_audit(event_type, guard, details=None, dedupe_key=''):
    """写独立AI JSONL审计，不写交易成交CSV；审计失败不得影响交易。"""
    if AI_RESEARCH_CONFIG.mode == 'off':
        return
    details = details or {}
    generated_at = str(guard.get('generated_at', '') or 'invalid')
    key = dedupe_key or f"{event_type}:{generated_at}:{details.get('actual_action', '')}:{details.get('shadow_action', '')}"
    seen = runtime_state.setdefault('ai_audit_keys', {})
    if key in seen:
        return
    try:
        event = {
            'event': event_type,
            'mode': AI_RESEARCH_CONFIG.mode,
            'guard': ai_guard_snapshot(guard),
            **details
        }
        path = append_ai_audit_event(AI_RESEARCH_CONFIG, event)
        seen[key] = time.monotonic()
        if len(seen) > 300:
            oldest_keys = sorted(seen, key=seen.get)[:100]
            for oldest_key in oldest_keys:
                seen.pop(oldest_key, None)
        logging.info(
            "AI_RESEARCH event=%s actual=%s shadow=%s valid=%s reason=%s audit=%s",
            event_type,
            details.get('actual_action', ''),
            details.get('shadow_action', ''),
            guard.get('valid'),
            details.get('reason') or guard.get('error', ''),
            path
        )
    except Exception as e:
        logging.warning(f"写AI审计日志失败，交易流程继续: {e}")

def elapsed_ms(start_ts):
    return (time.monotonic() - start_ts) * 1000.0


def add_perf(perf, key, value_ms):
    # 实盘版本关闭性能采集；保留空函数以兼容既有调用链。
    return


def record_perf(perf, key, start_ts):
    # 实盘版本关闭性能采集；保留空函数以兼容既有调用链。
    return


def get_adx_config(timeframe):
    """返回某个周期的 ADX 配置；未配置字段自动使用默认兜底。"""
    config = dict(DEFAULT_ADX_CONFIG)
    config.update(ADX_BY_TIMEFRAME.get(timeframe, {}))
    return config


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
    '入场触发价',
    '平仓时间', '平仓原因', '点数盈亏', '手续费', '净利润(USDT)', '是否盈利',
    '入场15M信号时间', '平仓15M信号时间', '平仓触发周期', '持仓秒数',
    '开仓订单ID', '平仓订单ID'
]
# 旧版 CSV 表头缺少订单ID字段/入场触发价字段，用它识别历史文件并自动补齐新列。
TRADE_CSV_HEADERS_WITHOUT_TRIGGER_PRICE = [
    '建仓时间', '趋势方向', '4H条件', '1H条件', '15M条件', '入场原因',
    '平仓时间', '平仓原因', '点数盈亏', '手续费', '净利润(USDT)', '是否盈利',
    '入场15M信号时间', '平仓15M信号时间', '平仓触发周期', '持仓秒数',
    '开仓订单ID', '平仓订单ID'
]
LEGACY_TRADE_CSV_HEADERS = TRADE_CSV_HEADERS_WITHOUT_TRIGGER_PRICE[:-2]


def extract_order_id(order):
    """从 CCXT 订单结果中尽量稳定地提取订单ID"""
    if not isinstance(order, dict):
        return ''

    info = order.get('info', {})
    # ↓ 对应 CCXT 统一格式的字段名

    candidates = [
        # 通常id和orderId是通用的，但algoId和clientOrderId可能需要根据具体情况判断
        order.get('id'), #CCXT 层级的订单 ID

        order.get('orderId'), # 普通挂单的订单 ID
        order.get('algoId'), #条件委托的算法订单 ID

        order.get('clientOrderId'), #普通挂单的客户端自定义 ID

        order.get('clientAlgoId')   #条件委托的客户端自定义 ID
    ]
    # ↓ 对应 Binance 原始响应里的字段名
    if isinstance(info, dict):
        candidates.extend([
            info.get('orderId'),
            info.get('id'),
            info.get('algoId'),
            info.get('clientOrderId'),
            info.get('clientAlgoId')
        ])

    for candidate in candidates:
        if candidate not in (None, ''):
            return str(candidate)
    return ''


def extract_order_client_id(order):
    """统一提取普通订单或 algo 条件单的客户端自定义ID。"""
    if not isinstance(order, dict):
        return ''
    info = order.get('info', {})
    if not isinstance(info, dict):
        info = {}
    for candidate in (
        order.get('clientOrderId'),
        order.get('clientAlgoId'),
        info.get('clientOrderId'),
        info.get('clientAlgoId'),
        info.get('origClientOrderId'),
    ):
        if candidate not in (None, ''):
            return str(candidate)
    return ''


def make_protective_client_order_id(prefix):
    """生成不超过 Binance 36 字符限制的保护单客户端ID。"""
    timestamp_suffix = str(int(time.time() * 1000))[-10:]
    random_suffix = uuid.uuid4().hex[:16]
    return f"{prefix}{timestamp_suffix}{random_suffix}"[:36]


def is_script_owned_protective_order(order, tracked_order_id='', prefixes=None):
    """判断条件单是否由本脚本创建；本地已记录的订单ID也视为脚本自有。"""
    order_id = extract_order_id(order)
    if tracked_order_id and order_id == str(tracked_order_id):
        return True
    client_id = extract_order_client_id(order)
    prefixes = prefixes or (STRATEGY_STOP_CLIENT_PREFIX, MANUAL_STOP_CLIENT_PREFIX)
    return any(client_id.startswith(prefix) for prefix in prefixes if prefix)


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
        info.get('createTime'),
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


def extract_order_side_upper(order):
    """统一提取订单方向，返回 BUY / SELL / ''。"""
    if not isinstance(order, dict):
        return ''
    info = order.get('info', {})
    if not isinstance(info, dict):
        info = {}
    return str(order.get('side') or info.get('side') or '').strip().upper()


def extract_order_type_upper(order):
    """统一提取订单类型，返回交易所大写类型。"""
    if not isinstance(order, dict):
        return ''
    info = order.get('info', {})
    if not isinstance(info, dict):
        info = {}
    return str(
        order.get('type') or
        order.get('orderType') or
        info.get('type') or
        info.get('orderType') or
        ''
    ).strip().upper()


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
    """兼容老版CSV表头，必要时补齐缺失列，避免新旧列数不一致。"""
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

    compatible_headers = (TRADE_CSV_HEADERS_WITHOUT_TRIGGER_PRICE, LEGACY_TRADE_CSV_HEADERS)
    if header not in compatible_headers:
        logging.warning(f"CSV表头不是预期格式，跳过自动升级: {filename}")
        return True

    upgraded_rows = [TRADE_CSV_HEADERS]
    source_headers = header
    source_len = len(source_headers)
    for row in rows[1:]:
        normalized_row = list(row[:source_len])
        if len(normalized_row) < source_len:
            normalized_row.extend([''] * (source_len - len(normalized_row)))
        row_by_header = dict(zip(source_headers, normalized_row))
        upgraded_rows.append([row_by_header.get(col, '') for col in TRADE_CSV_HEADERS])

    try:
        with open(filename, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerows(upgraded_rows)
        logging.info(f"已自动升级CSV表头，补充缺失交易记录列: {filename}")
        return True
    except Exception as e:
        logging.error(f"升级CSV表头失败: {filename}, error={e}")
        return False


def ohlcv_to_dataframe(ohlcv):
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    # 统一转成北京时间，后续信号去重、CSV 和日志都按同一时区比较。
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['timestamp'] = df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai')
    return df


def add_strategy_indicators(df, timeframe, perf=None):
    indicator_start = time.monotonic()
    # 交易所只给原始K线；策略用到的趋势/波动指标在本地补齐。
    df['ema20'] = ta.ema(df['close'], length=20)
    df['ema50'] = ta.ema(df['close'], length=50)

    df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)

    adx_config = get_adx_config(timeframe)
    adx_length = int(adx_config.get('length', ADX_LENGTH))
    # ADX / DI 用于新版背景趋势判断；不同 timeframe 可以使用不同 length。
    # 下游策略统一读取 adx/plus_di/minus_di，所以这里把 pandas_ta 的
    # ADX_14、ADX_21 等实际列名统一重命名成固定字段。
    adx = ta.adx(df['high'], df['low'], df['close'], length=adx_length)
    if adx is not None:
        df = pd.concat([df, adx], axis=1)
        df.rename(columns={
            f'ADX_{adx_length}': 'adx',
            f'DMP_{adx_length}': 'plus_di',
            f'DMN_{adx_length}': 'minus_di'
        }, inplace=True)
    record_perf(perf, f'fetch_df_{timeframe}_indicators', indicator_start)
    return df


def log_fetch_df_perf(timeframe, elapsed):
    if elapsed >= FETCH_DF_SLOW_LOG_SECONDS:
        logging.warning(f"获取数据较慢 ({timeframe}): {elapsed:.2f}s")


def fetch_df(symbol, timeframe, limit=100):
    """获取K线并计算技术指标"""
    start_ts = time.monotonic()
    perf = None
    try:
        fetch_start = time.monotonic()
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        record_perf(perf, f'fetch_df_{timeframe}_fetch_ohlcv', fetch_start)

        dataframe_start = time.monotonic()
        df = ohlcv_to_dataframe(ohlcv)
        record_perf(perf, f'fetch_df_{timeframe}_dataframe', dataframe_start)
        # 计算指标值：ema20,ema50,atr,adx
        df = add_strategy_indicators(df, timeframe, perf=perf)
        elapsed = time.monotonic() - start_ts
        log_fetch_df_perf(timeframe, elapsed)
        return df
    except Exception as e:
        logging.error(f"获取数据失败 ({timeframe}): {e}")
        return None


def kline_cache_key(symbol, timeframe, limit):
    return f"{symbol}|{timeframe}|{int(limit)}"


def store_kline_df_cache(symbol, timeframe, limit, df, now_dt=None):
    if df is None or len(df) == 0:
        return

    if now_dt is None:
        now_dt = datetime.datetime.now(EXCHANGE_TZ)
    bar_time = get_closed_bar_time(df, timeframe, now_dt=now_dt)
    if not bar_time:
        return

    cache = runtime_state.setdefault('kline_df_cache', {})
    cache[kline_cache_key(symbol, timeframe, limit)] = {
        'bar_time': bar_time, # 最后一个K线的收盘时间
        'df': df, # 缓存5分钟数据
        'updated_at_monotonic': time.monotonic() # 缓存更新时间
    }


def kline_cache_entry_is_current(entry, timeframe, now_dt):
    """判断缓存的最后已收盘K线是否仍是当前可用的最后已收盘K线。"""
    if not entry or entry.get('df') is None:
        return False

    tf_seconds = TIMEFRAME_SECONDS.get(timeframe)
    cached_bar_dt = parse_bar_time(entry.get('bar_time', ''))
    if tf_seconds is None or cached_bar_dt is None:
        return False

    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=EXCHANGE_TZ)
    else:
        now_dt = now_dt.astimezone(EXCHANGE_TZ)

    next_bar_close_dt = cached_bar_dt + datetime.timedelta(seconds=2 * tf_seconds)
    return now_dt < next_bar_close_dt


def fetch_df_cached(symbol, timeframe, limit=100, cache_timeframes=('15m', '1h', '4h', '1d')):
    """对背景周期复用已收盘K线级别的DataFrame，避免同一周期内重复拉全量K线。"""
    if timeframe not in cache_timeframes:
        return fetch_df(symbol, timeframe, limit)

    cache = runtime_state.setdefault('kline_df_cache', {})
    key = kline_cache_key(symbol, timeframe, limit)
    now_dt = datetime.datetime.now(EXCHANGE_TZ)
    entry = cache.get(key)
    if kline_cache_entry_is_current(entry, timeframe, now_dt):
        return entry['df'].copy(deep=False)

    df = fetch_df(symbol, timeframe, limit)
    if df is None:
        return None

    store_kline_df_cache(symbol, timeframe, limit, df, now_dt=now_dt)
    return df


def rebuild_5m_df_from_lightweight_cache(symbol, limit, lightweight_df):
    """用轻量gate刚抓到的5M raw K线更新上一轮完整5M缓存，避免同轮重复REST请求。"""
    try:
        if lightweight_df is None or len(lightweight_df) == 0:
            return None

        cache = runtime_state.setdefault('kline_df_cache', {})
        # 获取缓存的5分钟220根k线
        entry = cache.get(kline_cache_key(symbol, '5m', limit))
        if not entry or entry.get('df') is None:
            return None

        raw_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        cached_raw = entry['df'][raw_columns]
        # 轻量级5分钟K线 5根（包含未收盘的K线）
        lightweight_raw = lightweight_df[raw_columns]
        # 合并缓存和轻量级K线
        merged = pd.concat([cached_raw, lightweight_raw], ignore_index=True)
        # 去重并保留最新
        merged = merged.drop_duplicates(subset=['timestamp'], keep='last')
        # 按时间排序并取最新limit根K线
        merged = merged.sort_values('timestamp').tail(limit).reset_index(drop=True)

        if len(merged) < min(limit, len(cached_raw)):
            return None
        # 计算指标值：ema20,ema50,atr,adx
        df = add_strategy_indicators(merged, '5m')
        # 存储更新后的5分钟K线缓存
        store_kline_df_cache(symbol, '5m', limit, df)
        return df
    except Exception as e:
        logging.warning(f"复用轻量5M缓存失败，降级重新抓取完整5M: {e}")
        return None


def fetch_lightweight_5m_df(symbol, limit=LIGHTWEIGHT_5M_GATE_LIMIT):
    """空仓gate只取5M原始K线并统一时间格式，不计算任何指标。"""
    perf = {}
    fetch_start = time.monotonic()
    ohlcv = exchange.fetch_ohlcv(symbol, '5m', limit=limit)
    record_perf(perf, 'lightweight_5m_fetch_ohlcv', fetch_start)
    if not ohlcv:
        return None, perf

    dataframe_start = time.monotonic()
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['timestamp'] = df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai')
    record_perf(perf, 'lightweight_5m_dataframe', dataframe_start)
    return df, perf


def maybe_log_heartbeat():
    """定期输出心跳日志，确认主循环仍然存活且没有卡死"""
    now_ts = time.monotonic()
    last_heartbeat_ts = runtime_state.get('last_heartbeat_ts', 0.0)
    if now_ts - last_heartbeat_ts < HEARTBEAT_INTERVAL_SECONDS:
        # 主循环每秒跑一次；未到间隔时不刷心跳，避免日志被无效信息淹没。
        return

    runtime_state['last_heartbeat_ts'] = now_ts
    logging.info(
        "心跳: has_position=%s, side=%s, pending_entry_id=%s, pending_entry_side=%s, "
        "last_processed_15m=%s, last_entry_15m=%s, last_exit_15m=%s",
        trade_state.get('has_position'),
        trade_state.get('side'),
        trade_state.get('pending_entry_order_id', ''),
        trade_state.get('pending_entry_side', ''),
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


def sr_breakout_risk_distance_allowed(risk_distance, atr_4h):
    """SR breakout 专用风险距离过滤；超过 1.5 倍 4H ATR 则跳过。"""
    risk_distance = price_to_float(risk_distance)
    atr_4h = price_to_float(atr_4h)
    if risk_distance <= 0 or atr_4h <= 0:
        return False
    return risk_distance <= SR_BREAKOUT_RISK_ATR_MAX * atr_4h


def trend_trigger_risk_distance_allowed(risk_distance, strategy_atr, strategy_tf):
    """trend_trigger 专用风险距离过滤；止损太近时跳过。"""
    risk_distance = price_to_float(risk_distance)
    strategy_atr = price_to_float(strategy_atr)
    min_risk_atr = MIN_TREND_TRIGGER_RISK_ATR_BY_TF.get(strategy_tf)
    if min_risk_atr is None:
        return True
    if risk_distance <= 0 or strategy_atr <= 0:
        return False
    return risk_distance >= min_risk_atr * strategy_atr


def sr_breakout_zone_key(side, zone):
    if not zone:
        return ''
    return (
        f"{side}:"
        f"{zone.get('type', '')}:"
        f"{price_to_float(zone.get('lower')):.2f}-"
        f"{price_to_float(zone.get('upper')):.2f}:"
        f"{zone.get('break_time', '')}"
    )


def sr_breakout_cooldown_reason(side, zone_key, now_dt=None):
    now_dt = now_dt or get_server_now_dt()
    cooldowns = (
        (f"side:{side}", sr_breakout_failure_cooldowns.get('side', {}).get(side)),
        (f"zone:{zone_key}", sr_breakout_failure_cooldowns.get('zone', {}).get(zone_key)),
    )
    for label, until_dt in cooldowns:
        if until_dt and now_dt < until_dt:
            return f"SR breakout 失败冷却中 {label}, until={until_dt.strftime(BAR_TIME_FORMAT)}"
    return ''


def record_sr_breakout_failure_from_state(exit_time='', net_pnl_usdt=0.0):
    if net_pnl_usdt >= 0:
        return
    if trade_state.get('entry_module') != 'sr_breakout':
        return
    side = trade_state.get('side')
    zone_key = trade_state.get('entry_sr_breakout_key', '')
    if not side:
        return
    exit_dt = parse_bar_time(exit_time) if exit_time else get_server_now_dt()
    if not exit_dt:
        exit_dt = get_server_now_dt()
    side_until_dt = exit_dt + datetime.timedelta(seconds=SR_BREAKOUT_FAILURE_SIDE_COOLDOWN_SECONDS)
    zone_until_dt = exit_dt + datetime.timedelta(seconds=SR_BREAKOUT_FAILURE_ZONE_COOLDOWN_SECONDS)
    sr_breakout_failure_cooldowns['side'][side] = side_until_dt
    if zone_key:
        sr_breakout_failure_cooldowns['zone'][zone_key] = zone_until_dt
    logging.info(
        f"记录 SR breakout 失败冷却: side={side}, zone_key={zone_key}, "
        f"net={net_pnl_usdt:.4f}, side_until={side_until_dt.strftime(BAR_TIME_FORMAT)}, "
        f"zone_until={zone_until_dt.strftime(BAR_TIME_FORMAT)}"
    )


def sr_breakout_reverse_strength_reason(context, side, zone):
    """SR breakout 入场前，过滤已经被 1H/15M 强反向拉回关键区间的假突破。"""
    opposite = 'short' if side == 'long' else 'long'
    for timeframe in ('1h', '15m'):
        df = context['dfs'].get(timeframe)
        if df is None:
            continue
        df_closed = get_closed_df(df, timeframe, context['now_dt'])
        if len(df_closed) <= 0:
            continue
        large = latest_large_strong(df_closed)
        if large and large.get('direction') == opposite and sr_breakout_reverse_reclaimed_zone(side, zone, large):
            return f"{timeframe}最新大强{'阳' if opposite == 'long' else '阴'}线反向拉回SR区间"
        strong = latest_effective_strong(df_closed, opposite)
        if strong and sr_breakout_reverse_reclaimed_zone(side, zone, strong):
            return f"{timeframe}最新{strong.get('kind')}强{'阳' if opposite == 'long' else '阴'}线反向拉回SR区间"
    return ''


def sr_breakout_reverse_reclaimed_zone(side, zone, strong):
    if not zone or not strong:
        return False
    close = price_to_float(strong.get('close'))
    if side == 'short':
        return close >= price_to_float(zone.get('lower'))
    return close <= price_to_float(zone.get('upper'))


def calculate_candidate_amount(entry_price, stop_price, candidate=None, account_equity=None):
    """按候选信号计算仓位；SR breakout 只过滤过宽止损，其它保持原仓位模型。"""
    candidate = candidate or {}
    entry_price = price_to_float(entry_price)
    stop_price = price_to_float(stop_price)
    if entry_price <= 0:
        return 0, {'mode': 'invalid_entry_price'}

    try:
        if account_equity is None:
            balance = exchange.fetch_balance({'type': 'future'})
            account_equity = float(balance['total']['USDT'])
        else:
            account_equity = float(account_equity)
    except Exception as e:
        logging.error(f"计算候选仓位读取账户余额失败: {e}")
        return 0, {'mode': 'balance_failed', 'error': str(e)}

    normal_amount = account_equity * MARGIN_RATE * LEVERAGE / entry_price
    if candidate.get('module') != 'sr_breakout':
        return exchange.amount_to_precision(SYMBOL, normal_amount), {
            'mode': 'normal_notional',
            'normal_amount': normal_amount,
            'account_equity': account_equity
        }

    risk_distance = abs(entry_price - stop_price)
    atr_4h = price_to_float(candidate.get('profit_check_atr'))
    risk_atr = risk_distance / atr_4h if atr_4h > 0 else 0.0
    if not sr_breakout_risk_distance_allowed(risk_distance, atr_4h):
        return 0, {
            'mode': 'sr_breakout_risk_skip',
            'risk_distance': risk_distance,
            'atr_4h': atr_4h,
            'risk_atr': risk_atr
        }

    return exchange.amount_to_precision(SYMBOL, normal_amount), {
        'mode': 'sr_breakout_normal_notional_after_risk_filter',
        'risk_distance': risk_distance,
        'atr_4h': atr_4h,
        'risk_atr': risk_atr,
        'normal_amount': normal_amount,
        'account_equity': account_equity
    }


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


def get_latest_price(symbol=SYMBOL):
    """获取最新成交价，用于真实下单和风控"""
    ticker = exchange.fetch_ticker(symbol)
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
    raw_side = str(
        info.get('positionSide') or pos.get('positionSide') or pos.get('side') or ''
    ).strip().upper()
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
                'entry_price': price_to_float(info.get('entryPrice', pos.get('entryPrice', 0))),
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
        'workingType': STOP_WORKING_TYPE,
        # 客户端ID用于明确订单归属，后续动态更新只撤销本脚本创建的保护单。
        'newClientOrderId': make_protective_client_order_id(STRATEGY_STOP_CLIENT_PREFIX),
    }
    return exchange.create_order(SYMBOL, 'STOP_MARKET', stop_side, amount, None, params)


def normalize_exchange_bool(value):
    """把交易所返回的真假值统一转成 Python bool。"""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ('true', '1', 'yes')


def extract_order_position_side_upper(order):
    """提取 Binance 双向持仓字段，返回 BOTH / LONG / SHORT / ''。"""
    if not isinstance(order, dict):
        return ''
    info = order.get('info', {})
    if not isinstance(info, dict):
        info = {}
    return str(order.get('positionSide') or info.get('positionSide') or '').strip().upper()


def is_close_position_conditional_order(order, side=None, position_side=None):
    """判断某个 open order 是否是当前方向的全平仓条件单。"""
    if not isinstance(order, dict):
        return False

    info = order.get('info', {})
    if not isinstance(info, dict):
        info = {}

    #双向持仓模式下，平掉整个仓位的标志
    close_position = normalize_exchange_bool(
        info.get('closePosition', order.get('closePosition'))
    )
    #只减仓不加仓的标志
    reduce_only = normalize_exchange_bool(
        info.get('reduceOnly', order.get('reduceOnly'))
    )
    if not close_position and not reduce_only:
        return False

    #获取订单类型
    order_type = extract_order_type_upper(order)
    if order_type not in ('STOP', 'STOP_MARKET', 'TAKE_PROFIT', 'TAKE_PROFIT_MARKET'):
        return False

    if side:
        expected_side = 'SELL' if side == 'long' else 'BUY'
        #获取订单方向
        order_side = extract_order_side_upper(order)
        if order_side != expected_side:
            return False

    if position_side:
        raw_position_side = extract_order_position_side_upper(order)
        expected_position_side = str(position_side).strip().upper()
        # 单向模式通常返回 BOTH；没有字段时依靠订单买卖方向继续判断。
        if raw_position_side and raw_position_side not in ('BOTH', expected_position_side):
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


def normalize_futures_symbol(symbol):
    """把 BTCUSDT、BTC/USDT、BTC/USDT:USDT 统一成 CCXT U本位交易对。"""
    raw_symbol = str(symbol or '').strip().upper()
    if not raw_symbol:
        return ''
    if '/' in raw_symbol:
        base, quote_part = raw_symbol.split('/', 1)
        quote = quote_part.split(':', 1)[0]
        return f"{base}/{quote}"
    if raw_symbol.endswith('USDT') and len(raw_symbol) > 4:
        return f"{raw_symbol[:-4]}/USDT"
    return raw_symbol


def get_exchange_symbol_id(symbol=SYMBOL):
    """获取交易所原生symbol；未load_markets时按常见U本位格式兜底。"""
    symbol = normalize_futures_symbol(symbol)
    try:
        market = exchange.market(symbol)
        market_id = market.get('id')
        if market_id:
            return market_id
    except Exception:
        pass
    return symbol.replace('/', '').replace(':USDT', '')


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
        'clientOrderId': raw_order.get('clientOrderId'),
        'positionSide': raw_order.get('positionSide'),
        'symbol': raw_order.get('symbol'),
        'info': raw_order
    }


def normalize_raw_algo_order(raw_order):
    """把 Binance futures openAlgoOrders 里的条件委托包装成 CCXT-like 结构。"""
    if not isinstance(raw_order, dict):
        return raw_order
    return {
        'id': raw_order.get('algoId') or raw_order.get('clientAlgoId'),
        'type': raw_order.get('orderType') or raw_order.get('type'),
        'side': raw_order.get('side'),
        'status': raw_order.get('algoStatus'),
        'timestamp': raw_order.get('createTime') or raw_order.get('updateTime'),
        'stopPrice': raw_order.get('triggerPrice') or raw_order.get('stopPrice'),
        'price': raw_order.get('price'),
        'filled': raw_order.get('actualQty'),
        'closePosition': raw_order.get('closePosition'),
        'reduceOnly': raw_order.get('reduceOnly'),
        'clientAlgoId': raw_order.get('clientAlgoId'),
        'positionSide': raw_order.get('positionSide'),
        'symbol': raw_order.get('symbol'),
        'info': raw_order
    }


def fetch_raw_future_open_orders(symbol=SYMBOL):
    """直接调用币安U本位未成交单接口，补齐CCXT统一接口漏掉的条件单。"""
    method = getattr(exchange, 'fapiPrivateGetOpenOrders', None)
    if method is None:
        return []
    try:
        raw_orders = method({'symbol': get_exchange_symbol_id(symbol)})
    except Exception as e:
        logging.warning(f"原始接口查询U本位未成交单失败: {e}")
        return None
    if not isinstance(raw_orders, list):
        return []
    return [normalize_raw_open_order(order) for order in raw_orders]


def fetch_raw_future_open_algo_orders(symbol=SYMBOL):
    """直接调用 Binance U本位 openAlgoOrders，补齐 STOP_MARKET 条件委托。"""
    method = getattr(exchange, 'fapiPrivateGetOpenAlgoOrders', None)
    if method is None:
        return []
    try:
        raw_orders = method({'symbol': get_exchange_symbol_id(symbol)})
    except Exception as e:
        logging.warning(f"原始接口查询U本位未成交algo条件单失败: {e}")
        return None
    if not isinstance(raw_orders, list):
        return []
    return [normalize_raw_algo_order(order) for order in raw_orders]


def fetch_open_close_position_orders(side=None, perf=None, symbol=SYMBOL, position_side=None):
    """读取当前仓位方向上的全平仓条件单，便于做撤单确认和冲突排查。"""
    total_start = time.monotonic()
    open_orders = []
    unified_fetch_failed = False
    try:
        unified_start = time.monotonic()
        # 获取ccxt所有未成交的挂单
        open_orders = exchange.fetch_open_orders(symbol)
    except Exception as e:
        logging.warning(f"查询未成交条件单失败: {e}")
        unified_fetch_failed = True
        open_orders = []
    finally:
        record_perf(perf, 'fetch_open_close_position_orders_unified', unified_start)

    raw_open_start = time.monotonic()
    
    # 获取原始接口所有未成交的普通挂单
    raw_open_orders = fetch_raw_future_open_orders(symbol=symbol)
    record_perf(perf, 'fetch_open_close_position_orders_raw_open', raw_open_start)
    if raw_open_orders is None:
        if unified_fetch_failed:
            record_perf(perf, 'fetch_open_close_position_orders', total_start)
            return None
        raw_open_orders = []
    if raw_open_orders:
        known_order_ids = {extract_order_id(order) for order in open_orders if extract_order_id(order)}
        for raw_order in raw_open_orders:
            raw_order_id = extract_order_id(raw_order)
            if raw_order_id and raw_order_id in known_order_ids:
                continue
            open_orders.append(raw_order)

    raw_algo_start = time.monotonic()
    # 获取原始接口库所有未成交的条件委托挂单
    raw_algo_orders = fetch_raw_future_open_algo_orders(symbol=symbol)
    record_perf(perf, 'fetch_open_close_position_orders_raw_algo', raw_algo_start)
    if raw_algo_orders is None:
        if unified_fetch_failed and not open_orders:
            record_perf(perf, 'fetch_open_close_position_orders', total_start)
            return None
        raw_algo_orders = []
    if raw_algo_orders:
        known_order_ids = {extract_order_id(order) for order in open_orders if extract_order_id(order)}
        for raw_order in raw_algo_orders:
            raw_order_id = extract_order_id(raw_order)
            if raw_order_id and raw_order_id in known_order_ids:
                continue
            open_orders.append(raw_order)

    matched_orders = []
    for order in open_orders:
        # 过滤出平仓条件单，
        if is_close_position_conditional_order(order, side=side, position_side=position_side):
            matched_orders.append(order)
    record_perf(perf, 'fetch_open_close_position_orders', total_start)
    return matched_orders


def fetch_open_protective_stop_orders(side=None, symbol=SYMBOL, position_side=None, owned_only=False, tracked_order_id=''):
    """读取当前方向的 STOP / STOP_MARKET 全平仓条件单。"""
    matched_orders = fetch_open_close_position_orders(
        side=side,
        symbol=symbol,
        position_side=position_side,
    )
    if matched_orders is None:
        return None

    stop_orders = []
    for order in matched_orders:
        order_type = str(order.get('type') or order.get('info', {}).get('type') or '').strip().upper()
        if order_type in ('STOP', 'STOP_MARKET') and (
            not owned_only or is_script_owned_protective_order(order, tracked_order_id=tracked_order_id)
        ):
            stop_orders.append(order)
    return stop_orders


def cancel_conditional_order_exact(order, symbol=SYMBOL, silent=False, reason='清理条件委托'):
    """精确撤销一张普通或 algo 条件单，禁止用批量接口误伤其它订单。"""
    order_id = extract_order_id(order)
    if not order_id:
        return True
    info = order.get('info', {}) if isinstance(order, dict) else {}
    if not isinstance(info, dict):
        info = {}

    algo_id = info.get('algoId') or order.get('algoId')
    client_algo_id = info.get('clientAlgoId') or order.get('clientAlgoId')
    if algo_id or client_algo_id:
        raw_method = getattr(exchange, 'fapiPrivateDeleteAlgoOrder', None)
        if raw_method is None:
            logging.warning(f"{reason}: 缺少精确撤销algo条件单接口，拒绝批量撤单: id={order_id}")
            return False
        params = {'symbol': get_exchange_symbol_id(symbol)}
        if algo_id:
            params['algoId'] = algo_id
        else:
            params['clientAlgoId'] = client_algo_id
        try:
            raw_method(params)
            if not silent:
                logging.info(f"{reason}: 已精确撤销algo条件委托 {order_id}")
            return True
        except Exception as e:
            if is_order_already_absent_error(e):
                return True
            logging.warning(f"{reason}: 精确撤销algo条件委托失败({order_id}): {e}")
            return False

    try:
        exchange.cancel_order(order_id, symbol)
        if not silent:
            logging.info(f"{reason}: 已撤销条件委托 {order_id}")
        return True
    except Exception as e:
        if is_order_already_absent_error(e):
            return True
        logging.warning(f"{reason}: 撤销条件委托失败({order_id}): {e}")
        return False


def cancel_conditional_orders(orders, silent=False, reason='清理残留条件委托', perf=None, symbol=SYMBOL):
    """只精确撤销传入的 closePosition/reduceOnly 条件委托，不影响普通开仓挂单。"""
    cancel_start = time.monotonic()
    unique_orders = []
    seen_order_ids = set()
    for order in orders or []:
        order_id = extract_order_id(order)
        if not order_id or order_id in seen_order_ids:
            continue
        seen_order_ids.add(order_id)
        unique_orders.append(order)
    if not unique_orders:
        record_perf(perf, 'cancel_conditional_orders', cancel_start)
        return True

    cancel_failed = False
    for order in unique_orders:
        if not cancel_conditional_order_exact(order, symbol=symbol, silent=silent, reason=reason):
            cancel_failed = True

    record_perf(perf, 'cancel_conditional_orders', cancel_start)
    return not cancel_failed


def cancel_all_close_position_conditional_orders(silent=False, reason='无仓位清理残留条件委托', perf=None):
    """没有交易所仓位时，清掉本交易对所有平仓类条件委托。"""
    # 获取所有平仓条件单
    matched_orders = fetch_open_close_position_orders(side=None, perf=perf)
    if matched_orders is None:
        return False
    if not matched_orders:
        #重置本地止损单状态
        clear_local_stop_order_state()
        return True
    # 撤销所有平仓条件单
    ok = cancel_conditional_orders(matched_orders, silent=silent, reason=reason, perf=perf)
    if ok:
        ##重置本地止损单状态
        clear_local_stop_order_state()
    return ok


def reconcile_conditional_orders_for_position(side, silent=False):
    """ETH策略持仓存在时，只归并脚本自有保护单，绝不触碰手工条件单。"""
    
    matched_orders = fetch_open_close_position_orders(side=None)
    if matched_orders is None:
        return False

    expected_side = 'SELL' if side == 'long' else 'BUY'
    same_side_stops = []
    orders_to_cancel = []
    for order in matched_orders:
        if not is_script_owned_protective_order(order, tracked_order_id=trade_state.get('stop_order_id', '')):
            continue
        # 提取订单方向和类型
        order_side = extract_order_side_upper(order)
        order_type = extract_order_type_upper(order)
        if order_side != expected_side:
            orders_to_cancel.append(order)
            continue
        if order_type in ('STOP', 'STOP_MARKET'):
            # 条件委托
            same_side_stops.append(order)
        else:
            # 普通挂单
            orders_to_cancel.append(order)

    active_order = pick_active_protective_stop_order(
        same_side_stops,
        preferred_order_id=trade_state.get('stop_order_id', '')
    )
    active_order_id = extract_order_id(active_order) if active_order else ''
    for order in same_side_stops:
        if extract_order_id(order) != active_order_id:
            orders_to_cancel.append(order)

    if orders_to_cancel:
        order_ids = [extract_order_id(order) or 'unknown' for order in orders_to_cancel]
        if not silent:
            logging.warning(f"清理重复/反方向条件委托: side={side}, order_ids={order_ids}")
        if not cancel_conditional_orders(orders_to_cancel, silent=silent, reason='清理重复/反方向条件委托'):
            return False

    if active_order:
        active_stop_price = extract_order_stop_price(active_order)
        trade_state['stop_order_id'] = active_order_id
        trade_state['stop_order_price'] = float(active_stop_price or 0.0)
    else:
        clear_local_stop_order_state()
    return True


def cleanup_orphan_conditional_orders_if_needed(force=False, silent=True, perf=None):
    """本地无仓位时定期清理残留条件委托；有交易所仓位则不贸然撤单。"""
    global last_conditional_order_cleanup_monotonic
    now = time.monotonic()
    if not force and (now - last_conditional_order_cleanup_monotonic) < CONDITIONAL_ORDER_CLEANUP_INTERVAL_SECONDS:
        return True

    position_risk_start = time.monotonic()
    position_risk = get_position_risk()
    record_perf(perf, 'empty_position_cleanup_get_position_risk', position_risk_start)
    if position_risk and position_risk.get('fetch_failed'):
        logging.warning("清理残留条件委托前无法确认交易所仓位，本轮跳过，避免误撤")
        return False
    if position_risk is not None:
        logging.warning(
            f"本地无仓位但交易所仍有仓位，跳过无仓位条件委托清理: "
            f"side={position_risk.get('side')}, amount={position_risk.get('position_amt')}"
        )
        return False

    last_conditional_order_cleanup_monotonic = now
    cancel_all_start = time.monotonic()
    ok = cancel_all_close_position_conditional_orders(silent=silent, perf=perf)
    record_perf(perf, 'empty_position_cleanup_cancel_all_close_position_orders', cancel_all_start)
    return ok


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

    stop_orders = fetch_open_protective_stop_orders(
        side=side,
        owned_only=True,
        tracked_order_id=trade_state.get('stop_order_id', ''),
    )
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
        matched_orders = fetch_open_protective_stop_orders(
            side=side,
            owned_only=True,
            tracked_order_id=trade_state.get('stop_order_id', ''),
        )
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
    synced_orders = []
    if sync_result is not None:
        synced_orders = list(sync_result['orders'])
        stop_order_ids = [extract_order_id(order) for order in synced_orders if extract_order_id(order)]

    local_stop_order_id = str(trade_state.get('stop_order_id', '') or '') or local_stop_order_id_before_sync
    if local_stop_order_id and local_stop_order_id not in stop_order_ids:
        stop_order_ids.append(local_stop_order_id)

    stop_order_ids = list(dict.fromkeys([order_id for order_id in stop_order_ids if order_id]))
    if not stop_order_ids:
        clear_local_stop_order_state()
        return True

    if synced_orders:
        if not cancel_conditional_orders(synced_orders, silent=silent, reason='撤销旧服务端止损单'):
            return False
        local_only_ids = [
            order_id for order_id in stop_order_ids
            if order_id not in {extract_order_id(order) for order in synced_orders if extract_order_id(order)}
        ]
        stop_order_ids = local_only_ids
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

            matched_orders = fetch_open_protective_stop_orders(
                side=side or None,
                owned_only=True,
                tracked_order_id=stop_order_id,
            )
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
                # 冲突恢复也只能处理脚本自有保护单；手工止损/止盈必须原样保留。
                matched_orders = fetch_open_protective_stop_orders(
                    side=side,
                    owned_only=True,
                    tracked_order_id=trade_state.get('stop_order_id', ''),
                )
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
                        logging.warning(
                            "closePosition 冲突但没有发现脚本自有条件单；为保护手工委托，拒绝批量撤单"
                        )
                        return False
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
            entry_trigger_price=trade_state.get('entry_trigger_price', ''),
            entry_signal_bar_15m=trade_state.get('entry_signal_bar_15m', ''),
            exit_signal_bar_15m=exit_signal_bar_15m,
            exit_trigger='external_close_detected',
            holding_seconds=compute_holding_seconds(entry_time, detected_time),
            open_order_id=trade_state.get('open_order_id', ''),
            close_order_id=str(external_context.get('external_close_order_id', ''))
        )
        record_sr_breakout_failure_from_state(exit_time=detected_time, net_pnl_usdt=net_pnl_usdt)

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
        f"入场触发价: {trade_state.get('entry_trigger_price', 0)}\n"
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
    cancel_all_close_position_conditional_orders(silent=True, reason='外部平仓后清理残留条件委托')
    trade_state.update({
        'has_position': False,
        'side': None,
        'entry_price': 0,
        'entry_trigger_price': 0.0,
        'stop_loss_price': 0,
        'highest_price': 0,
        'lowest_price': 0,
        'amount': 0,
        'entry_initial_amount': 0.0,
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
        'entry_module': '',
        'entry_sr_breakout_key': '',
        'entry_sr_target': 0.0,
        'entry_initial_risk': 0.0,
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
        'position_miss_count': 0,
        'ai_snapshot_json': '',
        'ai_last_reduce_generated_at': '',
        'ai_last_reduce_target_ratio': 1.0,
        'ai_partial_reduce_count': 0,
        'ai_partial_reduce_amount': 0.0,
        'ai_partial_reduce_realized_pnl': 0.0,
        'ai_partial_reduce_fee': 0.0,
        'ai_reduce_reasons': []
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


def log_trade_to_csv(entry_time, side, cond_4h, cond_1h, cond_15m, entry_reason, exit_time, close_reason, pnl_points, fee_cost, net_pnl_usdt, is_profit, entry_trigger_price='', entry_signal_bar_15m='', exit_signal_bar_15m='', exit_trigger='', holding_seconds=0, open_order_id='', close_order_id=''):
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
                entry_trigger_price,
                exit_time, close_reason, pnl_points, fee_cost, net_pnl_usdt, is_profit,
                entry_signal_bar_15m, exit_signal_bar_15m, exit_trigger, holding_seconds,
                open_order_id, close_order_id
            ])
        update_daily_pnl_stats(exit_time, net_pnl_usdt)
    except Exception as e:
        logging.error(f"写入CSV失败: {e}")


PENDING_ENTRY_FIELDS = (
    'pending_entry_order_id',
    'pending_entry_side',
    'pending_entry_amount',
    'pending_entry_stop_price',
    'pending_entry_limit_price',
    'pending_entry_protective_stop',
    'pending_entry_target',
    'pending_entry_strategy_tf',
    'pending_entry_background_tf',
    'pending_entry_trigger_tf',
    'pending_entry_exit_tf',
    'pending_entry_module',
    'pending_entry_sr_breakout_key',
    'pending_entry_created_time',
    'pending_entry_signal_bar',
    'pending_entry_candidate_json',
    'pending_entry_triggered',
    'pending_entry_filled_amount',
    'pending_entry_missing_count',
    'pending_entry_initial_balance',
    'pending_entry_reason',
    'pending_entry_cond_4h',
    'pending_entry_cond_1h',
    'pending_entry_cond_15m',
    'pending_entry_condition_details',
    'pending_entry_profit_check_atr',
    'pending_entry_ai_snapshot_json',
)


def pending_entry_default_value(field):
    if field in (
        'pending_entry_amount',
        'pending_entry_stop_price',
        'pending_entry_limit_price',
        'pending_entry_protective_stop',
        'pending_entry_target',
        'pending_entry_filled_amount',
        'pending_entry_initial_balance',
        'pending_entry_profit_check_atr',
    ):
        return 0.0
    if field == 'pending_entry_triggered':
        return False
    if field == 'pending_entry_missing_count':
        return 0
    return ''


def clear_pending_entry_state(reason=''):
    """清空本地 pending STOP_LIMIT 入场状态。"""
    for field in PENDING_ENTRY_FIELDS:
        trade_state[field] = pending_entry_default_value(field)
    if reason:
        logging.info(f"已清空 pending 入场状态: {reason}")


def has_pending_entry():
    return bool(trade_state.get('pending_entry_order_id'))


def extract_order_status_lower(order):
    if not isinstance(order, dict):
        return ''
    info = order.get('info', {})
    if not isinstance(info, dict):
        info = {}
    return str(order.get('status') or info.get('status') or info.get('algoStatus') or '').strip().lower()


def extract_order_filled_amount(order):
    if not isinstance(order, dict):
        return 0.0
    info = order.get('info', {})
    if not isinstance(info, dict):
        info = {}
    for value in (
        #订单可能来自 CCXT 统一接口、Binance 原生 openOrders、或 Binance 原生 openAlgoOrders，每种格式的字段名不一样，兜底遍历保证不漏。
        order.get('filled'), #CCXT 统一格式的已成交数量
        order.get('filledAmount'), #CCXT 统一格式的已成交数量
        info.get('executedQty'), #Binance 原始格式的已成交数量
        info.get('cumQty'), #Binance 原始格式的已成交数量
        info.get('actualQty'), #Binance openAlgoOrders 里的实际成交数量
        info.get('filledAmount'), #info 里的 filledAmount 字段
    ):
        amount = price_to_float(value)
        if amount > 0:
            return amount
    return 0.0


def extract_order_average_price(order):
    if not isinstance(order, dict):
        return 0.0
    info = order.get('info', {})
    if not isinstance(info, dict):
        info = {}
    for value in (
        order.get('average'), #CCXT 统一格式的加权均价
        order.get('avgPrice'), #归一化后的均价
        order.get('price'), #订单委托价
        info.get('avgPrice'), #Binance info 里的均价
        info.get('price'), #Binance info 里的委托价
    ):
        price = price_to_float(value)
        if price > 0:
            return price
    return 0.0


def fetch_all_open_orders_for_symbol():
    """读取当前交易对全部未成交单，包含 CCXT 统一接口和 Binance 原始接口。"""
    open_orders = []
    try:
        # 获取 CCXT 统一接口的未成交单
        open_orders = exchange.fetch_open_orders(SYMBOL)
    except Exception as e:
        logging.warning(f"查询未成交单失败: {e}")
        open_orders = []

    # 添加 Binance 原生接口的普通挂单（LIMIT、STOP_LIMIT 等）,普通挂单是：限价成交	
    raw_open_orders = fetch_raw_future_open_orders()
    if raw_open_orders:
        known_order_ids = {extract_order_id(order) for order in open_orders if extract_order_id(order)}
        for raw_order in raw_open_orders:
            raw_order_id = extract_order_id(raw_order)
            if raw_order_id and raw_order_id in known_order_ids:
                continue
            open_orders.append(raw_order)

    # 添加 Binance 原生接口的条件委托挂单（STOP_MARKET、TAKE_PROFIT_MARKET、TRAILING_STOP_MARKET等），条件委托是：市价成交
    raw_algo_orders = fetch_raw_future_open_algo_orders()
    if raw_algo_orders:
        known_order_ids = {extract_order_id(order) for order in open_orders if extract_order_id(order)}
        for raw_order in raw_algo_orders:
            raw_order_id = extract_order_id(raw_order)
            if raw_order_id and raw_order_id in known_order_ids:
                continue
            open_orders.append(raw_order)

    return open_orders


def fetch_pending_entry_order():
    """优先按订单ID查询 pending 入场单，失败时从 open orders 里兜底查找。"""
    order_id = str(trade_state.get('pending_entry_order_id') or '')
    if not order_id:
        return None

    try:
        order = exchange.fetch_order(order_id, SYMBOL)
        if order:
            return order
    except Exception as e:
        if not is_order_already_absent_error(e):
            logging.warning(f"查询 pending 入场单详情失败，将用未成交列表兜底: id={order_id}, error={e}")

    # 获取所有未成交挂单
    for order in fetch_all_open_orders_for_symbol():
        if extract_order_id(order) == order_id:
            return order
    return None


def cancel_pending_entry_algo_order(order_id, reason='', silent=False):
    """撤销 Binance futures algo 条件入场单；返回 True/False/None，None 表示没找到目标 algo 单。"""
    order_id = str(order_id or '')
    if not order_id:
        return None

    get_method = getattr(exchange, 'fapiPrivateGetOpenAlgoOrders', None)
    if get_method is None:
        return None

    try:
        raw_orders = get_method({'symbol': get_exchange_symbol_id()})
    except Exception as e:
        logging.warning(f"查询 pending algo 入场单失败: id={order_id}, reason={reason}, error={e}")
        return False

    if not isinstance(raw_orders, list):
        raw_orders = []

    target_orders = []
    for raw_order in raw_orders:
        raw_algo_id = str(raw_order.get('algoId') or '')
        raw_client_algo_id = str(raw_order.get('clientAlgoId') or '')
        if order_id in (raw_algo_id, raw_client_algo_id):
            target_orders.append(raw_order)

    if not target_orders:
        return None

    delete_one_method = getattr(exchange, 'fapiPrivateDeleteAlgoOrder', None)
    if delete_one_method is not None:
        all_cancelled = True
        for raw_order in target_orders:
            params = {'symbol': get_exchange_symbol_id()}
            raw_algo_id = raw_order.get('algoId')
            raw_client_algo_id = raw_order.get('clientAlgoId')
            if raw_algo_id:
                params['algoId'] = raw_algo_id
            elif raw_client_algo_id:
                params['clientAlgoId'] = raw_client_algo_id
            try:
                delete_one_method(params)
                if not silent:
                    logging.info(f"{reason}: 已撤销 pending algo 入场单 {params}")
            except Exception as e:
                all_cancelled = False
                logging.warning(f"{reason}: 撤销 pending algo 入场单失败 {params}: {e}")
        if all_cancelled:
            return True

    # 精确撤销失败时不再调用 DeleteAlgoOpenOrders；批量接口可能同时删掉手工止损/止盈。
    logging.warning(f"{reason}: pending algo 入场单无法精确撤销，已拒绝批量撤单: id={order_id}")
    return False


def cancel_pending_entry_order(reason, silent=False, clear_state=True):
    """撤销 pending STOP_LIMIT 入场单并按需清空本地 pending 状态。"""
    order_id = str(trade_state.get('pending_entry_order_id') or '')
    if not order_id:
        if clear_state:
            clear_pending_entry_state(reason)
        return True

    ok = True
    try:
        exchange.cancel_order(order_id, SYMBOL)
        if not silent:
            logging.info(f"已撤销 pending 入场单: id={order_id}, reason={reason}")
    except Exception as e:
        algo_cancelled = cancel_pending_entry_algo_order(order_id, reason=reason, silent=silent)
        if algo_cancelled is True:
            ok = True
        elif algo_cancelled is False:
            ok = False
        elif is_order_already_absent_error(e):
            if not silent:
                logging.warning(f"pending 入场单已不存在，按撤销成功处理: id={order_id}, reason={reason}, error={e}")
        else:
            ok = False
            logging.warning(f"撤销 pending 入场单失败: id={order_id}, reason={reason}, error={e}")

    if ok and clear_state:
        clear_pending_entry_state(reason)
    return ok


def pending_entry_age_seconds(now_dt=None):
    created_dt = parse_bar_time(trade_state.get('pending_entry_created_time', ''))
    if created_dt is None:
        return 0
    if now_dt is None:
        now_dt = get_server_now_dt()
    elif now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=EXCHANGE_TZ)
    else:
        now_dt = now_dt.astimezone(EXCHANGE_TZ)
    return max(0, int((now_dt - created_dt).total_seconds()))


def pending_entry_trend_still_valid(context):
    side = trade_state.get('pending_entry_side')
    strategy_tf = trade_state.get('pending_entry_strategy_tf')
    if side not in ('long', 'short') or not strategy_tf:
        return False, 'pending 入场方向/策略周期缺失'

    background_tf = trade_state.get('pending_entry_background_tf') or STRATEGY_BACKGROUND_TF.get(strategy_tf)
    local_state = context.get('trend_states', {}).get(strategy_tf)
    background_state = context.get('trend_states', {}).get(background_tf)
    return context_allows_side(local_state, background_state, side)


def pending_entry_price_still_reasonable(curr_price):
    side = trade_state.get('pending_entry_side')
    stop_price = price_to_float(trade_state.get('pending_entry_stop_price'))
    limit_price = price_to_float(trade_state.get('pending_entry_limit_price'))
    tick = get_price_tick(curr_price)
    if side == 'long' and curr_price > limit_price + tick:
        return False, f"多单当前价已超过 pending 限价: current={curr_price}, limit={limit_price}"
    if side == 'short' and curr_price < limit_price - tick:
        return False, f"空单当前价已跌破 pending 限价: current={curr_price}, limit={limit_price}"
    if side == 'long' and limit_price < stop_price:
        return False, f"多单 pending 限价低于触发价: stop={stop_price}, limit={limit_price}"
    if side == 'short' and limit_price > stop_price:
        return False, f"空单 pending 限价高于触发价: stop={stop_price}, limit={limit_price}"
    return True, 'ok'


def panic_close_pending_entry(side, amount, actual_price, reason, order=None):
    """pending 入场成交后发现无效，未转正式持仓前直接反向市价撤退。"""
    close_order_id = ''
    try:
        panic_side = 'sell' if side == 'long' else 'buy'
        #平仓
        close_order = exchange.create_market_order(SYMBOL, panic_side, amount)
        close_order_id = extract_order_id(close_order)
        # 没有仓位的时候，清理残留的条件委托
        cancel_all_close_position_conditional_orders(silent=True, reason='pending 入场撤退后清理残留条件委托')
    except Exception as e:
        logging.error(f"pending 入场撤退平仓失败: {e}")
        logging.error(traceback.format_exc())
        return False

    order_id_lines = format_order_id_lines(
        open_order_id=str(trade_state.get('pending_entry_order_id') or extract_order_id(order)),
        close_order_id=close_order_id
    )
    order_id_suffix = f"\n{order_id_lines}" if order_id_lines else ''
    send_msg(
        "ETH交易: ⚠️STOP_LIMIT成交后立即撤退",
        f"原因: {reason}\n方向: {side}\n入场估算价: {actual_price}\n数量: {amount}"
        f"{order_id_suffix}"
    )
    # 清理 pending STOP_LIMIT 入场状态
    clear_pending_entry_state(reason)
    return False


def finalize_pending_entry_position(order=None, position_risk=None, signal_bar_15m=''):
    """pending 入场确认成交后，初始化本地持仓并挂保护止损。"""
    side = trade_state.get('pending_entry_side')
    if side not in ('long', 'short'):
        logging.error("pending 入场成交后方向缺失，无法初始化持仓")
        return False

    if position_risk is None:
        # 获取持仓的风险信息：包括，开仓价格，强平价格和持仓数量，标记价格，持仓方向
        position_risk = get_position_risk(side=side)
    if position_risk and position_risk.get('fetch_failed'):
        logging.warning("pending 入场可能已成交，但无法获取交易所仓位，本轮暂不初始化持仓")
        return False
    # 获取 pending 入场订单的成交数量
    filled_amount = abs(price_to_float(position_risk.get('position_amt'))) if position_risk else 0.0
    if filled_amount <= 0:
        # 是从不同来源的订单对象中提取已成交数量
        filled_amount = extract_order_filled_amount(order)
    if filled_amount <= 0:
        logging.warning("pending 入场尚未确认到有效成交数量")
        return False
    # 获取 pending 入场订单的实际成交价
    # 从持仓信息中获取实际成交价
    actual_price = price_to_float(position_risk.get('entry_price')) if position_risk else 0.0
    # 如果持仓信息中没有实际成交价，则从订单中获取
    if actual_price <= 0:
        # 从不同来源提取实际成交均价
        actual_price = extract_order_average_price(order)
    if actual_price <= 0:
        actual_price = price_to_float(trade_state.get('pending_entry_limit_price'))

    # 获取 pending 入场订单的保护止损价
    protective_stop = price_to_float(trade_state.get('pending_entry_protective_stop'))
    actual_liquidation_price = None
    if position_risk and not position_risk.get('fetch_failed'):
        # 从持仓信息中获取实际强平价
        actual_liquidation_price = position_risk.get('liquidation_price')
    # 把保护止损价格推向安全侧
    actual_safe_stop, actual_stop_meta = ensure_stop_price_safe(
        actual_price,
        protective_stop,
        side,
        liquidation_price=actual_liquidation_price
    )
    # 判断止损价格是否仍然有效
    if not stop_price_is_still_valid(actual_price, actual_safe_stop, side):
        logging.error(
            f"pending 入场成交后发现强平价/止损无安全空间，立即撤退: "
            f"side={side}, entry={actual_price}, stop={actual_safe_stop}, liq={actual_liquidation_price}"
        )
        # 无效的话，立马进行平仓，然后清空条件委托挂单，并重置本地止损状态和pending 入场状态
        return panic_close_pending_entry(
            side,
            filled_amount,
            actual_price,
            "真实强平价过近，止损无安全空间",
            order=order
        )

    target = price_to_float(trade_state.get('pending_entry_target'))
    profit_check_atr = price_to_float(trade_state.get('pending_entry_profit_check_atr'))
    # 判断目标价是否仍然有效
    target_ok, target_reason = target_profit_still_valid(side, actual_price, actual_safe_stop, target, profit_check_atr)
    if not target_ok:
        logging.error(f"pending 入场成交后目标位对实际成交价无效，立即撤退: {target_reason}")
        return panic_close_pending_entry(
            side,
            filled_amount,
            actual_price,
            f"目标位对实际成交价已无有效利润空间；{target_reason}",
            order=order
        )

    taker_fee_rate = get_trading_fee_rate()
    open_fee = actual_price * filled_amount * taker_fee_rate
    entry_time = get_server_time_str()
    open_order_id = str(trade_state.get('pending_entry_order_id') or extract_order_id(order))
    entry_trigger_price = price_to_float(trade_state.get('pending_entry_stop_price'))
    #触发周期
    entry_trigger_tf_display = trade_state.get('pending_entry_trigger_tf') or 'STOP_LIMIT'
    #入场信号k线时间
    entry_signal_bar_15m = signal_bar_15m or trade_state.get('pending_entry_signal_bar', '')
    #入场原因
    open_condition_details = trade_state.get('pending_entry_condition_details', '')

    trade_state.update({
        'has_position': True,
        'side': side,
        'entry_price': actual_price,
        'entry_trigger_price': entry_trigger_price,
        'stop_loss_price': actual_safe_stop,
        'highest_price': actual_price,
        'lowest_price': actual_price,
        'amount': filled_amount,
        'entry_initial_amount': filled_amount,
        'entry_time': entry_time,
        'cond_4h': trade_state.get('pending_entry_cond_4h', ''),
        'cond_1h': trade_state.get('pending_entry_cond_1h', ''),
        'cond_15m': trade_state.get('pending_entry_cond_15m', ''),
        'close_cond_4h': '',
        'close_cond_1h': '',
        'close_cond_15m': '',
        'entry_reason': trade_state.get('pending_entry_reason', ''),
        'entry_trigger_tf': entry_trigger_tf_display,
        'entry_strategy_tf': trade_state.get('pending_entry_strategy_tf', ''),
        'entry_exit_tf': trade_state.get('pending_entry_exit_tf', ''),
        'entry_module': trade_state.get('pending_entry_module', ''),
        'entry_sr_breakout_key': trade_state.get('pending_entry_sr_breakout_key', ''),
        'entry_sr_target': target,
        'entry_initial_risk': abs(float(actual_price) - float(actual_safe_stop)),
        'initial_balance': price_to_float(trade_state.get('pending_entry_initial_balance')),
        'open_fee': open_fee,
        'open_order_id': open_order_id,
        'close_order_id': '',
        'liquidation_price': actual_liquidation_price or 0.0,
        'stop_order_id': '',
        'stop_order_price': 0.0,
        'stop_order_refresh_fail_count': 0,
        'last_stop_order_refresh_error': '',
        'entry_signal_bar_15m': entry_signal_bar_15m,
        'last_entry_bar_15m': entry_signal_bar_15m,
        'position_miss_count': 0,
        'ai_snapshot_json': trade_state.get('pending_entry_ai_snapshot_json', ''),
        'ai_last_reduce_generated_at': '',
        'ai_last_reduce_target_ratio': 1.0,
        'ai_partial_reduce_count': 0,
        'ai_partial_reduce_amount': 0.0,
        'ai_partial_reduce_realized_pnl': 0.0,
        'ai_partial_reduce_fee': 0.0,
        'ai_reduce_reasons': []
    })
    # 更新保护止损
    if not refresh_protective_stop_order(actual_safe_stop):
        logging.error("pending 入场成交后服务端止损单挂单失败，立即主动平仓避免裸露风险")
        clear_pending_entry_state('pending 入场已成交但保护止损失败，转主动平仓')
        close_position(
            "服务端止损挂单失败，主动平仓",
            curr_price=actual_price,
            signal_bar_15m=entry_signal_bar_15m,
            trigger_label="服务端止损挂单失败"
        )
        return False

    # 重置 本地pending 状态
    clear_pending_entry_state('pending 入场已成交并转为持仓')

    order_id_lines = format_order_id_lines(
        open_order_id=open_order_id,
        stop_order_id=trade_state.get('stop_order_id', '')
    )
    order_id_suffix = f"\n{order_id_lines}" if order_id_lines else ''
    initial_usdt = price_to_float(trade_state.get('initial_balance'))
    msg = (
        f"🚀 【STOP_LIMIT已成交开仓】\n方向: {side}\n入场均价: {actual_price}\n"
        f"入场触发价: {entry_trigger_price}\n"
        f"止损价: {actual_safe_stop}\n目标位: {target}\n强平价: {actual_liquidation_price}\n数量: {filled_amount}\n"
        f"杠杆: {LEVERAGE}x\n挂单前账户资金(USDT): {initial_usdt:.4f}\n"
        f"入场原因: {trade_state.get('entry_reason', '')}\n触发周期: {entry_trigger_tf_display}\n"
        f"15M信号时间: {entry_signal_bar_15m}\n开仓条件明细:\n"
        f"{open_condition_details or trade_state.get('cond_15m', '')}"
        f"{order_id_suffix}"
    )
    send_msg(f"ETH交易: STOP_LIMIT成交开仓 {side}", msg)
    logging.info(
        f"pending STOP_LIMIT 入场已成交并完成保护止损: side={side}, entry={actual_price}, "
        f"amount={filled_amount}, stop={actual_safe_stop}, target={target}, "
        f"open_order_id={open_order_id}, stop_order_id={trade_state.get('stop_order_id', '')}, stop_meta={actual_stop_meta}"
    )
    return True


def place_pending_entry_order(side, candidate, curr_price, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='trend', entry_trigger_tf='', entry_meta=None):
    """通过 STOP_LIMIT 挂等待入场单；不设置持仓，也不提前挂保护止损。"""
    global trade_state
    entry_meta = entry_meta or {}
    if DRY_RUN_OPEN_ORDER:
        logging.warning(
            "DRY_RUN_OPEN_ORDER: 已拦截 STOP_LIMIT 入场 side=%s, candidate=%s, current=%s, signal_bar_15m=%s",
            side,
            candidate,
            curr_price,
            signal_bar_15m
        )
        return False
    
    entry_level = precision_price(price_to_float(candidate.get('entry_level')))
    #触发价格
    stop_price = entry_level
    # 计算允许的滑点
    allowed_slippage = candidate_entry_allowed_slippage(candidate, entry_level)
    # 计算限价
    if side == 'long':
        limit_price = precision_price(entry_level + allowed_slippage)
    else:
        limit_price = precision_price(entry_level - allowed_slippage)

    protective_stop = price_to_float(candidate.get('stop'))
    # 把止损价推至保护侧(不会触发强平)
    estimated_safe_stop, estimated_stop_meta = ensure_stop_price_safe(entry_level, protective_stop, side, liquidation_price=None)
    # 判断止损价格是否仍然有效
    if not stop_price_is_still_valid(entry_level, estimated_safe_stop, side):
        logging.warning(
            f"拒绝挂 STOP_LIMIT 入场：预估强平价过近，止损无效 side={side}, "
            f"entry={entry_level}, stop={protective_stop}, adjusted_stop={estimated_safe_stop}, meta={estimated_stop_meta}"
        )
        return False

    target = price_to_float(candidate.get('target'))
    profit_check_atr = price_to_float(candidate.get('profit_check_atr'))
    # 判断目标价是否仍然有效
    target_ok, target_reason = target_profit_still_valid(side, entry_level, estimated_safe_stop, target, profit_check_atr)
    if not target_ok:
        logging.info(f"拒绝挂 STOP_LIMIT 入场：候选目标利润无效: {target_reason}")
        return False

    try:
        balance_before = exchange.fetch_balance({'type': 'future'})
        initial_usdt = float(balance_before['total']['USDT']) # 开仓前账户资金(USDT)
    except Exception as e:
        logging.error(f"拒绝挂 STOP_LIMIT 入场：读取账户资金失败: {e}")
        return False

    amount_raw, amount_meta = calculate_candidate_amount(
        entry_level,
        estimated_safe_stop,
        candidate=candidate,
        account_equity=initial_usdt
    )
    amount = float(amount_raw)
    if amount <= 0:
        logging.info(
            f"拒绝挂 STOP_LIMIT 入场：候选仓位为0或风险过滤跳过 side={side}, "
            f"entry={entry_level}, stop={estimated_safe_stop}, amount_meta={amount_meta}"
        )
        return False

    def format_cond(state, side_dir):
        return format_entry_condition_for_mail(state, side_dir, entry_reason)

    def build_cond_str(state, side_dir):
        condensed = format_cond(state, side_dir)
        return f"原因:{entry_reason} | {condensed}" if condensed else f"原因:{entry_reason}"

    cond_4h_str = build_cond_str(state_4h, side)
    cond_1h_str = build_cond_str(state_1h, side)
    cond_15m_str = build_cond_str(state_15m, side)
    open_condition_lines = []
    if format_cond(state_4h, side):
        open_condition_lines.append(f"4H({state_4h.get('signal_bar_time', '')}): {cond_4h_str}")
    if format_cond(state_1h, side):
        open_condition_lines.append(f"1H({state_1h.get('signal_bar_time', '')}): {cond_1h_str}")
    if format_cond(state_15m, side):
        open_condition_lines.append(f"15M({state_15m.get('signal_bar_time', '')}): {cond_15m_str}")
    open_condition_details = '\n'.join(open_condition_lines) if open_condition_lines else f"原因:{entry_reason}"

    try:
        exchange.set_leverage(LEVERAGE, SYMBOL)

        order_side = 'buy' if side == 'long' else 'sell'
        params = {
            'stopPrice': stop_price, # 触发价：标记价 ≥ 3010 时激活
            'timeInForce': 'GTC', #  一直有效直到成交或 45 分钟超时
            'workingType': STOP_WORKING_TYPE # 按标记价触发（非最新成交价）
        }
        order = exchange.create_order(
            SYMBOL,
            ENTRY_STOP_LIMIT_ORDER_TYPE,
            order_side,
            amount,
            limit_price,
            params
        )
        order_id = extract_order_id(order)
        if not order_id:
            raise RuntimeError(f"STOP_LIMIT 入场挂单未返回订单ID: {order}")

        created_time = get_server_time_str()
        background_tf = STRATEGY_BACKGROUND_TF.get(candidate.get('strategy_tf'), '1h')
        trade_state.update({
            'pending_entry_order_id': order_id,
            'pending_entry_side': side,
            'pending_entry_amount': amount,
            'pending_entry_stop_price': stop_price,
            'pending_entry_limit_price': limit_price,
            'pending_entry_protective_stop': precision_price(estimated_safe_stop),
            'pending_entry_target': target,
            'pending_entry_strategy_tf': candidate.get('strategy_tf', ''),
            'pending_entry_background_tf': background_tf,
            'pending_entry_trigger_tf': entry_trigger_tf or f"{candidate.get('strategy_tf')}->{candidate.get('trigger_tf')}",
            'pending_entry_exit_tf': entry_meta.get('exit_tf') or candidate.get('exit_tf', ''),
            'pending_entry_module': candidate.get('module', ''),
            'pending_entry_sr_breakout_key': candidate.get('sr_breakout_key', ''),
            'pending_entry_created_time': created_time,
            'pending_entry_signal_bar': signal_bar_15m,
            'pending_entry_candidate_json': json.dumps(candidate, ensure_ascii=False, default=str),
            'pending_entry_triggered': False,
            'pending_entry_filled_amount': 0.0,
            'pending_entry_missing_count': 0,
            'pending_entry_initial_balance': initial_usdt,
            'pending_entry_reason': entry_reason,
            'pending_entry_cond_4h': cond_4h_str,
            'pending_entry_cond_1h': cond_1h_str,
            'pending_entry_cond_15m': cond_15m_str,
            'pending_entry_condition_details': open_condition_details,
            'pending_entry_profit_check_atr': profit_check_atr,
            'pending_entry_ai_snapshot_json': entry_meta.get('ai_snapshot_json', ''),
            'last_entry_bar_15m': signal_bar_15m
        })

        send_msg(
            f"ETH交易: 已挂STOP_LIMIT入场 {side}",
            f"方向: {side}\n触发价: {stop_price}\n限价: {limit_price}\n当前价: {curr_price}\n"
            f"数量: {amount}\n保护止损: {precision_price(estimated_safe_stop)}\n目标位: {target}\n"
            f"允许滑点: {allowed_slippage:.4f}\n入场原因: {entry_reason}\n"
            f"触发周期: {trade_state.get('pending_entry_trigger_tf')}\n仓位模式: {amount_meta.get('mode')}\n15M信号时间: {signal_bar_15m}\n"
            f"开仓条件明细:\n{open_condition_details}\n开仓订单ID: {order_id}"
        )
        logging.info(
            f"已挂 STOP_LIMIT pending 入场: id={order_id}, side={side}, stopPrice={stop_price}, "
            f"limitPrice={limit_price}, amount={amount}, target={target}, "
            f"protective_stop={precision_price(estimated_safe_stop)}, amount_meta={amount_meta}"
        )
        return True
    except Exception as e:
        error_msg = f"STOP_LIMIT 入场挂单失败: {e}"
        logging.error(error_msg)
        logging.error(traceback.format_exc())
        send_msg("ETH交易: ⚠️STOP_LIMIT入场挂单失败", error_msg)
        return False


def manage_pending_entry(context, signal_bar_15m='', perf=None):
    """管理 pending STOP_LIMIT 入场：成交后转持仓，失效/超时则撤单。"""
    if not has_pending_entry():
        return 'no_pending_entry'

    side = trade_state.get('pending_entry_side')
    # 获取peding挂单信息
    order = fetch_pending_entry_order()
    # 判断是否开仓
    position_risk = get_position_risk(side=side)
    if position_risk and position_risk.get('fetch_failed'):
        logging.warning("pending 入场管理无法确认仓位，本轮跳过，避免误处理")
        return 'pending_position_fetch_failed'

    if position_risk is not None:
        finalize_pending_entry_position(order=order, position_risk=position_risk, signal_bar_15m=signal_bar_15m)
        return 'pending_entry_filled'

    if order is None:
        trade_state['pending_entry_missing_count'] = int(trade_state.get('pending_entry_missing_count', 0) or 0) + 1
        if trade_state['pending_entry_missing_count'] < PENDING_ENTRY_MISSING_CONFIRM_COUNT:
            logging.warning(
                f"pending 入场单暂未在交易所查到，先保留本地状态 "
                f"({trade_state['pending_entry_missing_count']}/{PENDING_ENTRY_MISSING_CONFIRM_COUNT})"
            )
            return 'pending_entry_temporarily_missing'
        clear_pending_entry_state('pending 入场单连续缺失且无交易所仓位')
        return 'pending_entry_missing_cleared'

    trade_state['pending_entry_missing_count'] = 0
    #获取挂单状态
    status = extract_order_status_lower(order)
    #是从不同来源的订单对象中提取已成交数量
    filled_amount = extract_order_filled_amount(order)
    trade_state['pending_entry_filled_amount'] = filled_amount
    if filled_amount > 0:
        trade_state['pending_entry_triggered'] = True

    if status in ('closed', 'filled') or filled_amount >= price_to_float(trade_state.get('pending_entry_amount')):
        finalize_pending_entry_position(order=order, position_risk=position_risk, signal_bar_15m=signal_bar_15m)
        return 'pending_entry_order_filled'

    if filled_amount > 0:
        #部分成交
        cancel_pending_entry_order('pending 入场已部分成交，撤销剩余数量后等待仓位确认', silent=True, clear_state=False)
        return 'pending_entry_partial_filled_wait_position'

    # canceled/cancelled: 已被撤销（两种拼写Binance都返回）
    # expired: 订单已过期失效（限价单超时Time in Force到期）
    # rejected: 被交易所拒绝（保证金不足、价格超限等）
    if status in ('canceled', 'cancelled', 'expired', 'rejected'):
        # 订单已死亡且未成交，直接清空本地pending状态
        clear_pending_entry_state(f"pending 入场单已结束: status={status}")
        return f'pending_entry_{status}'
    # AI评估pending入场
    ai_guard = get_current_ai_guard()
    # AI评估结果
    ai_pending_decision = evaluate_entry_candidate(side, ai_guard, AI_RESEARCH_CONFIG)
    # AI是否建议取消pending
    ai_pending_should_cancel = bool(
        AI_RESEARCH_CONFIG.mode == 'manage'
        and ai_pending_decision.get('shadow_action') == 'would_filter'
    )
    pending_audit_decision = {
        'pending_order_id': trade_state.get('pending_entry_order_id', ''),
        'pending_side': side,
        'actual_action': 'cancel_pending' if ai_pending_should_cancel else (
            'log_only' if AI_RESEARCH_CONFIG.mode == 'log' else 'hold'
        ),
        'shadow_action': 'would_cancel_pending' if ai_pending_decision.get('shadow_action') == 'would_filter' else 'would_hold',
        'reason': ai_pending_decision.get('reason', '')
    }
    write_ai_audit(
        'pending_evaluation',
        ai_guard,
        pending_audit_decision,
        dedupe_key=(
            f"pending:{trade_state.get('pending_entry_order_id', '')}:{side}:"
            f"{ai_guard.get('generated_at', 'invalid')}:{pending_audit_decision['shadow_action']}"
        )
    )
    if ai_pending_should_cancel:
        cancel_pending_entry_order('AI明确反向信号，撤销尚未成交pending')
        return 'pending_entry_ai_filtered'

    curr_price = get_latest_price()
    # 判断pending 入场价格是否仍然合理
    price_ok, price_reason = pending_entry_price_still_reasonable(curr_price)
    if not price_ok:
        #取消条件委托
        cancel_pending_entry_order(price_reason)
        return 'pending_entry_price_invalid'

    # 判断pending 入场趋势是否仍然支持
    trend_ok, trend_reason = pending_entry_trend_still_valid(context)
    if not trend_ok:
        cancel_pending_entry_order(f"趋势不再支持 pending 方向: {trend_reason}")
        return 'pending_entry_trend_invalid'

    # 判断pending 入场订单是否超时
    age = pending_entry_age_seconds(now_dt=context.get('now_dt'))
    if age > PENDING_ENTRY_MAX_AGE_SECONDS:
        cancel_pending_entry_order(f"pending 入场单超时: age={age}s")
        return 'pending_entry_expired'

    logging.info(
        f"pending STOP_LIMIT 入场继续等待: id={trade_state.get('pending_entry_order_id')}, "
        f"side={side}, status={status or 'open'}, current={curr_price}, "
        f"stop={trade_state.get('pending_entry_stop_price')}, limit={trade_state.get('pending_entry_limit_price')}, age={age}s"
    )
    return 'pending_entry_waiting'


def sync_pending_entry_fill_if_needed(signal_bar_15m=''):
    """主循环早期快速检查 pending 是否已成交，避免K线抓取失败时延迟挂保护止损。"""
    if not has_pending_entry():
        return ''

    # 获取条件委托的开仓方向
    side = trade_state.get('pending_entry_side')
    # 获取peding挂单状态
    order = fetch_pending_entry_order()
    #判断是否开仓
    position_risk = get_position_risk(side=side)
    if position_risk and position_risk.get('fetch_failed'):
        logging.warning("pending 入场早期同步无法确认仓位，本轮继续后续流程")
        return ''

    if position_risk is not None:
        # pending 入场后，判断入场价格是否合理/利润预期空间是否足够，如果否，则立即平仓，并且清空挂单，重置本地止损状态，重置本地Pending状态,更新保护止损，持仓信息同步到本地
        finalize_pending_entry_position(order=order, position_risk=position_risk, signal_bar_15m=signal_bar_15m)
        return 'pending_entry_early_filled'

    if order is None:
        return ''

    # 获取挂单状态
    status = extract_order_status_lower(order)
    # 获取挂单成交数量
    filled_amount = extract_order_filled_amount(order)
    #已成交数量
    trade_state['pending_entry_filled_amount'] = filled_amount
    if filled_amount > 0:
        trade_state['pending_entry_triggered'] = True

    #这里是全部成交
    if status in ('closed', 'filled') or filled_amount >= price_to_float(trade_state.get('pending_entry_amount')):
        finalize_pending_entry_position(order=order, position_risk=position_risk, signal_bar_15m=signal_bar_15m)
        return 'pending_entry_early_order_filled'

    #这里是部分成交
    if filled_amount > 0:
        #部分成交会取消没成交的订单
        cancel_pending_entry_order('pending 入场已部分成交，早期同步撤销剩余数量后等待仓位确认', silent=True, clear_state=False)
        return 'pending_entry_early_partial_filled'

    return ''


# ==========================================
# 3. 核心逻辑：趋势、入场、监控
# ==========================================

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
        cancel_all_close_position_conditional_orders(silent=True, reason='平仓后清理残留条件委托')
        
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
            entry_trigger_price=trade_state.get('entry_trigger_price', ''),
            entry_signal_bar_15m=entry_signal_bar_15m,
            exit_signal_bar_15m=exit_signal_bar_15m,
            exit_trigger=trigger_label or reason,
            holding_seconds=holding_seconds,
            open_order_id=open_order_id,
            close_order_id=close_order_id
        )
        record_sr_breakout_failure_from_state(exit_time=exit_time, net_pnl_usdt=net_pnl_usdt)

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
               f"入场触发价: {trade_state.get('entry_trigger_price', 0)}\n"
               f"出场价: {actual_close_price}\n点数盈亏: {pnl_points:.2f}\n手续费: {fee_cost:.4f}\n净利润(USDT): {net_pnl_usdt:.2f}\n"
               f"平仓后账户资金(USDT): {final_usdt:.4f}\n15M信号时间: {exit_signal_bar_15m}\n触发来源: {trigger_label or reason}\n"
               f"平仓条件明细:\n{close_condition_details}"
               f"{order_id_suffix}")
        send_msg(f"ETH交易: 平仓通知", msg)

        post_exit_processed_bar_15m = exit_signal_bar_15m or trade_state.get('last_processed_bar_15m', '')
        cancel_all_close_position_conditional_orders(silent=True, reason='平仓后再次清理残留条件委托')
        trade_state.update({
            'has_position': False,
            'side': None,
            'entry_price': 0,
            'entry_trigger_price': 0.0,
            'stop_loss_price': 0,
            'highest_price': 0,
            'lowest_price': 0,
            'amount': 0,
            'entry_initial_amount': 0.0,
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
            'entry_module': '',
            'entry_sr_breakout_key': '',
            'entry_sr_target': 0.0,
            'entry_initial_risk': 0.0,
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
            'position_miss_count': 0,
            'ai_snapshot_json': '',
            'ai_last_reduce_generated_at': '',
            'ai_last_reduce_target_ratio': 1.0,
            'ai_partial_reduce_count': 0,
            'ai_partial_reduce_amount': 0.0,
            'ai_partial_reduce_realized_pnl': 0.0,
            'ai_partial_reduce_fee': 0.0,
            'ai_reduce_reasons': []
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
    # 新鲜度检查
    # 未来K线检查
    if stale_seconds < -5:
        return {'valid': False, 'is_new': False, 'reason': 'future_bar', 'stale_seconds': stale_seconds}
    # 过去K线检查
    if stale_seconds > max_stale_seconds:
        return {'valid': False, 'is_new': False, 'reason': 'stale_bar', 'stale_seconds': stale_seconds}

    max_seen_key = tf_state_key('max_seen_bar', timeframe)
    processed_key = tf_state_key('last_processed_bar', timeframe)
    max_seen_dt = parse_bar_time(trade_state.get(max_seen_key, ''))
    # 去重检查
    if max_seen_dt is not None and signal_dt < max_seen_dt:
        return {'valid': False, 'is_new': False, 'reason': 'bar_time_rollback', 'stale_seconds': stale_seconds}

    last_processed_dt = parse_bar_time(trade_state.get(processed_key, ''))
    is_new = last_processed_dt is None or signal_dt > last_processed_dt
    # 更新去重检查
    if max_seen_dt is None or signal_dt > max_seen_dt:
        trade_state[max_seen_key] = signal_bar

    return {'valid': True, 'is_new': is_new, 'reason': 'ok', 'stale_seconds': stale_seconds}


def check_empty_position_lightweight_5m_gate(perf):
    """空仓时先用轻量5M判断是否需要进入完整多周期扫描。"""
    gate_start = time.monotonic()
    try:
        if not trade_state.get('last_processed_bar_5m') or not trade_state.get('max_seen_bar_5m'):
            return {'action': 'full', 'reason': 'cold_start'}

        server_time_start = time.monotonic()
        server_now_dt = get_server_now_dt()
        record_perf(perf, 'lightweight_5m_server_time', server_time_start)

        try:
            #获取5m的5根k线（包含为收盘的k线）
            df_5m, fetch_perf = fetch_lightweight_5m_df(SYMBOL, limit=LIGHTWEIGHT_5M_GATE_LIMIT)
            # for key, value in fetch_perf.items():
            #     add_perf(perf, key, value)
        except Exception as e:
            logging.warning(f"轻量5M gate抓取失败，降级完整流程: {e}")
            return {'action': 'full', 'reason': 'lightweight_fetch_failed'}

        if df_5m is None or len(df_5m) == 0:
            logging.warning("轻量5M gate返回空K线，降级完整流程")
            return {'action': 'full', 'reason': 'lightweight_empty_fetch'}

        #获取最新收盘5分钟K线的开盘时间
        signal_bar_5m = get_closed_bar_time(df_5m, '5m', now_dt=server_now_dt)
        validate_start = time.monotonic()
        #验证5分钟K线是否有效：从新鲜度/去重/是否为新K线 判断
        signal_guard_5m = validate_signal_bar('5m', signal_bar_5m, now_dt=server_now_dt)
        record_perf(perf, 'lightweight_5m_validate_signal_bar', validate_start)

        if not signal_guard_5m.get('valid'):
            logging.warning(
                f"轻量5M gate跳过异常信号K线: signal={signal_bar_5m}, "
                f"reason={signal_guard_5m.get('reason')}"
            )
            return {'action': 'skip', 'reason': f"lightweight_invalid_{signal_guard_5m.get('reason')}"}

        if not signal_guard_5m.get('is_new'):
            return {'action': 'skip', 'reason': 'lightweight_no_new_5m'}

        return {
            'action': 'full',
            'reason': 'lightweight_new_5m',
            'df_5m': df_5m,
            'server_now_dt': server_now_dt
        }
    finally:
        record_perf(perf, 'lightweight_5m_gate_total', gate_start)


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


def precision_price(price, symbol=SYMBOL):
    try:
        return float(exchange.price_to_precision(symbol, price))
    except Exception:
        return float(price)


def get_price_tick(reference_price=None, symbol=SYMBOL):
    try:
        if not getattr(exchange, 'markets', None):
            exchange.load_markets()
        market = exchange.market(symbol)
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
    """
    从 K 线数据行中提取标准 OHLCV 分量。
    返回: (open, high, low, close, candle_range, body)
    - candle_range: 最高价 - 最低价（整根 K 线的振幅）
    - body: |收盘价 - 开盘价|（实体大小）
    """
    open_price = price_to_float(row.get('open'))
    high = price_to_float(row.get('high'))
    low = price_to_float(row.get('low'))
    close = price_to_float(row.get('close'))
    candle_range = max(high - low, 0.0)
    body = abs(close - open_price)
    return open_price, high, low, close, candle_range, body



# 强势K线检查
def strong_candle_check(open_price, high, low, close, atr, direction, body_override=None):
    if not is_number(atr) or atr <= 0:
        return {'ok': False, 'reason': 'bad_atr'}
    candle_range = high - low
    if candle_range <= 0:
        return {'ok': False, 'reason': 'zero_range'}

    # 实体
    raw_body = close - open_price if direction == 'long' else open_price - close
    body = body_override if body_override is not None else raw_body
    if direction == 'long':
        direction_ok = close > open_price
        # 收盘价在整个k线的27%以上
        close_zone_ok = close >= high - candle_range * STRONG_CLOSE_ZONE_RATIO
    else:
        direction_ok = close < open_price
        close_zone_ok = close <= low + candle_range * STRONG_CLOSE_ZONE_RATIO
    # 实体大于atr*0.55，过滤掉小实体的k线
    body_atr_ok = body >= STRONG_BODY_ATR_RATIO * atr
    
    # 实体占整个k线60%以上
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
    """
    判断单根 K 线是否为有效强 K（单根强K，非大强、非合成）。
    大强 K 会返回 None（大强由专门逻辑处理，不走正常强K流程）。
    返回: dict(kind='single', direction, open, high, low, close, atr, body, large=False, bar_time, details) 或 None
    """
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
    return large_strong_candle_from_row(row)


def large_strong_candle_from_row(row):
    open_price, high, low, close, _, _ = candle_parts(row)
    atr = price_to_float(row.get('atr'))
    for direction in ('long', 'short'):
        if is_large_strong_candle(open_price, high, low, close, atr, direction):
            return {
                'kind': 'large',
                'direction': direction,
                'open': open_price,
                'high': high,
                'low': low,
                'close': close,
                'atr': atr,
                'bar_time': format_bar_time(row.get('timestamp'))
            }
    return None


def find_previous_large_strong(df_closed, direction, lookback=OPPOSITE_STRONG_LOOKBACK):
    if df_closed is None or len(df_closed) < 2:
        return None
    end_idx = len(df_closed) - 2
    start_idx = max(0, end_idx - lookback + 1)
    for idx in range(end_idx, start_idx - 1, -1):
        large = large_strong_candle_from_row(df_closed.iloc[idx])
        if large and large.get('direction') == direction:
            return large
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
    """
    合成强 K 线：连续 bars_count 根同向小 K 线合并成一根"虚拟强 K"。
    条件：
        - 合并段内每根 K 线都不能是独立的强K/大强K（否则没必要合成）
        - 做多：每根都阳线；做空：每根都阴线
        - 合并后的 OHLC 检查 strong_candle_check 通过 且 不是大强K
    返回: dict(kind='synthetic_N', ...) 或 None
    """
    if end_idx - bars_count + 1 < 0:
        return None
    segment = df_closed.iloc[end_idx - bars_count + 1:end_idx + 1]
    if len(segment) != bars_count:
        return None
    # 如果段内任何一根 K 线已经是独立强K/大强K，就不需要合成了
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

    # 合成后检查：不能是大强（振幅过大），且必须通过强 K 检查
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
    """
    找最新一根有效强 K（先查单根，再查合成）。
    用于持有时判断是否需要依据强K收紧止损。
    """
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


def latest_entry_strong(df_closed, direction):
    if df_closed is None or len(df_closed) == 0:
        return None
    end_idx = len(df_closed) - 1
    if ENABLE_SINGLE_STRONG_ENTRY:
        single = simple_strong_candle(df_closed.iloc[end_idx], direction)
        if single:
            return single
    if not ENABLE_SYNTHETIC_STRONG_ENTRY:
        return None
    for bars_count in range(SYNTHETIC_STRONG_MIN_BARS, SYNTHETIC_STRONG_MAX_BARS + 1):
        synthetic = composite_strong_candle(df_closed, end_idx, bars_count, direction)
        if synthetic:
            return synthetic
    return None


def historical_effective_strong(df_closed, end_idx, direction, include_synthetic=True):
    """
    判断历史某个位置是否为有效强K（可指定是否包含合成强K）。
    用于回看历史K线（如判断震荡区间内的强K触碰历史）。
    """
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
    """
    在历史 K 线中找最近一根与持仓方向相反的有效强 K。
    用于 detect_entry_trigger 中判断是否应该突破前反向强K的关键位入场。
    """
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
    """
    统计最近 EMA_CROSS_LOOKBACK 根 K 线内，价格穿越 EMA20 的次数。
    分别统计上穿和下穿次数，用于判断价格是否在均线附近频繁震荡。
    返回: {'up': 上穿次数, 'down': 下穿次数, 'total': 总穿越次数}
    """
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


def persistent_direction_before_index(df_closed, end_idx):
    """
    检查在 end_idx 之前的 EMA_PERSISTENCE_BARS(20) 根 K 线中，
    价格是否持续站在 EMA20 上方（做多背景）或下方（做空背景）。
    返回 'long'（持续在上方）/ 'short'（持续在下方）/ None。
    """
    if df_closed is None or end_idx < EMA_PERSISTENCE_BARS:
        return None
    prior = df_closed.iloc[end_idx - EMA_PERSISTENCE_BARS:end_idx]
    if len(prior) < EMA_PERSISTENCE_BARS or prior['ema20'].isna().any():
        return None
    # 20根K线的低点都在EMA20之上 → 价格持续在均线上方运行
    if bool((prior['low'] >= prior['ema20']).all()):
        return 'long'
    # 20根K线的高点都在EMA20之下 → 价格持续在均线下方运行
    if bool((prior['high'] <= prior['ema20']).all()):
        return 'short'
    return None


def is_ema_persistence_first_break(row, persistent_direction):
    """
    判断是否首次跌破/突破 EMA20 持续性趋势。
    persistent_direction='long'（之前20根在EMA上方）：low < EMA20 即首次触碰均线下方
    persistent_direction='short'（之前20根在EMA下方）：high > EMA20 即首次触碰均线上方
    """
    if row is None or not is_number(row.get('ema20')):
        return False
    if persistent_direction == 'long':
        return is_number(row.get('low')) and row['low'] < row['ema20']
    if persistent_direction == 'short':
        return is_number(row.get('high')) and row['high'] > row['ema20']
    return False


def recent_persistent_trend_break(df_closed, persistent_direction, break_idx):
    """
    判断在 break_idx 发生首次突破后，后续3根K线是否确认了趋势反转。
    - 原本做多趋势（persistent_direction='long'）→ 后3根中 >=2 根收盘在 EMA20 下方 → 趋势确认打破
    - 原本做空趋势（persistent_direction='short'）→ 后3根中 >=2 根收盘在 EMA20 上方 → 趋势确认打破
    """
    if df_closed is None or break_idx is None or break_idx + 3 >= len(df_closed):
        return False
    # 获取突破后连续3根K线数据
    follow = df_closed.iloc[break_idx + 1:break_idx + 4]
    if len(follow) < 3:
        return False
    if persistent_direction == 'short':
        # 原本做空趋势，后续3根中至少2根收盘价高于EMA20 → 趋势确认打破
        bars_above = (follow['close'] > follow['ema20']).sum()
        if bars_above >= 2:
            return True
    else:
        # 原本做多趋势，后续3根中至少2根收盘价低于EMA20 → 趋势确认打破
        bars_below = (follow['close'] < follow['ema20']).sum()
        if bars_below >= 2:
            return True
    return False


def recent_ema_persistence(df_closed):
    """
    检查价格是否至少在最近 EMA_PERSISTENCE_BARS(20) 根 K 线内持续站在 EMA20 上方/下方。
    如果持续方向已确立，检查当前 K 线是否首次跌破/突破 EMA20（即"趋势首次中断"）。
    如果趋势刚被中断不久（1~3根K线内），判断趋势是否已被打破。

    返回字段:
        direction    - 'above'（持续在EMA上方）/ 'below'（持续在EMA下方）/ None
        first_break  - 当前K线是否首次跌破/突破（趋势刚中断的第一根）
        broken       - 趋势是否已被确认打破（中断后3根K线确认）
        break_pending - 趋势中断但还在观察期中（<3根K线）
    """
    if df_closed is None or len(df_closed) < EMA_PERSISTENCE_BARS + 1:
        return {'direction': None, 'first_break': False, 'broken': False, 'break_pending': False}

    last_idx = len(df_closed) - 1
    last = df_closed.iloc[-1]
    #判断是否有20根k线在ema20上/下面
    direction = persistent_direction_before_index(df_closed, last_idx)
    if direction:
        #判断是否第一次跌破ema20
        first_break = is_ema_persistence_first_break(last, direction)
        return {
            'direction': direction,
            'first_break': bool(first_break),
            'broken': False,
            'break_pending': bool(first_break)
        }

    #用于检测"最近几根K线内是否刚刚发生了 EMA20 持续性趋势的首次突破"。
    for break_idx in range(max(EMA_PERSISTENCE_BARS, last_idx - 3), last_idx):
        direction = persistent_direction_before_index(df_closed, break_idx)
        if not direction or not is_ema_persistence_first_break(df_closed.iloc[break_idx], direction):
            continue
        bars_after_break = last_idx - break_idx
        if 1 <= bars_after_break <= 3:
            trend_broken = (
                bars_after_break == 3
                #判断持续的ema20的趋势是否被打破
                and recent_persistent_trend_break(df_closed, direction, break_idx)
            )
            return {
                'direction': direction,
                'first_break': False,
                'broken': trend_broken,
                #break_pending 是一个"中间状态"——突破已出现，但 3 根 K 线的确认窗口还没过，系统暂时不下结论。
                'break_pending': bars_after_break < 3
            }
    return {'direction': None, 'first_break': False, 'broken': False, 'break_pending': False}


def evaluate_adx_ema_context(df, timeframe, now_dt=None):
    """
    核心趋势判断函数：综合 ADX/DI + EMA20 + EMA20持久性，评估某个周期的多头/空头方向。

    判断优先级：
        1. EMA20 持久性优先：如果 20 根K线持续在EMA一侧且趋势未被打破 → 直接采用
        2. ADX 分层：震荡(<range_max) → 过渡(<trend_min) → 趋势
        3. 趋势组合：DI方向 + EMA斜率方向 + EMA穿越干净度 同时成立才算有效趋势

    返回 dict 包含: direction, status, can_open_long, can_open_short, long_trend, short_trend, pullback, is_oscillation 等
    """
    df_closed = get_closed_df(df, timeframe, now_dt=now_dt)
    # 每个周期用自己的 ADX length 和阈值，避免 15m/1h/4h 共用同一套强度判断。
    adx_config = get_adx_config(timeframe)
    adx_length = int(adx_config.get('length', ADX_LENGTH))
    adx_range_max = price_to_float(adx_config.get('range_max', 20))
    adx_no_trade_max = price_to_float(adx_config.get('no_trade_max', ADX_NO_TRADE_MAX))
    adx_trend_min = price_to_float(adx_config.get('trend_min', ADX_TREND_MIN))
    adx_extreme = price_to_float(adx_config.get('extreme', ADX_EXTREME))
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
    min_len = max(adx_length + 2, EMA_TREND_LENGTH + EMA_SLOPE_LOOKBACK + 2, EMA_CROSS_LOOKBACK + 2)
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
    #判断最近4根k线是否上下穿ema20
    up_cross, down_cross = count_ema_crosses(df_closed)
    ema_clean = up_cross <= 1 and down_cross <= 1
    ema_up = ema20 > ema20_ref
    ema_down = ema20 < ema20_ref
    adx_rising = adx > prev_adx
    adx_falling = adx < prev_adx
    extreme = adx > adx_extreme
    persistence = recent_ema_persistence(df_closed) if timeframe in ('15m', '1h', '4h') else {'direction': None}

    # details 会写入日志/CSV/状态摘要，保留实际使用的 ADX 配置便于复盘。
    base['details'] = {
        'adx_length': adx_length,
        'adx_range_max': adx_range_max,
        'adx_no_trade_max': adx_no_trade_max,
        'adx_trend_min': adx_trend_min,
        'adx_extreme': adx_extreme,
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
        'extreme_adx': extreme
    }

    if (
        persistence.get('direction')
        and not persistence.get('broken')
        and not persistence.get('break_pending')
    ):
        direction = persistence['direction']
        status = f"ema20_{direction}_persistence"
        base['direction'] = direction
        base['status'] = status
        base['can_open_long'] = direction == 'long'
        base['can_open_short'] = direction == 'short'
        base['long_trend'] = direction == 'long'
        base['short_trend'] = direction == 'short'
        base['is_oscillation'] = False
        base['summary'] = (
            f"{timeframe}: status={status}, dir={direction}, EMA20连续背景优先, "
            f"ADX{adx_length}={adx:.2f}, +DI={plus_di:.2f}, -DI={minus_di:.2f}, "
            f"EMA clean={ema_clean}, cross={up_cross}/{down_cross}, extreme={extreme}"
        )
        return base

    # 先按 ADX 强度分层：震荡 -> 过渡 -> 趋势判断。
    if adx <= adx_range_max:
        base['status'] = 'range'
        base['summary'] = f"{timeframe}: ADX{adx_length}={adx:.2f} 震荡，禁止开仓"
        return base
    if adx <= adx_no_trade_max or adx < adx_trend_min:
        base['status'] = 'transition'
        base['summary'] = f"{timeframe}: ADX{adx_length}={adx:.2f} 过渡，等待方向确认"
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
        f"{timeframe}: status={status}, dir={direction}, ADX{adx_length}={adx:.2f}, "
        f"+DI={plus_di:.2f}, -DI={minus_di:.2f}, EMA clean={ema_clean}, "
        f"cross={up_cross}/{down_cross}, extreme={extreme}"
    )
    return base


def side_label(side):
    return '多' if side == 'long' else '空' if side == 'short' else str(side)


def format_context_metric(value):
    if value in (None, ''):
        return '-'
    try:
        return f"{float(value):.2f}"
    except Exception:
        return str(value)


def format_context_state(state):
    if not state:
        return 'missing'
    details = state.get('details') or {}
    adx_length = details.get('adx_length')
    adx_name = f"ADX{adx_length}" if adx_length else 'ADX'
    return (
        f"{state.get('timeframe', '?')} status={state.get('status')}, "
        f"dir={state.get('direction')}, "
        f"允许开多={bool(state.get('can_open_long'))}, "
        f"允许开空={bool(state.get('can_open_short'))}, "
        f"{adx_name}={format_context_metric(details.get('adx'))}, "
        f"+DI={format_context_metric(details.get('plus_di'))}, "
        f"-DI={format_context_metric(details.get('minus_di'))}, "
        f"EMAclean={details.get('ema_clean')}, "
        f"cross={details.get('up_cross', '-')}/{details.get('down_cross', '-')}"
    )


def context_deny_reason(side, reason, local_state, background_state):
    """格式化多周期趋势被拒绝的原因描述，用于日志和调试。"""
    return (
        f"{side_label(side)}方向过滤: {reason}; "
        f"local[{format_context_state(local_state)}]; "
        f"bg[{format_context_state(background_state)}]"
    )


def context_allows_side(local_state, background_state, side):
    """
    多周期趋势联合校验（入场前必须通过）。
    依次检查：
        1. 本地周期不能震荡
        2. 本地周期方向与交易方向一致
        3. 背景周期方向不能相反
        4. 背景周期状态不能是 range/transition/unclear
        5. 本地周期允许开仓
        6. 背景周期确认同向（允许 weakening/flat_adx/ema_persistence 软通过）
    返回: (allowed: bool, deny_reason: str)
    """
    if not local_state or not background_state:
        return False, context_deny_reason(side, '缺少趋势状态', local_state, background_state)
    opposite = 'short' if side == 'long' else 'long'
    # 本级别震荡 → 不开仓
    if local_state.get('is_oscillation'):
        return False, context_deny_reason(side, f"{local_state.get('timeframe')} 本级别震荡/趋势不明", local_state, background_state)
    # 本级别方向相反 → 不开仓
    if local_state.get('direction') == opposite:
        return False, context_deny_reason(side, f"{local_state.get('timeframe')} 本级别方向相反", local_state, background_state)
    # 背景周期方向相反 → 不开仓
    if background_state.get('direction') == opposite:
        return False, context_deny_reason(side, f"{background_state.get('timeframe')} 背景趋势相反", local_state, background_state)
    # 背景周期不允许开仓（震荡/过渡/不清/无数据）→ 不开仓
    if background_state.get('status') in ('range', 'transition', 'unclear', 'no_data'):
        return False, context_deny_reason(side, f"{background_state.get('timeframe')} 背景趋势不允许开仓: {background_state.get('status')}", local_state, background_state)

    local_open_key = 'can_open_long' if side == 'long' else 'can_open_short'
    if not local_state.get(local_open_key) and local_state.get('direction') != side:
        return False, context_deny_reason(side, f"{local_state.get('timeframe')} 本级别未给出{side_label(side)}方向", local_state, background_state)

    # 背景周期：方向一致 + (明确允许开仓 或 软通过状态 weakening/flat_adx/ema_persistence)
    bg_same_side = background_state.get('direction') == side
    bg_open_key = 'can_open_long' if side == 'long' else 'can_open_short'
    bg_soft_allow = background_state.get('status') in (
        f'{side}_weakening',
        f'{side}_flat_adx',
        f'ema20_{side}_persistence'
    )
    if not bg_same_side or (not background_state.get(bg_open_key) and not bg_soft_allow):
        return False, context_deny_reason(side, f"{background_state.get('timeframe')} 背景未确认{side_label(side)}方向", local_state, background_state)
    return True, 'ok'


def detect_entry_trigger(df, timeframe, side, now_dt=None):
    """
    入场触发检测器 — 在趋势确认后，寻找具体的 K 线形态入场信号。
    按优先级依次检查：
        1. 最新K线是否为大强K → 是则过滤不开仓（blocked=True）
        2. 前大强K线突破入场（large_strong_break）→ 收盘突破前大强线关键位
        3. 最新强K入场（strong_candle）→ 最新一根有效强K的高/低点
        4. 反向强K突破入场（opposite_strong_break）→ 收盘突破前反向强K关键位
    返回: dict(blocked, type, side, entry_level, stop, trigger, reason) 或 None
    """
    df_closed = get_closed_df(df, timeframe, now_dt=now_dt)
    adx_length = int(get_adx_config(timeframe).get('length', ADX_LENGTH))
    if len(df_closed) < max(adx_length + 3, SYNTHETIC_STRONG_MAX_BARS + 2):
        return None

    last = df_closed.iloc[-1]
    close = price_to_float(last.get('close'))
    atr = price_to_float(last.get('atr'))
    tick = get_price_tick(last.get('close'))
    # 最新一根 K线是否为大强K线
    large = latest_large_strong(df_closed)
    if large:
        return {
            'blocked': True,
            'reason': f"最新{timeframe}为大强{large['direction']}线，过滤不开仓",
            'large': large
        }
    # 往前找一根同方向的大强K线，等价格突破它的高/低点时入场，找的不是最新的
    previous_large = find_previous_large_strong(df_closed, side)
    if previous_large:
        if side == 'long':
            level = previous_large['high']
            entry_level = level + tick
            structure_stop = previous_large['low'] - tick
            atr_stop = entry_level - ENTRY_ATR_STOP_MULTIPLIER * atr if atr > 0 else structure_stop
            stop = max(structure_stop, atr_stop)
            reason = f"{timeframe}前大强阳线高点触发位"
        else:
            level = previous_large['low']
            entry_level = level - tick
            structure_stop = previous_large['high'] + tick
            atr_stop = entry_level + ENTRY_ATR_STOP_MULTIPLIER * atr if atr > 0 else structure_stop
            stop = min(structure_stop, atr_stop)
            reason = f"{timeframe}前大强阴线低点触发位"
        logging.info(
            f"detect_entry_trigger生成 large_strong_break: "
            f"timeframe={timeframe}, side={side}, level={level}, close={close}, "
            f"entry={entry_level}, structure_stop={structure_stop}, atr_stop={atr_stop}, final_stop={stop}"
        )
        return {
            'blocked': False,
            'type': 'large_strong_break',
            'side': side,
            'timeframe': timeframe,
            'entry_level': precision_price(entry_level),
            'stop': precision_price(stop),
            'trigger': previous_large,
            'reason': reason,
            'structure_stop': precision_price(structure_stop),
            'atr_stop': precision_price(atr_stop),
            'stop_model': 'structure_with_atr_cap'
        }
    def strong_candle_trigger():
        # 查找最后一根收盘K线是否是允许参与开仓的有效强K线。
        strong = latest_entry_strong(df_closed, side)
        if not strong:
            return None
        if side == 'long':
            entry_level = strong['high'] + tick
            stop = strong['low'] - tick
        else:
            entry_level = strong['low'] - tick
            stop = strong['high'] + tick
        logging.info(
            f"detect_entry_trigger命中 strong_candle: "
            f"timeframe={timeframe}, side={side}, kind={strong.get('kind')}, "
            f"entry={entry_level}, stop={stop}"
        )
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

    def opposite_strong_break_trigger():
        # 查找最近一根相反方向的有效强K线。
        previous = find_previous_opposite_strong(df_closed, side)
        if previous is None:
            return None
        high = price_to_float(last.get('high'))
        low = price_to_float(last.get('low'))
        if side == 'long':
            level = previous['high']
            stop = close - ENTRY_ATR_STOP_MULTIPLIER * atr
            entry_level = level + tick
        else:
            level = previous['low']
            stop = close + ENTRY_ATR_STOP_MULTIPLIER * atr
            entry_level = level - tick
        logging.info(
            f"detect_entry_trigger生成 opposite_strong_break: "
            f"timeframe={timeframe}, side={side}, level={level}, close={close}, "
            f"high={high}, low={low}, entry={entry_level}, stop={stop}"
        )
        return {
            'blocked': False,
            'type': 'opposite_strong_break',
            'side': side,
            'timeframe': timeframe,
            'entry_level': precision_price(entry_level),
            'stop': precision_price(stop),
            'trigger': previous,
            'reason': f"{timeframe}前强{'阴' if side == 'long' else '阳'}线关键位触发位"
        }

    trigger_builders = {
        'strong_candle': strong_candle_trigger,
        'opposite_strong_break': opposite_strong_break_trigger,
    }
    for trigger_name in ENTRY_TRIGGER_PRIORITY:
        builder = trigger_builders.get(trigger_name)
        if not builder:
            continue
        trigger = builder()
        if trigger:
            return trigger
    return None


def zone_distance(zone, price):
    if zone['lower'] <= price <= zone['upper']:
        return 0.0
    return min(abs(price - zone['lower']), abs(price - zone['upper']))


def add_or_merge_zone(zones, zone_type, price, atr, source_idx):
    merge_distance = SUP_RES_MERGE_ATR * atr
    buffer = SUP_RES_ZONE_BUFFER_ATR * atr
    max_width = SUP_RES_MAX_ZONE_WIDTH_ATR * atr
    matched = None
    for zone in [z for z in zones if z['type'] == zone_type]:
        #判断最地点价格是否在merge_distance范围内
        if zone_distance(zone, price) <= merge_distance:
            matched = zone
            break
    if matched:
        next_lower = min(matched['lower'], price) - buffer
        next_upper = max(matched['upper'], price) + buffer
        # 多次合并会让区域越来越宽；超过最大宽度时保留原边界，只记录新的来源。
        if next_upper - next_lower <= max_width:
            matched['lower'] = next_lower
            matched['upper'] = next_upper
            matched['created_idx'] = source_idx
        else:
            matched['last_source_idx'] = source_idx
        matched['touches'] = 0
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
        zone['status'] = 'active'
        start_idx = max(0, int(zone.get('created_idx', 0)))
        #从start_idx开始，确认缔造者的贡献
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


def enrich_broken_zone(zone, df_closed, invalid_idx, touches):
    """
    为已被突破/跌破的支撑/阻力区间补充突破质量指标。

    参数:
        zone:           原始区间 dict（会被浅拷贝，不修改原对象）
        df_closed:      已收盘的 4H K线 DataFrame
        invalid_idx:    突破/跌破发生的那根 K 线索引（收盘价首次越过区间边界）
        touches:        突破前该区间被触碰的次数（越多说明区间越重要）

    补充的字段:
        touches             - 被触碰次数（区间有效性权重）
        valid               - 固定 False，表示该区间已失效
        invalidated_idx     - 突破发生的 K 线索引
        break_idx           - 同上（兼容别名）
        break_time          - 突破发生的时间（格式化字符串）
        break_price         - 突破时的收盘价
        break_strength_atr  - 突破力度（突破距离 / ATR，越大越强）
        bars_after_break    - 突破后经过了多少根 K 线
        hold_count          - 突破后站稳次数（收盘持续在突破方向的数量）
        retest_count        - 回踩次数（价格回到原区间内的次数）
        retest_candidate    - 是否为回踩候选（retest_count > 0）
    """
    zone = dict(zone)
    row = df_closed.iloc[invalid_idx]
    close = price_to_float(row.get('close'))
    atr = price_to_float(row.get('atr'))

    if zone['type'] == 'support':
        # 支撑区：下沿为突破边界，收盘跌破 lower 即视为跌破
        boundary = zone['lower']
        # 突破力度 = (下沿 - 收盘价) / ATR，值越大说明跌得越深、突破越强
        break_strength_atr = (boundary - close) / atr if atr > 0 else 0.0
        # 站稳跌破次数：跌破后收盘价继续压在 lower 下方的根数
        hold_count = int((df_closed.iloc[invalid_idx:]['close'] < zone['lower']).sum())
        # 回踩次数：跌破后价格反抽回原支撑区域内（高点触碰下沿、收盘仍在区间内）
        retest_count = int((
            (df_closed.iloc[invalid_idx + 1:]['high'] >= zone['lower'])
            & (df_closed.iloc[invalid_idx + 1:]['close'] <= zone['upper'])
        ).sum())
    else:
        # 阻力区：上沿为突破边界，收盘突破 upper 即视为突破
        boundary = zone['upper']
        # 突破力度 = (收盘价 - 上沿) / ATR，值越大说明涨得越高、突破越强
        break_strength_atr = (close - boundary) / atr if atr > 0 else 0.0
        # 站稳突破次数：突破后收盘价继续站在 upper 上方的根数
        hold_count = int((df_closed.iloc[invalid_idx:]['close'] > zone['upper']).sum())
        # 回踩次数：突破后价格回踩原阻力区域内（低点触碰上沿、收盘仍在区间内）
        retest_count = int((
            (df_closed.iloc[invalid_idx + 1:]['low'] <= zone['upper'])
            & (df_closed.iloc[invalid_idx + 1:]['close'] >= zone['lower'])
        ).sum())

    zone['touches'] = touches                       # 突破前被触碰次数（区间重要性权重）
    zone['valid'] = False                           # 区间已失效，不再作为活跃支撑/阻力
    zone['invalidated_idx'] = invalid_idx           # 突破发生的 K 线索引
    zone['break_idx'] = invalid_idx                 # 同上，兼容别名
    zone['break_time'] = format_bar_time(row.get('timestamp'))  # 突破时间
    zone['break_price'] = close                     # 突破时的收盘价
    zone['break_strength_atr'] = break_strength_atr # 突破力度（ATR 倍数）
    zone['bars_after_break'] = len(df_closed) - invalid_idx - 1  # 突破后经过的 K 线根数
    zone['hold_count'] = hold_count                 # 站稳次数（收盘维持在突破方向）
    zone['retest_count'] = retest_count             # 回踩原区间的次数
    zone['retest_candidate'] = retest_count > 0     # 是否有回踩行为（用于支撑阻力互换策略）
    return zone


def recently_broken_zones(zones, df_closed):
    """
    从所有原始支撑/阻力区间中，筛选出已被突破（阻力）或跌破（支撑）的区间，
    并按突破时间远近和是否有回踩行为进行分类。

    筛选条件（两个缺一不可）：
        1. touches >= SUP_RES_VALID_TOUCHES  —— 区间被触碰足够多次，证明是有效区间
        2. invalid_idx is not None           —— 确实被收盘价突破/跌破了

    分类结果：
        broken_support        - 近期被跌破的支撑区（支撑变阻力候选）
        broken_resistance     - 近期被突破的阻力区（阻力变支撑候选）
        broken_old_support    - 较早被跌破的支撑区（超出近期窗口，用于复盘）
        broken_old_resistance - 较早被突破的阻力区（超出近期窗口，用于复盘）
        retest_candidate      - 突破后价格回踩到原区间内的区域（互换确认候选）
    """
    broken = {
        'broken_support': [],        # 近期跌破的支撑区
        'broken_resistance': [],     # 近期突破的阻力区
        'broken_old_support': [],    # 早期跌破的支撑区
        'broken_old_resistance': [], # 早期突破的阻力区
        'retest_candidate': []       # 有回踩行为的区间（与上面分类可重叠）
    }
    for raw_zone in zones:
        zone = dict(raw_zone)
        touches = 0           # 突破前该区间被触碰的次数
        invalid_idx = None    # 突破/跌破发生的 K 线索引
        # 从区间创建时刻开始向后遍历，逐根检查是否被突破
        start_idx = max(0, int(zone.get('created_idx', 0)))
        for idx in range(start_idx, len(df_closed)):
            row = df_closed.iloc[idx]
            close = price_to_float(row['close'])
            if zone['type'] == 'support':
                # 收盘价跌破支撑区下沿 → 确认跌破，记录索引并停止遍历
                if close < zone['lower']:
                    invalid_idx = idx
                    break
                # 低点触碰区间（low <= upper）且收盘仍在区间内 → 计一次有效触碰
                if row['low'] <= zone['upper'] and close >= zone['lower']:
                    touches += 1
            else:
                # 收盘价突破阻力区上沿 → 确认突破，记录索引并停止遍历
                if close > zone['upper']:
                    invalid_idx = idx
                    break
                # 高点触碰区间（high >= lower）且收盘仍在区间内 → 计一次有效触碰
                if row['high'] >= zone['lower'] and close <= zone['upper']:
                    touches += 1

        # 触碰次数不足 或 从未被突破 → 该区间不符合"已突破的有效区间"条件，跳过
        if touches < SUP_RES_VALID_TOUCHES or invalid_idx is None:
            continue

        # 补充突破质量指标：突破力度、站稳次数、回踩次数等
        zone = enrich_broken_zone(zone, df_closed, invalid_idx, touches)
        # 判断突破是否发生在近期窗口内（最近 SUP_RES_RECENT_BREAK_LOOKBACK 根 K 线）
        recent_break = invalid_idx >= len(df_closed) - SUP_RES_RECENT_BREAK_LOOKBACK
        zone['status'] = 'broken_recent' if recent_break else 'broken_old'
        # 如果有回踩行为，额外加入 retest_candidate 列表（用于支撑阻力互换确认策略）
        if zone.get('retest_candidate'):
            broken['retest_candidate'].append(dict(zone, status='retest_candidate'))

        # 按区间类型和突破时间远近，分别归入对应列表
        if zone['type'] == 'support':
            target = 'broken_support' if recent_break else 'broken_old_support'
        else:
            target = 'broken_resistance' if recent_break else 'broken_old_resistance'
        broken[target].append(zone)
    return broken


def build_support_resistance_zones(df_4h, now_dt=None):
    df_closed = get_closed_df(df_4h, '4h', now_dt=now_dt)
    if len(df_closed) > SUP_RES_LOOKBACK_4H:
        df_closed = df_closed.tail(SUP_RES_LOOKBACK_4H).reset_index(drop=True)
    if len(df_closed) < SUP_RES_SWING_WINDOW * 2 + 20:
        return {
            # 当前仍有效的支撑区（价格尚未跌破）
            'support': [],
            # 当前仍有效的阻力区（价格尚未突破）
            'resistance': [],
            # 近期被跌破的支撑区（支撑变阻力候选）
            'broken_support': [],
            # 近期被突破的阻力区（阻力变支撑候选）
            'broken_resistance': [],
            # 较早被跌破的支撑区（超出近期窗口，用于复盘），不是最近6根被跌破的
            'broken_old_support': [],
            # 较早被突破的阻力区（超出近期窗口，用于复盘），不是最近6根被跌破的
            'broken_old_resistance': [],
            # 突破/跌破后价格又回踩到原区间内的区域（支撑阻力互换确认候选）
            'retest_candidate': []
        }

    zones = []
    for idx in range(SUP_RES_SWING_WINDOW, len(df_closed) - SUP_RES_SWING_WINDOW):
        atr = price_to_float(df_closed.iloc[idx].get('atr'))
        if atr <= 0:
            continue
        #判断该点是不是左/右5根k线的低点
        if is_swing_low(df_closed, idx, SUP_RES_SWING_WINDOW):
            #如果是，则添加或合并支撑区
            add_or_merge_zone(zones, 'support', price_to_float(df_closed.iloc[idx]['low']), atr, idx)
        #判断该点是不是左/右5根k线的高点
        if is_swing_high(df_closed, idx, SUP_RES_SWING_WINDOW):
            #如果是，则添加或合并阻力区
            add_or_merge_zone(zones, 'resistance', price_to_float(df_closed.iloc[idx]['high']), atr, idx)

    #提取有效的支撑区和阻力区
    active = rebuild_zone_touches(zones, df_closed)
    
    #找出最近被打破的支撑区和阻力区
    broken = recently_broken_zones(zones, df_closed)
    #从高到低排序
    support = sorted([z for z in active if z['type'] == 'support'], key=lambda z: z['upper'], reverse=True)
    #从低到高排序
    resistance = sorted([z for z in active if z['type'] == 'resistance'], key=lambda z: z['lower'])
    broken_support = sorted(broken['broken_support'], key=lambda z: z.get('invalidated_idx', -1), reverse=True)
    broken_resistance = sorted(broken['broken_resistance'], key=lambda z: z.get('invalidated_idx', -1), reverse=True)
    broken_old_support = sorted(broken['broken_old_support'], key=lambda z: z.get('invalidated_idx', -1), reverse=True)
    broken_old_resistance = sorted(broken['broken_old_resistance'], key=lambda z: z.get('invalidated_idx', -1), reverse=True)
    retest_candidate = sorted(broken['retest_candidate'], key=lambda z: z.get('invalidated_idx', -1), reverse=True)
    return {
        # 当前仍有效的支撑区（按 upper 降序，离价格近的排前面）
        'support': support,
        # 当前仍有效的阻力区（按 lower 升序，离价格近的排前面）
        'resistance': resistance,
        # 近期被跌破的支撑区（按跌破时间倒序，用于支撑变阻力策略）
        'broken_support': broken_support,
        # 近期被突破的阻力区（按突破时间倒序，用于阻力变支撑策略）
        'broken_resistance': broken_resistance,
        # 较早被跌破/突破的区域（超出近期窗口，用于复盘和后续策略优化）
        'broken_old_support': broken_old_support,
        'broken_old_resistance': broken_old_resistance,
        # 突破后价格回踩到原区间内的区域（支撑阻力互换确认候选）
        'retest_candidate': retest_candidate
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
    rr = profit_distance / risk
    if rr < MIN_EXPECTED_REWARD_RISK:
        return False, f"盈亏比不足 rr={rr:.2f}, required={MIN_EXPECTED_REWARD_RISK:.2f}"
    fee_rate = 0.0004
    min_fee_price = entry * fee_rate * MIN_EXPECTED_PROFIT_FEE_MULTIPLIER
    min_atr_price = atr * MIN_EXPECTED_PROFIT_ATR if atr and atr > 0 else 0
    min_required = max(min_fee_price, min_atr_price)
    if profit_distance < min_required:
        return False, f"预期空间过小 profit={profit_distance:.4f}, required={min_required:.4f}"
    return True, 'ok'


def target_profit_still_valid(side, entry, stop, target, atr=0):
    ok, reason = expected_profit_ok(side, entry, stop, target, atr)
    if ok:
        return True, 'ok'
    return False, f"实际/当前成交价下目标位无效: entry={entry}, stop={stop}, target={target}, {reason}"


def sr_filter_atr_from_context(context):
    try:
        df_4h_closed = get_closed_df(context['dfs']['4h'], '4h', now_dt=context['now_dt'])
    except Exception:
        return 0.0
    if len(df_4h_closed) == 0:
        return 0.0
    return price_to_float(df_4h_closed.iloc[-1].get('atr'))


def price_in_or_near_zone(price, zone, buffer):
    return zone['lower'] - buffer <= price <= zone['upper'] + buffer


def sr_trend_entry_block_reason(side, price, zones, atr):
    buffer = SUP_RES_NEAR_ATR * atr if atr and atr > 0 else 0.0
    if side == 'long':
        for zone in zones.get('resistance', [])[:5]:
            if price >= zone['lower'] - buffer:
                return (
                    f"多单靠近/进入活跃阻力区，跳过普通趋势开仓: "
                    f"price={price}, zone={zone['lower']:.4f}-{zone['upper']:.4f}, buffer={buffer:.4f}"
                )
        for zone in zones.get('broken_resistance', [])[:5]:
            if price_in_or_near_zone(price, zone, buffer):
                return (
                    f"多单处于近期突破阻力回踩/确认区，交给SR突破逻辑，跳过普通趋势开仓: "
                    f"price={price}, zone={zone['lower']:.4f}-{zone['upper']:.4f}, buffer={buffer:.4f}"
                )
    elif side == 'short':
        for zone in zones.get('support', [])[:5]:
            if price <= zone['upper'] + buffer:
                return (
                    f"空单靠近/进入活跃支撑区，跳过普通趋势开仓: "
                    f"price={price}, zone={zone['lower']:.4f}-{zone['upper']:.4f}, buffer={buffer:.4f}"
                )
        for zone in zones.get('broken_support', [])[:5]:
            if price_in_or_near_zone(price, zone, buffer):
                return (
                    f"空单处于近期跌破支撑回踩/确认区，交给SR突破逻辑，跳过普通趋势开仓: "
                    f"price={price}, zone={zone['lower']:.4f}-{zone['upper']:.4f}, buffer={buffer:.4f}"
                )
    return ''


def candidate_sr_entry_zone_still_valid(candidate, context, curr_price):
    if candidate.get('module') != 'trend_trigger':
        return True, 'ok'
    atr = price_to_float(candidate.get('sr_filter_atr'))
    if atr <= 0:
        #4h的atr值
        atr = sr_filter_atr_from_context(context)
    #判断是否在趋势阻挡区域内
    reason = sr_trend_entry_block_reason(candidate.get('side'), curr_price, context.get('zones', {}), atr)
    if reason:
        return False, reason
    return True, 'ok'


def find_1h_sr_confirmation(df_1h, zone, side, now_dt=None):
    df_closed = get_closed_df(df_1h, '1h', now_dt=now_dt)
    if len(df_closed) < 3:
        return None
    #最近18根K线
    recent = df_closed.tail(SR_CONFIRM_LOOKBACK_1H)
    touched_until = []
    touched = False
    #查找最近18根最先站稳的K线这根为true，后面的所有的都为true
    for _, touch_row in recent.iterrows():
        _, touch_high, touch_low, touch_close, _, _ = candle_parts(touch_row)
        if side == 'long':
            touched = touched or (touch_low <= zone['upper'] and touch_close >= zone['lower'])
        else:
            touched = touched or (touch_high >= zone['lower'] and touch_close <= zone['upper'])
        touched_until.append(touched)

    ## 最近18根K线内，从新到旧查找最近一次站稳确认。
    # for pos in range(len(recent) - 1, -1, -1):
    # 最近18根K线内，从旧到新查找最近一次站稳确认。
    for pos in range(len(recent)):
        row = recent.iloc[pos]
        open_price, high, low, close, _, _ = candle_parts(row)
        body = abs(close - open_price)
        if side == 'long':
            lower_wick = min(open_price, close) - low
            confirm = touched_until[pos] and close > open_price and close > zone['upper'] and lower_wick <= body * 0.5
            if confirm:
                return {'high': high, 'low': low, 'timestamp': row.get('timestamp'), 'bar_time': format_bar_time(row.get('timestamp')), 'zone': zone}
        else:
            upper_wick = high - max(open_price, close)
            confirm = touched_until[pos] and close < open_price and close < zone['lower'] and upper_wick <= body * 0.5
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
        if bool((observed['close'] < confirm['low']).any()):
            return False
        return price_to_float(observed.iloc[-1]['close']) > confirm['high']
    if bool((observed['close'] > confirm['high']).any()):
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
        #查找1小时中有没有站稳支撑/阻力位置，并且k线是否突破了支撑和阻力位置，返回该k线
        confirm = find_1h_sr_confirmation(context['dfs']['1h'], zone, side, now_dt=context['now_dt'])
        # 检查后续的k线是否突破这个阻挡/支撑区域
        if not sr_15m_entry_ready(context['dfs']['15m'], confirm, side, now_dt=context['now_dt']):
            continue
        #获取入场价格
        entry_ref = price_to_float(get_closed_df(context['dfs']['15m'], '15m', context['now_dt']).iloc[-1]['close'])
        tick = get_price_tick(entry_ref)
        if side == 'long':
            stop = min(zone['lower'] - SUP_RES_STOP_BUFFER_ATR * atr_4h, confirm['low'] - tick)
        else:
            stop = max(zone['upper'] + SUP_RES_STOP_BUFFER_ATR * atr_4h, confirm['high'] + tick)
        #获取止盈价格，以及对应的zone
        target, target_zone = target_from_zones_or_rr(zones, side, entry_ref, stop)
        #判断止盈价格是否符合预期利润条件
        ok, reason = expected_profit_ok(side, entry_ref, stop, target, atr_4h)
        if not ok:
            logging.info(f"跳过SR反弹{side}: {reason}")
            continue
        candidates.append({
            'module': 'sr_rebound',
            'strategy_tf': '15m',
            'trigger_tf': '15m',
            'exit_tf': '15m',
            'side': side,
            'entry_level': entry_ref,
            'stop': precision_price(stop),
            'target': precision_price(target),
            'target_zone': target_zone,
            'profit_check_atr': atr_4h,
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
            zone_key = sr_breakout_zone_key(side, zone)
            cooldown_reason = sr_breakout_cooldown_reason(side, zone_key, context['now_dt'])
            if cooldown_reason:
                logging.info(f"跳过 SR breakout long: {cooldown_reason}")
                continue
            reverse_reason = sr_breakout_reverse_strength_reason(context, side, zone)
            if reverse_reason:
                logging.info(f"跳过 SR breakout long: {reverse_reason}, zone={zone_key}")
                continue
            #4小时K线中，第一根阻力突破后面连续2根收盘价都站稳
            if recent3.iloc[0]['close'] > zone['upper'] and bool((recent3.iloc[1:]['close'] >= zone['upper']).all()):
                entry_ref = price_to_float(recent3.iloc[-1]['close'])
                stop = zone['upper'] - SUP_RES_STOP_BUFFER_ATR * atr_4h
                target, target_zone = target_from_zones_or_rr(zones, side, entry_ref, stop)
                candidates.append({'module': 'sr_breakout', 'strategy_tf': '4h', 'trigger_tf': '4h', 'exit_tf': '1h', 'side': side, 'entry_level': entry_ref, 'stop': precision_price(stop), 'target': precision_price(target), 'target_zone': target_zone, 'profit_check_atr': atr_4h, 'sr_breakout_key': zone_key, 'sr_breakout_zone': dict(zone), 'reason': "4H有效阻力突破并连续2根收盘守住", 'state_summary': f"SR breakout long: previous_res={zone['lower']:.4f}-{zone['upper']:.4f}"})
    else:
        for zone in zones.get('broken_support', [])[:5]:
            zone_key = sr_breakout_zone_key(side, zone)
            cooldown_reason = sr_breakout_cooldown_reason(side, zone_key, context['now_dt'])
            if cooldown_reason:
                logging.info(f"跳过 SR breakout short: {cooldown_reason}")
                continue
            reverse_reason = sr_breakout_reverse_strength_reason(context, side, zone)
            if reverse_reason:
                logging.info(f"跳过 SR breakout short: {reverse_reason}, zone={zone_key}")
                continue
            if recent3.iloc[0]['close'] < zone['lower'] and bool((recent3.iloc[1:]['close'] <= zone['lower']).all()):
                entry_ref = price_to_float(recent3.iloc[-1]['close'])
                stop = zone['lower'] + SUP_RES_STOP_BUFFER_ATR * atr_4h
                target, target_zone = target_from_zones_or_rr(zones, side, entry_ref, stop)
                candidates.append({'module': 'sr_breakout', 'strategy_tf': '4h', 'trigger_tf': '4h', 'exit_tf': '1h', 'side': side, 'entry_level': entry_ref, 'stop': precision_price(stop), 'target': precision_price(target), 'target_zone': target_zone, 'profit_check_atr': atr_4h, 'sr_breakout_key': zone_key, 'sr_breakout_zone': dict(zone), 'reason': "4H有效支撑跌破并连续2根收盘压住", 'state_summary': f"SR breakout short: previous_sup={zone['lower']:.4f}-{zone['upper']:.4f}"})
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
    #4h>1h>15m
    tf_rank = {'4h': 0, '1h': 1, '15m': 2}
    #sr_breakout>sr_rebound>trend_trigger:关键位置被突破的趋势 > 回调趋势 > 趋势触发
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
    allowed_slippage = candidate_entry_allowed_slippage(candidate, entry_level)
    if side == 'long' and curr_price > entry_level + allowed_slippage:
        return False, (
            f"多单追价超限: module={candidate.get('module')}, current={curr_price}, "
            f"entry_level={entry_level}, allowed_slippage={allowed_slippage:.4f}"
        )
    if side == 'short' and curr_price < entry_level - allowed_slippage:
        return False, (
            f"空单追价超限: module={candidate.get('module')}, current={curr_price}, "
            f"entry_level={entry_level}, allowed_slippage={allowed_slippage:.4f}"
        )
    return True, 'ok'


def candidate_entry_allowed_slippage(candidate, entry_level=None):
    entry_level = price_to_float(entry_level if entry_level is not None else candidate.get('entry_level'))
    target = price_to_float(candidate.get('target'))
    side = candidate.get('side')
    if entry_level <= 0 or target <= 0:
        return 0.0

    if side == 'long':
        profit_distance = target - entry_level
    else:
        profit_distance = entry_level - target
    if profit_distance <= 0:
        return 0.0
    return profit_distance * ENTRY_MAX_SLIPPAGE_PROFIT_RATIO


def candidate_target_profit_still_valid(candidate, curr_price):
    side = candidate.get('side')
    stop = price_to_float(candidate.get('stop'))
    target = price_to_float(candidate.get('target'))
    atr = price_to_float(candidate.get('profit_check_atr'))
    return target_profit_still_valid(side, curr_price, stop, target, atr)


def build_trend_trigger_candidates(context):
    candidates = []
    zones = context['zones']
    sr_filter_atr = sr_filter_atr_from_context(context)
    for strategy_tf in STRATEGY_TIMEFRAMES:
        #当前时间
        local_state = context['trend_states'].get(strategy_tf)
        #背景时间
        background_tf = STRATEGY_BACKGROUND_TF[strategy_tf]
        background_state = context['trend_states'].get(background_tf)
        #触发时间
        trigger_tf = STRATEGY_TRIGGER_TF[strategy_tf]
        trigger_df = context['dfs'][trigger_tf]

        for side in ('long', 'short'):
            # 多周期校验背景趋势
            allowed, deny_reason = context_allows_side(local_state, background_state, side)
            if not allowed:
                logging.info(f"跳过{strategy_tf}->{trigger_tf} {side}: {deny_reason}")
                continue
            # 在确认趋势条件满足后，用来寻找开仓信号
            trigger = detect_entry_trigger(trigger_df, trigger_tf, side, now_dt=context['now_dt'])
            if not trigger:
                continue
            if trigger.get('blocked'):
                logging.info(trigger.get('reason'))
                continue

            entry_ref = price_to_float(trigger.get('entry_level'))
            stop = price_to_float(trigger['stop'])
            # 判断是否在阻挡区域
            sr_block_reason = sr_trend_entry_block_reason(side, entry_ref, zones, sr_filter_atr)
            if sr_block_reason:
                logging.info(f"跳过趋势触发{strategy_tf}->{trigger_tf} {side}: {sr_block_reason}")
                continue
            # 当 ADX 为极端值时，按原始止损距离收紧，而不是固定压到很窄的 ATR 距离。
            if background_state.get('details', {}).get('extreme_adx'):
                risk_distance = abs(entry_ref - stop)
                if risk_distance > 0:
                    if side == 'long':
                        stop = max(stop, entry_ref - EXTREME_ADX_STOP_RISK_RATIO * risk_distance)
                    else:
                        stop = min(stop, entry_ref + EXTREME_ADX_STOP_RISK_RATIO * risk_distance)


            strategy_df_closed = get_closed_df(context['dfs'][strategy_tf], strategy_tf, context['now_dt'])
            strategy_atr = price_to_float(strategy_df_closed.iloc[-1].get('atr')) if len(strategy_df_closed) > 0 else 0.0
            risk_distance = abs(entry_ref - stop)
            min_risk_atr = MIN_TREND_TRIGGER_RISK_ATR_BY_TF.get(strategy_tf)
            if not trend_trigger_risk_distance_allowed(risk_distance, strategy_atr, strategy_tf):
                risk_atr = risk_distance / strategy_atr if strategy_atr > 0 else 0.0
                logging.info(
                    f"跳过趋势触发{strategy_tf}->{trigger_tf} {side}: "
                    f"初始风险距离过近 risk={risk_distance:.4f}, "
                    f"{strategy_tf}ATR={strategy_atr:.4f}, risk_atr={risk_atr:.2f}, "
                    f"min={min_risk_atr:.2f}"
                )
                continue

            #查找止盈位置
            target, target_zone = target_from_zones_or_rr(zones, side, entry_ref, stop)
            trigger_atr = price_to_float(get_closed_df(trigger_df, trigger_tf, context['now_dt']).iloc[-1].get('atr'))
            #判断止盈位置是否符合利润预期
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
                'profit_check_atr': trigger_atr,
                'sr_filter_atr': sr_filter_atr,
                'reason': f"{strategy_tf}趋势 + {trigger['reason']}",
                'state_summary': f"{strategy_tf}/{background_tf}/{trigger_tf}: {trigger['reason']}; bg={background_state.get('status')}",
                'trigger': trigger
            })
    return candidates


def build_entry_candidates(context):
    """
    汇总所有入场候选信号，按权重排序后返回。
    三种候选来源：
        1. 趋势触发候选（trend_trigger）— 基于多周期 ADX+EMA 趋势 + K线形态
        2. SR反弹候选（sr_rebound）— 15m+1h 趋势允许时，在支撑/阻力位反弹
        3. SR突破候选（sr_breakout）— 4h+1d 趋势允许时，突破关键价位后回踩确认
    排序规则：高周期优先（4h > 1h > 15m），同周期内 SR突破 > SR反弹 > 趋势触发
    """
    # 构建趋势触发候选信号
    candidates = build_trend_trigger_candidates(context)
    for side in ('long', 'short'):
        # 15m+1h 多周期校验趋势 → 决定是否生成 SR反弹候选
        allowed, deny_reason = context_allows_side(context['trend_states'].get('15m'), context['trend_states'].get('1h'), side)
        if allowed:
            # 构建SR反弹候选信号：价格到达支撑/阻力位置并站稳，使用1h和15m确认
            candidates.extend(build_sr_rebound_candidates(context, side))
        else:
            logging.info(f"跳过SR反弹{side}: {deny_reason}")

        # 4h+1d 多周期校验趋势 → 决定是否生成 SR突破候选
        allowed, deny_reason = context_allows_side(context['trend_states'].get('4h'), context['trend_states'].get('1d'), side)
        if allowed:
            # 构建SR突破候选信号：K线突破了关键位置并且站稳，使用4h
            candidates.extend(build_sr_breakout_candidates(context, side))
        else:
            logging.info(f"跳过SR突破{side}: {deny_reason}")
    # 根据权重进行排序（高周期 + 高优先级模块优先）
    candidates.sort(key=candidate_priority)
    return candidates


def position_key(symbol, position_side, side=None):
    """手动仓位以交易对和 Binance positionSide 作为独立生命周期键。"""
    normalized_position_side = str(position_side or 'BOTH').upper()
    key = f"{normalize_futures_symbol(symbol)}|{normalized_position_side}"
    # 单向模式 positionSide 永远是 BOTH，额外带上实际多空方向才能识别快速反手的新生命周期。
    if normalized_position_side == 'BOTH' and side in ('long', 'short'):
        key = f"{key}|{side.upper()}"
    return key


def normalize_account_position(pos):
    """把 CCXT 仓位统一为手动接管模块使用的结构。"""
    if not isinstance(pos, dict):
        return None
    info = pos.get('info', {})
    if not isinstance(info, dict):
        info = {}
    raw_amount = price_to_float(info.get('positionAmt', pos.get('positionAmt', pos.get('contracts', 0))))
    if abs(raw_amount) <= POSITION_AMT_EPSILON:
        return None

    symbol = normalize_futures_symbol(pos.get('symbol') or info.get('symbol'))
    if not symbol or not symbol.endswith('/USDT'):
        return None
    side = infer_position_side(raw_amount, info=info, pos=pos)
    raw_position_side = str(info.get('positionSide') or pos.get('positionSide') or 'BOTH').strip().upper()
    if raw_position_side not in ('BOTH', 'LONG', 'SHORT'):
        raw_position_side = 'BOTH'
    entry_price = price_to_float(info.get('entryPrice', pos.get('entryPrice', 0)))
    mark_price = price_to_float(info.get('markPrice', pos.get('markPrice', 0)))
    liquidation_price = normalize_liquidation_price(
        info.get('liquidationPrice', pos.get('liquidationPrice', 0))
    )
    return {
        'key': position_key(symbol, raw_position_side, side=side),
        'symbol': symbol,
        'exchange_symbol_id': get_exchange_symbol_id(symbol),
        'position_side': raw_position_side,
        'side': side,
        'position_amt': raw_amount,
        'amount': abs(raw_amount),
        'entry_price': entry_price,
        'mark_price': mark_price,
        'liquidation_price': liquidation_price,
        'info': info,
    }


def fetch_all_account_positions():
    """读取账户内全部非零U本位仓位；风险接口失败时降级到标准仓位接口。"""
    errors = []
    positions = None
    # 不传 symbol 的 CCXT 统一接口可能先加载现货市场；Demo 环境优先直连U本位原生接口。
    raw_position_methods = (
        ('v3', getattr(exchange, 'fapiPrivateV3GetPositionRisk', None)),
        ('v2', getattr(exchange, 'fapiPrivateV2GetPositionRisk', None)),
        ('v1', getattr(exchange, 'fapiPrivateGetPositionRisk', None)),
    )
    for raw_version, raw_position_method in raw_position_methods:
        if raw_position_method is None:
            continue
        try:
            positions = raw_position_method({})
            break
        except Exception as e:
            errors.append(f"raw_position_risk_{raw_version}={e}")
    if positions is None:
        try:
            positions = exchange.fetch_positions_risk()
        except Exception as e:
            errors.append(f"fetch_positions_risk={e}")
    if positions is None:
        try:
            positions = exchange.fetch_positions()
        except Exception as e:
            errors.append(f"fetch_positions={e}")
            return {'fetch_failed': True, 'positions': [], 'error': '; '.join(errors)}

    normalized = []
    for pos in positions or []:
        item = normalize_account_position(pos)
        if item is not None:
            normalized.append(item)
    return {'fetch_failed': False, 'positions': normalized, 'error': '; '.join(errors)}


def is_local_strategy_position(position):
    """现有 trade_state 对应的 ETH 仓位仍由原策略管理，不重复归入手动仓位。"""
    return bool(
        trade_state.get('has_position')
        and normalize_futures_symbol(position.get('symbol')) == normalize_futures_symbol(SYMBOL)
        and position.get('side') == trade_state.get('side')
    )


def is_pending_strategy_position(position):
    """pending ETH 已成交但尚未同步时，先交给 pending 流程初始化，避免误判为手动仓位。"""
    return bool(
        has_pending_entry()
        and normalize_futures_symbol(position.get('symbol')) == normalize_futures_symbol(SYMBOL)
        and position.get('side') == trade_state.get('pending_entry_side')
    )


def manual_position_stop_is_valid(position, stop_price, atr):
    """手动仓位止损只和当前标记价比较，允许盈利后的止损越过入场价。"""
    side = position.get('side')
    symbol = position.get('symbol')
    entry = price_to_float(position.get('entry_price'))
    mark = price_to_float(position.get('mark_price'))
    if mark <= 0:
        return None, '缺少标记价格'
    safe_stop, _ = ensure_stop_price_safe(
        entry or mark,
        stop_price,
        side,
        liquidation_price=position.get('liquidation_price'),
    )
    safe_stop = precision_price(safe_stop, symbol=symbol)
    tick = get_price_tick(mark, symbol=symbol)
    min_distance = max(price_to_float(atr) * MANUAL_STOP_MIN_DISTANCE_ATR, tick)
    if side == 'long':
        if safe_stop <= 0 or safe_stop > mark - tick:
            return None, '多单止损不在当前价下方'
        if mark - safe_stop < min_distance:
            return None, '多单止损距离当前价过近'
    else:
        if safe_stop <= 0 or safe_stop < mark + tick:
            return None, '空单止损不在当前价上方'
        if safe_stop - mark < min_distance:
            return None, '空单止损距离当前价过近'
    return safe_stop, 'ok'


def choose_tightest_stop(side, candidates):
    """多单取最高止损、空单取最低止损，即选择预期亏损更小的保护位。"""
    valid = [(price_to_float(stop), reason) for stop, reason in candidates if price_to_float(stop) > 0]
    if not valid:
        return None
    return max(valid, key=lambda item: item[0]) if side == 'long' else min(valid, key=lambda item: item[0])


def manual_profile_stop_candidate(position, profile, dfs, now_dt, highest, lowest):
    """计算一套背景/出场周期对应的初始止损候选。"""
    side = position['side']
    mark = price_to_float(position.get('mark_price'))
    entry = price_to_float(position.get('entry_price'))
    symbol = position['symbol']
    exit_tf = profile['exit_tf']
    background_tf = profile['background_tf']
    df_exit = dfs.get(exit_tf)
    df_background = dfs.get(background_tf)
    if df_exit is None or df_background is None:
        return None

    df_closed = get_closed_df(df_exit, exit_tf, now_dt=now_dt)
    if len(df_closed) == 0:
        return None
    atr = price_to_float(df_closed.iloc[-1].get('atr'))
    if atr <= 0:
        return None
    tick = get_price_tick(mark, symbol=symbol)
    raw_candidates = []

    # 最近结构低点/高点给手动仓位提供基础保护，ATR缓冲避免贴在影线上。
    recent = df_closed.tail(MANUAL_STOP_SWING_LOOKBACK)
    if side == 'long':
        structure_stop = price_to_float(recent['low'].min()) - MANUAL_STOP_STRUCTURE_ATR_BUFFER * atr
    else:
        structure_stop = price_to_float(recent['high'].max()) + MANUAL_STOP_STRUCTURE_ATR_BUFFER * atr
    raw_candidates.append((structure_stop, f"{exit_tf}最近结构位"))

    large = latest_large_strong(df_closed)
    if large:
        raw_candidates.append((strong_candle_stop_for_position(side, large, tick), f"{exit_tf}大强K结构"))
    same_strong = latest_effective_strong(df_closed, side)
    if same_strong:
        raw_candidates.append((strong_candle_stop_for_position(side, same_strong, tick), f"{exit_tf}同向强K结构"))
    opposite = 'short' if side == 'long' else 'long'
    opposite_strong = latest_effective_strong(df_closed, opposite)
    if opposite_strong:
        raw_candidates.append((strong_candle_stop_for_position(side, opposite_strong, tick), f"{exit_tf}反向强K结构"))

    # 接管时已经达到1 ATR浮盈，也立即沿用原策略的30%/50%/70%/90%利润锁定。
    profit_lock = profit_atr_lock_stop_for_position(side, entry, highest, lowest, atr)
    if profit_lock:
        raw_candidates.append((
            profit_lock['stop'],
            f"已盈利{profit_lock['profit_atr']:.2f}ATR，保留{profit_lock['retain_ratio']:.0%}利润",
        ))

    valid_candidates = []
    rejected = []
    for raw_stop, reason in raw_candidates:
        valid_stop, invalid_reason = manual_position_stop_is_valid(position, raw_stop, atr)
        if valid_stop is None:
            rejected.append(f"{reason}:{invalid_reason}")
            continue
        valid_candidates.append((valid_stop, reason))

    # 极端行情下结构位可能全部失效，仍用1 ATR兜底，避免新接管仓位长期裸奔。
    if not valid_candidates:
        fallback_stop = mark - atr if side == 'long' else mark + atr
        valid_stop, invalid_reason = manual_position_stop_is_valid(position, fallback_stop, atr)
        if valid_stop is None:
            return None
        valid_candidates.append((valid_stop, f"{exit_tf} ATR应急保护"))

    best_stop, reason = choose_tightest_stop(side, valid_candidates)
    background_state = evaluate_adx_ema_context(df_background, background_tf, now_dt=now_dt)
    return {
        'profile_name': profile['name'],
        'background_tf': background_tf,
        'exit_tf': exit_tf,
        'stop': best_stop,
        'atr': atr,
        'reason': reason,
        'background_summary': background_state.get('summary', ''),
        'rejected': rejected,
    }


def select_manual_position_profile(position, dfs, now_dt, highest, lowest):
    """同时评估两套周期并选择亏损更小的一套，选定后由 state 锁定。"""
    candidates = []
    for profile in MANUAL_POSITION_PROFILES:
        candidate = manual_profile_stop_candidate(position, profile, dfs, now_dt, highest, lowest)
        if candidate:
            candidates.append(candidate)
    if not candidates:
        return None
    if position['side'] == 'long':
        return max(candidates, key=lambda item: item['stop'])
    return min(candidates, key=lambda item: item['stop'])


def fetch_manual_position_dfs(symbol):
    """手动仓位只抓15M/1H/4H，缓存按 symbol 隔离，不运行ETH开仓策略。"""
    dfs = {
        '15m': fetch_df_cached(symbol, '15m', 220),
        '1h': fetch_df_cached(symbol, '1h', 220),
        '4h': fetch_df_cached(symbol, '4h', SUP_RES_LOOKBACK_4H),
    }
    return None if any(df is None for df in dfs.values()) else dfs


def manual_owned_stop_orders(state):
    """读取该手动仓位的脚本自有保护单，手工订单不会进入结果。"""
    orders = fetch_open_protective_stop_orders(
        side=state['side'],
        symbol=state['symbol'],
        position_side=state.get('position_side'),
        owned_only=False,
    )
    if orders is None:
        return None
    tracked_id = state.get('stop_order_id', '')
    return [
        order for order in orders
        if is_script_owned_protective_order(
            order,
            tracked_order_id=tracked_id,
            prefixes=(MANUAL_STOP_CLIENT_PREFIX,),
        )
    ]


def place_manual_protective_stop_order(state, stop_price):
    """为一笔手动仓位创建带 TAM_ 前缀的服务端 STOP_MARKET 保护单。"""
    side = state['side']
    symbol = state['symbol']
    stop_side = 'sell' if side == 'long' else 'buy'
    client_order_id = make_protective_client_order_id(MANUAL_STOP_CLIENT_PREFIX)
    params = {
        'stopPrice': precision_price(stop_price, symbol=symbol),
        'workingType': STOP_WORKING_TYPE,
        'newClientOrderId': client_order_id,
    }
    raw_position_side = state.get('position_side', 'BOTH')
    amount = price_to_float(state.get('amount'))
    if raw_position_side in ('LONG', 'SHORT'):
        # 对冲模式使用 positionSide + closePosition，避免 reduceOnly 参数被 Binance 拒绝。
        params['positionSide'] = raw_position_side
        params['closePosition'] = True
        amount = None
    else:
        params['reduceOnly'] = True
    order = exchange.create_order(symbol, 'STOP_MARKET', stop_side, amount, None, params)
    return order, client_order_id


def refresh_manual_protective_stop_order(state, stop_price, reason, force_quantity_refresh=False):
    """精确替换手动仓位的脚本保护单；交易所上的手工条件单保持不动。"""
    owned_orders = manual_owned_stop_orders(state)
    if owned_orders is None:
        return False
    for order in owned_orders:
        if not cancel_conditional_order_exact(
            order,
            symbol=state['symbol'],
            silent=True,
            reason='替换手动仓位脚本保护单',
        ):
            state['last_stop_order_error'] = f"无法撤销脚本保护单 {extract_order_id(order)}"
            return False
    if owned_orders:
        time.sleep(STOP_ORDER_POST_CANCEL_DELAY_SECONDS)

    try:
        order, client_order_id = place_manual_protective_stop_order(state, stop_price)
        state['stop_order_id'] = extract_order_id(order)
        state['stop_client_order_id'] = extract_order_client_id(order) or client_order_id
        state['stop_order_price'] = precision_price(stop_price, symbol=state['symbol'])
        state['stop_loss_price'] = state['stop_order_price']
        state['last_stop_order_error'] = ''
        state['last_order_amount'] = price_to_float(state.get('amount'))
        logging.info(
            "手动仓位保护单已更新: symbol=%s side=%s profile=%s stop=%s amount=%s reason=%s quantity_refresh=%s",
            state['symbol'], state['side'], state.get('profile_name'), state['stop_order_price'],
            state.get('amount'), reason, force_quantity_refresh,
        )
        return True
    except Exception as e:
        state['last_stop_order_error'] = format_exception_message(e)
        logging.error(
            "手动仓位保护单创建失败（不会撤销手工委托）: symbol=%s side=%s stop=%s error=%s",
            state['symbol'], state['side'], stop_price, state['last_stop_order_error'],
        )
        return False


def manual_stop_is_tighter(side, new_stop, current_stop, mark, symbol):
    tick = get_price_tick(mark, symbol=symbol)
    if side == 'long':
        return new_stop > current_stop + tick / 2 and new_stop <= mark - tick
    return (current_stop <= 0 or new_stop < current_stop - tick / 2) and new_stop >= mark + tick


def build_manual_dynamic_stop(state, position, dfs, now_dt):
    """按已锁定的出场周期生成动态止损，并允许4H阻力/支撑进一步收紧。"""
    side = state['side']
    exit_tf = state['exit_tf']
    mark = price_to_float(position.get('mark_price'))
    symbol = state['symbol']
    df_exit_closed = get_closed_df(dfs[exit_tf], exit_tf, now_dt=now_dt)
    if len(df_exit_closed) == 0:
        return None
    atr = price_to_float(df_exit_closed.iloc[-1].get('atr'))
    if atr <= 0:
        return None
    tick = get_price_tick(mark, symbol=symbol)
    raw_candidates = []

    large = latest_large_strong(df_exit_closed)
    if large:
        raw_candidates.append((strong_candle_stop_for_position(side, large, tick), f"{exit_tf}大强K"))
    same = latest_effective_strong(df_exit_closed, side)
    if same:
        raw_candidates.append((strong_candle_stop_for_position(side, same, tick), f"{exit_tf}同向强K"))
    opposite = latest_effective_strong(df_exit_closed, 'short' if side == 'long' else 'long')
    if opposite:
        raw_candidates.append((strong_candle_stop_for_position(side, opposite, tick), f"{exit_tf}反向强K"))

    profit_lock = profit_atr_lock_stop_for_position(
        side,
        price_to_float(state.get('entry_price')),
        price_to_float(state.get('highest_price')),
        price_to_float(state.get('lowest_price')),
        atr,
    )
    if profit_lock:
        raw_candidates.append((
            profit_lock['stop'],
            f"浮盈{profit_lock['profit_atr']:.2f}ATR，保留{profit_lock['retain_ratio']:.0%}利润",
        ))

    df_4h_closed = get_closed_df(dfs['4h'], '4h', now_dt=now_dt)
    if len(df_4h_closed) > 0:
        atr_4h = price_to_float(df_4h_closed.iloc[-1].get('atr'))
        zones = build_support_resistance_zones(dfs['4h'], now_dt=now_dt)
        near_buffer = SUP_RES_NEAR_ATR * atr_4h
        if side == 'long':
            resistance = nearest_opposite_zone(zones, side, mark - near_buffer)
            if resistance and mark >= resistance['lower'] - near_buffer:
                raw_candidates.append((resistance['lower'] - 0.4 * atr_4h, '靠近4H阻力区'))
        else:
            support = nearest_opposite_zone(zones, side, mark + near_buffer)
            if support and mark <= support['upper'] + near_buffer:
                raw_candidates.append((support['upper'] + 0.4 * atr_4h, '靠近4H支撑区'))

    valid_candidates = []
    for raw_stop, reason in raw_candidates:
        valid_stop, _ = manual_position_stop_is_valid(position, raw_stop, atr)
        if valid_stop is not None:
            valid_candidates.append((valid_stop, reason))
    return choose_tightest_stop(side, valid_candidates)


def initialize_manual_position_state(position):
    """首次发现手动仓位时选择并锁定周期，同时立即创建保护单。"""
    symbol = position['symbol']
    mark = price_to_float(position.get('mark_price')) or get_latest_price(symbol)
    position = dict(position, mark_price=mark)
    entry = price_to_float(position.get('entry_price')) or mark
    highest = max(entry, mark)
    lowest = min(entry, mark)
    dfs = fetch_manual_position_dfs(symbol)
    if dfs is None:
        logging.warning(f"手动仓位K线不完整，暂缓接管: symbol={symbol}")
        return None
    now_dt = get_server_now_dt()
    selected = select_manual_position_profile(position, dfs, now_dt, highest, lowest)
    if selected is None:
        logging.error(f"手动仓位两套周期都无法生成有效止损: symbol={symbol}, side={position['side']}")
        return None

    state = {
        'key': position['key'],
        'symbol': symbol,
        'position_side': position['position_side'],
        'side': position['side'],
        'entry_price': entry,
        'amount': position['amount'],
        'last_order_amount': 0.0,
        'liquidation_price': position.get('liquidation_price'),
        'highest_price': highest,
        'lowest_price': lowest,
        'profile_name': selected['profile_name'],
        'background_tf': selected['background_tf'],
        'exit_tf': selected['exit_tf'],
        'initial_atr': selected['atr'],
        'stop_loss_price': selected['stop'],
        'stop_order_price': 0.0,
        'stop_order_id': '',
        'stop_client_order_id': '',
        'detected_time': get_server_time_str(),
        'miss_count': 0,
        'last_stop_order_error': '',
        'background_summary': selected.get('background_summary', ''),
        'ai_initial_amount': position['amount'],
        'ai_last_reduce_generated_at': '',
        'ai_partial_reduce_count': 0,
        'ai_partial_reduce_amount': 0.0,
        'ai_last_decision': {},
        'ai_guard_snapshot': {},
        'ai_bias_file': manual_ai_bias_file(symbol),
        'ai_research_status': '',
    }
    if not refresh_manual_protective_stop_order(state, selected['stop'], selected['reason']):
        logging.error(f"手动仓位已发现但脚本保护单尚未挂成功: key={state['key']}")
    logging.warning(
        "已接管手动仓位并锁定周期: key=%s side=%s entry=%s mark=%s profile=%s stop=%s background=%s",
        state['key'], state['side'], entry, mark, state['profile_name'], state['stop_loss_price'],
        state.get('background_summary', ''),
    )
    # 保护止损先落地，再异步启动AI研究；AI任务慢或失败不会延迟仓位保护。
    ensure_manual_ai_research_job(symbol, state=state)
    return state


def cleanup_closed_position_exit_orders(state):
    """确认仓位完全归零后，清理该 symbol/side 的全部止损止盈，普通开仓委托不受影响。"""
    orders = fetch_open_close_position_orders(
        side=state['side'],
        symbol=state['symbol'],
        position_side=state.get('position_side'),
    )
    if orders is None:
        return False
    return cancel_conditional_orders(
        orders,
        silent=True,
        reason='确认手动仓位全平后清理该方向出场条件单',
        symbol=state['symbol'],
    )


def confirm_manual_position_fully_closed(state):
    """连续缺失后再用标准仓位接口复查，防止平仓后立刻重开的竞态。"""
    try:
        positions = exchange.fetch_positions([state['symbol']])
    except Exception as e:
        logging.warning(f"手动仓位全平复查失败，保留状态等待下轮: key={state['key']}, error={e}")
        return False
    for raw_position in positions or []:
        position = normalize_account_position(raw_position)
        if position is None:
            continue
        if position['key'] == state['key'] and position['side'] == state['side']:
            return False
    return True


def manual_exchange_min_amount(symbol):
    try:
        market = exchange.market(symbol)
        return price_to_float(market.get('limits', {}).get('amount', {}).get('min'))
    except Exception:
        return 0.0


def apply_manual_ai_position_guard(state, position, mark):
    """按该交易对独立bias评估手动仓位；AI只能减仓，不能加仓、全平或反手。"""
    guard = ensure_manual_ai_research_job(state['symbol'], state=state)
    current_amount = price_to_float(state.get('amount'))
    initial_amount = price_to_float(state.get('ai_initial_amount'))
    if initial_amount <= 0:
        initial_amount = current_amount
        state['ai_initial_amount'] = initial_amount
    if current_amount <= POSITION_AMT_EPSILON or initial_amount <= POSITION_AMT_EPSILON:
        return False

    decision = evaluate_position_reduction(
        state['side'],
        initial_amount,
        current_amount,
        guard,
        AI_RESEARCH_CONFIG,
        last_reduce_generated_at=state.get('ai_last_reduce_generated_at', ''),
    )
    state_key = state.get('key') or position_key(
        state['symbol'],
        state.get('position_side', 'BOTH'),
        side=state.get('side'),
    )
    write_ai_audit(
        'manual_position_evaluation',
        guard,
        {
            'symbol': compact_usdt_symbol(state['symbol']),
            'position_side': state['side'],
            'entry_initial_amount': initial_amount,
            'current_amount': current_amount,
            **decision,
        },
        dedupe_key=(
            f"manual-position:{compact_usdt_symbol(state['symbol'])}:{state_key}:"
            f"{guard.get('generated_at', 'invalid')}:{current_amount:.12f}:{decision.get('shadow_action')}"
        ),
    )
    if not decision.get('should_reduce'):
        return False

    requested_amount = min(current_amount, price_to_float(decision.get('reduce_amount')))
    try:
        reduce_amount = float(exchange.amount_to_precision(state['symbol'], requested_amount))
    except Exception as e:
        logging.warning(f"手动仓位AI减仓数量精度处理失败: key={state['key']}, error={e}")
        return False
    min_amount = manual_exchange_min_amount(state['symbol'])
    if reduce_amount <= POSITION_AMT_EPSILON or (min_amount > 0 and reduce_amount < min_amount):
        return False
    if reduce_amount >= current_amount - POSITION_AMT_EPSILON:
        # 即使上游配置异常，也硬性禁止AI把手动仓位完全平掉。
        logging.error(f"拒绝手动仓位AI全平: key={state['key']}, current={current_amount}, reduce={reduce_amount}")
        return False

    order_side = 'sell' if state['side'] == 'long' else 'buy'
    params = {}
    raw_position_side = state.get('position_side', 'BOTH')
    if raw_position_side in ('LONG', 'SHORT'):
        params['positionSide'] = raw_position_side
    else:
        params['reduceOnly'] = True
    try:
        order = exchange.create_order(
            state['symbol'],
            'market',
            order_side,
            reduce_amount,
            None,
            params,
        )
    except Exception as e:
        logging.error(f"手动仓位AI减仓失败: key={state['key']}, error={e}")
        return False

    reported_fill = extract_order_filled_amount(order)
    actual_reduced = reported_fill if reported_fill > POSITION_AMT_EPSILON else reduce_amount
    remaining_amount = max(0.0, current_amount - actual_reduced)
    if remaining_amount <= POSITION_AMT_EPSILON:
        logging.error(f"手动仓位AI减仓返回异常剩余数量，保留旧状态等待交易所复核: key={state['key']}")
        return False

    state['amount'] = remaining_amount
    state['ai_last_reduce_generated_at'] = guard.get('generated_at', '')
    state['ai_partial_reduce_count'] = int(state.get('ai_partial_reduce_count', 0) or 0) + 1
    state['ai_partial_reduce_amount'] = price_to_float(state.get('ai_partial_reduce_amount')) + actual_reduced
    state['ai_last_decision'] = decision
    stop_price = price_to_float(state.get('stop_loss_price'))
    stop_refreshed = False
    if stop_price > 0:
        # 减仓成交后只刷新 TAM_ 保护单数量，用户自己的止损止盈仍然保留。
        stop_refreshed = refresh_manual_protective_stop_order(
            state,
            stop_price,
            'TradingAgents减仓后刷新保护单数量',
            force_quantity_refresh=True,
        )
    write_ai_audit(
        'manual_position_reduction_executed',
        guard,
        {
            'symbol': compact_usdt_symbol(state['symbol']),
            'position_side': state['side'],
            **decision,
            'reduce_order_id': extract_order_id(order),
            'actual_reduce_amount': actual_reduced,
            'remaining_amount': remaining_amount,
            'stop_refreshed': stop_refreshed,
            'actual_action': 'reduced',
        },
        dedupe_key=(
            f"manual-reduced:{compact_usdt_symbol(state['symbol'])}:"
            f"{guard.get('generated_at', '')}:{extract_order_id(order)}"
        ),
    )
    logging.warning(
        "TradingAgents已对手动仓位执行减仓: key=%s reduced=%s remaining=%s order_id=%s stop_refreshed=%s",
        state['key'], actual_reduced, remaining_amount, extract_order_id(order), stop_refreshed,
    )
    return True


def manage_manual_position(state, position):
    """同步一笔手动仓位的价格、数量和锁定周期动态止损。"""
    mark = price_to_float(position.get('mark_price')) or get_latest_price(state['symbol'])
    position = dict(position, mark_price=mark)
    state['miss_count'] = 0
    state['liquidation_price'] = position.get('liquidation_price')
    if state['side'] == 'long':
        state['highest_price'] = max(price_to_float(state.get('highest_price')), mark)
    else:
        previous_low = price_to_float(state.get('lowest_price'))
        state['lowest_price'] = mark if previous_low <= 0 else min(previous_low, mark)

    old_amount = price_to_float(state.get('amount'))
    new_amount = price_to_float(position.get('amount'))
    state['amount'] = new_amount
    quantity_changed = abs(new_amount - old_amount) > POSITION_AMT_EPSILON
    apply_manual_ai_position_guard(state, position, mark)
    dfs = fetch_manual_position_dfs(state['symbol'])
    if dfs is None:
        return
    dynamic = build_manual_dynamic_stop(state, position, dfs, get_server_now_dt())
    current_stop = price_to_float(state.get('stop_loss_price'))
    if dynamic:
        new_stop, reason = dynamic
        if manual_stop_is_tighter(state['side'], new_stop, current_stop, mark, state['symbol']):
            refresh_manual_protective_stop_order(state, new_stop, reason)
            return

    owned_orders = manual_owned_stop_orders(state)
    own_stop_missing = owned_orders is not None and not owned_orders
    if quantity_changed or own_stop_missing:
        refresh_manual_protective_stop_order(
            state,
            current_stop,
            '仓位数量变化，刷新脚本保护单数量' if quantity_changed else '脚本保护单缺失，重新补挂',
            force_quantity_refresh=quantity_changed,
        )


def manage_all_manual_positions(account_positions):
    """发现、更新并确认关闭全部手动仓位，返回当前是否仍需暂停ETH开仓。"""
    reap_manual_ai_jobs()
    visible_manual = {}
    for position in account_positions:
        if is_local_strategy_position(position) or is_pending_strategy_position(position):
            continue
        visible_manual[position['key']] = position

    # 同一个 one-way key 如果方向已反转，先清掉旧方向出场单，再按新仓位重新接管。
    for key, position in list(visible_manual.items()):
        existing = manual_position_states.get(key)
        if existing and existing.get('side') != position.get('side'):
            cleanup_closed_position_exit_orders(existing)
            manual_position_states.pop(key, None)

    for key, position in visible_manual.items():
        state = manual_position_states.get(key)
        if state is None:
            state = initialize_manual_position_state(position)
            if state is not None:
                manual_position_states[key] = state
        else:
            manage_manual_position(state, position)

    for key, state in list(manual_position_states.items()):
        if key in visible_manual:
            continue
        state['miss_count'] = int(state.get('miss_count', 0) or 0) + 1
        if state['miss_count'] < MANUAL_POSITION_MISS_CONFIRM_COUNT:
            logging.warning(
                "手动仓位暂未查到，等待连续确认: key=%s miss=%s/%s",
                key, state['miss_count'], MANUAL_POSITION_MISS_CONFIRM_COUNT,
            )
            continue
        if not confirm_manual_position_fully_closed(state):
            state['miss_count'] = 0
            continue
        cleanup_ok = cleanup_closed_position_exit_orders(state)
        if not cleanup_ok:
            state['miss_count'] = MANUAL_POSITION_MISS_CONFIRM_COUNT
            logging.warning(f"手动仓位已全平但出场条件单尚未清理完成，下轮继续重试: key={key}")
            continue
        closed_time = get_server_time_str()
        if normalize_futures_symbol(state['symbol']) == normalize_futures_symbol(SYMBOL):
            # 手动ETH平仓仍执行20分钟冷却；BTC/SOL等非ETH平仓不写入ETH冷却时间。
            trade_state['last_exit_time'] = closed_time
        manual_position_states.pop(key, None)
        logging.warning(
            "手动仓位已确认全平并完成出场单清理: key=%s cooldown=%s",
            key,
            '20分钟ETH冷却' if normalize_futures_symbol(state['symbol']) == normalize_futures_symbol(SYMBOL) else '无ETH冷却',
        )

    return bool(visible_manual or manual_position_states)


def build_context(dfs, now_dt, perf=None):
    """
    上下文聚合器 — 每轮交易决策前，合并所需的市场状态信息。
    两大部分：
        1. 各周期趋势状态（15m/1h/4h/1d 的 ADX+EMA 评估）
        2. 4H 支撑阻力区间（含当前有效区间和被突破区间，带缓存）
    返回: dict(dfs, now_dt, trend_states, zones)
    """
    trend_start = time.monotonic()
    # 对四大周期并行或依次评估趋势状态
    trend_states = {
        timeframe: evaluate_adx_ema_context(df, timeframe, now_dt=now_dt)
        for timeframe, df in dfs.items()
        if timeframe in ('15m', '1h', '4h', '1d')
    }
    record_perf(perf, 'build_context_trend_states', trend_start)

    # 构建4H支撑阻力区间，按4H K线时间戳做缓存（同根K线内复用）
    sr_start = time.monotonic()
    sr_bar_time = get_closed_bar_time(dfs['4h'], '4h', now_dt=now_dt)
    sr_cache = runtime_state.get('support_resistance_cache', {})
    if sr_bar_time and sr_cache.get('bar_time') == sr_bar_time and sr_cache.get('zones') is not None:
        zones = copy.deepcopy(sr_cache['zones'])
        add_perf(perf, 'build_context_support_resistance_cache_hit', elapsed_ms(sr_start))
    else:
        #每4小时执行一次
        zones = build_support_resistance_zones(dfs['4h'], now_dt=now_dt)
        if sr_bar_time:
            runtime_state['support_resistance_cache'] = {
                'bar_time': sr_bar_time,
                'zones': copy.deepcopy(zones)
            }
    record_perf(perf, 'build_context_support_resistance', sr_start)
    return {'dfs': dfs, 'now_dt': now_dt, 'trend_states': trend_states, 'zones': zones}


def is_in_post_exit_cooldown(now_dt):
    """
    出场冷却期判断：上次平仓后需等待 POST_EXIT_COOLDOWN_SECONDS 秒，
    防止刚平仓就立刻重新开仓（频繁交易）。
    """
    last_exit_dt = parse_bar_time(trade_state.get('last_exit_time', ''))
    if last_exit_dt is None:
        return False
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=EXCHANGE_TZ)
    else:
        now_dt = now_dt.astimezone(EXCHANGE_TZ)
    return (now_dt - last_exit_dt).total_seconds() < POST_EXIT_COOLDOWN_SECONDS


def update_stop_if_tighter(side, new_stop, reason, curr_price, signal_bar_15m='', perf=None):
    current_stop = price_to_float(trade_state.get('stop_loss_price'))
    new_stop = precision_price(new_stop)
    tick = get_price_tick(curr_price)
    if new_stop <= 0:
        return False
    if side == 'long':
        if new_stop <= current_stop or new_stop > curr_price - tick:
            return False
    else:
        if (current_stop > 0 and new_stop >= current_stop) or new_stop < curr_price + tick:
            return False
    refresh_start = time.monotonic()
    refreshed = refresh_protective_stop_order(new_stop)
    record_perf(perf, 'refresh_protective_stop_order', refresh_start)
    if refreshed:
        trade_state['stop_loss_price'] = new_stop
        logging.info(f"{reason}: 已收紧服务端止损到 {new_stop}")
        return True
    handle_stop_order_refresh_failure(reason, curr_price, signal_bar_15m=signal_bar_15m, trigger_label=reason)
    return False


def strong_candle_stop_for_position(side, candle, tick):
    if not candle:
        return None
    if side == 'long':
        return price_to_float(candle.get('low')) - tick
    return price_to_float(candle.get('high')) + tick


def profit_atr_lock_stop_for_position(side, entry, highest, lowest, atr):
    if entry <= 0 or atr <= 0:
        return None
    max_profit = highest - entry if side == 'long' else entry - lowest
    if max_profit <= 0:
        return None
    profit_atr = max_profit / atr
    retain_ratio = None
    for threshold, ratio in PROFIT_ATR_LOCK_TIERS:
        if profit_atr >= threshold:
            retain_ratio = ratio
            break
    if retain_ratio is None:
        return None

    if side == 'long':
        stop = entry + max_profit * retain_ratio
    else:
        stop = entry - max_profit * retain_ratio
    return {
        'stop': stop,
        'profit_atr': profit_atr,
        'retain_ratio': retain_ratio,
        'max_profit': max_profit,
    }


def apply_new_exit_rules(context, signal_bar_15m='', curr_price=None, price_ts=None, perf=None):
    """持仓期间动态收紧止损：收集所有候选止损位，取最有利于仓位的那个。

    6 条规则（候选收集模式，互不干扰，最后取 max/min 择优）：
      1. 大强K线止损  → 出场周期出现大实体强K，止损移到强K极端位预留空间
      2. 同向强K止损  → 出场周期出现与仓位同向的普通强K，止损移到强K结构位
      3. 反向强K止损  → 出场周期出现反向强K，止损移到反向K极端位（防止反转）
      4. ATR利润锁定   → 浮盈达到N倍ATR后，锁住一部分利润不回撤
      5. 第一目标位     → 价格触及入场时计算的第一目标，止损移到保本位
      6. SR区间止盈    → 价格靠近4H支撑/阻力区，止损缩到区间边缘附近
    """
    if not trade_state.get('has_position'):
        return
    apply_start = time.monotonic()
    side = trade_state.get('side')
    price_start = time.monotonic()
    now_ts = time.monotonic()
    # 复用调用方传入的价格（若未过期），否则重新获取最新价格
    if curr_price is not None and price_ts is not None and (now_ts - price_ts) * 1000.0 <= EXIT_RULE_PRICE_MAX_AGE_MS:
        curr_price = float(curr_price)
    else:
        curr_price = get_latest_price()
    record_perf(perf, 'apply_new_exit_rules_get_latest_price', price_start)
    try:
        tick = get_price_tick(curr_price)
        stop_candidates = []  # 收集所有候选止损位，后续择优选取

        def add_stop_candidate(stop, reason):
            stop = price_to_float(stop)
            if stop > 0:
                stop_candidates.append((stop, reason))

        # 获取出场周期（默认15m）的K线数据，出场周期也是触发周期
        exit_tf = trade_state.get('entry_exit_tf') or STRATEGY_EXIT_TF.get(trade_state.get('entry_strategy_tf'), '15m')
        df_exit = context['dfs'].get(exit_tf)
        exit_atr = 0.0
        if df_exit is not None:
            df_exit_closed = get_closed_df(df_exit, exit_tf, context['now_dt'])
            if len(df_exit_closed) > 0:
                exit_atr = price_to_float(df_exit_closed.iloc[-1].get('atr'))

            # 规则1：最新收盘大强K线 → 止损移到强K极端位
            large = latest_large_strong(df_exit_closed)
            if large:
                add_stop_candidate(
                    strong_candle_stop_for_position(side, large, tick),
                    f"{exit_tf}出现大强{large.get('direction')}线，按强K结构收紧止损"
                )

            # 规则2：同向普通强K → 止损移到同向强K结构位
            same_strong = latest_effective_strong(df_exit_closed, side)
            if same_strong:
                add_stop_candidate(
                    strong_candle_stop_for_position(side, same_strong, tick),
                    f"{exit_tf}出现同向{same_strong.get('kind')}强K，按强K结构收紧止损"
                )

            # 规则3：反向强K → 止损移到反向K极端位（防反转）
            opposite = 'short' if side == 'long' else 'long'
            strong_opposite = latest_effective_strong(df_exit_closed, opposite)
            if strong_opposite:
                add_stop_candidate(
                    strong_candle_stop_for_position(side, strong_opposite, tick),
                    f"{exit_tf}出现反向{strong_opposite.get('kind')}强K，按强K结构收紧止损"
                )

        # 规则4：ATR利润锁定 → 浮盈达到阈值后锁住部分利润
        entry = price_to_float(trade_state.get('entry_price'))
        highest = price_to_float(trade_state.get('highest_price'))
        lowest = price_to_float(trade_state.get('lowest_price'))
        # exit_atr是触发周期的值，比如：4小时作为背景周期，1小时作为策略周期，15m作为触发周期
        profit_lock = profit_atr_lock_stop_for_position(side, entry, highest, lowest, exit_atr)
        if profit_lock:
            add_stop_candidate(
                profit_lock['stop'],
                (
                    f"最高浮盈达到{profit_lock['profit_atr']:.2f}ATR，"
                    f"至少保留{profit_lock['retain_ratio']:.0%}利润"
                )
            )

        # 规则5：第一目标位 → 价格触及入场目标，止损收紧到保本附近
        target = price_to_float(trade_state.get('entry_sr_target'))
        risk = price_to_float(trade_state.get('entry_initial_risk'))
        if target <= 0 and entry > 0 and risk > 0:
            target = entry + 2 * risk if side == 'long' else entry - 2 * risk  # 无目标时默认2R
        if target > 0:
            if side == 'long' and curr_price >= target:
                add_stop_candidate(max(entry, target - 0.5 * risk), "达到第一目标位，收紧保护止损")
            elif side == 'short' and curr_price <= target:
                add_stop_candidate(min(entry, target + 0.5 * risk), "达到第一目标位，收紧保护止损")

        # 规则6：SR区间止盈 → 价格靠近4H支撑/阻力区，止损缩到区间边缘
        df_4h_closed = get_closed_df(context['dfs']['4h'], '4h', context['now_dt'])
        if len(df_4h_closed) > 0:
            atr_4h = price_to_float(df_4h_closed.iloc[-1].get('atr'))
            near_buffer = SUP_RES_NEAR_ATR * atr_4h
            zones = context.get('zones', {})
            if side == 'long':
                # 做多：找上方最近的阻力区 ,curr_price-near_buffer:是为了提前缩紧止损，避免价格触及阻力区
                resistance = nearest_opposite_zone(zones, side, curr_price - near_buffer)
                if resistance and curr_price >= resistance['lower'] - near_buffer:
                    add_stop_candidate(resistance['lower'] - 0.4 * atr_4h, "靠近有效阻力区，缩紧止盈")
            else:
                # 做空：找下方最近的支撑区
                support = nearest_opposite_zone(zones, side, curr_price + near_buffer)
                if support and curr_price <= support['upper'] + near_buffer:
                    add_stop_candidate(support['upper'] + 0.4 * atr_4h, "靠近有效支撑区，缩紧止盈")

        # 无候选止损位 → 不用调整
        if not stop_candidates:
            return
        # 做多取最大止损价（最高防线）、做空取最小止损价（最低防线）
        if side == 'long':
            best_stop, reason = max(stop_candidates, key=lambda item: item[0])
        else:
            best_stop, reason = min(stop_candidates, key=lambda item: item[0])
        # 仅当新止损比当前止损更有利于仓位时才更新
        update_stop_if_tighter(side, best_stop, reason, curr_price, signal_bar_15m=signal_bar_15m, perf=perf)
    finally:
        record_perf(perf, 'apply_new_exit_rules', apply_start)


def exchange_min_amount():
    try:
        market = exchange.market(SYMBOL)
        return price_to_float(market.get('limits', {}).get('amount', {}).get('min'))
    except Exception:
        return 0.0


def ai_reduce_existing_position(position_risk, decision, guard, signal_bar_15m=''):
    """按AI目标仓位执行单向持仓reduceOnly市价减仓，并同步保护止损数量。"""
    side = position_risk.get('side')
    current_amount = abs(price_to_float(position_risk.get('position_amt')))
    info = position_risk.get('info') if isinstance(position_risk.get('info'), dict) else {}
    raw_position_side = str(info.get('positionSide', 'BOTH') or 'BOTH').upper()
    # 单边模式（BOTH）：账户只有多头 OR 空头，reduceOnly: True 会正确减少现有方向仓位
    # 对冲模式（LONG/SHORT）：账户可同时有双向仓位，Binance API 对 reduceOnly 的行为不同步——可能"只允许平仓不允许部分减仓"，或者需要额外参数指定减少哪个方向的仓位
    if raw_position_side in ('LONG', 'SHORT'):
        logging.warning("AI减仓暂不支持Binance对冲持仓模式，本轮跳过: positionSide=%s", raw_position_side)
        write_ai_audit(
            'position_reduction_skipped',
            guard,
            {
                **decision,
                'position_side': side,
                'current_amount': current_amount,
                'actual_action': 'hold',
                'reason': 'hedge position mode is unsupported'
            },
            dedupe_key=f"reduce-hedge:{guard.get('generated_at', '')}:{side}:{raw_position_side}"
        )
        return position_risk

    requested_amount = min(current_amount, price_to_float(decision.get('reduce_amount')))
    try:
        # 精度处理
        reduce_amount = float(exchange.amount_to_precision(SYMBOL, requested_amount))
    except Exception as e:
        logging.warning(f"AI减仓数量精度处理失败，本轮跳过: {e}")
        return position_risk
    # 检查减仓数量是否低于交易所最小值
    min_amount = exchange_min_amount()
    if reduce_amount <= POSITION_AMT_EPSILON or (min_amount > 0 and reduce_amount < min_amount):
        logging.info(
            "AI减仓数量低于交易所最小值，跳过: requested=%s, precision=%s, min=%s",
            requested_amount,
            reduce_amount,
            min_amount
        )
        return position_risk
    if reduce_amount >= current_amount - POSITION_AMT_EPSILON:
        logging.error("AI减仓计算可能导致全平，已拒绝: current=%s reduce=%s", current_amount, reduce_amount)
        return position_risk

    order_side = 'sell' if side == 'long' else 'buy'
    try:
        order = exchange.create_order(
            SYMBOL,
            'market',
            order_side,
            reduce_amount,
            None,
            {'reduceOnly': True}
        )
    except Exception as e:
        logging.error(f"AI reduceOnly部分减仓失败: {e}")
        write_ai_audit(
            'position_reduction_failed',
            guard,
            {
                **decision,
                'position_side': side,
                'current_amount': current_amount,
                'requested_reduce_amount': reduce_amount,
                'actual_action': 'reduce_failed',
                'reason': str(e)
            }
        )
        return position_risk

    
    order_id = extract_order_id(order)
    # 获取减仓数量
    reported_fill = extract_order_filled_amount(order)
    fallback_reduced_amount = reported_fill if reported_fill > POSITION_AMT_EPSILON else reduce_amount
    # 计算减仓后剩余仓位
    expected_remaining = max(0.0, current_amount - fallback_reduced_amount)
    confirmed_position = None
    position_confirmed = False
    # 等待减仓确认
    for confirm_delay in (0.0, 0.5, 1):
        if confirm_delay > 0:
            time.sleep(confirm_delay)
        candidate_position = get_position_risk(side=side)
        if not candidate_position or candidate_position.get('fetch_failed'):
            continue
        # 检查仓位是否减少
        candidate_amount = abs(price_to_float(candidate_position.get('position_amt')))
        if POSITION_AMT_EPSILON < candidate_amount < current_amount - POSITION_AMT_EPSILON:
            confirmed_position = candidate_position
            position_confirmed = True
            break
    # 获取减仓后剩余仓位
    remaining_amount = (
        abs(price_to_float(confirmed_position.get('position_amt')))
        if position_confirmed
        else expected_remaining
    )
    actual_reduced_amount = max(0.0, current_amount - remaining_amount)
    if actual_reduced_amount <= POSITION_AMT_EPSILON:
        actual_reduced_amount = fallback_reduced_amount
        remaining_amount = expected_remaining

    # 计算减仓价格和手续费
    reduce_price = price_to_float(order.get('average')) if isinstance(order, dict) else 0.0
    if reduce_price <= 0:
        reduce_price = price_to_float(position_risk.get('mark_price'))
    if reduce_price <= 0:
        reduce_price = price_to_float(trade_state.get('entry_price'))
    fee_rate = get_trading_fee_rate()
    # 计算减仓手续费
    reduce_fee = reduce_price * actual_reduced_amount * fee_rate
    entry_price = price_to_float(trade_state.get('entry_price'))
    # 计算减仓已实现盈亏
    if side == 'long':
        realized_pnl = (reduce_price - entry_price) * actual_reduced_amount
    else:
        realized_pnl = (entry_price - reduce_price) * actual_reduced_amount

    # 更新仓位信息
    trade_state['amount'] = remaining_amount
    # 累加减仓手续费
    trade_state['open_fee'] = price_to_float(trade_state.get('open_fee')) + reduce_fee
    # 更新AI保护止损快照
    trade_state['ai_snapshot_json'] = json.dumps(ai_guard_snapshot(guard), ensure_ascii=False, default=str)
    trade_state['ai_last_reduce_generated_at'] = guard.get('generated_at', '')
    # 更新AI减仓目标比例
    trade_state['ai_last_reduce_target_ratio'] = price_to_float(decision.get('target_ratio'))
    # 更新AI减仓次数
    trade_state['ai_partial_reduce_count'] = int(trade_state.get('ai_partial_reduce_count', 0) or 0) + 1
    # 累加减仓数量
    trade_state['ai_partial_reduce_amount'] = price_to_float(trade_state.get('ai_partial_reduce_amount')) + actual_reduced_amount
    # 累加减仓已实现盈亏
    trade_state['ai_partial_reduce_realized_pnl'] = (
        price_to_float(trade_state.get('ai_partial_reduce_realized_pnl')) + realized_pnl
    )
    # 累加减仓手续费
    trade_state['ai_partial_reduce_fee'] = price_to_float(trade_state.get('ai_partial_reduce_fee')) + reduce_fee
    reasons = list(trade_state.get('ai_reduce_reasons') or [])
    reasons.append({
        'generated_at': guard.get('generated_at', ''),
        'trigger': decision.get('trigger', ''),
        'target_ratio': decision.get('target_ratio'),
        'amount': actual_reduced_amount,
        'order_id': order_id
    })
    trade_state['ai_reduce_reasons'] = reasons[-20:]

    stop_price = price_to_float(trade_state.get('stop_loss_price'))
    stop_refreshed = False
    if stop_price > 0:
        # 刷新保护止损订单
        stop_refreshed = refresh_protective_stop_order(stop_price)
        if not stop_refreshed:
            handle_stop_order_refresh_failure(
                'AI减仓后保护止损数量刷新失败',
                reduce_price,
                signal_bar_15m=signal_bar_15m,
                trigger_label='AI减仓止损刷新失败'
            )
    else:
        logging.error("AI减仓后本地没有有效保护止损价，需要人工检查")

    execution = {
        **decision,
        'position_side': side,
        'position_confirmed': position_confirmed,
        'previous_amount': current_amount,
        'requested_reduce_amount': reduce_amount,
        'actual_reduce_amount': actual_reduced_amount,
        'remaining_amount': remaining_amount,
        'reduce_price': reduce_price,
        'reduce_fee': reduce_fee,
        'realized_pnl': realized_pnl,
        'reduce_order_id': order_id,
        'stop_refreshed': stop_refreshed,
        'actual_action': 'reduced'
    }
    write_ai_audit(
        'position_reduction_executed',
        guard,
        execution,
        dedupe_key=f"reduce-executed:{guard.get('generated_at', '')}:{side}:{order_id}"
    )
    send_msg(
        f"ETH交易: AI部分减仓 {side}",
        f"触发: {decision.get('trigger')}\n原仓位: {current_amount}\n减仓: {actual_reduced_amount}\n"
        f"剩余仓位: {remaining_amount}\n目标比例: {decision.get('target_ratio')}\n"
        f"成交估价: {reduce_price}\n手续费: {reduce_fee:.4f}\n已实现盈亏: {realized_pnl:.4f}\n"
        f"减仓订单ID: {order_id}\n保护止损数量刷新: {stop_refreshed}"
    )
    logging.warning(
        "AI部分减仓完成: side=%s previous=%s reduced=%s remaining=%s order_id=%s stop_refreshed=%s",
        side,
        current_amount,
        actual_reduced_amount,
        remaining_amount,
        order_id,
        stop_refreshed
    )
    if position_confirmed:
        return confirmed_position
    return {**position_risk, 'position_amt': remaining_amount if side == 'long' else -remaining_amount}


def apply_ai_position_guard(position_risk, signal_bar_15m=''):
    """管理脚本ETH仓位：自动刷新ETH研究，再按安全开关评估受限减仓。"""
    side = position_risk.get('side')
    current_amount = abs(price_to_float(position_risk.get('position_amt')))
    initial_amount = price_to_float(trade_state.get('entry_initial_amount'))
    if initial_amount <= 0:
        initial_amount = max(current_amount, price_to_float(trade_state.get('amount')))
        trade_state['entry_initial_amount'] = initial_amount
        logging.warning("AI仓位基准缺失，使用当前交易所仓位建立安全baseline: %s", initial_amount)
    trade_state['amount'] = current_amount

    # 和手动 BTC/SOL 一样按实际 symbol 调度；不同交易对各写自己的 JSON，并可并行研究。
    guard = ensure_manual_ai_research_job(SYMBOL, state=trade_state)
    decision = evaluate_position_reduction(
        side,
        initial_amount,
        current_amount,
        guard,
        AI_RESEARCH_CONFIG,
        last_reduce_generated_at=trade_state.get('ai_last_reduce_generated_at', '')
    )
    audit = {
        'position_side': side,
        'entry_initial_amount': initial_amount,
        'current_amount': current_amount,
        **decision
    }
    write_ai_audit(
        'position_evaluation',
        guard,
        audit,
        dedupe_key=(
            f"position:{guard.get('generated_at', 'invalid')}:{side}:{current_amount:.12f}:"
            f"{decision.get('shadow_action')}:{decision.get('target_ratio')}"
        )
    )
    # 不需要减仓
    if not decision.get('should_reduce'):
        return position_risk
    return ai_reduce_existing_position(position_risk, decision, guard, signal_bar_15m=signal_bar_15m)


def monitor_position_new(context, signal_bar_15m='', allow_strategy_close=False, perf=None):
    """新版持仓管理：只保留仓位同步、保护止损和新策略出场规则。"""
    try:
        side = trade_state.get('side')
        if side not in ('long', 'short'):
            cleanup_orphan_conditional_orders_if_needed(silent=True, perf=perf)
            return

        position_risk_start = time.monotonic()
        #判断是否有仓位
        position_risk = get_position_risk(side=side)
        record_perf(perf, 'monitor_position_get_position_risk', position_risk_start)
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

            fallback_start = time.monotonic()
            fallback_check = has_open_position_on_exchange(side=side)
            record_perf(perf, 'monitor_position_fallback_position_check', fallback_start)
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
            external_cleanup_start = time.monotonic()
            cancel_all_close_position_conditional_orders(silent=True, reason='确认无仓位后清理残留条件委托', perf=perf)
            record_perf(perf, 'monitor_position_external_close_cleanup', external_cleanup_start)
            reset_trade_state_after_external_close(
                signal_bar_15m=signal_bar_15m,
                reason="检测到交易所实际已无仓位，可能是服务端止损或人工操作已成交，本地状态已重置",
                external_context=external_close_context
            )
            return

        trade_state['position_miss_count'] = 0
        trade_state['liquidation_price'] = position_risk.get('liquidation_price') or 0.0
        position_risk = apply_ai_position_guard(position_risk, signal_bar_15m=signal_bar_15m)
        reconcile_start = time.monotonic()
        # 有仓位时保留一张出场方向的止损单（做多→SELL，做空→BUY），清理重复和反方向的条件委托
        reconcile_conditional_orders_for_position(side, silent=True)
        record_perf(perf, 'monitor_position_reconcile_conditional_orders', reconcile_start)
        trade_state['close_cond_4h'] = context['trend_states'].get('4h', {}).get('summary', '')
        trade_state['close_cond_1h'] = context['trend_states'].get('1h', {}).get('summary', '')
        trade_state['close_cond_15m'] = context['trend_states'].get('15m', {}).get('summary', '')

        price_start = time.monotonic()
        curr_price = get_latest_price()
        price_ts = time.monotonic()
        record_perf(perf, 'monitor_position_get_latest_price', price_start)
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

        apply_new_exit_rules(context, signal_bar_15m=signal_bar_15m, curr_price=curr_price, price_ts=price_ts, perf=perf)
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
    return _run_strategy_impl(None)


def _run_strategy_impl(perf):
    """run_strategy主体；返回退出原因，便于统一控制流程。"""
    global trade_state
    gate_result = {}

    # 每轮最先扫描全账户仓位。手动仓位接管不受ETH冷却影响，也不依赖交易对白名单。
    account_snapshot = fetch_all_account_positions()
    if account_snapshot.get('fetch_failed'):
        logging.warning(f"无法读取全账户仓位，本轮禁止新开ETH: {account_snapshot.get('error')}")
        # 已有本地ETH持仓时仍继续走原持仓管理；空仓时宁可跳过，也不能在未知仓位下开新单。
        if not trade_state.get('has_position'):
            return 'account_positions_fetch_failed'
        account_positions = []
    else:
        account_positions = account_snapshot.get('positions', [])

    if has_pending_entry():
        # pending 可能刚成交，先让原流程把对应ETH仓位初始化为策略仓位。
        pending_sync_result = sync_pending_entry_fill_if_needed()
        if pending_sync_result:
            # pending 同步可能主动撤退或完成持仓初始化，重新读取仓位避免使用旧快照。
            refreshed_snapshot = fetch_all_account_positions()
            if not refreshed_snapshot.get('fetch_failed'):
                account_positions = refreshed_snapshot.get('positions', [])
        if has_pending_entry() and account_positions:
            cancel_pending_entry_order(
                '账户已存在持仓，暂停ETH自动开仓并撤销脚本pending入场单',
                silent=False,
            )

    manual_positions_active = manage_all_manual_positions(account_positions)
    any_account_position = bool(account_positions)

    # 只有手动仓位时，完成动态止损后直接结束本轮，不抓ETH策略K线、不寻找ETH开仓信号。
    if not trade_state.get('has_position') and (any_account_position or manual_positions_active):
        return 'manual_positions_managed_entry_paused'

    #没有仓位并且有未完成的开仓委托， has_pending_entry: 是否有未完成的开仓委托
    if not trade_state.get('has_position') and has_pending_entry():
        #检查peding挂单是否开仓，判断peding开仓位置是否合理，利润预期是否满足，不合理不够就就立马平仓，并且清空挂单,并且重置本地止损状态，重置本地pending状态;
        #如果开仓合理，就挂止损单，也要重置本地pending状态，持仓信息同步到本地
        early_pending_result = sync_pending_entry_fill_if_needed()
        if early_pending_result:
            return early_pending_result

    #没有仓位并且没有pending挂单
    if not trade_state.get('has_position') and not has_pending_entry():
        cleanup_start = time.monotonic()
        #清理无用的挂单
        cleanup_ok = cleanup_orphan_conditional_orders_if_needed(silent=True, perf=perf)
        record_perf(perf, 'empty_position_cleanup', cleanup_start)
        if not cleanup_ok:
            logging.warning("无本地仓位时条件委托/交易所仓位状态未清理干净，本轮跳过开仓")
            return 'empty_position_cleanup_failed'
        #获取5m的5根k线(包含未收盘的k线)，并判断是否有效
        gate_result = check_empty_position_lightweight_5m_gate(perf)
        if gate_result.get('action') == 'skip':
            return gate_result.get('reason', 'lightweight_gate_skip')

    fetch_all_start = time.monotonic()
    reused_5m_df = None
    #轻量级5分钟K线数据有效
    if gate_result.get('reason') == 'lightweight_new_5m':
        # 轻量级5分钟K线和缓存中5分钟K线数据合并，并更新缓存数据
        reused_5m_df = rebuild_5m_df_from_lightweight_cache(SYMBOL, 220, gate_result.get('df_5m'))

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
    try:
        futures = {
            '15m': executor.submit(fetch_df_cached, SYMBOL, '15m', 220),
            '1h': executor.submit(fetch_df_cached, SYMBOL, '1h', 220),
            '4h': executor.submit(fetch_df_cached, SYMBOL, '4h', SUP_RES_LOOKBACK_4H),
            '1d': executor.submit(fetch_df_cached, SYMBOL, '1d', 140),
        }
        if reused_5m_df is None:
            futures['5m'] = executor.submit(fetch_df, SYMBOL, '5m', 220)
        dfs = {
            timeframe: future.result(timeout=FETCH_DF_TASK_TIMEOUT_SECONDS)
            for timeframe, future in futures.items()
        }
        if reused_5m_df is not None:
            dfs['5m'] = reused_5m_df
        record_perf(perf, 'fetch_all_klines_total', fetch_all_start)
    except concurrent.futures.TimeoutError:
        record_perf(perf, 'fetch_all_klines_total', fetch_all_start)
        logging.error(f"抓取K线任务超时，已跳过本轮策略计算: timeout={FETCH_DF_TASK_TIMEOUT_SECONDS}s")
        executor.shutdown(wait=False, cancel_futures=True)
        return 'fetch_all_klines_timeout'
    except Exception:
        record_perf(perf, 'fetch_all_klines_total', fetch_all_start)
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)

    if any(df is None for df in dfs.values()):
        # 多周期数据必须齐全；缺一个周期就放弃本轮，避免用残缺背景做交易决策。
        return 'fetch_all_klines_incomplete'

    server_now_dt = get_server_now_dt()
    #这里也是更新5分钟K线缓存，
    store_kline_df_cache(SYMBOL, '5m', 220, dfs['5m'], now_dt=server_now_dt)
    validate_start = time.monotonic()
    #获取最新收盘k线的开盘时间
    signal_bar_5m = get_closed_bar_time(dfs['5m'], '5m', now_dt=server_now_dt)
    signal_bar_15m = get_closed_bar_time(dfs['15m'], '15m', now_dt=server_now_dt)
    #验证最新收盘5分钟K线是否有效
    signal_guard_5m = validate_signal_bar('5m', signal_bar_5m, now_dt=server_now_dt)
    signal_guard_15m = validate_signal_bar('15m', signal_bar_15m, now_dt=server_now_dt)
    record_perf(perf, 'validate_signal_bar', validate_start)

    if not signal_guard_5m['valid']:
        logging.warning(f"跳过异常5M信号K线: signal={signal_bar_5m}, reason={signal_guard_5m.get('reason')}")
        return f"invalid_5m_signal_{signal_guard_5m.get('reason')}"

    is_new_signal_5m = bool(signal_guard_5m['is_new'])
    is_new_signal_15m = bool(signal_guard_15m.get('valid') and signal_guard_15m.get('is_new'))

    context_start = time.monotonic()
    #判断各个周期的趋势条件和4h的支撑和阻挡区间
    context = build_context(dfs, server_now_dt, perf=perf)
    record_perf(perf, 'build_context', context_start)

    if trade_state['has_position']:
        monitor_start = time.monotonic()
        monitor_position_new(
            context,
            signal_bar_15m=signal_bar_15m if signal_guard_15m.get('valid') else '',
            allow_strategy_close=is_new_signal_5m,
            perf=perf
        )
        record_perf(perf, 'monitor_position_new', monitor_start)
        if is_new_signal_5m:
            trade_state['last_processed_bar_5m'] = signal_bar_5m
        if is_new_signal_15m:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        # 持仓期间只做仓位管理，不在同一轮继续寻找新开仓。
        return 'position_managed'

    #这里判断是用来管理pending挂单的，前面的判断pending挂单状态的
    if has_pending_entry():
        pending_start = time.monotonic()
        #管理pending挂单:用最新趋势判断是否要撤单
        pending_result = manage_pending_entry(
            context,
            signal_bar_15m=signal_bar_15m if signal_guard_15m.get('valid') else '',
            perf=perf
        )
        record_perf(perf, 'manage_pending_entry', pending_start)
        if is_new_signal_5m:
            trade_state['last_processed_bar_5m'] = signal_bar_5m
        if is_new_signal_15m:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        # pending 入场期间不扫描新信号，避免同一交易对叠加多个入场单。
        return pending_result
    
    if not is_new_signal_5m:
        # 同一根 5M K 线只处理一次，避免轮询频率高导致重复开仓。
        return 'no_new_5m_after_full_fetch'

    # 判断是否在平仓冷却期
    if is_in_post_exit_cooldown(server_now_dt):
        logging.info("平仓后冷却中：至少等待4根5M K线后再开新仓")
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        if is_new_signal_15m:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        return 'post_exit_cooldown'

    # 趋势开仓候选信号
    # 回调候选信号
    # 突破关键位置候选信号
    candidates_start = time.monotonic()
    candidates = build_entry_candidates(context)
    record_perf(perf, 'build_entry_candidates', candidates_start)
    if not candidates:
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        if is_new_signal_15m:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        return 'no_candidates'

    #这里选择优先级最高的候选信号，如果优先级相同并且有多个多空冲突则跳过
    best_priority = candidate_priority(candidates[0])
    top_candidates = [candidate for candidate in candidates if candidate_priority(candidate) == best_priority]
    top_sides = {candidate['side'] for candidate in top_candidates}
    if len(top_sides) > 1:
        logging.info(f"同权重候选多空冲突，跳过本轮: {top_candidates}")
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        if is_new_signal_15m:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        return 'top_candidate_side_conflict'

    candidate = top_candidates[0]
    if candidate['strategy_tf'] == '15m' and signal_bar_15m in (trade_state.get('last_entry_bar_15m'), trade_state.get('last_exit_bar_15m')):
        logging.info(f"同一15M K线内已交易/刚平仓，跳过15M策略开仓: {signal_bar_15m}")
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        if is_new_signal_15m:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        return 'same_15m_bar_traded'
    price_start = time.monotonic()
    curr_price = get_latest_price()
    record_perf(perf, 'get_latest_price', price_start)
    #判断候选信号的入场价格是否仍然有效
    entry_still_valid, entry_invalid_reason = candidate_entry_price_still_valid(candidate, curr_price)
    if not entry_still_valid:
        logging.info(entry_invalid_reason)
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        if is_new_signal_15m:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        return 'entry_price_invalid'
    #判断候选信号的入场价格是否仍然在支撑阻力区域内
    sr_zone_still_valid, sr_zone_invalid_reason = candidate_sr_entry_zone_still_valid(candidate, context, curr_price)
    if not sr_zone_still_valid:
        logging.info(sr_zone_invalid_reason)
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        if is_new_signal_15m:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        return 'sr_zone_invalid'
    #判断候选信号的入场价格是否仍然在目标利润区域内
    target_still_valid, target_invalid_reason = candidate_target_profit_still_valid(candidate, curr_price)
    if not target_still_valid:
        logging.info(target_invalid_reason)
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        if is_new_signal_15m:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        return 'target_profit_invalid'

    ai_guard = get_current_ai_guard()
    ai_entry_decision = evaluate_entry_candidate(candidate['side'], ai_guard, AI_RESEARCH_CONFIG)
    write_ai_audit(
        'candidate_evaluation',
        ai_guard,
        {
            'candidate_side': candidate.get('side'),
            'candidate_module': candidate.get('module', ''),
            'candidate_strategy_tf': candidate.get('strategy_tf', ''),
            'candidate_trigger_tf': candidate.get('trigger_tf', ''),
            'signal_bar_15m': signal_bar_15m,
            **ai_entry_decision
        },
        dedupe_key=(
            f"candidate:{signal_bar_15m}:{candidate.get('side')}:{candidate.get('module', '')}:"
            f"{ai_guard.get('generated_at', 'invalid')}:{ai_entry_decision.get('shadow_action')}"
        )
    )
    if not ai_entry_decision['allowed']:
        logging.info(
            "AI明确反向信号过滤开仓候选: side=%s, module=%s, generated_at=%s",
            candidate.get('side'),
            candidate.get('module', ''),
            ai_guard.get('generated_at', '')
        )
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        if is_new_signal_15m:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        return 'ai_candidate_filtered'

    #获取候选信号的4h、1h、15m状态
    state_4h, state_1h, state_15m = states_for_candidate(context, candidate)
    entry_meta = {
        'strategy_tf': candidate.get('strategy_tf'),
        'exit_tf': candidate.get('exit_tf'),
        'sr_target': candidate.get('target', 0.0),
        'profit_check_atr': candidate.get('profit_check_atr', 0.0),
        'ai_snapshot_json': json.dumps(ai_guard_snapshot(ai_guard), ensure_ascii=False, default=str)
    }
    try:
        open_order_start = time.monotonic()
        opened = place_pending_entry_order(
            candidate['side'],
            candidate,
            curr_price,
            state_4h,
            state_1h,
            state_15m,
            signal_bar_15m if signal_guard_15m.get('valid') else '',
            entry_reason=candidate.get('reason', candidate.get('module', 'new_strategy')),
            entry_trigger_tf=f"{candidate.get('strategy_tf')}->{candidate.get('trigger_tf')}",
            entry_meta=entry_meta
        )
        record_perf(perf, 'place_pending_entry_order', open_order_start)
        if opened:
            logging.info(
                f"新版策略已挂 STOP_LIMIT pending 入场: side={candidate['side']}, module={candidate['module']}, "
                f"strategy_tf={candidate['strategy_tf']}, trigger_tf={candidate['trigger_tf']}, "
                f"target={candidate.get('target')}"
            )
        else:
            logging.warning(f"新版策略 STOP_LIMIT pending 入场未成功: {candidate}")
    except Exception:
        record_perf(perf, 'place_pending_entry_order', open_order_start)
        logging.error(f"新版策略 STOP_LIMIT pending 入场失败:\n{traceback.format_exc()}")

    trade_state['last_processed_bar_5m'] = signal_bar_5m
    if is_new_signal_15m:
        trade_state['last_processed_bar_15m'] = signal_bar_15m
    return 'pending_entry_order_attempted'


def cleanup_conditional_orders_once():
    """手动清理入口：无仓位清全部平仓条件单；有仓位只保留当前方向一张止损单。"""
    position_risk = get_position_risk()
    if position_risk and position_risk.get('fetch_failed'):
        logging.error(f"无法确认交易所仓位，已放弃清理条件委托: {position_risk.get('error')}")
        return False
    if position_risk is None:
        ok = cancel_all_close_position_conditional_orders(silent=False, reason='手动无仓位清理残留条件委托')
        logging.info(f"手动无仓位条件委托清理完成: ok={ok}")
        return ok

    side = position_risk.get('side')
    trade_state['side'] = side
    ok = reconcile_conditional_orders_for_position(side, silent=False)
    logging.info(
        f"手动持仓条件委托归并完成: ok={ok}, side={side}, amount={position_risk.get('position_amt')}, "
        f"active_stop_order_id={trade_state.get('stop_order_id')}, active_stop={trade_state.get('stop_order_price')}"
    )
    return ok


def cli_arg_value(name):
    """读取形如 --observe-seconds 180 的简单命令行参数。"""
    if name not in sys.argv:
        return None
    idx = sys.argv.index(name)
    if idx + 1 >= len(sys.argv):
        return None
    return sys.argv[idx + 1]


# ==========================================
# 4. 程序入口
# ==========================================
if __name__ == '__main__':
    if '--dry-run-open-order' in sys.argv:
        DRY_RUN_OPEN_ORDER = True
        logging.warning("DRY_RUN_OPEN_ORDER 已启用：本进程会拦截所有开仓，不会创建开仓订单")

    if '--cleanup-conditions' in sys.argv:
        try:
            current_time_str = get_server_time_str()
            logging.info(f"网络连通成功！服务器时间: {current_time_str}")
            ok = cleanup_conditional_orders_once()
            sys.exit(0 if ok else 1)
        except Exception:
            logging.error(f"手动清理条件委托失败:\n{traceback.format_exc()}")
            sys.exit(1)

    try:
        current_time_str = get_server_time_str()
        logging.info(f"网络连通成功！服务器时间: {current_time_str}")
        balance_after = exchange.fetch_balance({'type': 'future'})
        final_usdt = float(balance_after['total']['USDT'])
        logging.info(f"🚀 自动化交易策略系统启动，初始金额：{final_usdt}")
    except Exception as e:
        logging.error(f"启动自检失败，但主循环会继续尝试运行: {e}")
    #exchange.load_markets()
    observe_seconds = None
    observe_seconds_raw = cli_arg_value('--observe-seconds')
    if observe_seconds_raw:
        try:
            observe_seconds = max(1, int(observe_seconds_raw))
            logging.info(f"观察模式已启用：最多运行 {observe_seconds}s 后退出")
        except ValueError:
            logging.error(f"--observe-seconds 参数必须是整数: {observe_seconds_raw}")
            sys.exit(2)
    observe_end_ts = time.time() + observe_seconds if observe_seconds is not None else None
    loop_count = 0
    while True:
        try:
            maybe_log_heartbeat()
            run_strategy()
            loop_count += 1
        except Exception as e:
            logging.error(traceback.format_exc())
            logging.error(f"系统运行报错: {e}")
            logging.error("主循环将继续运行，休眠后自动进入下一轮")
            time.sleep(MAIN_LOOP_ERROR_SLEEP_SECONDS)
            continue
        if observe_end_ts is not None and time.time() >= observe_end_ts:
            logging.info(f"观察模式结束：loops={loop_count}")
            break
        time.sleep(MAIN_LOOP_SLEEP_SECONDS)
