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
import os  # 导入os模块，用于处理系统级操作和环境变量
# pandas_ta 会触发 numba 缓存；部分服务器/打包环境没有可用locator，会导致启动即失败。
# 注释: 调用 os.environ.setdefault 执行对应处理。
os.environ.setdefault('NUMBA_DISABLE_JIT', '1')

import ccxt  # 导入ccxt库，用于连接加密货币交易所API
import pandas as pd  # 导入pandas库，用于数据处理和分析
import pandas_ta as ta  # 导入pandas_ta库，用于计算技术指标
import time  # 导入time库，用于控制循环执行的时间间隔
import logging  # 导入logging库，用于记录程序运行日志
import smtplib  # 导入smtplib库，用于发送邮件通知
from email.mime.text import MIMEText  # 导入MIMEText，用于构建纯文本格式的邮件内容
from email.header import Header  # 导入Header，用于设置邮件头信息（如主题等）
import sys  # 导入sys模块，用于处理系统级操作和路径（目前未深入使用）
import concurrent.futures  # 导入concurrent.futures模块，用于并行执行多个任务
import csv  # 导入csv库，用于将交易记录写入文件
import datetime  # 导入datetime库，用于获取和格式化时间
import traceback  # 导入traceback模块，用于在异常时打印详细的调用栈信息
# 注释: 导入 sqlite3，供后续逻辑使用。
import sqlite3

BAR_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'  # 定义统一的K线时间格式字符串，用于日志记录和CSV输出
TIMEFRAME_SECONDS = {  # 定义各时间周期对应的秒数，用于计算K线是否已收盘
    '5m': 5 * 60,      # 5分钟 = 300秒
    '15m': 15 * 60,    # 15分钟 = 900秒
    '1h': 60 * 60,     # 1小时 = 3600秒
    '4h': 4 * 60 * 60, # 4小时 = 14400秒
    '1d': 24 * 60 * 60 # 1天 = 86400秒
# 注释: 结束当前多行结构。
}
EXCHANGE_TZ = datetime.timezone(datetime.timedelta(hours=8))  # 定义交易所时区为东八区（北京时间），用于时间戳转换
# 当前脚本所在目录，用来定位同目录下的 .env.local 和统计数据库文件。
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 本地环境变量文件路径，API密钥和邮件密码优先从这里读取，避免写死在代码里。
LOCAL_ENV_PATH = os.path.join(BASE_DIR, '.env.local')
# 交易统计 SQLite 数据库路径，用于持久化复盘/统计数据。
STATS_DB_PATH = os.path.join(BASE_DIR, 'trade_stats.db')


# 注释: 定义 load_local_env 函数，封装对应业务逻辑。
def load_local_env(env_path=LOCAL_ENV_PATH):
    """从本地 .env.local 读取环境变量，避免把密钥写进代码仓库。"""
    # 注释: 判断条件是否成立：not os.path.exists(env_path)。
    if not os.path.exists(env_path):
        # 注释: 直接结束当前函数。
        return

    # 注释: 使用上下文管理器处理资源：open(env_path, 'r', encoding='utf-8') as env_file。
    with open(env_path, 'r', encoding='utf-8') as env_file:
        # 注释: 遍历 env_file，逐项处理 raw_line。
        for raw_line in env_file:
            # 注释: 调用 raw_line.strip 并保存到 line。
            line = raw_line.strip()
            # 注释: 判断条件是否成立：not line or line.startswith('#') or '=' not in line。
            if not line or line.startswith('#') or '=' not in line:
                # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                continue

            # 注释: 调用 line.split 并保存到 key, value。
            key, value = line.split('=', 1)
            # 注释: 调用 key.strip 并保存到 key。
            key = key.strip()
            # 注释: 调用 value.strip 并保存到 value。
            value = value.strip().strip('"').strip("'")
            # 注释: 调用 os.environ.setdefault 执行对应处理。
            os.environ.setdefault(key, value)


# 注释: 定义 require_env 函数，封装对应业务逻辑。
def require_env(name):
    """读取必须存在的环境变量。"""
    # 注释: 调用 os.getenv 并保存到 value。
    value = os.getenv(name, '').strip()
    # 注释: 判断条件是否成立：not value。
    if not value:
        # 注释: 抛出异常并中断当前流程：RuntimeError(f'缺少环境变量: {name}')。
        raise RuntimeError(f'缺少环境变量: {name}')
    # 注释: 返回计算结果：value。
    return value


# 注释: 调用 load_local_env 执行对应处理。
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
EMA_CROSS_LOOKBACK = 8
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
trade_state = {  # 定义一个字典用于保存当前交易的状态信息
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
# 注释: 结束当前多行结构。
}

# 初始化币安合约 API
exchange = ccxt.binance({  # 实例化ccxt的binance对象，用于调用币安API
    'apiKey': API_KEY,  # 传入API公钥
    'secret': SECRET_KEY,  # 传入API私钥
    'options': {'defaultType': 'future'},  # 设置默认交易类型为U本位合约 (future)
    'enableRateLimit': True,  # 开启内置的速率限制功能，防止请求频率过高被封IP
    'timeout': EXCHANGE_HTTP_TIMEOUT_MS,  # 给交易所HTTP请求设置硬超时，避免网络卡死时无限等待
# 注释: 结束当前多行结构。
})
exchange.enable_demo_trading(True)  # 开启模拟交易模式（测试网），不会产生真实交易

# 日志配置
logging.basicConfig(  # 配置日志记录的全局基本设置
    level=logging.INFO,  # 设置日志输出级别为INFO，过滤掉DEBUG级别的日志
    format='%(asctime)s - %(levelname)s - %(message)s'  # 设置日志输出格式：时间 - 级别 - 具体信息
# 注释: 结束当前多行结构。
)

# 记录程序层面的临时运行状态，不属于某一笔交易。
runtime_state = {
    # 上一次打印心跳日志的单调时间戳，用来控制心跳输出频率。
    'last_heartbeat_ts': 0.0
# 注释: 结束当前多行结构。
}


# ==========================================
# 2. 功能模块
# ==========================================

def send_msg(subject, content):  # 定义发送邮件通知的函数，接收邮件主题和内容两个参数
    """发送邮件通知"""  # 函数的文档字符串
    # 注释: 判断条件是否成立：not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER。
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        # 注释: 写入警告日志，提示需要关注的非致命问题。
        logging.warning("未配置邮件通知环境变量，已跳过邮件发送。")
        # 注释: 直接结束当前函数。
        return

    try:  # 尝试执行发送邮件的代码块，捕获可能出现的异常
        message = MIMEText(content, 'plain', 'utf-8')  # 使用传入的内容创建纯文本邮件对象，编码为utf-8
        message['From'] = EMAIL_SENDER  # 在邮件头中设置发件人信息
        message['To'] = EMAIL_RECEIVER  # 在邮件头中设置收件人信息
        message['Subject'] = Header(subject, 'utf-8')  # 使用Header处理邮件主题，并指定编码为utf-8，防止乱码
        smtp_obj = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT)  # 连接到指定的SMTP_SSL服务器和端口
        smtp_obj.login(EMAIL_SENDER, EMAIL_PASSWORD)  # 使用发件人邮箱和授权码进行登录
        smtp_obj.sendmail(EMAIL_SENDER, [EMAIL_RECEIVER], message.as_string())  # 发送邮件（发件人，收件人列表，邮件内容的字符串形式）
        smtp_obj.quit()  # 发送完毕后退出并关闭SMTP连接
    except Exception as e:  # 捕获所有继承自Exception的错误
        logging.error(f"邮件发送失败: {e}")  # 将发送邮件失败的错误信息写入日志


# 当前版本交易记录 CSV 的表头，新增字段时要同步兼容旧文件升级逻辑。
TRADE_CSV_HEADERS = [
    # 注释: 拼接或传入文本内容。
    '建仓时间', '趋势方向', '4H条件', '1H条件', '15M条件', '入场原因',
    # 注释: 拼接或传入文本内容。
    '平仓时间', '平仓原因', '点数盈亏', '手续费', '净利润(USDT)', '是否盈利',
    # 注释: 拼接或传入文本内容。
    '入场15M信号时间', '平仓15M信号时间', '平仓触发周期', '持仓秒数',
    # 注释: 拼接或传入文本内容。
    '开仓订单ID', '平仓订单ID'
# 注释: 结束当前多行结构。
]
# 旧版 CSV 表头缺少订单ID字段，用它识别历史文件并自动补齐新列。
LEGACY_TRADE_CSV_HEADERS = TRADE_CSV_HEADERS[:-2]


# 注释: 定义 extract_order_id 函数，封装对应业务逻辑。
def extract_order_id(order):
    """从 CCXT 订单结果中尽量稳定地提取订单ID"""
    # 注释: 判断条件是否成立：not isinstance(order, dict)。
    if not isinstance(order, dict):
        # 注释: 返回计算结果：''。
        return ''

    # 注释: 调用 order.get 并保存到 info。
    info = order.get('info', {})
    # 注释: 初始化 candidates 列表。
    candidates = [
        # 注释: 调用 order.get 执行对应处理。
        order.get('id'),
        # 注释: 调用 order.get 执行对应处理。
        order.get('orderId'),
        # 注释: 调用 order.get 执行对应处理。
        order.get('clientOrderId')
    # 注释: 结束当前多行结构。
    ]
    # 注释: 判断条件是否成立：isinstance(info, dict)。
    if isinstance(info, dict):
        # 注释: 调用 candidates.extend 执行对应处理。
        candidates.extend([
            # 注释: 调用 info.get 执行对应处理。
            info.get('orderId'),
            # 注释: 调用 info.get 执行对应处理。
            info.get('id'),
            # 注释: 调用 info.get 执行对应处理。
            info.get('clientOrderId')
        # 注释: 结束当前多行结构。
        ])

    # 注释: 遍历 candidates，逐项处理 candidate。
    for candidate in candidates:
        # 注释: 判断条件是否成立：candidate not in (None, '')。
        if candidate not in (None, ''):
            # 注释: 返回计算结果：str(candidate)。
            return str(candidate)
    # 注释: 返回计算结果：''。
    return ''


# 注释: 定义 format_exception_message 函数，封装对应业务逻辑。
def format_exception_message(error):
    """尽量把异常格式化成稳定可读的字符串。"""
    # 注释: 判断条件是否成立：error is None。
    if error is None:
        # 注释: 返回计算结果：''。
        return ''
    # 注释: 调用 str 并保存到 text。
    text = str(error).strip()
    # 注释: 判断条件是否成立：text。
    if text:
        # 注释: 返回计算结果：text。
        return text
    # 注释: 返回计算结果：repr(error)。
    return repr(error)


# 注释: 定义 clear_local_stop_order_state 函数，封装对应业务逻辑。
def clear_local_stop_order_state():
    """清空本地缓存的服务端止损单状态。"""
    # 注释: 更新 trade_state['stop_order_id'] 的值。
    trade_state['stop_order_id'] = ''
    # 注释: 更新 trade_state['stop_order_price'] 的值。
    trade_state['stop_order_price'] = 0.0


# 注释: 定义 extract_order_timestamp_ms 函数，封装对应业务逻辑。
def extract_order_timestamp_ms(order):
    """尽量提取订单时间戳，便于在多张条件单里选最新的一张。"""
    # 注释: 判断条件是否成立：not isinstance(order, dict)。
    if not isinstance(order, dict):
        # 注释: 返回计算结果：0。
        return 0

    # 注释: 调用 order.get 并保存到 info。
    info = order.get('info', {})
    # 注释: 判断条件是否成立：not isinstance(info, dict)。
    if not isinstance(info, dict):
        # 注释: 初始化 info 字典。
        info = {}

    # 注释: 初始化 candidates 列表。
    candidates = [
        # 注释: 调用 order.get 执行对应处理。
        order.get('timestamp'),
        # 注释: 调用 order.get 执行对应处理。
        order.get('lastTradeTimestamp'),
        # 注释: 调用 info.get 执行对应处理。
        info.get('updateTime'),
        # 注释: 调用 info.get 执行对应处理。
        info.get('workingTime'),
        # 注释: 调用 info.get 执行对应处理。
        info.get('time')
    # 注释: 结束当前多行结构。
    ]
    # 注释: 遍历 candidates，逐项处理 candidate。
    for candidate in candidates:
        # 注释: 开始执行可能抛出异常的逻辑。
        try:
            # 注释: 判断条件是否成立：candidate not in (None, '')。
            if candidate not in (None, ''):
                # 注释: 返回计算结果：int(candidate)。
                return int(candidate)
        # 注释: 捕获异常分支：except Exception。
        except Exception:
            # 注释: 跳过本次循环剩余逻辑，进入下一轮。
            continue
    # 注释: 返回计算结果：0。
    return 0


# 注释: 定义 extract_order_stop_price 函数，封装对应业务逻辑。
def extract_order_stop_price(order):
    """从交易所订单对象里提取条件触发价。"""
    # 注释: 判断条件是否成立：not isinstance(order, dict)。
    if not isinstance(order, dict):
        # 注释: 返回固定结果 None。
        return None

    # 注释: 调用 order.get 并保存到 info。
    info = order.get('info', {})
    # 注释: 判断条件是否成立：not isinstance(info, dict)。
    if not isinstance(info, dict):
        # 注释: 初始化 info 字典。
        info = {}

    # 注释: 初始化 candidates 列表。
    candidates = [
        # 注释: 调用 order.get 执行对应处理。
        order.get('stopPrice'),
        # 注释: 调用 order.get 执行对应处理。
        order.get('triggerPrice'),
        # 注释: 调用 info.get 执行对应处理。
        info.get('stopPrice'),
        # 注释: 调用 info.get 执行对应处理。
        info.get('triggerPrice'),
        # 注释: 调用 info.get 执行对应处理。
        info.get('activatePrice')
    # 注释: 结束当前多行结构。
    ]
    # 注释: 遍历 candidates，逐项处理 candidate。
    for candidate in candidates:
        # 注释: 开始执行可能抛出异常的逻辑。
        try:
            # 注释: 判断条件是否成立：candidate not in (None, '')。
            if candidate not in (None, ''):
                # 注释: 返回计算结果：float(candidate)。
                return float(candidate)
        # 注释: 捕获异常分支：except Exception。
        except Exception:
            # 注释: 跳过本次循环剩余逻辑，进入下一轮。
            continue
    # 注释: 返回固定结果 None。
    return None


# 注释: 定义 format_order_id_lines 函数，封装对应业务逻辑。
def format_order_id_lines(open_order_id='', close_order_id='', stop_order_id=''):
    """把可用的订单ID格式化成邮件/日志可直接复用的多行文本"""
    # 注释: 初始化 lines 列表。
    lines = []
    # 注释: 判断条件是否成立：open_order_id。
    if open_order_id:
        # 注释: 调用 lines.append 执行对应处理。
        lines.append(f"开仓订单ID: {open_order_id}")
    # 注释: 判断条件是否成立：close_order_id。
    if close_order_id:
        # 注释: 调用 lines.append 执行对应处理。
        lines.append(f"平仓订单ID: {close_order_id}")
    # 注释: 判断条件是否成立：stop_order_id。
    if stop_order_id:
        # 注释: 调用 lines.append 执行对应处理。
        lines.append(f"止损订单ID: {stop_order_id}")
    # 注释: 返回计算结果：'\n'.join(lines)。
    return '\n'.join(lines)


# 注释: 定义 ensure_trade_csv_schema 函数，封装对应业务逻辑。
def ensure_trade_csv_schema(filename):
    """兼容老版CSV表头，必要时补齐订单ID列，避免新旧列数不一致"""
    # 注释: 判断条件是否成立：not os.path.isfile(filename)。
    if not os.path.isfile(filename):
        # 注释: 返回固定结果 True。
        return True

    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 使用上下文管理器处理资源：open(filename, mode='r', newline='', encoding='utf-8-sig') as f。
        with open(filename, mode='r', newline='', encoding='utf-8-sig') as f:
            # 注释: 调用 list 并保存到 rows。
            rows = list(csv.reader(f))
    # 注释: 捕获异常分支：except Exception as e。
    except Exception as e:
        # 注释: 写入错误日志，便于排查运行异常。
        logging.error(f"读取CSV表头失败: {filename}, error={e}")
        # 注释: 返回固定结果 False。
        return False

    # 注释: 判断条件是否成立：not rows。
    if not rows:
        # 注释: 返回固定结果 True。
        return True

    # 注释: 计算并保存 header。
    header = rows[0]
    # 注释: 判断条件是否成立：header == TRADE_CSV_HEADERS。
    if header == TRADE_CSV_HEADERS:
        # 注释: 返回固定结果 True。
        return True

    # 注释: 判断条件是否成立：header != LEGACY_TRADE_CSV_HEADERS。
    if header != LEGACY_TRADE_CSV_HEADERS:
        # 注释: 写入警告日志，提示需要关注的非致命问题。
        logging.warning(f"CSV表头不是预期格式，跳过自动升级: {filename}")
        # 注释: 返回固定结果 True。
        return True

    # 注释: 初始化 upgraded_rows 列表。
    upgraded_rows = [TRADE_CSV_HEADERS]
    # 注释: 调用 len 并保存到 legacy_len。
    legacy_len = len(LEGACY_TRADE_CSV_HEADERS)
    # 注释: 遍历 rows[1:]，逐项处理 row。
    for row in rows[1:]:
        # 注释: 调用 list 并保存到 normalized_row。
        normalized_row = list(row[:legacy_len])
        # 注释: 判断条件是否成立：len(normalized_row) < legacy_len。
        if len(normalized_row) < legacy_len:
            # 注释: 调用 normalized_row.extend 执行对应处理。
            normalized_row.extend([''] * (legacy_len - len(normalized_row)))
        # 注释: 调用 normalized_row.extend 执行对应处理。
        normalized_row.extend(['', ''])
        # 注释: 调用 upgraded_rows.append 执行对应处理。
        upgraded_rows.append(normalized_row)

    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 使用上下文管理器处理资源：open(filename, mode='w', newline='', encoding='utf-8-sig') as f。
        with open(filename, mode='w', newline='', encoding='utf-8-sig') as f:
            # 注释: 调用 csv.writer 并保存到 writer。
            writer = csv.writer(f)
            # 注释: 调用 writer.writerows 执行对应处理。
            writer.writerows(upgraded_rows)
        # 注释: 写入运行日志，记录当前处理进度。
        logging.info(f"已自动升级CSV表头，补充订单ID列: {filename}")
        # 注释: 返回固定结果 True。
        return True
    # 注释: 捕获异常分支：except Exception as e。
    except Exception as e:
        # 注释: 写入错误日志，便于排查运行异常。
        logging.error(f"升级CSV表头失败: {filename}, error={e}")
        # 注释: 返回固定结果 False。
        return False


def fetch_df(symbol, timeframe, limit=100):  # 定义获取K线数据并计算指标的函数
    """获取K线并计算技术指标"""  # 函数的文档字符串
    # 注释: 调用 time.monotonic 并保存到 start_ts。
    start_ts = time.monotonic()
    try:  # 尝试执行获取和计算数据的代码块
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)  # 调用API获取指定交易对、时间周期和数量的K线数据
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])  # 将K线数据转换为pandas DataFrame，并指定列名
        # 将 timestamp (毫秒) 转换为 datetime 格式
        # 注释: 更新 df['timestamp'] 的值。
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        # 如果你想调整时区到本地时间（比如北京时间 东八区），可以加上下面这行：
        # 注释: 更新 df['timestamp'] 的值。
        df['timestamp'] = df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai')
       
        
        # 指标计算，只能通过计算来，没有提供具体指标的接口
        df['ema20'] = ta.ema(df['close'], length=20)  # 计算收盘价的20周期指数移动平均线 (EMA20)，并新增一列
        df['ema50'] = ta.ema(df['close'], length=50)  # 计算收盘价的50周期指数移动平均线 (EMA50)，并新增一列

        # ATR 14
        df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)  # 计算14周期的平均真实波幅 (ATR)，需要最高、最低、收盘价，新增一列

        # ADX / DI 14，用于新版背景趋势判断。
        # 注释: 调用 ta.adx 并保存到 adx。
        adx = ta.adx(df['high'], df['low'], df['close'], length=ADX_LENGTH)
        # 注释: 判断条件是否成立：adx is not None。
        if adx is not None:
            # 注释: 调用 pd.concat 并保存到 df。
            df = pd.concat([df, adx], axis=1)
            # 注释: 初始化 df.rename(columns 字典。
            df.rename(columns={
                # 注释: 拼接或传入文本内容。
                f'ADX_{ADX_LENGTH}': 'adx',
                # 注释: 拼接或传入文本内容。
                f'DMP_{ADX_LENGTH}': 'plus_di',
                # 注释: 拼接或传入文本内容。
                f'DMN_{ADX_LENGTH}': 'minus_di'
            # 注释: 计算并保存 }, inplace。
            }, inplace=True)
        # 注释: 调用 time.monotonic 并保存到 elapsed。
        elapsed = time.monotonic() - start_ts
        # 注释: 判断条件是否成立：elapsed >= FETCH_DF_SLOW_LOG_SECONDS。
        if elapsed >= FETCH_DF_SLOW_LOG_SECONDS:
            # 注释: 写入警告日志，提示需要关注的非致命问题。
            logging.warning(f"获取数据较慢 ({timeframe}): {elapsed:.2f}s")
        return df  # 返回计算好所有指标的DataFrame
    except Exception as e:  # 捕获获取数据或计算指标过程中的异常
        logging.error(f"获取数据失败 ({timeframe}): {e}")  # 记录错误日志，包含时间周期和具体错误信息
        return None  # 如果发生错误，返回None


# 注释: 定义 maybe_log_heartbeat 函数，封装对应业务逻辑。
def maybe_log_heartbeat():
    """定期输出心跳日志，确认主循环仍然存活且没有卡死"""
    # 注释: 调用 time.monotonic 并保存到 now_ts。
    now_ts = time.monotonic()
    # 注释: 调用 runtime_state.get 并保存到 last_heartbeat_ts。
    last_heartbeat_ts = runtime_state.get('last_heartbeat_ts', 0.0)
    # 注释: 判断条件是否成立：now_ts - last_heartbeat_ts < HEARTBEAT_INTERVAL_SECONDS。
    if now_ts - last_heartbeat_ts < HEARTBEAT_INTERVAL_SECONDS:
        # 注释: 直接结束当前函数。
        return

    # 注释: 更新 runtime_state['last_heartbeat_ts'] 的值。
    runtime_state['last_heartbeat_ts'] = now_ts
    # 注释: 写入运行日志，记录当前处理进度。
    logging.info(
        # 注释: 拼接或传入文本内容。
        "心跳: has_position=%s, side=%s, last_processed_15m=%s, last_entry_15m=%s, last_exit_15m=%s",
        # 注释: 调用 trade_state.get 执行对应处理。
        trade_state.get('has_position'),
        # 注释: 调用 trade_state.get 执行对应处理。
        trade_state.get('side'),
        # 注释: 调用 trade_state.get 执行对应处理。
        trade_state.get('last_processed_bar_15m', ''),
        # 注释: 调用 trade_state.get 执行对应处理。
        trade_state.get('last_entry_bar_15m', ''),
        # 注释: 调用 trade_state.get 执行对应处理。
        trade_state.get('last_exit_bar_15m', '')
    # 注释: 结束当前多行结构。
    )


def calculate_amount(price):  # 定义计算下单数量的函数，参数为当前价格
    """根据余额计算下单数量"""  # 函数的文档字符串
    try:  # 尝试执行计算逻辑
        balance = exchange.fetch_balance({'type': 'future'})  # 调用API获取当前账户的资产余额信息
        # 获取可用 USDT 余额
        usdt_free = float(balance['total']['USDT'])  # 提取USDT的总可用余额，并转换为浮点数
        # 计算开仓价值 = 余额 * 占比 * 杠杆
        position_value = usdt_free * MARGIN_RATE * LEVERAGE  # 根据策略参数计算本次开仓的理论总价值
        amount = position_value / price  # 将开仓总价值除以当前价格，得到理论应下单的代币数量
        # 格式化为交易所要求的精度
        return exchange.amount_to_precision(SYMBOL, amount)  # 调用API内置方法，将数量格式化为符合该交易对精度要求的数值并返回
    except Exception as e:  # 捕获执行过程中的异常
        logging.error(f"计算下单数量失败: {e}")  # 记录计算失败的错误日志
        return 0  # 发生错误时返回0，表示无法下单


# 注释: 定义 get_trading_fee_rate 函数，封装对应业务逻辑。
def get_trading_fee_rate():
    """获取指定交易对的交易手续费率"""
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 使用CCXT标准方法获取指定交易对的费率
        # 注释: 调用 exchange.fetch_trading_fee 并保存到 fee_info。
        fee_info = exchange.fetch_trading_fee(SYMBOL)
       
        # 注释: 调用 fee_info.get 并保存到 taker_fee_rate。
        taker_fee_rate = fee_info.get('taker', 0)
        # 注释: 返回计算结果：taker_fee_rate。
        return taker_fee_rate
    # 注释: 捕获异常分支：except Exception as e。
    except Exception as e:
        # 注释: 写入错误日志，便于排查运行异常。
        logging.error(f"获取 {SYMBOL} 手续费率失败: {e}，使用默认费率 Maker 0.02%, Taker 0.04%")
        # 注释: 返回计算结果：0.0004。
        return  0.0004


# 注释: 定义 get_server_time_str 函数，封装对应业务逻辑。
def get_server_time_str():
    """获取交易所服务器时间并格式化输出"""
    # 注释: 调用 exchange.fetch_time 并保存到 server_time_ms。
    server_time_ms = exchange.fetch_time()
    # 注释: 返回计算结果：datetime.datetime.fromtimestamp(。
    return datetime.datetime.fromtimestamp(
        # 注释: 说明当前代码行的作用：server_time_ms / 1000.0,。
        server_time_ms / 1000.0,
        # 注释: 计算并保存 tz。
        tz=EXCHANGE_TZ
    # 注释: 说明当前代码行的作用：).strftime(BAR_TIME_FORMAT)。
    ).strftime(BAR_TIME_FORMAT)


# 注释: 定义 format_bar_time 函数，封装对应业务逻辑。
def format_bar_time(ts):
    """统一格式化K线时间戳，便于状态去重和CSV记录"""
    # 注释: 判断条件是否成立：ts is None or pd.isna(ts)。
    if ts is None or pd.isna(ts):
        # 注释: 返回计算结果：''。
        return ''
    # 注释: 判断条件是否成立：isinstance(ts, pd.Timestamp)。
    if isinstance(ts, pd.Timestamp):
        # 注释: 返回计算结果：ts.to_pydatetime().strftime(BAR_TIME_FORMAT)。
        return ts.to_pydatetime().strftime(BAR_TIME_FORMAT)
    # 注释: 判断条件是否成立：isinstance(ts, datetime.datetime)。
    if isinstance(ts, datetime.datetime):
        # 注释: 返回计算结果：ts.strftime(BAR_TIME_FORMAT)。
        return ts.strftime(BAR_TIME_FORMAT)
    # 注释: 返回计算结果：str(ts)。
    return str(ts)


# 注释: 定义 get_server_now_dt 函数，封装对应业务逻辑。
def get_server_now_dt():
    """获取交易所服务器时间(datetime)"""
    # 注释: 调用 exchange.fetch_time 并保存到 server_time_ms。
    server_time_ms = exchange.fetch_time()
    # 注释: 返回计算结果：datetime.datetime.fromtimestamp(server_time_ms / 1000.0, tz=EXCHANGE_TZ)。
    return datetime.datetime.fromtimestamp(server_time_ms / 1000.0, tz=EXCHANGE_TZ)


# 注释: 定义 get_last_closed_index 函数，封装对应业务逻辑。
def get_last_closed_index(df, timeframe, now_dt=None):
    """按timeframe定位最近一根已收盘K线的iloc索引"""
    # 注释: 判断条件是否成立：df is None or len(df) == 0。
    if df is None or len(df) == 0:
        # 注释: 返回固定结果 None。
        return None

    # 注释: 调用 TIMEFRAME_SECONDS.get 并保存到 tf_seconds。
    tf_seconds = TIMEFRAME_SECONDS.get(timeframe)
    # 注释: 判断条件是否成立：tf_seconds is None。
    if tf_seconds is None:
        # 注释: 抛出异常并中断当前流程：ValueError(f"Unsupported timeframe: {timeframe}")。
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    # 注释: 判断条件是否成立：now_dt is None。
    if now_dt is None:
        # 注释: 调用 get_server_now_dt 并保存到 now_dt。
        now_dt = get_server_now_dt()
    # 注释: 继续判断备选条件：now_dt.tzinfo is None。
    elif now_dt.tzinfo is None:
        # 注释: 调用 now_dt.replace 并保存到 now_dt。
        now_dt = now_dt.replace(tzinfo=EXCHANGE_TZ)
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 调用 now_dt.astimezone 并保存到 now_dt。
        now_dt = now_dt.astimezone(EXCHANGE_TZ)

    # 注释: 计算并保存 ts_series。
    ts_series = df['timestamp']
    # 判断时间戳是否包含时区信息，如果包含则进行转换，否则进行本地化
    # 注释: 判断条件是否成立：getattr(ts_series.dt, 'tz', None) is None。
    if getattr(ts_series.dt, 'tz', None) is None:
        # 注释: 调用 ts_series.dt.tz_localize 并保存到 ts_series。
        ts_series = ts_series.dt.tz_localize(EXCHANGE_TZ)
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 调用 ts_series.dt.tz_convert 并保存到 ts_series。
        ts_series = ts_series.dt.tz_convert(EXCHANGE_TZ)

    # 注释: 说明当前代码行的作用：closed_mask = (ts_series + pd.to_timedelta(tf_seconds, unit='s')) <= now_dt。
    closed_mask = (ts_series + pd.to_timedelta(tf_seconds, unit='s')) <= now_dt
    # 注释: 调用 closed_mask.to_numpy 并保存到 closed_positions。
    closed_positions = closed_mask.to_numpy().nonzero()[0]
    # 注释: 判断条件是否成立：len(closed_positions) == 0。
    if len(closed_positions) == 0:
        # 注释: 返回固定结果 None。
        return None
    # 注释: 返回计算结果：int(closed_positions[-1])。
    return int(closed_positions[-1])


# 注释: 定义 get_closed_bar_time 函数，封装对应业务逻辑。
def get_closed_bar_time(df, timeframe, now_dt=None):
    """获取最近一根已收盘K线的时间"""
    # 注释: 调用 get_last_closed_index 并保存到 closed_idx。
    closed_idx = get_last_closed_index(df, timeframe, now_dt=now_dt)
    # 注释: 判断条件是否成立：closed_idx is None。
    if closed_idx is None:
        # 注释: 返回计算结果：''。
        return ''
    # 注释: 返回计算结果：format_bar_time(df.iloc[closed_idx]['timestamp'])。
    return format_bar_time(df.iloc[closed_idx]['timestamp'])


# 注释: 定义 parse_bar_time 函数，封装对应业务逻辑。
def parse_bar_time(value):
    """把CSV/状态里的K线时间字符串解析成东八区datetime。"""
    # 注释: 判断条件是否成立：not value。
    if not value:
        # 注释: 返回固定结果 None。
        return None
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 调用 datetime.datetime.strptime 并保存到 parsed。
        parsed = datetime.datetime.strptime(str(value), BAR_TIME_FORMAT)
        # 注释: 返回计算结果：parsed.replace(tzinfo=EXCHANGE_TZ)。
        return parsed.replace(tzinfo=EXCHANGE_TZ)
    # 注释: 捕获异常分支：except Exception。
    except Exception:
        # 注释: 返回固定结果 None。
        return None


# 注释: 定义 validate_signal_bar_15m 函数，封装对应业务逻辑。
def validate_signal_bar_15m(signal_bar_15m, now_dt=None):
    """校验15M信号K线是否新鲜且相对历史最大值单调向前。"""
    # 注释: 判断条件是否成立：not signal_bar_15m。
    if not signal_bar_15m:
        # 注释: 返回包含当前处理结果的字典。
        return {'valid': False, 'is_new': False, 'reason': 'empty'}

    # 注释: 调用 parse_bar_time 并保存到 signal_dt。
    signal_dt = parse_bar_time(signal_bar_15m)
    # 注释: 判断条件是否成立：signal_dt is None。
    if signal_dt is None:
        # 注释: 返回包含当前处理结果的字典。
        return {'valid': False, 'is_new': False, 'reason': 'parse_failed'}

    # 注释: 判断条件是否成立：now_dt is None。
    if now_dt is None:
        # 注释: 调用 get_server_now_dt 并保存到 now_dt。
        now_dt = get_server_now_dt()
    # 注释: 继续判断备选条件：now_dt.tzinfo is None。
    elif now_dt.tzinfo is None:
        # 注释: 调用 now_dt.replace 并保存到 now_dt。
        now_dt = now_dt.replace(tzinfo=EXCHANGE_TZ)
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 调用 now_dt.astimezone 并保存到 now_dt。
        now_dt = now_dt.astimezone(EXCHANGE_TZ)

    # 注释: 计算并保存 signal_close_dt。
    signal_close_dt = signal_dt + datetime.timedelta(seconds=TIMEFRAME_SECONDS['15m'])
    # 注释: 初始化 stale_seconds 元组或多行表达式。
    stale_seconds = (now_dt - signal_close_dt).total_seconds()
    # 注释: 判断条件是否成立：stale_seconds < -5。
    if stale_seconds < -5:
        # 注释: 返回包含当前处理结果的字典。
        return {'valid': False, 'is_new': False, 'reason': 'future_bar', 'stale_seconds': stale_seconds}
    # 注释: 判断条件是否成立：stale_seconds > MAX_SIGNAL_BAR_STALENESS_SECONDS。
    if stale_seconds > MAX_SIGNAL_BAR_STALENESS_SECONDS:
        # 注释: 返回包含当前处理结果的字典。
        return {'valid': False, 'is_new': False, 'reason': 'stale_bar', 'stale_seconds': stale_seconds}

    # 注释: 调用 parse_bar_time 并保存到 max_seen_dt。
    max_seen_dt = parse_bar_time(trade_state.get('max_seen_bar_15m', ''))
    # 注释: 判断条件是否成立：max_seen_dt is not None and signal_dt < max_seen_dt。
    if max_seen_dt is not None and signal_dt < max_seen_dt:
        # 注释: 返回包含当前处理结果的字典。
        return {
            # 注释: 设置 valid 字段的取值。
            'valid': False,
            # 注释: 设置 is_new 字段的取值。
            'is_new': False,
            # 注释: 设置 reason 字段的取值。
            'reason': 'bar_time_rollback',
            # 注释: 设置 max_seen_bar_15m 字段的取值。
            'max_seen_bar_15m': trade_state.get('max_seen_bar_15m', ''),
            # 注释: 设置 stale_seconds 字段的取值。
            'stale_seconds': stale_seconds
        # 注释: 结束当前多行结构。
        }

    # 注释: 调用 parse_bar_time 并保存到 last_processed_dt。
    last_processed_dt = parse_bar_time(trade_state.get('last_processed_bar_15m', ''))
    # 注释: 计算并保存 is_new。
    is_new = last_processed_dt is None or signal_dt > last_processed_dt
    # 注释: 判断条件是否成立：max_seen_dt is None or signal_dt > max_seen_dt。
    if max_seen_dt is None or signal_dt > max_seen_dt:
        # 注释: 更新 trade_state['max_seen_bar_15m'] 的值。
        trade_state['max_seen_bar_15m'] = signal_bar_15m

    # 注释: 返回包含当前处理结果的字典。
    return {'valid': True, 'is_new': is_new, 'reason': 'ok', 'stale_seconds': stale_seconds}


# 注释: 定义 get_latest_price 函数，封装对应业务逻辑。
def get_latest_price():
    """获取最新成交价，用于真实下单和风控"""
    # 注释: 调用 exchange.fetch_ticker 并保存到 ticker。
    ticker = exchange.fetch_ticker(SYMBOL)
    # 注释: 返回计算结果：float(ticker['last'])。
    return float(ticker['last'])


# 注释: 定义 estimate_liquidation_price 函数，封装对应业务逻辑。
def estimate_liquidation_price(entry_price, side):
    """按杠杆做一个保守的强平价预估，只用于开仓前风险过滤"""
    # 这里故意用 0.8 / 杠杆 作为估算比例，比理想情况下更保守，宁可少开也不把止损放到强平外面
    # 注释: 计算并保存 guard_ratio。
    guard_ratio = ESTIMATED_LIQUIDATION_GUARD_RATIO / LEVERAGE
    # 注释: 判断条件是否成立：side == 'long'。
    if side == 'long':
        # 注释: 返回计算结果：entry_price * (1 - guard_ratio)。
        return entry_price * (1 - guard_ratio)
    # 注释: 返回计算结果：entry_price * (1 + guard_ratio)。
    return entry_price * (1 + guard_ratio)


# 注释: 定义 normalize_liquidation_price 函数，封装对应业务逻辑。
def normalize_liquidation_price(liquidation_price):
    """把交易所返回的强平价转换成可用数值，0 或负数都视为无效"""
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 调用 float 并保存到 liq。
        liq = float(liquidation_price)
        # 注释: 判断条件是否成立：liq <= 0。
        if liq <= 0:
            # 注释: 返回固定结果 None。
            return None
        # 注释: 返回计算结果：liq。
        return liq
    # 注释: 捕获异常分支：except Exception。
    except Exception:
        # 注释: 返回固定结果 None。
        return None


# 注释: 定义 infer_position_side 函数，封装对应业务逻辑。
def infer_position_side(position_amt, info=None, pos=None):
    """优先用 positionSide 判断方向，兼容对冲模式下 SHORT 仓位数量为正数的情况。"""
    # 注释: 计算并保存 info。
    info = info or {}
    # 注释: 计算并保存 pos。
    pos = pos or {}
    # 注释: 调用 str 并保存到 raw_side。
    raw_side = str(info.get('positionSide') or pos.get('side') or '').strip().upper()
    # 注释: 判断条件是否成立：raw_side in ('LONG', 'SHORT')。
    if raw_side in ('LONG', 'SHORT'):
        # 注释: 返回计算结果：raw_side.lower()。
        return raw_side.lower()
    # 注释: 返回计算结果：'long' if float(position_amt) > 0 else 'short'。
    return 'long' if float(position_amt) > 0 else 'short'


# 注释: 定义 get_position_risk 函数，封装对应业务逻辑。
def get_position_risk(side=None):
    """获取当前合约仓位风险信息，包括强平价和标记价格"""
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 调用 exchange.fetch_positions_risk 并保存到 positions。
        positions = exchange.fetch_positions_risk([SYMBOL])
        # 注释: 遍历 positions，逐项处理 pos。
        for pos in positions:
            # 注释: 调用 pos.get 并保存到 info。
            info = pos.get('info', {})

            # 注释: 调用 float 并保存到 position_amt。
            position_amt = float(info.get('positionAmt', pos.get('contracts', 0)) or 0)
            # 注释: 判断条件是否成立：abs(position_amt) <= POSITION_AMT_EPSILON。
            if abs(position_amt) <= POSITION_AMT_EPSILON:
                # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                continue
            # 注释: 调用 infer_position_side 并保存到 pos_side。
            pos_side = infer_position_side(position_amt, info=info, pos=pos)
            # 注释: 判断条件是否成立：side and pos_side != side。
            if side and pos_side != side:
                # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                continue
            #liquidationPrice：强平价
            # 注释: 调用 normalize_liquidation_price 并保存到 liquidation_price。
            liquidation_price = normalize_liquidation_price(info.get('liquidationPrice', 0))
            #markPrice：标记价格
            # 注释: 调用 normalize_liquidation_price 并保存到 mark_price。
            mark_price = normalize_liquidation_price(info.get('markPrice', 0))
            # 注释: 返回包含当前处理结果的字典。
            return {
                # 注释: 设置 side 字段的取值。
                'side': pos_side,
                # 注释: 设置 position_amt 字段的取值。
                'position_amt': position_amt,
                # 注释: 设置 liquidation_price 字段的取值。
                'liquidation_price': liquidation_price,
                # 注释: 设置 mark_price 字段的取值。
                'mark_price': mark_price,
                # 注释: 设置 info 字段的取值。
                'info': info
            # 注释: 结束当前多行结构。
            }
    # 注释: 捕获异常分支：except Exception as e。
    except Exception as e:
        # 注释: 写入警告日志，提示需要关注的非致命问题。
        logging.warning(f"获取仓位风险信息失败: {e}")
        # 注释: 返回包含当前处理结果的字典。
        return {'fetch_failed': True, 'error': str(e)}
    # 注释: 返回固定结果 None。
    return None


# 注释: 定义 has_open_position_on_exchange 函数，封装对应业务逻辑。
def has_open_position_on_exchange(side=None):
    """使用标准仓位接口做二次确认，避免仓位接口瞬时返回空导致误判。"""
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 调用 exchange.fetch_positions 并保存到 positions。
        positions = exchange.fetch_positions([SYMBOL])
        # 注释: 遍历 positions，逐项处理 pos。
        for pos in positions:
            # 注释: 调用 pos.get 并保存到 info。
            info = pos.get('info', {})
            # 注释: 调用 float 并保存到 position_amt。
            position_amt = float(info.get('positionAmt', pos.get('contracts', 0)) or 0)
            # 注释: 判断条件是否成立：abs(position_amt) <= POSITION_AMT_EPSILON。
            if abs(position_amt) <= POSITION_AMT_EPSILON:
                # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                continue
            # 注释: 调用 infer_position_side 并保存到 pos_side。
            pos_side = infer_position_side(position_amt, info=info, pos=pos)
            # 注释: 判断条件是否成立：side and pos_side != side。
            if side and pos_side != side:
                # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                continue
            # 注释: 返回包含当前处理结果的字典。
            return {'has_position': True, 'fetch_failed': False, 'side': pos_side, 'position_amt': position_amt}
        # 注释: 返回包含当前处理结果的字典。
        return {'has_position': False, 'fetch_failed': False}
    # 注释: 捕获异常分支：except Exception as e。
    except Exception as e:
        # 注释: 写入警告日志，提示需要关注的非致命问题。
        logging.warning(f"二次确认仓位失败(fetch_positions): {e}")
        # 注释: 返回包含当前处理结果的字典。
        return {'has_position': False, 'fetch_failed': True, 'error': str(e)}


# 注释: 定义 ensure_stop_price_safe 函数，封装对应业务逻辑。
def ensure_stop_price_safe(entry_price, stop_price, side, liquidation_price=None):
    """确保止损价在强平价的安全一侧；如果太危险，则自动往安全方向推回去"""
    # 注释: 判断条件是否成立：stop_price is None or pd.isna(stop_price)。
    if stop_price is None or pd.isna(stop_price):
        # 注释: 返回计算结果：stop_price, {'adjusted': False, 'source': 'none', 'liquidation_price': None, 'saf…。
        return stop_price, {'adjusted': False, 'source': 'none', 'liquidation_price': None, 'safe_buffer': 0.0}

    # 注释: 调用 normalize_liquidation_price 并保存到 liq。
    liq = normalize_liquidation_price(liquidation_price)
    # 注释: 计算并保存 source。
    source = 'actual'
    # 注释: 判断条件是否成立：liq is None。
    if liq is None:
        # 注释: 调用 estimate_liquidation_price 并保存到 liq。
        liq = estimate_liquidation_price(entry_price, side)
        # 注释: 计算并保存 source。
        source = 'estimated'

    # 注释: 计算并保存 safe_buffer。
    safe_buffer = entry_price * LIQUIDATION_SAFE_BUFFER_RATIO
    # 注释: 调用 float 并保存到 adjusted_stop。
    adjusted_stop = float(stop_price)
    # 注释: 判断条件是否成立：side == 'long'。
    if side == 'long':
        # 注释: 计算并保存 min_safe_stop。
        min_safe_stop = liq + safe_buffer
        # 注释: 调用 max 并保存到 adjusted_stop。
        adjusted_stop = max(adjusted_stop, min_safe_stop)
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 计算并保存 max_safe_stop。
        max_safe_stop = liq - safe_buffer
        # 注释: 调用 min 并保存到 adjusted_stop。
        adjusted_stop = min(adjusted_stop, max_safe_stop)

    # 注释: 返回计算结果：adjusted_stop, {。
    return adjusted_stop, {
        # 注释: 设置 adjusted 字段的取值。
        'adjusted': abs(adjusted_stop - float(stop_price)) > 1e-12,
        # 注释: 设置 source 字段的取值。
        'source': source,
        # 注释: 设置 liquidation_price 字段的取值。
        'liquidation_price': liq,
        # 注释: 设置 safe_buffer 字段的取值。
        'safe_buffer': safe_buffer
    # 注释: 结束当前多行结构。
    }


# 注释: 定义 stop_price_is_still_valid 函数，封装对应业务逻辑。
def stop_price_is_still_valid(entry_price, stop_price, side):
    """检查止损价是否仍处在合理方向，避免止损价比入场价还离谱"""
    # 注释: 判断条件是否成立：stop_price is None or pd.isna(stop_price)。
    if stop_price is None or pd.isna(stop_price):
        # 注释: 返回固定结果 False。
        return False
    # 注释: 判断条件是否成立：side == 'long'。
    if side == 'long':
        # 注释: 返回计算结果：stop_price < entry_price。
        return stop_price < entry_price
    # 注释: 返回计算结果：stop_price > entry_price。
    return stop_price > entry_price


# 注释: 定义 place_protective_stop_order 函数，封装对应业务逻辑。
def place_protective_stop_order(side, stop_price):
    """在交易所挂一个服务端 STOP_MARKET 止损单，避免本地轮询来不及止损"""
    # 注释: 说明当前代码行的作用：stop_side = 'sell' if side == 'long' else 'buy'。
    stop_side = 'sell' if side == 'long' else 'buy'
    # 注释: 调用 float 并保存到 amount。
    amount = float(trade_state.get('amount', 0.0) or 0.0)
    # 注释: 判断条件是否成立：amount <= 0。
    if amount <= 0:
        # 注释: 抛出异常并中断当前流程：RuntimeError("缺少有效持仓数量，无法挂 reduceOnly 服务端止损单")。
        raise RuntimeError("缺少有效持仓数量，无法挂 reduceOnly 服务端止损单")

    # 使用显式数量 + reduceOnly，避免 closePosition 条件单在 demo/futures 环境里查不到、
    # 撤不掉，进而导致后续重挂持续 -4130 冲突。
    # 注释: 初始化 params 字典。
    params = {
        # 注释: 设置 stopPrice 字段的取值。
        'stopPrice': stop_price,
        # 注释: 设置 reduceOnly 字段的取值。
        'reduceOnly': True,
        # 注释: 设置 workingType 字段的取值。
        'workingType': STOP_WORKING_TYPE
    # 注释: 结束当前多行结构。
    }
    # 注释: 返回计算结果：exchange.create_order(SYMBOL, 'STOP_MARKET', stop_side, amount, None, params)。
    return exchange.create_order(SYMBOL, 'STOP_MARKET', stop_side, amount, None, params)


# 注释: 定义 normalize_exchange_bool 函数，封装对应业务逻辑。
def normalize_exchange_bool(value):
    """把交易所返回的真假值统一转成 Python bool。"""
    # 注释: 判断条件是否成立：isinstance(value, bool)。
    if isinstance(value, bool):
        # 注释: 返回计算结果：value。
        return value
    # 注释: 判断条件是否成立：value is None。
    if value is None:
        # 注释: 返回固定结果 False。
        return False
    # 注释: 返回计算结果：str(value).strip().lower() in ('true', '1', 'yes')。
    return str(value).strip().lower() in ('true', '1', 'yes')


# 注释: 定义 is_close_position_conditional_order 函数，封装对应业务逻辑。
def is_close_position_conditional_order(order, side=None):
    """判断某个 open order 是否是当前方向的全平仓条件单。"""
    # 注释: 判断条件是否成立：not isinstance(order, dict)。
    if not isinstance(order, dict):
        # 注释: 返回固定结果 False。
        return False

    # 注释: 调用 order.get 并保存到 info。
    info = order.get('info', {})
    # 注释: 判断条件是否成立：not isinstance(info, dict)。
    if not isinstance(info, dict):
        # 注释: 初始化 info 字典。
        info = {}

    # 注释: 调用 normalize_exchange_bool 并保存到 close_position。
    close_position = normalize_exchange_bool(
        # 注释: 调用 info.get 执行对应处理。
        info.get('closePosition', order.get('closePosition'))
    # 注释: 结束当前多行结构。
    )
    # 注释: 调用 normalize_exchange_bool 并保存到 reduce_only。
    reduce_only = normalize_exchange_bool(
        # 注释: 调用 info.get 执行对应处理。
        info.get('reduceOnly', order.get('reduceOnly'))
    # 注释: 结束当前多行结构。
    )
    # 注释: 判断条件是否成立：not close_position and not reduce_only。
    if not close_position and not reduce_only:
        # 注释: 返回固定结果 False。
        return False

    # 注释: 调用 str 并保存到 order_type。
    order_type = str(order.get('type') or info.get('type') or '').strip().upper()
    # 注释: 判断条件是否成立：order_type not in ('STOP', 'STOP_MARKET', 'TAKE_PROFIT', 'TAKE_PROFIT_MARKET')。
    if order_type not in ('STOP', 'STOP_MARKET', 'TAKE_PROFIT', 'TAKE_PROFIT_MARKET'):
        # 注释: 返回固定结果 False。
        return False

    # 注释: 判断条件是否成立：side。
    if side:
        # 注释: 说明当前代码行的作用：expected_side = 'SELL' if side == 'long' else 'BUY'。
        expected_side = 'SELL' if side == 'long' else 'BUY'
        # 注释: 调用 str 并保存到 order_side。
        order_side = str(order.get('side') or info.get('side') or '').strip().upper()
        # 注释: 判断条件是否成立：order_side != expected_side。
        if order_side != expected_side:
            # 注释: 返回固定结果 False。
            return False

    # 注释: 返回固定结果 True。
    return True


# 注释: 定义 ensure_stats_db 函数，封装对应业务逻辑。
def ensure_stats_db():
    # 注释: 使用上下文管理器处理资源：sqlite3.connect(STATS_DB_PATH) as conn。
    with sqlite3.connect(STATS_DB_PATH) as conn:
        # 注释: 调用 conn.execute 执行对应处理。
        conn.execute(
            # 注释: 拼接或传入文本内容。
            """
            CREATE TABLE IF NOT EXISTS daily_pnl (
                trade_day TEXT PRIMARY KEY,
                pnl REAL NOT NULL DEFAULT 0,
                trade_count INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
            """
        # 注释: 结束当前多行结构。
        )


# 注释: 定义 update_daily_pnl_stats 函数，封装对应业务逻辑。
def update_daily_pnl_stats(exit_time, net_pnl_usdt):
    """按平仓日期更新 SQLite 日收益汇总。"""
    # 注释: 判断条件是否成立：not exit_time or len(exit_time) < 10。
    if not exit_time or len(exit_time) < 10:
        # 注释: 直接结束当前函数。
        return

    # 注释: 计算并保存 trade_day。
    trade_day = exit_time[:10]
    # 注释: 调用 datetime.datetime.now 并保存到 updated_at。
    updated_at = datetime.datetime.now().isoformat(timespec='seconds')

    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 调用 ensure_stats_db 执行对应处理。
        ensure_stats_db()
        # 注释: 使用上下文管理器处理资源：sqlite3.connect(STATS_DB_PATH) as conn。
        with sqlite3.connect(STATS_DB_PATH) as conn:
            # 注释: 调用 conn.execute 执行对应处理。
            conn.execute(
                # 注释: 拼接或传入文本内容。
                """
                INSERT INTO daily_pnl (trade_day, pnl, trade_count, updated_at)
                VALUES (?, ?, 1, ?)
                ON CONFLICT(trade_day) DO UPDATE SET
                    pnl = daily_pnl.pnl + excluded.pnl,
                    trade_count = daily_pnl.trade_count + 1,
                    updated_at = excluded.updated_at
                """,
                # 注释: 说明当前代码行的作用：(trade_day, float(net_pnl_usdt), updated_at)。
                (trade_day, float(net_pnl_usdt), updated_at)
            # 注释: 结束当前多行结构。
            )
    # 注释: 捕获异常分支：except Exception as e。
    except Exception as e:
        # 注释: 写入警告日志，提示需要关注的非致命问题。
        logging.warning(f"写入 SQLite 日收益失败: {e}")


# 注释: 定义 get_exchange_symbol_id 函数，封装对应业务逻辑。
def get_exchange_symbol_id():
    """获取交易所原生symbol，未load_markets时用ETHUSDT兜底。"""
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 调用 exchange.market 并保存到 market。
        market = exchange.market(SYMBOL)
        # 注释: 调用 market.get 并保存到 market_id。
        market_id = market.get('id')
        # 注释: 判断条件是否成立：market_id。
        if market_id:
            # 注释: 返回计算结果：market_id。
            return market_id
    # 注释: 捕获异常分支：except Exception。
    except Exception:
        # 注释: 占位语句，当前分支不执行额外操作。
        pass
    # 注释: 返回计算结果：SYMBOL.replace('/', '').replace(':USDT', '')。
    return SYMBOL.replace('/', '').replace(':USDT', '')


# 注释: 定义 normalize_raw_open_order 函数，封装对应业务逻辑。
def normalize_raw_open_order(raw_order):
    """把交易所原始未成交单包装成可复用的CCXT-like结构。"""
    # 注释: 判断条件是否成立：not isinstance(raw_order, dict)。
    if not isinstance(raw_order, dict):
        # 注释: 返回计算结果：raw_order。
        return raw_order
    # 注释: 返回包含当前处理结果的字典。
    return {
        # 注释: 设置 id 字段的取值。
        'id': raw_order.get('orderId') or raw_order.get('id') or raw_order.get('clientOrderId'),
        # 注释: 设置 type 字段的取值。
        'type': raw_order.get('type'),
        # 注释: 设置 side 字段的取值。
        'side': raw_order.get('side'),
        # 注释: 设置 timestamp 字段的取值。
        'timestamp': raw_order.get('time') or raw_order.get('updateTime') or raw_order.get('workingTime'),
        # 注释: 设置 stopPrice 字段的取值。
        'stopPrice': raw_order.get('stopPrice') or raw_order.get('triggerPrice') or raw_order.get('activatePrice'),
        # 注释: 设置 closePosition 字段的取值。
        'closePosition': raw_order.get('closePosition'),
        # 注释: 设置 reduceOnly 字段的取值。
        'reduceOnly': raw_order.get('reduceOnly'),
        # 注释: 设置 info 字段的取值。
        'info': raw_order
    # 注释: 结束当前多行结构。
    }


# 注释: 定义 fetch_raw_future_open_orders 函数，封装对应业务逻辑。
def fetch_raw_future_open_orders():
    """直接调用币安U本位未成交单接口，补齐CCXT统一接口漏掉的条件单。"""
    # 注释: 调用 getattr 并保存到 method。
    method = getattr(exchange, 'fapiPrivateGetOpenOrders', None)
    # 注释: 判断条件是否成立：method is None。
    if method is None:
        # 注释: 返回包含当前处理结果的列表。
        return []
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 调用 method 并保存到 raw_orders。
        raw_orders = method({'symbol': get_exchange_symbol_id()})
    # 注释: 捕获异常分支：except Exception as e。
    except Exception as e:
        # 注释: 写入警告日志，提示需要关注的非致命问题。
        logging.warning(f"原始接口查询U本位未成交单失败: {e}")
        # 注释: 返回固定结果 None。
        return None
    # 注释: 判断条件是否成立：not isinstance(raw_orders, list)。
    if not isinstance(raw_orders, list):
        # 注释: 返回包含当前处理结果的列表。
        return []
    # 注释: 返回包含当前处理结果的列表。
    return [normalize_raw_open_order(order) for order in raw_orders]


# 注释: 定义 fetch_open_close_position_orders 函数，封装对应业务逻辑。
def fetch_open_close_position_orders(side=None):
    """读取当前仓位方向上的全平仓条件单，便于做撤单确认和冲突排查。"""
    # 注释: 初始化 open_orders 列表。
    open_orders = []
    # 注释: 计算并保存 unified_fetch_failed。
    unified_fetch_failed = False
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 调用 exchange.fetch_open_orders 并保存到 open_orders。
        open_orders = exchange.fetch_open_orders(SYMBOL)
    # 注释: 捕获异常分支：except Exception as e。
    except Exception as e:
        # 注释: 写入警告日志，提示需要关注的非致命问题。
        logging.warning(f"查询未成交条件单失败: {e}")
        # 注释: 计算并保存 unified_fetch_failed。
        unified_fetch_failed = True
        # 注释: 初始化 open_orders 列表。
        open_orders = []

    # 注释: 调用 fetch_raw_future_open_orders 并保存到 raw_open_orders。
    raw_open_orders = fetch_raw_future_open_orders()
    # 注释: 判断条件是否成立：raw_open_orders is None。
    if raw_open_orders is None:
        # 注释: 判断条件是否成立：unified_fetch_failed。
        if unified_fetch_failed:
            # 注释: 返回固定结果 None。
            return None
        # 注释: 初始化 raw_open_orders 列表。
        raw_open_orders = []
    # 注释: 判断条件是否成立：raw_open_orders。
    if raw_open_orders:
        # 注释: 初始化 known_order_ids 字典。
        known_order_ids = {extract_order_id(order) for order in open_orders if extract_order_id(order)}
        # 注释: 遍历 raw_open_orders，逐项处理 raw_order。
        for raw_order in raw_open_orders:
            # 注释: 调用 extract_order_id 并保存到 raw_order_id。
            raw_order_id = extract_order_id(raw_order)
            # 注释: 判断条件是否成立：raw_order_id and raw_order_id in known_order_ids。
            if raw_order_id and raw_order_id in known_order_ids:
                # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                continue
            # 注释: 调用 open_orders.append 执行对应处理。
            open_orders.append(raw_order)

    # 注释: 初始化 matched_orders 列表。
    matched_orders = []
    # 注释: 遍历 open_orders，逐项处理 order。
    for order in open_orders:
        # 注释: 判断条件是否成立：is_close_position_conditional_order(order, side=side)。
        if is_close_position_conditional_order(order, side=side):
            # 注释: 调用 matched_orders.append 执行对应处理。
            matched_orders.append(order)
    # 注释: 返回计算结果：matched_orders。
    return matched_orders


# 注释: 定义 fetch_open_protective_stop_orders 函数，封装对应业务逻辑。
def fetch_open_protective_stop_orders(side=None):
    """读取当前方向的 STOP / STOP_MARKET 全平仓条件单。"""
    # 注释: 调用 fetch_open_close_position_orders 并保存到 matched_orders。
    matched_orders = fetch_open_close_position_orders(side=side)
    # 注释: 判断条件是否成立：matched_orders is None。
    if matched_orders is None:
        # 注释: 返回固定结果 None。
        return None

    # 注释: 初始化 stop_orders 列表。
    stop_orders = []
    # 注释: 遍历 matched_orders，逐项处理 order。
    for order in matched_orders:
        # 注释: 调用 str 并保存到 order_type。
        order_type = str(order.get('type') or order.get('info', {}).get('type') or '').strip().upper()
        # 注释: 判断条件是否成立：order_type in ('STOP', 'STOP_MARKET')。
        if order_type in ('STOP', 'STOP_MARKET'):
            # 注释: 调用 stop_orders.append 执行对应处理。
            stop_orders.append(order)
    # 注释: 返回计算结果：stop_orders。
    return stop_orders


# 注释: 定义 cancel_all_open_orders_for_symbol 函数，封装对应业务逻辑。
def cancel_all_open_orders_for_symbol(silent=False):
    """撤销当前交易对的所有未成交单，用作 closePosition 隐藏冲突的最后恢复手段。"""
    # 注释: 计算并保存 cancel_succeeded。
    cancel_succeeded = False
    # 注释: 初始化 error_messages 列表。
    error_messages = []

    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 调用交易所接口执行对应操作。
        exchange.cancel_all_orders(SYMBOL, {'type': 'future'})
        # 注释: 计算并保存 cancel_succeeded。
        cancel_succeeded = True
        # 注释: 判断条件是否成立：not silent。
        if not silent:
            # 注释: 写入警告日志，提示需要关注的非致命问题。
            logging.warning(f"已通过 cancel_all_orders 撤销 {SYMBOL} 全部未成交单")
    # 注释: 捕获异常分支：except Exception as unified_error。
    except Exception as unified_error:
        # 注释: 调用 error_messages.append 执行对应处理。
        error_messages.append(f"cancel_all_orders failed: {format_exception_message(unified_error)}")

    # 注释: 调用 getattr 并保存到 raw_method。
    raw_method = getattr(exchange, 'fapiPrivateDeleteAllOpenOrders', None)
    # 注释: 判断条件是否成立：raw_method is not None。
    if raw_method is not None:
        # 注释: 开始执行可能抛出异常的逻辑。
        try:
            # 注释: 调用 raw_method 执行对应处理。
            raw_method({'symbol': get_exchange_symbol_id()})
            # 注释: 计算并保存 cancel_succeeded。
            cancel_succeeded = True
            # 注释: 判断条件是否成立：not silent。
            if not silent:
                # 注释: 写入警告日志，提示需要关注的非致命问题。
                logging.warning(f"已通过原始接口撤销 {SYMBOL} 全部未成交单")
        # 注释: 捕获异常分支：except Exception as raw_error。
        except Exception as raw_error:
            # 注释: 调用 error_messages.append 执行对应处理。
            error_messages.append(f"raw cancel all failed: {format_exception_message(raw_error)}")
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 调用 error_messages.append 执行对应处理。
        error_messages.append("raw cancel all failed: fapiPrivateDeleteAllOpenOrders unavailable")

    # 注释: 判断条件是否成立：cancel_succeeded。
    if cancel_succeeded:
        # 注释: 调用 clear_local_stop_order_state 执行对应处理。
        clear_local_stop_order_state()
        # 注释: 返回固定结果 True。
        return True

    # 注释: 更新 trade_state['last_stop_order_refresh_error'] 的值。
    trade_state['last_stop_order_refresh_error'] = '; '.join(error_messages)
    # 注释: 判断条件是否成立：not silent。
    if not silent:
        # 注释: 写入警告日志，提示需要关注的非致命问题。
        logging.warning(f"批量撤销 {SYMBOL} 未成交单失败: {trade_state['last_stop_order_refresh_error']}")
    # 注释: 返回固定结果 False。
    return False


# 注释: 定义 pick_active_protective_stop_order 函数，封装对应业务逻辑。
def pick_active_protective_stop_order(orders, preferred_order_id=''):
    """在多张候选止损单里优先选本地记录对应的，否则选最新的一张。"""
    # 注释: 判断条件是否成立：not orders。
    if not orders:
        # 注释: 返回固定结果 None。
        return None

    # 注释: 调用 str 并保存到 preferred_order_id。
    preferred_order_id = str(preferred_order_id or '')
    # 注释: 判断条件是否成立：preferred_order_id。
    if preferred_order_id:
        # 注释: 遍历 orders，逐项处理 order。
        for order in orders:
            # 注释: 判断条件是否成立：extract_order_id(order) == preferred_order_id。
            if extract_order_id(order) == preferred_order_id:
                # 注释: 返回计算结果：order。
                return order

    # 注释: 返回计算结果：max(orders, key=extract_order_timestamp_ms)。
    return max(orders, key=extract_order_timestamp_ms)


# 注释: 定义 sync_protective_stop_order_state 函数，封装对应业务逻辑。
def sync_protective_stop_order_state(side=None, silent=False):
    """用交易所未成交条件单刷新本地 stop_order 缓存。"""
    # 注释: 计算并保存 side。
    side = side or trade_state.get('side', '')
    # 注释: 判断条件是否成立：not side。
    if not side:
        # 注释: 返回固定结果 None。
        return None

    # 注释: 调用 fetch_open_protective_stop_orders 并保存到 stop_orders。
    stop_orders = fetch_open_protective_stop_orders(side=side)
    # 注释: 判断条件是否成立：stop_orders is None。
    if stop_orders is None:
        # 注释: 返回固定结果 None。
        return None

    # 注释: 调用 str 并保存到 local_order_id。
    local_order_id = str(trade_state.get('stop_order_id', '') or '')
    # 注释: 调用 float 并保存到 local_stop_price。
    local_stop_price = float(trade_state.get('stop_order_price', 0.0) or 0.0)

    # 注释: 判断条件是否成立：not stop_orders。
    if not stop_orders:
        # 注释: 判断条件是否成立：local_order_id or local_stop_price。
        if local_order_id or local_stop_price:
            # 注释: 判断条件是否成立：not silent。
            if not silent:
                # 注释: 写入警告日志，提示需要关注的非致命问题。
                logging.warning(
                    # 注释: 拼接或传入文本内容。
                    f"交易所未找到当前方向的服务端止损单，已清空本地缓存: "
                    # 注释: 拼接或传入文本内容。
                    f"side={side}, local_order_id={local_order_id}, local_stop={local_stop_price}"
                # 注释: 结束当前多行结构。
                )
            # 注释: 调用 clear_local_stop_order_state 执行对应处理。
            clear_local_stop_order_state()
        # 注释: 返回包含当前处理结果的字典。
        return {'orders': [], 'active_order': None, 'active_order_id': '', 'active_stop_price': 0.0}

    # 注释: 调用 pick_active_protective_stop_order 并保存到 active_order。
    active_order = pick_active_protective_stop_order(stop_orders, preferred_order_id=local_order_id)
    # 注释: 调用 extract_order_id 并保存到 active_order_id。
    active_order_id = extract_order_id(active_order)
    # 注释: 调用 extract_order_stop_price 并保存到 active_stop_price。
    active_stop_price = extract_order_stop_price(active_order)
    # 注释: 判断条件是否成立：active_stop_price is None。
    if active_stop_price is None:
        # 注释: 计算并保存 active_stop_price。
        active_stop_price = local_stop_price

    # 注释: 判断条件是否成立：len(stop_orders) > 1 and not silent。
    if len(stop_orders) > 1 and not silent:
        # 注释: 初始化 order_ids 列表。
        order_ids = [extract_order_id(order) or 'unknown' for order in stop_orders]
        # 注释: 写入警告日志，提示需要关注的非致命问题。
        logging.warning(f"检测到同方向存在多张服务端止损单: side={side}, order_ids={order_ids}，将优先处理 {active_order_id}")

    # 注释: 调用 if 执行对应处理。
    if (local_order_id != active_order_id) or (
        # 注释: 说明当前代码行的作用：active_stop_price and abs(local_stop_price - active_stop_price) > 1e-12。
        active_stop_price and abs(local_stop_price - active_stop_price) > 1e-12
    # 注释: 说明当前代码行的作用：):。
    ):
        # 注释: 判断条件是否成立：not silent。
        if not silent:
            # 注释: 写入运行日志，记录当前处理进度。
            logging.info(
                # 注释: 拼接或传入文本内容。
                f"已按交易所状态刷新服务端止损缓存: side={side}, "
                # 注释: 拼接或传入文本内容。
                f"local_order_id={local_order_id}, exchange_order_id={active_order_id}, "
                # 注释: 拼接或传入文本内容。
                f"local_stop={local_stop_price}, exchange_stop={active_stop_price}"
            # 注释: 结束当前多行结构。
            )
        # 注释: 更新 trade_state['stop_order_id'] 的值。
        trade_state['stop_order_id'] = active_order_id
        # 注释: 更新 trade_state['stop_order_price'] 的值。
        trade_state['stop_order_price'] = float(active_stop_price or 0.0)

    # 注释: 返回包含当前处理结果的字典。
    return {
        # 注释: 设置 orders 字段的取值。
        'orders': stop_orders,
        # 注释: 设置 active_order 字段的取值。
        'active_order': active_order,
        # 注释: 设置 active_order_id 字段的取值。
        'active_order_id': active_order_id,
        # 注释: 设置 active_stop_price 字段的取值。
        'active_stop_price': float(active_stop_price or 0.0)
    # 注释: 结束当前多行结构。
    }


# 注释: 定义 wait_until_stop_order_disappears 函数，封装对应业务逻辑。
def wait_until_stop_order_disappears(stop_order_ids, side, retries=STOP_ORDER_CANCEL_CONFIRM_RETRIES, sleep_seconds=STOP_ORDER_CANCEL_CONFIRM_SLEEP_SECONDS):
    """轮询确认指定止损单已经不在交易所未成交列表里。"""
    # 注释: 判断条件是否成立：not stop_order_ids。
    if not stop_order_ids:
        # 注释: 返回固定结果 True。
        return True

    # 注释: 判断条件是否成立：isinstance(stop_order_ids, (list, tuple, set))。
    if isinstance(stop_order_ids, (list, tuple, set)):
        # 注释: 初始化 target_order_ids 字典。
        target_order_ids = {str(order_id) for order_id in stop_order_ids if order_id}
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 初始化 target_order_ids 字典。
        target_order_ids = {str(stop_order_ids)}
    # 注释: 判断条件是否成立：not target_order_ids。
    if not target_order_ids:
        # 注释: 返回固定结果 True。
        return True

    # 注释: 遍历 range(1, retries + 1)，逐项处理 attempt。
    for attempt in range(1, retries + 1):
        # 注释: 调用 fetch_open_protective_stop_orders 并保存到 matched_orders。
        matched_orders = fetch_open_protective_stop_orders(side=side)
        # 注释: 判断条件是否成立：matched_orders is None。
        if matched_orders is None:
            # 注释: 暂停执行一段时间，控制轮询、重试或主循环节奏。
            time.sleep(sleep_seconds)
            # 注释: 跳过本次循环剩余逻辑，进入下一轮。
            continue

        # 注释: 初始化 remaining_ids 列表。
        remaining_ids = [
            # 注释: 调用 extract_order_id 执行对应处理。
            extract_order_id(order) for order in matched_orders
            # 注释: 说明当前代码行的作用：if extract_order_id(order) in target_order_ids。
            if extract_order_id(order) in target_order_ids
        # 注释: 结束当前多行结构。
        ]
        # 注释: 判断条件是否成立：not remaining_ids。
        if not remaining_ids:
            # 注释: 返回固定结果 True。
            return True

        # 注释: 写入运行日志，记录当前处理进度。
        logging.info(
            # 注释: 拼接或传入文本内容。
            f"等待旧服务端止损单从交易所消失: order_ids={remaining_ids}, "
            # 注释: 拼接或传入文本内容。
            f"attempt={attempt}/{retries}"
        # 注释: 结束当前多行结构。
        )
        # 注释: 暂停执行一段时间，控制轮询、重试或主循环节奏。
        time.sleep(sleep_seconds)

    # 注释: 返回固定结果 False。
    return False


# 注释: 定义 is_close_position_conflict_error 函数，封装对应业务逻辑。
def is_close_position_conflict_error(error):
    """识别 Binance 同方向 closePosition 条件单冲突(-4130)。"""
    # 注释: 调用 format_exception_message 并保存到 error_text。
    error_text = format_exception_message(error)
    # 注释: 返回计算结果：'code":-4130' in error_text or 'closePosition in the direction is existing' in er…。
    return 'code":-4130' in error_text or 'closePosition in the direction is existing' in error_text


# 注释: 定义 is_order_already_absent_error 函数，封装对应业务逻辑。
def is_order_already_absent_error(error):
    """识别撤单时常见的“订单已不存在”类错误。"""
    # 注释: 调用 format_exception_message 并保存到 error_text。
    error_text = format_exception_message(error).lower()
    # 注释: 初始化 absent_markers 元组或多行表达式。
    absent_markers = (
        # 注释: 拼接或传入文本内容。
        'code":-2011',
        # 注释: 拼接或传入文本内容。
        'unknown order',
        # 注释: 拼接或传入文本内容。
        'order does not exist',
        # 注释: 拼接或传入文本内容。
        'order not found',
        # 注释: 拼接或传入文本内容。
        'cancel rejected'
    # 注释: 结束当前多行结构。
    )
    # 注释: 返回计算结果：any(marker in error_text for marker in absent_markers)。
    return any(marker in error_text for marker in absent_markers)


# 注释: 定义 reset_stop_order_refresh_failure_state 函数，封装对应业务逻辑。
def reset_stop_order_refresh_failure_state():
    """服务端止损单一旦成功更新，就把连续失败计数清零。"""
    # 注释: 更新 trade_state['stop_order_refresh_fail_count'] 的值。
    trade_state['stop_order_refresh_fail_count'] = 0
    # 注释: 更新 trade_state['last_stop_order_refresh_error'] 的值。
    trade_state['last_stop_order_refresh_error'] = ''


# 注释: 定义 handle_stop_order_refresh_failure 函数，封装对应业务逻辑。
def handle_stop_order_refresh_failure(close_reason, curr_price, signal_bar_15m='', trigger_label=''):
    """服务端止损单更新失败时累计次数，连续达到阈值后才保护性平仓。"""
    # 注释: 调用 int 并保存到 fail_count。
    fail_count = int(trade_state.get('stop_order_refresh_fail_count', 0) or 0) + 1
    # 注释: 更新 trade_state['stop_order_refresh_fail_count'] 的值。
    trade_state['stop_order_refresh_fail_count'] = fail_count
    # 注释: 调用 trade_state.get 并保存到 error_text。
    error_text = trade_state.get('last_stop_order_refresh_error', '')
    # 注释: 写入警告日志，提示需要关注的非致命问题。
    logging.warning(
        # 注释: 拼接或传入文本内容。
        f"服务端止损更新失败，第{fail_count}/{STOP_ORDER_REFRESH_FAILURE_CLOSE_THRESHOLD}次；"
        # 注释: 拼接或传入文本内容。
        f"本轮先不平仓，等待后续重试。error={error_text}"
    # 注释: 结束当前多行结构。
    )
    # 注释: 判断条件是否成立：fail_count < STOP_ORDER_REFRESH_FAILURE_CLOSE_THRESHOLD。
    if fail_count < STOP_ORDER_REFRESH_FAILURE_CLOSE_THRESHOLD:
        # 注释: 返回固定结果 False。
        return False

    # 注释: 计算并保存 close_position(close_reason, curr_price, signal_bar_15m。
    close_position(close_reason, curr_price, signal_bar_15m=signal_bar_15m, trigger_label=trigger_label)
    # 注释: 返回固定结果 True。
    return True


# 注释: 定义 cancel_protective_stop_order 函数，封装对应业务逻辑。
def cancel_protective_stop_order(silent=False):
    """撤销当前记录的服务端止损单，平仓或替换止损时要先撤旧单"""
    # 注释: 调用 trade_state.get 并保存到 side。
    side = trade_state.get('side', '')
    # 注释: 调用 str 并保存到 local_stop_order_id_before_sync。
    local_stop_order_id_before_sync = str(trade_state.get('stop_order_id', '') or '')
    # 注释: 调用 sync_protective_stop_order_state 并保存到 sync_result。
    sync_result = sync_protective_stop_order_state(side=side, silent=True) if side else None
    # 注释: 初始化 stop_order_ids 列表。
    stop_order_ids = []
    # 注释: 判断条件是否成立：sync_result is not None。
    if sync_result is not None:
        # 注释: 初始化 stop_order_ids 列表。
        stop_order_ids = [extract_order_id(order) for order in sync_result['orders'] if extract_order_id(order)]

    # 注释: 调用 str 并保存到 local_stop_order_id。
    local_stop_order_id = str(trade_state.get('stop_order_id', '') or '') or local_stop_order_id_before_sync
    # 注释: 判断条件是否成立：local_stop_order_id and local_stop_order_id not in stop_order_ids。
    if local_stop_order_id and local_stop_order_id not in stop_order_ids:
        # 注释: 调用 stop_order_ids.append 执行对应处理。
        stop_order_ids.append(local_stop_order_id)

    # 注释: 调用 list 并保存到 stop_order_ids。
    stop_order_ids = list(dict.fromkeys([order_id for order_id in stop_order_ids if order_id]))
    # 注释: 判断条件是否成立：not stop_order_ids。
    if not stop_order_ids:
        # 注释: 调用 clear_local_stop_order_state 执行对应处理。
        clear_local_stop_order_state()
        # 注释: 返回固定结果 True。
        return True

    # 注释: 计算并保存 cancel_failed。
    cancel_failed = False
    # 注释: 遍历 stop_order_ids，逐项处理 stop_order_id。
    for stop_order_id in stop_order_ids:
        # 注释: 开始执行可能抛出异常的逻辑。
        try:
            # 注释: 调用交易所接口执行对应操作。
            exchange.cancel_order(stop_order_id, SYMBOL)
            # 注释: 判断条件是否成立：not silent。
            if not silent:
                # 注释: 写入运行日志，记录当前处理进度。
                logging.info(f"已撤销旧服务端止损单: {stop_order_id}")
        # 注释: 捕获异常分支：except Exception as e。
        except Exception as e:
            # 注释: 调用 format_exception_message 并保存到 error_text。
            error_text = format_exception_message(e)
            # 注释: 更新 trade_state['last_stop_order_refresh_error'] 的值。
            trade_state['last_stop_order_refresh_error'] = error_text

            # 注释: 判断条件是否成立：is_order_already_absent_error(e)。
            if is_order_already_absent_error(e):
                # 注释: 判断条件是否成立：not silent。
                if not silent:
                    # 注释: 写入警告日志，提示需要关注的非致命问题。
                    logging.warning(f"撤销服务端止损单时提示已不存在，按成功处理({stop_order_id}): {error_text}")
                # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                continue

            # 注释: 调用 fetch_open_protective_stop_orders 并保存到 matched_orders。
            matched_orders = fetch_open_protective_stop_orders(side=side or None)
            # 注释: 判断条件是否成立：matched_orders is not None。
            if matched_orders is not None:
                # 注释: 说明当前代码行的作用：still_exists = any(extract_order_id(order) == str(stop_order_id) for order in mat…。
                still_exists = any(extract_order_id(order) == str(stop_order_id) for order in matched_orders)
                # 注释: 判断条件是否成立：not still_exists。
                if not still_exists:
                    # 注释: 判断条件是否成立：not silent。
                    if not silent:
                        # 注释: 写入警告日志，提示需要关注的非致命问题。
                        logging.warning(f"撤单接口报错，但旧服务端止损单已不在交易所未成交列表中，按成功处理({stop_order_id}): {error_text}")
                    # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                    continue

            # 注释: 计算并保存 cancel_failed。
            cancel_failed = True
            # 注释: 判断条件是否成立：not silent。
            if not silent:
                # 注释: 写入警告日志，提示需要关注的非致命问题。
                logging.warning(f"撤销服务端止损单失败({stop_order_id}): {error_text}")

    # 注释: 判断条件是否成立：cancel_failed。
    if cancel_failed:
        # 注释: 返回固定结果 False。
        return False

    # 注释: 调用 clear_local_stop_order_state 执行对应处理。
    clear_local_stop_order_state()
    # 注释: 返回固定结果 True。
    return True


# 注释: 定义 refresh_protective_stop_order 函数，封装对应业务逻辑。
def refresh_protective_stop_order(stop_price):
    """更新服务端止损单：先撤旧单，再按新的止损价重挂"""
    # 注释: 判断条件是否成立：not trade_state.get('has_position') or not trade_state.get('side')。
    if not trade_state.get('has_position') or not trade_state.get('side'):
        # 注释: 返回固定结果 True。
        return True

    # 注释: 计算并保存 side。
    side = trade_state['side']
    # 注释: 调用 str 并保存到 local_stop_order_id_before_sync。
    local_stop_order_id_before_sync = str(trade_state.get('stop_order_id', '') or '')
    # 注释: 调用 sync_protective_stop_order_state 并保存到 sync_result。
    sync_result = sync_protective_stop_order_state(side=side, silent=True)
    # 注释: 初始化 previous_stop_order_ids 列表。
    previous_stop_order_ids = []
    # 注释: 判断条件是否成立：sync_result is not None。
    if sync_result is not None:
        # 注释: 初始化 previous_stop_order_ids 列表。
        previous_stop_order_ids = [extract_order_id(order) for order in sync_result['orders'] if extract_order_id(order)]
    # 注释: 调用 str 并保存到 local_stop_order_id。
    local_stop_order_id = str(trade_state.get('stop_order_id', '') or '') or local_stop_order_id_before_sync
    # 注释: 判断条件是否成立：local_stop_order_id and local_stop_order_id not in previous_stop_order_ids。
    if local_stop_order_id and local_stop_order_id not in previous_stop_order_ids:
        # 注释: 调用 previous_stop_order_ids.append 执行对应处理。
        previous_stop_order_ids.append(local_stop_order_id)

    # 注释: 判断条件是否成立：previous_stop_order_ids。
    if previous_stop_order_ids:
        # 注释: 判断条件是否成立：not cancel_protective_stop_order(silent=True)。
        if not cancel_protective_stop_order(silent=True):
            # 注释: 更新 trade_state['last_stop_order_refresh_error'] 的值。
            trade_state['last_stop_order_refresh_error'] = (
                # 注释: 调用 trade_state.get 执行对应处理。
                trade_state.get('last_stop_order_refresh_error', '') or
                # 注释: 拼接或传入文本内容。
                f"cancel stop order failed: order_ids={previous_stop_order_ids}"
            # 注释: 结束当前多行结构。
            )
            # 注释: 写入错误日志，便于排查运行异常。
            logging.error(f"撤销旧服务端止损单失败，已取消重挂: order_ids={previous_stop_order_ids}")
            # 注释: 返回固定结果 False。
            return False

        # 注释: 判断条件是否成立：not wait_until_stop_order_disappears(previous_stop_order_ids, side=side)。
        if not wait_until_stop_order_disappears(previous_stop_order_ids, side=side):
            # 注释: 更新 trade_state['last_stop_order_refresh_error'] 的值。
            trade_state['last_stop_order_refresh_error'] = (
                # 注释: 拼接或传入文本内容。
                f"old stop order still visible after cancel confirm retries: order_ids={previous_stop_order_ids}"
            # 注释: 结束当前多行结构。
            )
            # 注释: 写入错误日志，便于排查运行异常。
            logging.error(
                # 注释: 拼接或传入文本内容。
                f"旧服务端止损单撤销后仍未从交易所消失，已取消重挂: order_ids={previous_stop_order_ids}"
            # 注释: 结束当前多行结构。
            )
            # 注释: 返回固定结果 False。
            return False

        # 注释: 暂停执行一段时间，控制轮询、重试或主循环节奏。
        time.sleep(STOP_ORDER_POST_CANCEL_DELAY_SECONDS)

    # 注释: 初始化 retry_delays 元组或多行表达式。
    retry_delays = (0.0,) + STOP_ORDER_REFRESH_RETRY_DELAYS_SECONDS
    # 注释: 遍历 enumerate(retry_delays, start=1)，逐项处理 attempt, retry_delay。
    for attempt, retry_delay in enumerate(retry_delays, start=1):
        # 注释: 判断条件是否成立：retry_delay > 0。
        if retry_delay > 0:
            # 注释: 暂停执行一段时间，控制轮询、重试或主循环节奏。
            time.sleep(retry_delay)

        # 注释: 开始执行可能抛出异常的逻辑。
        try:
            # 注释: 调用 place_protective_stop_order 并保存到 stop_order。
            stop_order = place_protective_stop_order(side, stop_price)
            # 注释: 更新 trade_state['stop_order_id'] 的值。
            trade_state['stop_order_id'] = extract_order_id(stop_order)
            # 注释: 更新 trade_state['stop_order_price'] 的值。
            trade_state['stop_order_price'] = float(stop_price)
            # 注释: 调用 reset_stop_order_refresh_failure_state 执行对应处理。
            reset_stop_order_refresh_failure_state()
            # 注释: 写入运行日志，记录当前处理进度。
            logging.info(
                # 注释: 拼接或传入文本内容。
                f"已更新服务端止损单: id={trade_state['stop_order_id']}, stop={stop_price}, attempt={attempt}"
            # 注释: 结束当前多行结构。
            )
            # 注释: 返回固定结果 True。
            return True
        # 注释: 捕获异常分支：except Exception as e。
        except Exception as e:
            # 注释: 调用 format_exception_message 并保存到 error_text。
            error_text = format_exception_message(e)
            # 注释: 更新 trade_state['last_stop_order_refresh_error'] 的值。
            trade_state['last_stop_order_refresh_error'] = error_text
            # 注释: 判断条件是否成立：is_close_position_conflict_error(e) and attempt < len(retry_delays)。
            if is_close_position_conflict_error(e) and attempt < len(retry_delays):
                # 注释: 调用 fetch_open_close_position_orders 并保存到 matched_orders。
                matched_orders = fetch_open_close_position_orders(side=side)
                # 注释: 初始化 matched_order_ids 列表。
                matched_order_ids = []
                # 注释: 判断条件是否成立：matched_orders is not None。
                if matched_orders is not None:
                    # 注释: 初始化 matched_order_ids 列表。
                    matched_order_ids = [extract_order_id(order) or 'unknown' for order in matched_orders]
                    # 注释: 判断条件是否成立：matched_order_ids。
                    if matched_order_ids:
                        # 注释: 写入警告日志，提示需要关注的非致命问题。
                        logging.warning(
                            # 注释: 拼接或传入文本内容。
                            f"发现导致 closePosition 冲突的未成交条件单，准备撤销后重试: order_ids={matched_order_ids}"
                        # 注释: 结束当前多行结构。
                        )
                        # 注释: 判断条件是否成立：not cancel_protective_stop_order(silent=True)。
                        if not cancel_protective_stop_order(silent=True):
                            # 注释: 写入错误日志，便于排查运行异常。
                            logging.error(
                                # 注释: 拼接或传入文本内容。
                                f"closePosition 冲突恢复失败：撤销旧条件单失败，order_ids={matched_order_ids}"
                            # 注释: 结束当前多行结构。
                            )
                            # 注释: 返回固定结果 False。
                            return False
                        # 注释: 判断条件是否成立：not wait_until_stop_order_disappears(matched_order_ids, side=side)。
                        if not wait_until_stop_order_disappears(matched_order_ids, side=side):
                            # 注释: 写入警告日志，提示需要关注的非致命问题。
                            logging.warning(
                                # 注释: 拼接或传入文本内容。
                                f"closePosition 冲突恢复时旧条件单仍可见，准备继续重试: order_ids={matched_order_ids}"
                            # 注释: 结束当前多行结构。
                            )
                    # 注释: 当前面的条件都不成立时进入该分支。
                    else:
                        # 注释: 写入警告日志，提示需要关注的非致命问题。
                        logging.warning("closePosition 冲突但未查询到具体条件单，准备批量撤销当前交易对未成交单后重试")
                        # 注释: 判断条件是否成立：not cancel_all_open_orders_for_symbol(silent=True)。
                        if not cancel_all_open_orders_for_symbol(silent=True):
                            # 注释: 写入错误日志，便于排查运行异常。
                            logging.error(
                                # 注释: 拼接或传入文本内容。
                                f"closePosition 冲突恢复失败：批量撤销未成交单失败，error={trade_state.get('last_stop_order_refresh_error', '')}"
                            # 注释: 结束当前多行结构。
                            )
                            # 注释: 返回固定结果 False。
                            return False
                        # 注释: 暂停执行一段时间，控制轮询、重试或主循环节奏。
                        time.sleep(max(STOP_ORDER_POST_CANCEL_DELAY_SECONDS, 0.5))
                # 注释: 写入警告日志，提示需要关注的非致命问题。
                logging.warning(
                    # 注释: 拼接或传入文本内容。
                    f"重挂服务端止损单遇到 closePosition 冲突，准备重试: attempt={attempt}, "
                    # 注释: 拼接或传入文本内容。
                    f"stop={stop_price}, open_close_position_orders={matched_order_ids}, error={error_text}"
                # 注释: 结束当前多行结构。
                )
                # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                continue

            # 注释: 写入错误日志，便于排查运行异常。
            logging.error(f"重挂服务端止损单失败: {error_text}")
            # 注释: 返回固定结果 False。
            return False

    # 注释: 返回固定结果 False。
    return False


# 注释: 定义 reset_trade_state_after_external_close 函数，封装对应业务逻辑。
def reset_trade_state_after_external_close(signal_bar_15m='', reason='检测到交易所仓位已关闭', external_context=None):
    """当服务端止损或人工操作已把仓位关掉时，重置本地状态，避免下个循环误操作"""
    # 注释: 计算并保存 external_context。
    external_context = external_context or {}

    # 注释: 调用 external_context.get 并保存到 stop_order_id_for_notify。
    stop_order_id_for_notify = external_context.get('stop_order_id_before_cancel') or trade_state.get('stop_order_id', '')
    # 注释: 调用 external_context.get 并保存到 stop_order_price_for_notify。
    stop_order_price_for_notify = external_context.get('stop_order_price_before_cancel', trade_state.get('stop_order_price', 0.0))
    # 注释: 计算并保存 exit_signal_bar_15m。
    exit_signal_bar_15m = signal_bar_15m or trade_state.get('last_processed_bar_15m', '')
    # 注释: 调用 get_server_time_str 并保存到 detected_time。
    detected_time = get_server_time_str()

    # 外部平仓（服务端止损/人工）不会经过 close_position，因此在这里补写 CSV，避免漏单。
    # 注释: 调用 trade_state.get 并保存到 side。
    side = trade_state.get('side')
    # 注释: 调用 trade_state.get 并保存到 entry_time。
    entry_time = trade_state.get('entry_time', '')
    # 注释: 调用 bool 并保存到 has_trade_snapshot。
    has_trade_snapshot = bool(side) and bool(entry_time)
    # 注释: 计算并保存 estimated_close_price。
    estimated_close_price = None
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 调用 float 并保存到 estimated_close_price。
        estimated_close_price = float(stop_order_price_for_notify)
    # 注释: 捕获异常分支：except Exception。
    except Exception:
        # 注释: 计算并保存 estimated_close_price。
        estimated_close_price = None
    # 注释: 判断条件是否成立：estimated_close_price is None。
    if estimated_close_price is None:
        # 注释: 开始执行可能抛出异常的逻辑。
        try:
            # 注释: 调用 float 并保存到 estimated_close_price。
            estimated_close_price = float(get_latest_price())
        # 注释: 捕获异常分支：except Exception as e。
        except Exception as e:
            # 注释: 写入警告日志，提示需要关注的非致命问题。
            logging.warning(f"外部平仓估算平仓价失败，将按0点数记录: {e}")

    # 注释: 计算并保存 net_pnl_usdt。
    net_pnl_usdt = 0.0
    # 注释: 计算并保存 final_usdt。
    final_usdt = None
    # 注释: 计算并保存 balance_pnl_available。
    balance_pnl_available = False
    # 注释: 计算并保存 is_profit。
    is_profit = False
    # 注释: 调用 float 并保存到 open_fee。
    open_fee = float(trade_state.get('open_fee', 0.0) or 0.0)
    # 注释: 计算并保存 fee_cost。
    fee_cost = open_fee
    # 注释: 判断条件是否成立：has_trade_snapshot。
    if has_trade_snapshot:
        # 注释: 开始执行可能抛出异常的逻辑。
        try:
            # 注释: 调用 exchange.fetch_balance 并保存到 balance_after。
            balance_after = exchange.fetch_balance({'type': 'future'})
            # 注释: 调用 float 并保存到 final_usdt。
            final_usdt = float(balance_after['total']['USDT'])
            # 注释: 调用 float 并保存到 initial_balance。
            initial_balance = float(trade_state.get('initial_balance', 0.0) or 0.0)
            # 注释: 计算并保存 net_pnl_usdt。
            net_pnl_usdt = final_usdt - initial_balance
            # 注释: 计算并保存 balance_pnl_available。
            balance_pnl_available = initial_balance > 0
            # 注释: 计算并保存 is_profit。
            is_profit = net_pnl_usdt > 0
        # 注释: 捕获异常分支：except Exception as e。
        except Exception as e:
            # 注释: 写入警告日志，提示需要关注的非致命问题。
            logging.warning(f"外部平仓读取账户余额失败，将回退到价格估算盈亏: {e}")

    # 注释: 计算并保存 pnl_points。
    pnl_points = 0.0
    # 注释: 调用 float 并保存到 entry_price。
    entry_price = float(trade_state.get('entry_price', 0.0) or 0.0)
    # 注释: 调用 float 并保存到 amount。
    amount = float(trade_state.get('amount', 0.0) or 0.0)
    # 注释: 判断条件是否成立：estimated_close_price is not None and entry_price > 0 and side in ('long', 'short…。
    if estimated_close_price is not None and entry_price > 0 and side in ('long', 'short'):
        # 注释: 判断条件是否成立：side == 'long'。
        if side == 'long':
            # 注释: 计算并保存 pnl_points。
            pnl_points = estimated_close_price - entry_price
        # 注释: 当前面的条件都不成立时进入该分支。
        else:
            # 注释: 计算并保存 pnl_points。
            pnl_points = entry_price - estimated_close_price
        # 注释: 开始执行可能抛出异常的逻辑。
        try:
            # 注释: 调用 get_trading_fee_rate 并保存到 taker_fee_rate。
            taker_fee_rate = get_trading_fee_rate()
            # 注释: 计算并保存 close_fee_est。
            close_fee_est = estimated_close_price * amount * taker_fee_rate
            # 注释: 计算并保存 fee_cost。
            fee_cost = open_fee + close_fee_est
        # 注释: 捕获异常分支：except Exception as e。
        except Exception as e:
            # 注释: 写入警告日志，提示需要关注的非致命问题。
            logging.warning(f"外部平仓估算手续费失败，改为仅记录开仓手续费: {e}")

    # 注释: 判断条件是否成立：not balance_pnl_available。
    if not balance_pnl_available:
        # 注释: 计算并保存 is_profit。
        is_profit = pnl_points > 0

    # 注释: 判断条件是否成立：has_trade_snapshot。
    if has_trade_snapshot:
        # 注释: 计算并保存 close_reason。
        close_reason = f"{reason}（外部平仓检测）"
        # 注释: 调用 log_trade_to_csv 执行对应处理。
        log_trade_to_csv(
            # 注释: 说明当前代码行的作用：entry_time,。
            entry_time,
            # 注释: 说明当前代码行的作用：side,。
            side,
            # 注释: 调用 trade_state.get 执行对应处理。
            trade_state.get('cond_4h', ''),
            # 注释: 调用 trade_state.get 执行对应处理。
            trade_state.get('cond_1h', ''),
            # 注释: 调用 trade_state.get 执行对应处理。
            trade_state.get('cond_15m', ''),
            # 注释: 调用 trade_state.get 执行对应处理。
            trade_state.get('entry_reason', ''),
            # 注释: 说明当前代码行的作用：detected_time,。
            detected_time,
            # 注释: 说明当前代码行的作用：close_reason,。
            close_reason,
            # 注释: 调用 round 执行对应处理。
            round(pnl_points, 4),
            # 注释: 调用 round 执行对应处理。
            round(fee_cost, 4),
            # 注释: 调用 round 执行对应处理。
            round(net_pnl_usdt, 4),
            # 注释: 说明当前代码行的作用：is_profit,。
            is_profit,
            # 注释: 调用 trade_state.get 并保存到 entry_signal_bar_15m。
            entry_signal_bar_15m=trade_state.get('entry_signal_bar_15m', ''),
            # 注释: 计算并保存 exit_signal_bar_15m。
            exit_signal_bar_15m=exit_signal_bar_15m,
            # 注释: 计算并保存 exit_trigger。
            exit_trigger='external_close_detected',
            # 注释: 调用 compute_holding_seconds 并保存到 holding_seconds。
            holding_seconds=compute_holding_seconds(entry_time, detected_time),
            # 注释: 调用 trade_state.get 并保存到 open_order_id。
            open_order_id=trade_state.get('open_order_id', ''),
            # 注释: 调用 str 并保存到 close_order_id。
            close_order_id=str(external_context.get('external_close_order_id', ''))
        # 注释: 结束当前多行结构。
        )

    # 注释: 调用 format_order_id_lines 并保存到 order_id_lines。
    order_id_lines = format_order_id_lines(
        # 注释: 调用 trade_state.get 并保存到 open_order_id。
        open_order_id=trade_state.get('open_order_id', ''),
        # 注释: 计算并保存 stop_order_id。
        stop_order_id=stop_order_id_for_notify
    # 注释: 结束当前多行结构。
    )
    # 注释: 计算并保存 order_id_suffix。
    order_id_suffix = f"\n{order_id_lines}" if order_id_lines else ''
    # 注释: 初始化 estimated_close_price_text 元组或多行表达式。
    estimated_close_price_text = (
        # 注释: 拼接或传入文本内容。
        f"{estimated_close_price:.4f}" if estimated_close_price is not None else "未知"
    # 注释: 结束当前多行结构。
    )
    # 注释: 计算并保存 final_usdt_text。
    final_usdt_text = f"{final_usdt:.4f}" if final_usdt is not None else "未知"
    # 注释: 计算并保存 pnl_points_text。
    pnl_points_text = f"{pnl_points:.2f}"
    # 注释: 计算并保存 fee_cost_text。
    fee_cost_text = f"{fee_cost:.4f}"
    # 注释: 计算并保存 net_pnl_text。
    net_pnl_text = f"{net_pnl_usdt:.2f}"
    # 注释: 发送邮件通知当前交易事件。
    send_msg(
        # 注释: 拼接或传入文本内容。
        "ETH交易: ⚠️检测到外部平仓",
        # 注释: 拼接或传入文本内容。
        f"原因: {reason}\n"
        # 注释: 拼接或传入文本内容。
        f"检测时间: {detected_time}\n"
        # 注释: 拼接或传入文本内容。
        f"方向: {trade_state.get('side')}\n"
        # 注释: 拼接或传入文本内容。
        f"入场时间: {trade_state.get('entry_time', '')}\n"
        # 注释: 拼接或传入文本内容。
        f"入场价: {trade_state.get('entry_price', 0)}\n"
        # 注释: 拼接或传入文本内容。
        f"估算出场价: {estimated_close_price_text}\n"
        # 注释: 拼接或传入文本内容。
        f"点数盈亏: {pnl_points_text}\n"
        # 注释: 拼接或传入文本内容。
        f"手续费: {fee_cost_text}\n"
        # 注释: 拼接或传入文本内容。
        f"净利润(USDT): {net_pnl_text}\n"
        # 注释: 拼接或传入文本内容。
        f"平仓后账户资金(USDT): {final_usdt_text}\n"
        # 注释: 拼接或传入文本内容。
        f"触发周期: {trade_state.get('entry_trigger_tf', '')}\n"
        # 注释: 拼接或传入文本内容。
        f"本地止损价: {trade_state.get('stop_loss_price', 0)}\n"
        # 注释: 拼接或传入文本内容。
        f"服务端止损价: {stop_order_price_for_notify}\n"
        # 注释: 拼接或传入文本内容。
        f"持仓数量: {trade_state.get('amount', 0)}\n"
        # 注释: 拼接或传入文本内容。
        f"开仓原因: {trade_state.get('entry_reason', '')}\n"
        # 注释: 拼接或传入文本内容。
        f"15M信号时间: {exit_signal_bar_15m}\n"
        # 注释: 拼接或传入文本内容。
        f"说明: 已检测到交易所无仓位，可能是服务端止损成交或人工平仓。"
        # 注释: 拼接或传入文本内容。
        f"{order_id_suffix}"
    # 注释: 结束当前多行结构。
    )

    # 注释: 计算并保存 post_exit_processed_bar_15m。
    post_exit_processed_bar_15m = exit_signal_bar_15m or trade_state.get('last_processed_bar_15m', '')
    # 注释: 批量更新本地交易状态。
    trade_state.update({
        # 注释: 设置 has_position 字段的取值。
        'has_position': False,
        # 注释: 设置 side 字段的取值。
        'side': None,
        # 注释: 设置 entry_price 字段的取值。
        'entry_price': 0,
        # 注释: 设置 stop_loss_price 字段的取值。
        'stop_loss_price': 0,
        # 注释: 设置 highest_price 字段的取值。
        'highest_price': 0,
        # 注释: 设置 lowest_price 字段的取值。
        'lowest_price': 0,
        # 注释: 设置 amount 字段的取值。
        'amount': 0,
        # 注释: 设置 entry_time 字段的取值。
        'entry_time': '',
        # 注释: 设置 cond_4h 字段的取值。
        'cond_4h': '',
        # 注释: 设置 cond_1h 字段的取值。
        'cond_1h': '',
        # 注释: 设置 cond_15m 字段的取值。
        'cond_15m': '',
        # 注释: 设置 close_cond_4h 字段的取值。
        'close_cond_4h': '',
        # 注释: 设置 close_cond_1h 字段的取值。
        'close_cond_1h': '',
        # 注释: 设置 close_cond_15m 字段的取值。
        'close_cond_15m': '',
        # 注释: 设置 entry_reason 字段的取值。
        'entry_reason': '',
        # 注释: 设置 entry_trigger_tf 字段的取值。
        'entry_trigger_tf': '',
        # 注释: 设置 entry_strategy_tf 字段的取值。
        'entry_strategy_tf': '',
        # 注释: 设置 entry_exit_tf 字段的取值。
        'entry_exit_tf': '',
        # 注释: 设置 entry_sr_target 字段的取值。
        'entry_sr_target': 0.0,
        # 注释: 设置 entry_initial_risk 字段的取值。
        'entry_initial_risk': 0.0,
        # 注释: 设置 partial_taken 字段的取值。
        'partial_taken': False,
        # 注释: 设置 last_exit_time 字段的取值。
        'last_exit_time': detected_time,
        # 注释: 设置 initial_balance 字段的取值。
        'initial_balance': 0.0,
        # 注释: 设置 open_fee 字段的取值。
        'open_fee': 0.0,
        # 注释: 设置 open_order_id 字段的取值。
        'open_order_id': '',
        # 注释: 设置 close_order_id 字段的取值。
        'close_order_id': '',
        # 注释: 设置 liquidation_price 字段的取值。
        'liquidation_price': 0.0,
        # 注释: 设置 stop_order_id 字段的取值。
        'stop_order_id': '',
        # 注释: 设置 stop_order_price 字段的取值。
        'stop_order_price': 0.0,
        # 注释: 设置 stop_order_refresh_fail_count 字段的取值。
        'stop_order_refresh_fail_count': 0,
        # 注释: 设置 last_stop_order_refresh_error 字段的取值。
        'last_stop_order_refresh_error': '',
        # 注释: 设置 entry_signal_bar_15m 字段的取值。
        'entry_signal_bar_15m': '',
        # 注释: 设置 last_exit_bar_15m 字段的取值。
        'last_exit_bar_15m': exit_signal_bar_15m,
        # 注释: 设置 last_processed_bar_15m 字段的取值。
        'last_processed_bar_15m': post_exit_processed_bar_15m,
        # 注释: 设置 position_miss_count 字段的取值。
        'position_miss_count': 0
    # 注释: 结束当前多行结构。
    })
    # 注释: 写入警告日志，提示需要关注的非致命问题。
    logging.warning(reason)


# 注释: 定义 compute_holding_seconds 函数，封装对应业务逻辑。
def compute_holding_seconds(entry_time, exit_time):
    """计算持仓秒数，便于复盘"""
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 调用 datetime.datetime.strptime 并保存到 entry_dt。
        entry_dt = datetime.datetime.strptime(entry_time, BAR_TIME_FORMAT)
        # 注释: 调用 datetime.datetime.strptime 并保存到 exit_dt。
        exit_dt = datetime.datetime.strptime(exit_time, BAR_TIME_FORMAT)
        # 注释: 返回计算结果：int((exit_dt - entry_dt).total_seconds())。
        return int((exit_dt - entry_dt).total_seconds())
    # 注释: 捕获异常分支：except Exception。
    except Exception:
        # 注释: 返回计算结果：0。
        return 0


# 注释: 定义 normalize_mail_value 函数，封装对应业务逻辑。
def normalize_mail_value(value):
    """把 numpy 标量转成普通 Python 值，避免邮件里出现 np.True_ 这类文本。"""
    # 注释: 判断条件是否成立：hasattr(value, 'item')。
    if hasattr(value, 'item'):
        # 注释: 开始执行可能抛出异常的逻辑。
        try:
            # 注释: 返回计算结果：value.item()。
            return value.item()
        # 注释: 捕获异常分支：except Exception。
        except Exception:
            # 注释: 返回计算结果：value。
            return value
    # 注释: 返回计算结果：value。
    return value


# 注释: 定义 format_mail_bool 函数，封装对应业务逻辑。
def format_mail_bool(value):
    """把布尔值压缩成更易读的中文。"""
    # 注释: 返回计算结果：'是' if bool(normalize_mail_value(value)) else '否'。
    return '是' if bool(normalize_mail_value(value)) else '否'


# 注释: 定义 format_mail_scalar 函数，封装对应业务逻辑。
def format_mail_scalar(value):
    """把数字和值格式化成适合邮件阅读的短文本。"""
    # 注释: 调用 normalize_mail_value 并保存到 value。
    value = normalize_mail_value(value)
    # 注释: 判断条件是否成立：isinstance(value, bool)。
    if isinstance(value, bool):
        # 注释: 返回计算结果：format_mail_bool(value)。
        return format_mail_bool(value)
    # 注释: 判断条件是否成立：isinstance(value, int)。
    if isinstance(value, int):
        # 注释: 返回计算结果：str(value)。
        return str(value)
    # 注释: 判断条件是否成立：isinstance(value, float)。
    if isinstance(value, float):
        # 注释: 判断条件是否成立：value.is_integer()。
        if value.is_integer():
            # 注释: 返回计算结果：str(int(value))。
            return str(int(value))
        # 注释: 返回计算结果：f"{value:.4f}".rstrip('0').rstrip('.')。
        return f"{value:.4f}".rstrip('0').rstrip('.')
    # 注释: 返回计算结果：str(value)。
    return str(value)


# 注释: 定义 format_mail_checks 函数，封装对应业务逻辑。
def format_mail_checks(checks, label_map=None, default_labels=None):
    """把 dict/list 形式的条件检查压缩成一行短文本。"""
    # 注释: 计算并保存 label_map。
    label_map = label_map or {}

    # 注释: 判断条件是否成立：isinstance(checks, dict)。
    if isinstance(checks, dict):
        # 注释: 初始化 normalized 字典。
        normalized = {}
        # 注释: 遍历 checks.items()，逐项处理 key, value。
        for key, value in checks.items():
            # 注释: 调用 normalize_mail_value 并保存到 value。
            value = normalize_mail_value(value)
            # 注释: 判断条件是否成立：isinstance(value, dict)。
            if isinstance(value, dict):
                # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                continue
            # 注释: 更新 normalized[key] 的值。
            normalized[key] = value

        # 注释: 初始化 parts 列表。
        parts = []
        # 注释: 调用 normalized.get 并保存到 score。
        score = normalized.get('score')
        # 注释: 调用 normalized.get 并保存到 threshold。
        threshold = normalized.get('threshold')
        # 注释: 判断条件是否成立：score is not None and threshold is not None。
        if score is not None and threshold is not None:
            # 注释: 初始化 parts.append(f"得分 字典。
            parts.append(f"得分={format_mail_scalar(score)}/{format_mail_scalar(threshold)}")

        # 注释: 遍历 normalized.items()，逐项处理 key, value。
        for key, value in normalized.items():
            # 注释: 判断条件是否成立：key in ('score', 'threshold')。
            if key in ('score', 'threshold'):
                # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                continue
            # 注释: 调用 label_map.get 并保存到 label。
            label = label_map.get(key, key)
            # 注释: 初始化 parts.append(f"{label} 字典。
            parts.append(f"{label}={format_mail_scalar(value)}")
        # 注释: 返回计算结果：'，'.join(parts) if parts else '无'。
        return '，'.join(parts) if parts else '无'

    # 注释: 判断条件是否成立：isinstance(checks, (list, tuple))。
    if isinstance(checks, (list, tuple)):
        # 注释: 计算并保存 labels。
        labels = default_labels or [f'条件{i + 1}' for i in range(len(checks))]
        # 注释: 初始化 parts 列表。
        parts = []
        # 注释: 遍历 enumerate(checks)，逐项处理 idx, value。
        for idx, value in enumerate(checks):
            # 注释: 计算并保存 label。
            label = labels[idx] if idx < len(labels) else f'条件{idx + 1}'
            # 注释: 初始化 parts.append(f"{label} 字典。
            parts.append(f"{label}={format_mail_scalar(value)}")
        # 注释: 返回计算结果：'，'.join(parts) if parts else '无'。
        return '，'.join(parts) if parts else '无'

    # 注释: 判断条件是否成立：checks in (None, '')。
    if checks in (None, ''):
        # 注释: 返回计算结果：'无'。
        return '无'
    # 注释: 返回计算结果：format_mail_scalar(checks)。
    return format_mail_scalar(checks)


# 注释: 定义 format_entry_condition_for_mail 函数，封装对应业务逻辑。
def format_entry_condition_for_mail(state, side_dir, entry_reason=''):
    """新版策略只写入 summary，避免旧策略条件混入复盘。"""
    # 注释: 判断条件是否成立：not isinstance(state, dict)。
    if not isinstance(state, dict):
        # 注释: 返回计算结果：'无状态数据'。
        return '无状态数据'
    # 注释: 判断条件是否成立：state.get('summary')。
    if state.get('summary'):
        # 注释: 返回计算结果：str(state.get('summary'))。
        return str(state.get('summary'))
    # 注释: 返回计算结果：state.get('status', '') or ''。
    return state.get('status', '') or ''


# 注释: 定义 format_condition_snapshot_for_mail 函数，封装对应业务逻辑。
def format_condition_snapshot_for_mail(timeframe, state):
    """把新版趋势状态压缩成简洁快照，便于写入通知和复盘。"""
    # 注释: 判断条件是否成立：not isinstance(state, dict)。
    if not isinstance(state, dict):
        # 注释: 返回计算结果：f"{timeframe}: 无状态数据"。
        return f"{timeframe}: 无状态数据"
    # 注释: 判断条件是否成立：state.get('summary')。
    if state.get('summary'):
        # 注释: 返回计算结果：f"{timeframe}: {state.get('summary')}"。
        return f"{timeframe}: {state.get('summary')}"
    # 注释: 返回计算结果：f"{timeframe}信号时间={state.get('signal_bar_time', '')}"。
    return f"{timeframe}信号时间={state.get('signal_bar_time', '')}"


# 注释: 定义 log_trade_to_csv 函数，封装对应业务逻辑。
def log_trade_to_csv(entry_time, side, cond_4h, cond_1h, cond_15m, entry_reason, exit_time, close_reason, pnl_points, fee_cost, net_pnl_usdt, is_profit, entry_signal_bar_15m='', exit_signal_bar_15m='', exit_trigger='', holding_seconds=0, open_order_id='', close_order_id=''):
    """将交易记录写入CSV文件，按月分表"""
    # 根据平仓时间生成当月的 CSV 文件名，例如 trades_log_2024-05.csv
    month_str = exit_time[:7]  # 提取 'YYYY-MM' 部分
    # 注释: 调用 os.path.join 并保存到 filename。
    filename = os.path.join(BASE_DIR, f'trades_log_{month_str}.csv')
    
    # 注释: 调用 os.path.isfile 并保存到 file_exists。
    file_exists = os.path.isfile(filename)
    # 注释: 判断条件是否成立：file_exists and not ensure_trade_csv_schema(filename)。
    if file_exists and not ensure_trade_csv_schema(filename):
        # 注释: 直接结束当前函数。
        return
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 使用上下文管理器处理资源：open(filename, mode='a', newline='', encoding='utf-8-sig') as f。
        with open(filename, mode='a', newline='', encoding='utf-8-sig') as f:
            # 注释: 调用 csv.writer 并保存到 writer。
            writer = csv.writer(f)
            # 注释: 判断条件是否成立：not file_exists。
            if not file_exists:
                # 写入表头
                # 这里特意新增“入场原因”一列，方便后面复盘开仓来源
                # 注释: 调用 writer.writerow 执行对应处理。
                writer.writerow(TRADE_CSV_HEADERS)
            # 写入具体数据
            # 注释: 调用 writer.writerow 执行对应处理。
            writer.writerow([
                # 注释: 说明当前代码行的作用：entry_time, side, cond_4h, cond_1h, cond_15m, entry_reason,。
                entry_time, side, cond_4h, cond_1h, cond_15m, entry_reason,
                # 注释: 说明当前代码行的作用：exit_time, close_reason, pnl_points, fee_cost, net_pnl_usdt, is_profit,。
                exit_time, close_reason, pnl_points, fee_cost, net_pnl_usdt, is_profit,
                # 注释: 说明当前代码行的作用：entry_signal_bar_15m, exit_signal_bar_15m, exit_trigger, holding_seconds,。
                entry_signal_bar_15m, exit_signal_bar_15m, exit_trigger, holding_seconds,
                # 注释: 说明当前代码行的作用：open_order_id, close_order_id。
                open_order_id, close_order_id
            # 注释: 结束当前多行结构。
            ])
        # 注释: 调用 update_daily_pnl_stats 执行对应处理。
        update_daily_pnl_stats(exit_time, net_pnl_usdt)
    # 注释: 捕获异常分支：except Exception as e。
    except Exception as e:
        # 注释: 写入错误日志，便于排查运行异常。
        logging.error(f"写入CSV失败: {e}")


# ==========================================
# 3. 核心逻辑：趋势、入场、监控
# ==========================================

def open_order(side, price, sl_price, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='trend', entry_trigger_tf='', entry_meta=None):  # 定义执行开仓指令的函数，接收方向、当前价格、止损价格和各级别状态
    """执行开仓指令"""  # 函数的文档字符串
    global trade_state  # 声明trade_state为全局变量，以便修改它的状态
    # 注释: 计算并保存 entry_meta。
    entry_meta = entry_meta or {}
    amount = calculate_amount(price)  # 调用calculate_amount函数，计算本次下单的数量
    # 注释: 调用 float 并保存到 amount。
    amount = float(amount)
    # 注释: 判断条件是否成立：amount == 0。
    if amount == 0:
        return False  # 如果计算出的下单数量为0（可能余额不足或出错），则直接返回，不开仓
    # 注释: 计算并保存 open_order_id。
    open_order_id = ''

    # 开仓前先用“保守估算强平价”预校验止损，防止止损本来就落到强平外面
    # 注释: 调用 ensure_stop_price_safe 并保存到 estimated_safe_stop, estimated_stop_meta。
    estimated_safe_stop, estimated_stop_meta = ensure_stop_price_safe(price, sl_price, side, liquidation_price=None)
    # 注释: 判断条件是否成立：not stop_price_is_still_valid(price, estimated_safe_stop, side)。
    if not stop_price_is_still_valid(price, estimated_safe_stop, side):
        # 注释: 写入警告日志，提示需要关注的非致命问题。
        logging.warning(
            # 注释: 拼接或传入文本内容。
            f"拒绝开仓：预估强平价过近，止损无效 side={side}, "
            # 注释: 拼接或传入文本内容。
            f"entry={price}, stop={sl_price}, adjusted_stop={estimated_safe_stop}, meta={estimated_stop_meta}"
        # 注释: 结束当前多行结构。
        )
        # 注释: 发送邮件通知当前交易事件。
        send_msg(
            # 注释: 拼接或传入文本内容。
            "ETH交易: ⚠️开仓已拒绝",
            # 注释: 拼接或传入文本内容。
            f"原因: 止损价可能落在强平价外侧\n方向: {side}\n原始止损: {sl_price}\n"
            # 注释: 拼接或传入文本内容。
            f"修正后止损: {estimated_safe_stop}\n估算强平价: {estimated_stop_meta.get('liquidation_price')}"
        # 注释: 结束当前多行结构。
        )
        # 注释: 返回固定结果 False。
        return False

    try:  # 尝试执行开仓流程
        # 获取开仓前的可用余额，用于后续平仓时计算精确净利润
        # 注释: 调用 exchange.fetch_balance 并保存到 balance_before。
        balance_before = exchange.fetch_balance({'type': 'future'})
        # 注释: 调用 float 并保存到 initial_usdt。
        initial_usdt = float(balance_before['total']['USDT'])
        
        # 设置杠杆
        exchange.set_leverage(LEVERAGE, SYMBOL)  # 调用交易所API设置当前交易对的杠杆倍数

        # 市价单开仓
        order_side = 'buy' if side == 'long' else 'sell'  # 判断下单方向：做多为'buy'，做空为'sell'
        order = exchange.create_market_order(SYMBOL, order_side, amount)  # 调用API发送市价开仓订单
        # 注释: 调用 extract_order_id 并保存到 open_order_id。
        open_order_id = extract_order_id(order)

        # 记录实际成交均价 (如有滑点以实际为准)
        actual_price = order.get('average', price)  # 尝试从订单结果中获取实际成交均价，如果没有则使用传入的理论价格
        if actual_price is None or actual_price == 0: actual_price = price  # 防止获取到的average为None，作为保底

        # 成交后再拿交易所真实仓位风险信息，获取真正的强平价
        # 注释: 调用 get_position_risk 并保存到 position_risk。
        position_risk = get_position_risk(side=side)
        # 注释: 计算并保存 actual_liquidation_price。
        actual_liquidation_price = None
        # 注释: 判断条件是否成立：position_risk and not position_risk.get('fetch_failed')。
        if position_risk and not position_risk.get('fetch_failed'):
            # 注释: 计算并保存 actual_liquidation_price。
            actual_liquidation_price = position_risk['liquidation_price']
        # 注释: 调用 ensure_stop_price_safe 并保存到 actual_safe_stop, actual_stop_meta。
        actual_safe_stop, actual_stop_meta = ensure_stop_price_safe(actual_price, estimated_safe_stop, side, liquidation_price=actual_liquidation_price)
        # 注释: 判断条件是否成立：not stop_price_is_still_valid(actual_price, actual_safe_stop, side)。
        if not stop_price_is_still_valid(actual_price, actual_safe_stop, side):
            # 注释: 写入错误日志，便于排查运行异常。
            logging.error(
                # 注释: 拼接或传入文本内容。
                f"开仓后发现真实强平价过近，立即撤退 side={side}, entry={actual_price}, "
                # 注释: 拼接或传入文本内容。
                f"stop={actual_safe_stop}, liq={actual_liquidation_price}, meta={actual_stop_meta}"
            # 注释: 结束当前多行结构。
            )
            # 先反向市价平掉刚开的仓位，避免把仓位裸露在强平附近
            # 注释: 说明当前代码行的作用：panic_side = 'sell' if side == 'long' else 'buy'。
            panic_side = 'sell' if side == 'long' else 'buy'
            # 注释: 调用 exchange.create_market_order 并保存到 panic_close_order。
            panic_close_order = exchange.create_market_order(SYMBOL, panic_side, amount)
            # 注释: 调用 extract_order_id 并保存到 panic_close_order_id。
            panic_close_order_id = extract_order_id(panic_close_order)
            # 注释: 调用 format_order_id_lines 并保存到 order_id_lines。
            order_id_lines = format_order_id_lines(
                # 注释: 计算并保存 open_order_id。
                open_order_id=open_order_id,
                # 注释: 计算并保存 close_order_id。
                close_order_id=panic_close_order_id
            # 注释: 结束当前多行结构。
            )
            # 注释: 计算并保存 order_id_suffix。
            order_id_suffix = f"\n{order_id_lines}" if order_id_lines else ''
            # 注释: 发送邮件通知当前交易事件。
            send_msg(
                # 注释: 拼接或传入文本内容。
                "ETH交易: ⚠️开仓后立即撤退",
                # 注释: 拼接或传入文本内容。
                f"原因: 真实强平价过近，止损无安全空间\n方向: {side}\n"
                # 注释: 拼接或传入文本内容。
                f"入场价: {actual_price}\n真实强平价: {actual_liquidation_price}\n修正后止损: {actual_safe_stop}"
                # 注释: 拼接或传入文本内容。
                f"{order_id_suffix}"
            # 注释: 结束当前多行结构。
            )
            # 注释: 返回固定结果 False。
            return False

        # 计算开仓手续费 (参考 test.py 方式)
        # 注释: 调用 get_trading_fee_rate 并保存到 taker_fee_rate。
        taker_fee_rate = get_trading_fee_rate()
        # 注释: 计算并保存 open_fee。
        open_fee = actual_price * amount * taker_fee_rate
        # 注释: 向控制台输出当前运行信息。
        print("openfree:",open_fee)

        # 提取各个级别的具体成立条件字符串
        # 注释: 定义 format_cond 函数，封装对应业务逻辑。
        def format_cond(state, side_dir):
            # 注释: 返回计算结果：format_entry_condition_for_mail(state, side_dir, entry_reason)。
            return format_entry_condition_for_mail(state, side_dir, entry_reason)

        # 注释: 定义 build_cond_str 函数，封装对应业务逻辑。
        def build_cond_str(state, side_dir):
            # 注释: 调用 format_cond 并保存到 condensed。
            condensed = format_cond(state, side_dir)
            # 注释: 返回计算结果：f"原因:{entry_reason} | {condensed}" if condensed else f"原因:{entry_reason}"。
            return f"原因:{entry_reason} | {condensed}" if condensed else f"原因:{entry_reason}"
                
        # 这三段字符串后面会一起写进 trade_state 和 CSV，方便复盘每次进场的上下文
        # 注释: 调用 build_cond_str 并保存到 cond_4h_str。
        cond_4h_str = build_cond_str(state_4h, side)
        # 注释: 调用 build_cond_str 并保存到 cond_1h_str。
        cond_1h_str = build_cond_str(state_1h, side)
        # 注释: 调用 build_cond_str 并保存到 cond_15m_str。
        cond_15m_str = build_cond_str(state_15m, side)
        # 注释: 计算并保存 entry_trigger_tf_display。
        entry_trigger_tf_display = entry_trigger_tf or '4H+1H+15M'

        # 注释: 初始化 open_condition_lines 列表。
        open_condition_lines = []
        # 注释: 判断条件是否成立：format_cond(state_4h, side)。
        if format_cond(state_4h, side):
            # 注释: 调用 open_condition_lines.append 执行对应处理。
            open_condition_lines.append(f"4H({state_4h.get('signal_bar_time', '')}): {cond_4h_str}")
        # 注释: 判断条件是否成立：format_cond(state_1h, side)。
        if format_cond(state_1h, side):
            # 注释: 调用 open_condition_lines.append 执行对应处理。
            open_condition_lines.append(f"1H({state_1h.get('signal_bar_time', '')}): {cond_1h_str}")
        # 注释: 判断条件是否成立：format_cond(state_15m, side)。
        if format_cond(state_15m, side):
            # 注释: 调用 open_condition_lines.append 执行对应处理。
            open_condition_lines.append(f"15M({state_15m.get('signal_bar_time', '')}): {cond_15m_str}")
        # 注释: 计算并保存 open_condition_details。
        open_condition_details = '\n'.join(open_condition_lines) if open_condition_lines else f"原因:{entry_reason}"
        
        # 注释: 调用 get_server_time_str 并保存到 entry_time。
        entry_time = get_server_time_str()

        trade_state.update({  # 更新全局交易状态字典，标记已持仓
            'has_position': True,  # 状态改为已持仓
            'side': side,  # 记录持仓方向
            'entry_price': actual_price,  # 记录实际开仓均价
            'stop_loss_price': actual_safe_stop,  # 记录校验强平价后最终可用的止损价格
            'highest_price': actual_price,  # 将最高价初始化为入场价
            'lowest_price': actual_price,  # 将最低价初始化为入场价
            'amount': amount,  # 记录持仓数量
            'entry_time': entry_time,  # 记录实际开仓时间
            'cond_4h': cond_4h_str,  # 记录4H当时为什么成立
            'cond_1h': cond_1h_str,  # 记录1H当时为什么成立
            'cond_15m': cond_15m_str,  # 记录15M当时为什么成立
            # 注释: 设置 close_cond_4h 字段的取值。
            'close_cond_4h': '',
            # 注释: 设置 close_cond_1h 字段的取值。
            'close_cond_1h': '',
            # 注释: 设置 close_cond_15m 字段的取值。
            'close_cond_15m': '',
            'entry_reason': entry_reason,  # 记录这笔单的开仓来源
            'entry_trigger_tf': entry_trigger_tf_display,  # 记录真正触发本次开仓的周期
            # 注释: 设置 entry_strategy_tf 字段的取值。
            'entry_strategy_tf': entry_meta.get('strategy_tf', ''),
            # 注释: 设置 entry_exit_tf 字段的取值。
            'entry_exit_tf': entry_meta.get('exit_tf', ''),
            # 注释: 设置 entry_sr_target 字段的取值。
            'entry_sr_target': float(entry_meta.get('sr_target', 0.0) or 0.0),
            # 注释: 设置 entry_initial_risk 字段的取值。
            'entry_initial_risk': abs(float(actual_price) - float(actual_safe_stop)),
            # 注释: 设置 partial_taken 字段的取值。
            'partial_taken': False,
            # 注释: 设置 initial_balance 字段的取值。
            'initial_balance': initial_usdt,
            # 注释: 设置 open_fee 字段的取值。
            'open_fee': open_fee,
            # 注释: 设置 open_order_id 字段的取值。
            'open_order_id': open_order_id,
            # 注释: 设置 close_order_id 字段的取值。
            'close_order_id': '',
            # 注释: 设置 liquidation_price 字段的取值。
            'liquidation_price': actual_liquidation_price or 0.0,
            # 注释: 设置 stop_order_id 字段的取值。
            'stop_order_id': '',
            # 注释: 设置 stop_order_price 字段的取值。
            'stop_order_price': 0.0,
            # 注释: 设置 stop_order_refresh_fail_count 字段的取值。
            'stop_order_refresh_fail_count': 0,
            # 注释: 设置 last_stop_order_refresh_error 字段的取值。
            'last_stop_order_refresh_error': '',
            # 注释: 设置 entry_signal_bar_15m 字段的取值。
            'entry_signal_bar_15m': signal_bar_15m,
            # 注释: 设置 last_entry_bar_15m 字段的取值。
            'last_entry_bar_15m': signal_bar_15m,
            # 注释: 设置 position_miss_count 字段的取值。
            'position_miss_count': 0
        # 注释: 结束当前多行结构。
        })

        # 开仓成功后立刻把服务端止损单挂上，真正的触发由交易所负责，不依赖本地轮询
        # 注释: 判断条件是否成立：not refresh_protective_stop_order(actual_safe_stop)。
        if not refresh_protective_stop_order(actual_safe_stop):
            # 注释: 写入错误日志，便于排查运行异常。
            logging.error("服务端止损单挂单失败，立即主动平仓避免裸奔风险")
            # 注释: 计算并保存 close_position("服务端止损挂单失败，主动平仓", curr_price。
            close_position("服务端止损挂单失败，主动平仓", curr_price=actual_price, signal_bar_15m=signal_bar_15m, trigger_label="服务端止损挂单失败")
            # 注释: 返回固定结果 False。
            return False

        # 注释: 调用 format_order_id_lines 并保存到 order_id_lines。
        order_id_lines = format_order_id_lines(
            # 注释: 计算并保存 open_order_id。
            open_order_id=open_order_id,
            # 注释: 调用 trade_state.get 并保存到 stop_order_id。
            stop_order_id=trade_state.get('stop_order_id', '')
        # 注释: 结束当前多行结构。
        )
        # 注释: 计算并保存 order_id_suffix。
        order_id_suffix = f"\n{order_id_lines}" if order_id_lines else ''
        msg = (f"🚀 【已开仓】\n方向: {side}\n入场价: {actual_price}\n"  # 构建通知邮件的内容字符串，包含方向和入场价
               # 注释: 拼接或传入文本内容。
               f"止损价: {actual_safe_stop}\n强平价: {actual_liquidation_price}\n数量: {amount}\n"
               # 注释: 拼接或传入文本内容。
               f"杠杆: {LEVERAGE}x\n开仓前账户资金(USDT): {initial_usdt:.4f}\n入场原因: {entry_reason}\n触发周期: {entry_trigger_tf_display}\n15M信号时间: {signal_bar_15m}\n"
               # 注释: 拼接或传入文本内容。
               f"开仓条件明细:\n{open_condition_details}"
               f"{order_id_suffix}")  # 继续构建字符串，包含止损价、数量和杠杆倍数
        send_msg(f"ETH交易: 开仓 {side}", msg)  # 调用发邮件函数，发送开仓通知
        # 注释: 写入运行日志，记录当前处理进度。
        logging.info(
            # 注释: 拼接或传入文本内容。
            f"开仓成功: {side} at {actual_price}, SL: {actual_safe_stop}, "
            # 注释: 拼接或传入文本内容。
            f"liq={actual_liquidation_price}, reason={entry_reason}, "
            # 注释: 拼接或传入文本内容。
            f"open_order_id={open_order_id}, stop_order_id={trade_state.get('stop_order_id', '')}, stop_meta={actual_stop_meta}"
        )  # 在系统日志中记录开仓成功的详细信息
        # 注释: 返回固定结果 True。
        return True

    except Exception as e:  # 捕获开仓过程中的所有异常
        # 注释: 计算并保存 error_msg。
        error_msg = f"开仓失败: {e}"
        # 注释: 调用 format_order_id_lines 并保存到 order_id_lines。
        order_id_lines = format_order_id_lines(open_order_id=open_order_id)
        # 注释: 判断条件是否成立：order_id_lines。
        if order_id_lines:
            # 注释: 计算并保存 error_msg。
            error_msg = f"{error_msg}\n{order_id_lines}"
        # 注释: 写入错误日志，便于排查运行异常。
        logging.error(error_msg)
        # 注释: 写入错误日志，便于排查运行异常。
        logging.error(traceback.format_exc())
        # 可以加上邮件通知
        # 注释: 发送邮件通知当前交易事件。
        send_msg("ETH交易: ⚠️开仓失败警告", error_msg)
        # 注释: 返回固定结果 False。
        return False


def close_position(reason, curr_price=None, signal_bar_15m='', trigger_label=''):  # 定义平仓函数，接收平仓原因和当前触发价格
    """执行平仓指令"""  # 函数的文档字符串
    global trade_state  # 声明全局变量，以便在平仓后重置状态
    # 平仓方向与持仓方向相反
    side = 'sell' if trade_state['side'] == 'long' else 'buy'  # 确定平仓的订单方向：多单平仓为'sell'，空单平仓为'buy'
    # 注释: 计算并保存 close_order_id。
    close_order_id = ''
    # 注释: 开始执行可能抛出异常的逻辑。
    try:  
        # 注释: 判断条件是否成立：curr_price is None。
        if curr_price is None:
            # 注释: 调用 get_latest_price 并保存到 curr_price。
            curr_price = get_latest_price()
        # 在主动市价平仓前先撤掉旧的服务端止损单，避免平仓后残留条件单
        # 注释: 计算并保存 cancel_protective_stop_order(silent。
        cancel_protective_stop_order(silent=True)
        # 尝试执行平仓交易流程
        order = exchange.create_market_order(SYMBOL, side, trade_state['amount'])  # 调用API发送市价单，按照持仓数量全部平仓
        # 注释: 调用 extract_order_id 并保存到 close_order_id。
        close_order_id = extract_order_id(order)
        # 注释: 更新 trade_state['close_order_id'] 的值。
        trade_state['close_order_id'] = close_order_id
        
        # 为了获取绝对准确的净利润，平仓后再次查询账户余额
        # (稍微休眠一下等待交易所结算完成)
        # 注释: 暂停执行一段时间，控制轮询、重试或主循环节奏。
        time.sleep(1)
        # 注释: 调用 exchange.fetch_balance 并保存到 balance_after。
        balance_after = exchange.fetch_balance({'type': 'future'})
        # 注释: 调用 float 并保存到 final_usdt。
        final_usdt = float(balance_after['total']['USDT'])
        
        # 净利润 = 平仓后的总余额 - 开仓前的总余额
        # 注释: 计算并保存 net_pnl_usdt。
        net_pnl_usdt = final_usdt - trade_state['initial_balance']
        
        # 从订单结果中获取实际平仓均价和手续费
        # 注释: 调用 order.get 并保存到 actual_close_price。
        actual_close_price = order.get('average', curr_price)
        # 注释: 判断条件是否成立：actual_close_price is None or actual_close_price == 0。
        if actual_close_price is None or actual_close_price == 0:
            # 注释: 计算并保存 actual_close_price。
            actual_close_price = curr_price
            
        # 计算平仓手续费 (参考 test.py 方式)
        # 注释: 调用 get_trading_fee_rate 并保存到 taker_fee_rate。
        taker_fee_rate = get_trading_fee_rate()
        # 注释: 计算并保存 close_fee。
        close_fee = actual_close_price * trade_state['amount'] * taker_fee_rate
        
        # 总手续费 = 开仓手续费 + 平仓手续费
        # 注释: 计算并保存 fee_cost。
        fee_cost = trade_state['open_fee'] + close_fee
        
        # 计算点数盈亏 (用于展示和记录)
        if trade_state['side'] == 'long':  # 如果原持仓是多单
            pnl_points = actual_close_price - trade_state['entry_price']  # 盈亏 = 现价 - 开仓价
        else:  # 如果原持仓是空单
            pnl_points = trade_state['entry_price'] - actual_close_price  # 盈亏 = 开仓价 - 现价

        # 注释: 计算并保存 is_profit。
        is_profit = net_pnl_usdt > 0
        # 注释: 调用 get_server_time_str 并保存到 exit_time。
        exit_time = get_server_time_str()
        # 注释: 调用 compute_holding_seconds 并保存到 holding_seconds。
        holding_seconds = compute_holding_seconds(trade_state['entry_time'], exit_time)
        # 注释: 计算并保存 exit_signal_bar_15m。
        exit_signal_bar_15m = signal_bar_15m
        # 注释: 判断条件是否成立：not exit_signal_bar_15m。
        if not exit_signal_bar_15m:
            # 注释: 写入警告日志，提示需要关注的非致命问题。
            logging.warning("平仓时未获取到当前15M信号时间，CSV将写空值")

        # 注释: 调用 trade_state.get 并保存到 entry_signal_bar_15m。
        entry_signal_bar_15m = trade_state.get('entry_signal_bar_15m', '')
        # 注释: 判断条件是否成立：exit_signal_bar_15m and entry_signal_bar_15m。
        if exit_signal_bar_15m and entry_signal_bar_15m:
            # 注释: 开始执行可能抛出异常的逻辑。
            try:
                # 注释: 调用 datetime.datetime.strptime 并保存到 exit_signal_dt。
                exit_signal_dt = datetime.datetime.strptime(exit_signal_bar_15m, BAR_TIME_FORMAT)
                # 注释: 调用 datetime.datetime.strptime 并保存到 entry_signal_dt。
                entry_signal_dt = datetime.datetime.strptime(entry_signal_bar_15m, BAR_TIME_FORMAT)
                # 注释: 判断条件是否成立：exit_signal_dt < entry_signal_dt。
                if exit_signal_dt < entry_signal_dt:
                    # 注释: 写入警告日志，提示需要关注的非致命问题。
                    logging.warning(
                        # 注释: 拼接或传入文本内容。
                        f"检测到平仓信号时间倒退: exit={exit_signal_bar_15m}, "
                        # 注释: 拼接或传入文本内容。
                        f"entry={entry_signal_bar_15m}，已提升到入场信号时间"
                    # 注释: 结束当前多行结构。
                    )
                    # 注释: 计算并保存 exit_signal_bar_15m。
                    exit_signal_bar_15m = entry_signal_bar_15m
            # 注释: 捕获异常分支：except Exception as e。
            except Exception as e:
                # 注释: 写入警告日志，提示需要关注的非致命问题。
                logging.warning(f"平仓信号时间比较失败: {e}")

        # 注释: 调用 trade_state.get 并保存到 open_order_id。
        open_order_id = trade_state.get('open_order_id', '')
        # 记录到 CSV
        # 注释: 调用 log_trade_to_csv 执行对应处理。
        log_trade_to_csv(
            # 注释: 说明当前代码行的作用：trade_state['entry_time'],。
            trade_state['entry_time'],
            # 注释: 说明当前代码行的作用：trade_state['side'],。
            trade_state['side'],
            # 注释: 说明当前代码行的作用：trade_state['cond_4h'],。
            trade_state['cond_4h'],
            # 注释: 说明当前代码行的作用：trade_state['cond_1h'],。
            trade_state['cond_1h'],
            # 注释: 说明当前代码行的作用：trade_state['cond_15m'],。
            trade_state['cond_15m'],
            # 注释: 调用 trade_state.get 执行对应处理。
            trade_state.get('entry_reason', ''),
            # 注释: 说明当前代码行的作用：exit_time,。
            exit_time,
            # 注释: 说明当前代码行的作用：reason,。
            reason,
            # 注释: 调用 round 执行对应处理。
            round(pnl_points, 4),
            # 注释: 调用 round 执行对应处理。
            round(fee_cost, 4),
            # 注释: 调用 round 执行对应处理。
            round(net_pnl_usdt, 4),
            # 注释: 说明当前代码行的作用：is_profit,。
            is_profit,
            # 注释: 计算并保存 entry_signal_bar_15m。
            entry_signal_bar_15m=entry_signal_bar_15m,
            # 注释: 计算并保存 exit_signal_bar_15m。
            exit_signal_bar_15m=exit_signal_bar_15m,
            # 注释: 计算并保存 exit_trigger。
            exit_trigger=trigger_label or reason,
            # 注释: 计算并保存 holding_seconds。
            holding_seconds=holding_seconds,
            # 注释: 计算并保存 open_order_id。
            open_order_id=open_order_id,
            # 注释: 计算并保存 close_order_id。
            close_order_id=close_order_id
        # 注释: 结束当前多行结构。
        )

        # 注释: 调用 format_order_id_lines 并保存到 order_id_lines。
        order_id_lines = format_order_id_lines(
            # 注释: 计算并保存 open_order_id。
            open_order_id=open_order_id,
            # 注释: 计算并保存 close_order_id。
            close_order_id=close_order_id
        # 注释: 结束当前多行结构。
        )
        # 注释: 调用 trade_state.get 并保存到 close_cond_4h。
        close_cond_4h = trade_state.get('close_cond_4h') or "4H: 无可用平仓条件快照"
        # 注释: 调用 trade_state.get 并保存到 close_cond_1h。
        close_cond_1h = trade_state.get('close_cond_1h') or "1H: 无可用平仓条件快照"
        # 注释: 调用 trade_state.get 并保存到 close_cond_15m。
        close_cond_15m = trade_state.get('close_cond_15m') or "15M: 无可用平仓条件快照"
        # 注释: 初始化 close_condition_lines 列表。
        close_condition_lines = []
        # 注释: 遍历 (close_cond_4h, close_cond_1h, close_cond_15m)，逐项处理 close_cond。
        for close_cond in (close_cond_4h, close_cond_1h, close_cond_15m):
            # 注释: 判断条件是否成立：close_cond and '无关键平仓条件' not in close_cond。
            if close_cond and '无关键平仓条件' not in close_cond:
                # 注释: 调用 close_condition_lines.append 执行对应处理。
                close_condition_lines.append(close_cond)
        # 注释: 计算并保存 close_condition_details。
        close_condition_details = '\n'.join(close_condition_lines) if close_condition_lines else f"触发来源: {trigger_label or reason}"
        # 注释: 计算并保存 order_id_suffix。
        order_id_suffix = f"\n{order_id_lines}" if order_id_lines else ''
        msg = (f"🏁 【已平仓】\n原因: {reason}\n入场价: {trade_state['entry_price']}\n"  # 构建平仓通知邮件的文本内容
               # 注释: 拼接或传入文本内容。
               f"出场价: {actual_close_price}\n点数盈亏: {pnl_points:.2f}\n手续费: {fee_cost:.4f}\n净利润(USDT): {net_pnl_usdt:.2f}\n"
               # 注释: 拼接或传入文本内容。
               f"平仓后账户资金(USDT): {final_usdt:.4f}\n15M信号时间: {exit_signal_bar_15m}\n触发来源: {trigger_label or reason}\n"
               # 注释: 拼接或传入文本内容。
               f"平仓条件明细:\n{close_condition_details}"
               f"{order_id_suffix}")  # 拼接出场价格和最终点数盈亏信息
        send_msg(f"ETH交易: 平仓通知", msg)  # 发送带有平仓结果的邮件通知

        # 注释: 计算并保存 post_exit_processed_bar_15m。
        post_exit_processed_bar_15m = exit_signal_bar_15m or trade_state.get('last_processed_bar_15m', '')
        # 注释: 批量更新本地交易状态。
        trade_state.update({
            # 注释: 设置 has_position 字段的取值。
            'has_position': False,
            # 注释: 设置 side 字段的取值。
            'side': None,
            # 注释: 设置 entry_price 字段的取值。
            'entry_price': 0,
            # 注释: 设置 stop_loss_price 字段的取值。
            'stop_loss_price': 0,
            # 注释: 设置 highest_price 字段的取值。
            'highest_price': 0,
            # 注释: 设置 lowest_price 字段的取值。
            'lowest_price': 0,
            # 注释: 设置 amount 字段的取值。
            'amount': 0,
            # 注释: 设置 entry_time 字段的取值。
            'entry_time': '',
            # 注释: 设置 cond_4h 字段的取值。
            'cond_4h': '',
            # 注释: 设置 cond_1h 字段的取值。
            'cond_1h': '',
            # 注释: 设置 cond_15m 字段的取值。
            'cond_15m': '',
            # 注释: 设置 close_cond_4h 字段的取值。
            'close_cond_4h': '',
            # 注释: 设置 close_cond_1h 字段的取值。
            'close_cond_1h': '',
            # 注释: 设置 close_cond_15m 字段的取值。
            'close_cond_15m': '',
            # 注释: 设置 entry_reason 字段的取值。
            'entry_reason': '',
            # 注释: 设置 entry_trigger_tf 字段的取值。
            'entry_trigger_tf': '',
            # 注释: 设置 entry_strategy_tf 字段的取值。
            'entry_strategy_tf': '',
            # 注释: 设置 entry_exit_tf 字段的取值。
            'entry_exit_tf': '',
            # 注释: 设置 entry_sr_target 字段的取值。
            'entry_sr_target': 0.0,
            # 注释: 设置 entry_initial_risk 字段的取值。
            'entry_initial_risk': 0.0,
            # 注释: 设置 partial_taken 字段的取值。
            'partial_taken': False,
            # 注释: 设置 last_exit_time 字段的取值。
            'last_exit_time': exit_time,
            # 注释: 设置 initial_balance 字段的取值。
            'initial_balance': 0.0,
            # 注释: 设置 open_fee 字段的取值。
            'open_fee': 0.0,
            # 注释: 设置 open_order_id 字段的取值。
            'open_order_id': '',
            # 注释: 设置 close_order_id 字段的取值。
            'close_order_id': '',
            # 注释: 设置 liquidation_price 字段的取值。
            'liquidation_price': 0.0,
            # 注释: 设置 stop_order_id 字段的取值。
            'stop_order_id': '',
            # 注释: 设置 stop_order_price 字段的取值。
            'stop_order_price': 0.0,
            # 注释: 设置 stop_order_refresh_fail_count 字段的取值。
            'stop_order_refresh_fail_count': 0,
            # 注释: 设置 last_stop_order_refresh_error 字段的取值。
            'last_stop_order_refresh_error': '',
            # 注释: 设置 entry_signal_bar_15m 字段的取值。
            'entry_signal_bar_15m': '',
            # 注释: 设置 last_exit_bar_15m 字段的取值。
            'last_exit_bar_15m': exit_signal_bar_15m,
            # 注释: 设置 last_processed_bar_15m 字段的取值。
            'last_processed_bar_15m': post_exit_processed_bar_15m,
            # 注释: 设置 position_miss_count 字段的取值。
            'position_miss_count': 0
        })  # 平仓成功后，将全局持仓状态重置
        # 注释: 写入运行日志，记录当前处理进度。
        logging.info(
            # 注释: 拼接或传入文本内容。
            f"清仓成功: {reason}, PnL: {pnl_points:.2f}, Net USDT: {net_pnl_usdt:.2f}, "
            # 注释: 拼接或传入文本内容。
            f"open_order_id={open_order_id}, close_order_id={close_order_id}"
        )  # 记录清仓成功的日志，包含原因和点数盈亏
        # 注释: 返回固定结果 True。
        return True
    except Exception as e:  # 捕获调用API平仓时可能发生的异常
        logging.error(f"清仓失败: {e}")  # 将清仓失败的报错信息写入日志
        # 注释: 返回固定结果 False。
        return False


# 注释: 定义 tf_state_key 函数，封装对应业务逻辑。
def tf_state_key(prefix, timeframe):
    # 注释: 返回计算结果：f"{prefix}_{timeframe}"。
    return f"{prefix}_{timeframe}"


# 注释: 定义 validate_signal_bar 函数，封装对应业务逻辑。
def validate_signal_bar(timeframe, signal_bar, now_dt=None):
    """通用K线新鲜度/去重校验，新版策略以5M为主循环节拍。"""
    # 注释: 判断条件是否成立：not signal_bar。
    if not signal_bar:
        # 注释: 返回包含当前处理结果的字典。
        return {'valid': False, 'is_new': False, 'reason': 'empty'}

    # 注释: 调用 parse_bar_time 并保存到 signal_dt。
    signal_dt = parse_bar_time(signal_bar)
    # 注释: 判断条件是否成立：signal_dt is None。
    if signal_dt is None:
        # 注释: 返回包含当前处理结果的字典。
        return {'valid': False, 'is_new': False, 'reason': 'parse_failed'}

    # 注释: 判断条件是否成立：now_dt is None。
    if now_dt is None:
        # 注释: 调用 get_server_now_dt 并保存到 now_dt。
        now_dt = get_server_now_dt()
    # 注释: 继续判断备选条件：now_dt.tzinfo is None。
    elif now_dt.tzinfo is None:
        # 注释: 调用 now_dt.replace 并保存到 now_dt。
        now_dt = now_dt.replace(tzinfo=EXCHANGE_TZ)
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 调用 now_dt.astimezone 并保存到 now_dt。
        now_dt = now_dt.astimezone(EXCHANGE_TZ)

    # 注释: 调用 TIMEFRAME_SECONDS.get 并保存到 tf_seconds。
    tf_seconds = TIMEFRAME_SECONDS.get(timeframe)
    # 注释: 判断条件是否成立：tf_seconds is None。
    if tf_seconds is None:
        # 注释: 返回包含当前处理结果的字典。
        return {'valid': False, 'is_new': False, 'reason': f'bad_timeframe:{timeframe}'}

    # 注释: 计算并保存 signal_close_dt。
    signal_close_dt = signal_dt + datetime.timedelta(seconds=tf_seconds)
    # 注释: 初始化 stale_seconds 元组或多行表达式。
    stale_seconds = (now_dt - signal_close_dt).total_seconds()
    # 注释: 调用 max 并保存到 max_stale_seconds。
    max_stale_seconds = max(3 * tf_seconds, 15 * 60)
    # 注释: 判断条件是否成立：stale_seconds < -5。
    if stale_seconds < -5:
        # 注释: 返回包含当前处理结果的字典。
        return {'valid': False, 'is_new': False, 'reason': 'future_bar', 'stale_seconds': stale_seconds}
    # 注释: 判断条件是否成立：stale_seconds > max_stale_seconds。
    if stale_seconds > max_stale_seconds:
        # 注释: 返回包含当前处理结果的字典。
        return {'valid': False, 'is_new': False, 'reason': 'stale_bar', 'stale_seconds': stale_seconds}

    # 注释: 调用 tf_state_key 并保存到 max_seen_key。
    max_seen_key = tf_state_key('max_seen_bar', timeframe)
    # 注释: 调用 tf_state_key 并保存到 processed_key。
    processed_key = tf_state_key('last_processed_bar', timeframe)
    # 注释: 调用 parse_bar_time 并保存到 max_seen_dt。
    max_seen_dt = parse_bar_time(trade_state.get(max_seen_key, ''))
    # 注释: 判断条件是否成立：max_seen_dt is not None and signal_dt < max_seen_dt。
    if max_seen_dt is not None and signal_dt < max_seen_dt:
        # 注释: 返回包含当前处理结果的字典。
        return {'valid': False, 'is_new': False, 'reason': 'bar_time_rollback', 'stale_seconds': stale_seconds}

    # 注释: 调用 parse_bar_time 并保存到 last_processed_dt。
    last_processed_dt = parse_bar_time(trade_state.get(processed_key, ''))
    # 注释: 计算并保存 is_new。
    is_new = last_processed_dt is None or signal_dt > last_processed_dt
    # 注释: 判断条件是否成立：max_seen_dt is None or signal_dt > max_seen_dt。
    if max_seen_dt is None or signal_dt > max_seen_dt:
        # 注释: 更新 trade_state[max_seen_key] 的值。
        trade_state[max_seen_key] = signal_bar

    # 注释: 返回包含当前处理结果的字典。
    return {'valid': True, 'is_new': is_new, 'reason': 'ok', 'stale_seconds': stale_seconds}


# 注释: 定义 get_closed_df 函数，封装对应业务逻辑。
def get_closed_df(df, timeframe, now_dt=None):
    # 注释: 调用 get_last_closed_index 并保存到 last_idx。
    last_idx = get_last_closed_index(df, timeframe, now_dt=now_dt)
    # 注释: 判断条件是否成立：last_idx is None。
    if last_idx is None:
        # 注释: 返回计算结果：pd.DataFrame()。
        return pd.DataFrame()
    # 注释: 返回计算结果：df.iloc[:last_idx + 1].copy()。
    return df.iloc[:last_idx + 1].copy()


# 注释: 定义 is_number 函数，封装对应业务逻辑。
def is_number(value):
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 返回计算结果：value is not None and not pd.isna(value)。
        return value is not None and not pd.isna(value)
    # 注释: 捕获异常分支：except Exception。
    except Exception:
        # 注释: 返回固定结果 False。
        return False


# 注释: 定义 price_to_float 函数，封装对应业务逻辑。
def price_to_float(value):
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 返回计算结果：float(value)。
        return float(value)
    # 注释: 捕获异常分支：except Exception。
    except Exception:
        # 注释: 返回计算结果：0.0。
        return 0.0


# 注释: 定义 precision_price 函数，封装对应业务逻辑。
def precision_price(price):
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 返回计算结果：float(exchange.price_to_precision(SYMBOL, price))。
        return float(exchange.price_to_precision(SYMBOL, price))
    # 注释: 捕获异常分支：except Exception。
    except Exception:
        # 注释: 返回计算结果：float(price)。
        return float(price)


# 注释: 定义 get_price_tick 函数，封装对应业务逻辑。
def get_price_tick(reference_price=None):
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 判断条件是否成立：not getattr(exchange, 'markets', None)。
        if not getattr(exchange, 'markets', None):
            # 注释: 调用交易所接口执行对应操作。
            exchange.load_markets()
        # 注释: 调用 exchange.market 并保存到 market。
        market = exchange.market(SYMBOL)
        # 注释: 调用 market.get 并保存到 precision。
        precision = market.get('precision', {}).get('price')
        # 注释: 判断条件是否成立：isinstance(precision, int)。
        if isinstance(precision, int):
            # 注释: 返回计算结果：10 ** (-precision)。
            return 10 ** (-precision)
        # 注释: 判断条件是否成立：isinstance(precision, float) and precision > 0。
        if isinstance(precision, float) and precision > 0:
            # 注释: 返回计算结果：precision。
            return precision
    # 注释: 捕获异常分支：except Exception。
    except Exception:
        # 注释: 占位语句，当前分支不执行额外操作。
        pass
    # 注释: 判断条件是否成立：reference_price。
    if reference_price:
        # 注释: 返回计算结果：max(float(reference_price) * 0.00001, 0.01)。
        return max(float(reference_price) * 0.00001, 0.01)
    # 注释: 返回计算结果：0.01。
    return 0.01


# 注释: 定义 candle_parts 函数，封装对应业务逻辑。
def candle_parts(row):
    # 注释: 调用 price_to_float 并保存到 open_price。
    open_price = price_to_float(row.get('open'))
    # 注释: 调用 price_to_float 并保存到 high。
    high = price_to_float(row.get('high'))
    # 注释: 调用 price_to_float 并保存到 low。
    low = price_to_float(row.get('low'))
    # 注释: 调用 price_to_float 并保存到 close。
    close = price_to_float(row.get('close'))
    # 注释: 调用 max 并保存到 candle_range。
    candle_range = max(high - low, 0.0)
    # 注释: 调用 abs 并保存到 body。
    body = abs(close - open_price)
    # 注释: 返回计算结果：open_price, high, low, close, candle_range, body。
    return open_price, high, low, close, candle_range, body


# 注释: 定义 strong_candle_check 函数，封装对应业务逻辑。
def strong_candle_check(open_price, high, low, close, atr, direction, body_override=None):
    # 注释: 判断条件是否成立：not is_number(atr) or atr <= 0。
    if not is_number(atr) or atr <= 0:
        # 注释: 返回包含当前处理结果的字典。
        return {'ok': False, 'reason': 'bad_atr'}
    # 注释: 计算并保存 candle_range。
    candle_range = high - low
    # 注释: 判断条件是否成立：candle_range <= 0。
    if candle_range <= 0:
        # 注释: 返回包含当前处理结果的字典。
        return {'ok': False, 'reason': 'zero_range'}

    # 注释: 说明当前代码行的作用：raw_body = close - open_price if direction == 'long' else open_price - close。
    raw_body = close - open_price if direction == 'long' else open_price - close
    # 注释: 计算并保存 body。
    body = body_override if body_override is not None else raw_body
    # 注释: 判断条件是否成立：direction == 'long'。
    if direction == 'long':
        # 注释: 计算并保存 direction_ok。
        direction_ok = close > open_price
        # 注释: 说明当前代码行的作用：close_zone_ok = close >= high - candle_range * STRONG_CLOSE_ZONE_RATIO。
        close_zone_ok = close >= high - candle_range * STRONG_CLOSE_ZONE_RATIO
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 计算并保存 direction_ok。
        direction_ok = close < open_price
        # 注释: 说明当前代码行的作用：close_zone_ok = close <= low + candle_range * STRONG_CLOSE_ZONE_RATIO。
        close_zone_ok = close <= low + candle_range * STRONG_CLOSE_ZONE_RATIO

    # 注释: 说明当前代码行的作用：body_atr_ok = body >= STRONG_BODY_ATR_RATIO * atr。
    body_atr_ok = body >= STRONG_BODY_ATR_RATIO * atr
    # 注释: 计算并保存 body_range_ratio。
    body_range_ratio = body / candle_range
    # 注释: 说明当前代码行的作用：body_range_ok = body_range_ratio >= STRONG_BODY_RANGE_RATIO。
    body_range_ok = body_range_ratio >= STRONG_BODY_RANGE_RATIO
    # 注释: 计算并保存 ok。
    ok = direction_ok and body_atr_ok and close_zone_ok and body_range_ok
    # 注释: 返回包含当前处理结果的字典。
    return {
        # 注释: 设置 ok 字段的取值。
        'ok': ok,
        # 注释: 设置 direction_ok 字段的取值。
        'direction_ok': direction_ok,
        # 注释: 设置 body 字段的取值。
        'body': body,
        # 注释: 设置 body_atr_ratio 字段的取值。
        'body_atr_ratio': body / atr,
        # 注释: 设置 close_zone_ok 字段的取值。
        'close_zone_ok': close_zone_ok,
        # 注释: 设置 body_range_ratio 字段的取值。
        'body_range_ratio': body_range_ratio,
        # 注释: 设置 body_atr_ok 字段的取值。
        'body_atr_ok': body_atr_ok,
        # 注释: 设置 body_range_ok 字段的取值。
        'body_range_ok': body_range_ok
    # 注释: 结束当前多行结构。
    }


# 注释: 定义 is_large_strong_candle 函数，封装对应业务逻辑。
def is_large_strong_candle(open_price, high, low, close, atr, direction, body_override=None):
    # 注释: 判断条件是否成立：not is_number(atr) or atr <= 0。
    if not is_number(atr) or atr <= 0:
        # 注释: 返回固定结果 False。
        return False
    # 注释: 计算并保存 candle_range。
    candle_range = high - low
    # 注释: 判断条件是否成立：candle_range < LARGE_STRONG_RANGE_ATR_RATIO * atr。
    if candle_range < LARGE_STRONG_RANGE_ATR_RATIO * atr:
        # 注释: 返回固定结果 False。
        return False
    # 注释: 调用 strong_candle_check 并保存到 check。
    check = strong_candle_check(open_price, high, low, close, atr, direction, body_override=body_override)
    # 注释: 返回计算结果：bool(check.get('direction_ok') and check.get('body_range_ok'))。
    return bool(check.get('direction_ok') and check.get('body_range_ok'))


# 注释: 定义 simple_strong_candle 函数，封装对应业务逻辑。
def simple_strong_candle(row, direction):
    # 注释: 调用 candle_parts 并保存到 open_price, high, low, close, _, _。
    open_price, high, low, close, _, _ = candle_parts(row)
    # 注释: 调用 price_to_float 并保存到 atr。
    atr = price_to_float(row.get('atr'))
    # 注释: 调用 is_large_strong_candle 并保存到 large。
    large = is_large_strong_candle(open_price, high, low, close, atr, direction)
    # 注释: 调用 strong_candle_check 并保存到 check。
    check = strong_candle_check(open_price, high, low, close, atr, direction)
    # 注释: 判断条件是否成立：not check.get('ok') or large。
    if not check.get('ok') or large:
        # 注释: 返回固定结果 None。
        return None
    # 注释: 返回包含当前处理结果的字典。
    return {
        # 注释: 设置 kind 字段的取值。
        'kind': 'single',
        # 注释: 设置 direction 字段的取值。
        'direction': direction,
        # 注释: 设置 open 字段的取值。
        'open': open_price,
        # 注释: 设置 high 字段的取值。
        'high': high,
        # 注释: 设置 low 字段的取值。
        'low': low,
        # 注释: 设置 close 字段的取值。
        'close': close,
        # 注释: 设置 atr 字段的取值。
        'atr': atr,
        # 注释: 设置 body 字段的取值。
        'body': check.get('body', 0.0),
        # 注释: 设置 large 字段的取值。
        'large': False,
        # 注释: 设置 bar_time 字段的取值。
        'bar_time': format_bar_time(row.get('timestamp')),
        # 注释: 设置 details 字段的取值。
        'details': check
    # 注释: 结束当前多行结构。
    }


# 注释: 定义 latest_large_strong 函数，封装对应业务逻辑。
def latest_large_strong(df_closed):
    # 注释: 判断条件是否成立：df_closed is None or len(df_closed) == 0。
    if df_closed is None or len(df_closed) == 0:
        # 注释: 返回固定结果 None。
        return None
    # 注释: 计算并保存 row。
    row = df_closed.iloc[-1]
    # 注释: 调用 candle_parts 并保存到 open_price, high, low, close, _, _。
    open_price, high, low, close, _, _ = candle_parts(row)
    # 注释: 调用 price_to_float 并保存到 atr。
    atr = price_to_float(row.get('atr'))
    # 注释: 遍历 ('long', 'short')，逐项处理 direction。
    for direction in ('long', 'short'):
        # 注释: 判断条件是否成立：is_large_strong_candle(open_price, high, low, close, atr, direction)。
        if is_large_strong_candle(open_price, high, low, close, atr, direction):
            # 注释: 返回包含当前处理结果的字典。
            return {
                # 注释: 设置 direction 字段的取值。
                'direction': direction,
                # 注释: 设置 high 字段的取值。
                'high': high,
                # 注释: 设置 low 字段的取值。
                'low': low,
                # 注释: 设置 close 字段的取值。
                'close': close,
                # 注释: 设置 atr 字段的取值。
                'atr': atr,
                # 注释: 设置 bar_time 字段的取值。
                'bar_time': format_bar_time(row.get('timestamp'))
            # 注释: 结束当前多行结构。
            }
    # 注释: 返回固定结果 None。
    return None


# 注释: 定义 composite_strong_candle 函数，封装对应业务逻辑。
def composite_strong_candle(df_closed, end_idx, bars_count, direction):
    # 注释: 判断条件是否成立：end_idx - bars_count + 1 < 0。
    if end_idx - bars_count + 1 < 0:
        # 注释: 返回固定结果 None。
        return None
    # 注释: 计算并保存 segment。
    segment = df_closed.iloc[end_idx - bars_count + 1:end_idx + 1]
    # 注释: 判断条件是否成立：len(segment) != bars_count。
    if len(segment) != bars_count:
        # 注释: 返回固定结果 None。
        return None

    # 注释: 判断条件是否成立：direction == 'long'。
    if direction == 'long':
        # 注释: 判断条件是否成立：not all(segment['close'] > segment['open'])。
        if not all(segment['close'] > segment['open']):
            # 注释: 返回固定结果 None。
            return None
        # 注释: 计算并保存 first。
        first = segment.iloc[0]
        # 注释: 计算并保存 last。
        last = segment.iloc[-1]
        # 注释: 调用 price_to_float 并保存到 open_price。
        open_price = price_to_float(first['open'])
        # 注释: 调用 price_to_float 并保存到 close。
        close = price_to_float(last['close'])
        # 注释: 调用 price_to_float 并保存到 low。
        low = price_to_float(first['low'])
        # 注释: 调用 price_to_float 并保存到 high。
        high = price_to_float(last['high'])
        # 注释: 调用 float 并保存到 body。
        body = float((segment['close'] - segment['open']).sum())
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 判断条件是否成立：not all(segment['close'] < segment['open'])。
        if not all(segment['close'] < segment['open']):
            # 注释: 返回固定结果 None。
            return None
        # 注释: 计算并保存 first。
        first = segment.iloc[0]
        # 注释: 计算并保存 last。
        last = segment.iloc[-1]
        # 注释: 调用 price_to_float 并保存到 open_price。
        open_price = price_to_float(first['open'])
        # 注释: 调用 price_to_float 并保存到 close。
        close = price_to_float(last['close'])
        # 注释: 调用 price_to_float 并保存到 high。
        high = price_to_float(first['high'])
        # 注释: 调用 price_to_float 并保存到 low。
        low = price_to_float(last['low'])
        # 注释: 调用 float 并保存到 body。
        body = float((segment['open'] - segment['close']).sum())

    # 注释: 判断条件是否成立：high <= low。
    if high <= low:
        # 注释: 调用 float 并保存到 high。
        high = float(segment['high'].max())
        # 注释: 调用 float 并保存到 low。
        low = float(segment['low'].min())
    # 注释: 调用 price_to_float 并保存到 atr。
    atr = price_to_float(segment.iloc[-1].get('atr'))
    # 注释: 判断条件是否成立：(not is_number(atr) or atr <= 0) and 'atr' in segment.columns and segment['atr'].…。
    if (not is_number(atr) or atr <= 0) and 'atr' in segment.columns and segment['atr'].notna().any():
        # 注释: 调用 price_to_float 并保存到 atr。
        atr = price_to_float(segment['atr'].dropna().mean())

    # 注释: 调用 is_large_strong_candle 并保存到 large。
    large = is_large_strong_candle(open_price, high, low, close, atr, direction, body_override=body)
    # 注释: 调用 strong_candle_check 并保存到 check。
    check = strong_candle_check(open_price, high, low, close, atr, direction, body_override=body)
    # 注释: 判断条件是否成立：not check.get('ok') or large。
    if not check.get('ok') or large:
        # 注释: 返回固定结果 None。
        return None
    # 注释: 返回包含当前处理结果的字典。
    return {
        # 注释: 设置 kind 字段的取值。
        'kind': f'synthetic_{bars_count}',
        # 注释: 设置 direction 字段的取值。
        'direction': direction,
        # 注释: 设置 open 字段的取值。
        'open': open_price,
        # 注释: 设置 high 字段的取值。
        'high': high,
        # 注释: 设置 low 字段的取值。
        'low': low,
        # 注释: 设置 close 字段的取值。
        'close': close,
        # 注释: 设置 atr 字段的取值。
        'atr': atr,
        # 注释: 设置 body 字段的取值。
        'body': body,
        # 注释: 设置 large 字段的取值。
        'large': False,
        # 注释: 设置 bar_time 字段的取值。
        'bar_time': format_bar_time(last.get('timestamp')),
        # 注释: 设置 details 字段的取值。
        'details': check
    # 注释: 结束当前多行结构。
    }


# 注释: 定义 latest_effective_strong 函数，封装对应业务逻辑。
def latest_effective_strong(df_closed, direction):
    # 注释: 判断条件是否成立：df_closed is None or len(df_closed) == 0。
    if df_closed is None or len(df_closed) == 0:
        # 注释: 返回固定结果 None。
        return None
    # 注释: 调用 len 并保存到 end_idx。
    end_idx = len(df_closed) - 1
    # 注释: 调用 simple_strong_candle 并保存到 single。
    single = simple_strong_candle(df_closed.iloc[end_idx], direction)
    # 注释: 判断条件是否成立：single。
    if single:
        # 注释: 返回计算结果：single。
        return single
    # 注释: 遍历 range(SYNTHETIC_STRONG_MIN_BARS, SYNTHETIC_STRONG_MAX_BARS + 1)，逐项处理 bars_count。
    for bars_count in range(SYNTHETIC_STRONG_MIN_BARS, SYNTHETIC_STRONG_MAX_BARS + 1):
        # 注释: 调用 composite_strong_candle 并保存到 synthetic。
        synthetic = composite_strong_candle(df_closed, end_idx, bars_count, direction)
        # 注释: 判断条件是否成立：synthetic。
        if synthetic:
            # 注释: 返回计算结果：synthetic。
            return synthetic
    # 注释: 返回固定结果 None。
    return None


# 注释: 定义 historical_effective_strong 函数，封装对应业务逻辑。
def historical_effective_strong(df_closed, end_idx, direction):
    # 注释: 调用 simple_strong_candle 并保存到 single。
    single = simple_strong_candle(df_closed.iloc[end_idx], direction)
    # 注释: 判断条件是否成立：single。
    if single:
        # 注释: 返回计算结果：single。
        return single
    # 注释: 遍历 range(SYNTHETIC_STRONG_MIN_BARS, SYNTHETIC_STRONG_MAX_BARS + 1)，逐项处理 bars_count。
    for bars_count in range(SYNTHETIC_STRONG_MIN_BARS, SYNTHETIC_STRONG_MAX_BARS + 1):
        # 注释: 调用 composite_strong_candle 并保存到 synthetic。
        synthetic = composite_strong_candle(df_closed, end_idx, bars_count, direction)
        # 注释: 判断条件是否成立：synthetic。
        if synthetic:
            # 注释: 返回计算结果：synthetic。
            return synthetic
    # 注释: 返回固定结果 None。
    return None


# 注释: 定义 find_previous_opposite_strong 函数，封装对应业务逻辑。
def find_previous_opposite_strong(df_closed, side, lookback=OPPOSITE_STRONG_LOOKBACK):
    # 注释: 判断条件是否成立：df_closed is None or len(df_closed) < 3。
    if df_closed is None or len(df_closed) < 3:
        # 注释: 返回固定结果 None。
        return None
    # 注释: 说明当前代码行的作用：opposite = 'short' if side == 'long' else 'long'。
    opposite = 'short' if side == 'long' else 'long'
    # 注释: 调用 len 并保存到 end_idx。
    end_idx = len(df_closed) - 2
    # 注释: 调用 max 并保存到 start_idx。
    start_idx = max(0, end_idx - lookback + 1)
    # 注释: 遍历 range(end_idx, start_idx - 1, -1)，逐项处理 idx。
    for idx in range(end_idx, start_idx - 1, -1):
        # 注释: 调用 historical_effective_strong 并保存到 strong。
        strong = historical_effective_strong(df_closed, idx, opposite)
        # 注释: 判断条件是否成立：strong。
        if strong:
            # 注释: 返回计算结果：strong。
            return strong
    # 注释: 返回固定结果 None。
    return None


# 注释: 定义 count_ema_crosses 函数，封装对应业务逻辑。
def count_ema_crosses(df_closed):
    # 注释: 调用 df_closed.tail 并保存到 recent。
    recent = df_closed.tail(EMA_CROSS_LOOKBACK + 1)
    # 注释: 计算并保存 up_cross。
    up_cross = 0
    # 注释: 计算并保存 down_cross。
    down_cross = 0
    # 注释: 计算并保存 prev_side。
    prev_side = None
    # 注释: 遍历 recent.iterrows()，逐项处理 _, row。
    for _, row in recent.iterrows():
        # 注释: 判断条件是否成立：not is_number(row.get('ema20')) or not is_number(row.get('close'))。
        if not is_number(row.get('ema20')) or not is_number(row.get('close')):
            # 注释: 跳过本次循环剩余逻辑，进入下一轮。
            continue
        # 注释: 判断条件是否成立：row['close'] > row['ema20']。
        if row['close'] > row['ema20']:
            # 注释: 计算并保存 side。
            side = 1
        # 注释: 继续判断备选条件：row['close'] < row['ema20']。
        elif row['close'] < row['ema20']:
            # 注释: 计算并保存 side。
            side = -1
        # 注释: 当前面的条件都不成立时进入该分支。
        else:
            # 注释: 计算并保存 side。
            side = 0
        # 注释: 判断条件是否成立：prev_side == -1 and side == 1。
        if prev_side == -1 and side == 1:
            # 注释: 按 += 更新 up_cross 的值。
            up_cross += 1
        # 注释: 继续判断备选条件：prev_side == 1 and side == -1。
        elif prev_side == 1 and side == -1:
            # 注释: 按 += 更新 down_cross 的值。
            down_cross += 1
        # 注释: 判断条件是否成立：side != 0。
        if side != 0:
            # 注释: 计算并保存 prev_side。
            prev_side = side
    # 注释: 返回计算结果：up_cross, down_cross。
    return up_cross, down_cross


# 注释: 定义 recent_persistent_trend_break 函数，封装对应业务逻辑。
def recent_persistent_trend_break(df_closed, persistent_direction):
    # 注释: 判断条件是否成立：df_closed is None or len(df_closed) < 4。
    if df_closed is None or len(df_closed) < 4:
        # 注释: 返回固定结果 False。
        return False
    # 注释: 调用 df_closed.tail 并保存到 recent。
    recent = df_closed.tail(3)
    # 注释: 判断条件是否成立：persistent_direction == 'short'。
    if persistent_direction == 'short':
        # 注释: 计算并保存 bullish。
        bullish = recent[recent['close'] > recent['open']]
        # 注释: 判断条件是否成立：len(bullish) >= 2 and bool((bullish['close'] > bullish['ema20']).any())。
        if len(bullish) >= 2 and bool((bullish['close'] > bullish['ema20']).any()):
            # 注释: 返回固定结果 True。
            return True
        # 注释: 遍历 range(3, 1, -1)，逐项处理 offset。
        for offset in range(3, 1, -1):
            # 注释: 调用 len 并保存到 idx。
            idx = len(df_closed) - offset
            # 注释: 判断条件是否成立：idx < 0 or idx + 2 >= len(df_closed)。
            if idx < 0 or idx + 2 >= len(df_closed):
                # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                continue
            # 注释: 调用 historical_effective_strong 并保存到 strong。
            strong = historical_effective_strong(df_closed, idx, 'long')
            # 注释: 判断条件是否成立：strong and strong['close'] > price_to_float(df_closed.iloc[idx]['ema20'])。
            if strong and strong['close'] > price_to_float(df_closed.iloc[idx]['ema20']):
                # 注释: 计算并保存 follows。
                follows = df_closed.iloc[idx + 1:idx + 3]
                # 注释: 判断条件是否成立：len(follows) == 2 and bool((follows['close'] > follows['ema20']).all())。
                if len(follows) == 2 and bool((follows['close'] > follows['ema20']).all()):
                    # 注释: 返回固定结果 True。
                    return True
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 计算并保存 bearish。
        bearish = recent[recent['close'] < recent['open']]
        # 注释: 判断条件是否成立：len(bearish) >= 2 and bool((bearish['close'] < bearish['ema20']).any())。
        if len(bearish) >= 2 and bool((bearish['close'] < bearish['ema20']).any()):
            # 注释: 返回固定结果 True。
            return True
        # 注释: 遍历 range(3, 1, -1)，逐项处理 offset。
        for offset in range(3, 1, -1):
            # 注释: 调用 len 并保存到 idx。
            idx = len(df_closed) - offset
            # 注释: 判断条件是否成立：idx < 0 or idx + 2 >= len(df_closed)。
            if idx < 0 or idx + 2 >= len(df_closed):
                # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                continue
            # 注释: 调用 historical_effective_strong 并保存到 strong。
            strong = historical_effective_strong(df_closed, idx, 'short')
            # 注释: 判断条件是否成立：strong and strong['close'] < price_to_float(df_closed.iloc[idx]['ema20'])。
            if strong and strong['close'] < price_to_float(df_closed.iloc[idx]['ema20']):
                # 注释: 计算并保存 follows。
                follows = df_closed.iloc[idx + 1:idx + 3]
                # 注释: 判断条件是否成立：len(follows) == 2 and bool((follows['close'] < follows['ema20']).all())。
                if len(follows) == 2 and bool((follows['close'] < follows['ema20']).all()):
                    # 注释: 返回固定结果 True。
                    return True
    # 注释: 返回固定结果 False。
    return False


# 注释: 定义 recent_ema_persistence 函数，封装对应业务逻辑。
def recent_ema_persistence(df_closed):
    # 注释: 判断条件是否成立：df_closed is None or len(df_closed) < EMA_PERSISTENCE_BARS + 3。
    if df_closed is None or len(df_closed) < EMA_PERSISTENCE_BARS + 3:
        # 注释: 返回包含当前处理结果的字典。
        return {'direction': None, 'first_break': False, 'broken': False}

    # 注释: 计算并保存 prior。
    prior = df_closed.iloc[-EMA_PERSISTENCE_BARS - 1:-1]
    # 注释: 计算并保存 last。
    last = df_closed.iloc[-1]
    # 注释: 判断条件是否成立：len(prior) < EMA_PERSISTENCE_BARS or prior['ema20'].isna().any()。
    if len(prior) < EMA_PERSISTENCE_BARS or prior['ema20'].isna().any():
        # 注释: 返回包含当前处理结果的字典。
        return {'direction': None, 'first_break': False, 'broken': False}

    # 注释: 说明当前代码行的作用：above_20 = bool((prior['low'] >= prior['ema20']).all())。
    above_20 = bool((prior['low'] >= prior['ema20']).all())
    # 注释: 说明当前代码行的作用：below_20 = bool((prior['high'] <= prior['ema20']).all())。
    below_20 = bool((prior['high'] <= prior['ema20']).all())
    # 注释: 判断条件是否成立：above_20。
    if above_20:
        # 注释: 调用 is_number 并保存到 first_break。
        first_break = is_number(last.get('ema20')) and last['low'] < last['ema20']
        # 注释: 调用 recent_persistent_trend_break 并保存到 trend_broken。
        trend_broken = recent_persistent_trend_break(df_closed, 'long')
        # 注释: 返回包含当前处理结果的字典。
        return {'direction': 'long', 'first_break': bool(first_break), 'broken': trend_broken}
    # 注释: 判断条件是否成立：below_20。
    if below_20:
        # 注释: 调用 is_number 并保存到 first_break。
        first_break = is_number(last.get('ema20')) and last['high'] > last['ema20']
        # 注释: 调用 recent_persistent_trend_break 并保存到 trend_broken。
        trend_broken = recent_persistent_trend_break(df_closed, 'short')
        # 注释: 返回包含当前处理结果的字典。
        return {'direction': 'short', 'first_break': bool(first_break), 'broken': trend_broken}
    # 注释: 返回包含当前处理结果的字典。
    return {'direction': None, 'first_break': False, 'broken': False}


# 注释: 定义 detect_strong_chop 函数，封装对应业务逻辑。
def detect_strong_chop(df_closed):
    # 注释: 判断条件是否成立：df_closed is None or len(df_closed) < STRONG_CHOP_LOOKBACK_BARS。
    if df_closed is None or len(df_closed) < STRONG_CHOP_LOOKBACK_BARS:
        # 注释: 返回包含当前处理结果的字典。
        return {'is_chop': False}

    # 注释: 定义 count_strong_hits 函数，封装对应业务逻辑。
    def count_strong_hits(start_idx, end_idx):
        # 注释: 计算并保存 long_hits。
        long_hits = 0
        # 注释: 计算并保存 short_hits。
        short_hits = 0
        # 注释: 遍历 range(start_idx, end_idx)，逐项处理 idx。
        for idx in range(start_idx, end_idx):
            # 注释: 判断条件是否成立：historical_effective_strong(df_closed, idx, 'long')。
            if historical_effective_strong(df_closed, idx, 'long'):
                # 注释: 按 += 更新 long_hits 的值。
                long_hits += 1
            # 注释: 判断条件是否成立：historical_effective_strong(df_closed, idx, 'short')。
            if historical_effective_strong(df_closed, idx, 'short'):
                # 注释: 按 += 更新 short_hits 的值。
                short_hits += 1
        # 注释: 返回计算结果：long_hits, short_hits。
        return long_hits, short_hits

    # 注释: 定义 make_result 函数，封装对应业务逻辑。
    def make_result(is_chop, zone_window, long_hits, short_hits, resolved=False, breakout_side='none', zone_source='recent'):
        # 注释: 返回包含当前处理结果的字典。
        return {
            # 注释: 设置 is_chop 字段的取值。
            'is_chop': is_chop,
            # 注释: 设置 resolved 字段的取值。
            'resolved': resolved,
            # 注释: 设置 zone_high 字段的取值。
            'zone_high': float(zone_window['high'].max()),
            # 注释: 设置 zone_low 字段的取值。
            'zone_low': float(zone_window['low'].min()),
            # 注释: 设置 long_hits 字段的取值。
            'long_hits': long_hits,
            # 注释: 设置 short_hits 字段的取值。
            'short_hits': short_hits,
            # 注释: 设置 breakout_side 字段的取值。
            'breakout_side': breakout_side,
            # 注释: 设置 zone_source 字段的取值。
            'zone_source': zone_source
        # 注释: 结束当前多行结构。
        }

    # 注释: 调用 len 并保存到 last_idx。
    last_idx = len(df_closed) - 1
    # 注释: 调用 float 并保存到 last_close。
    last_close = float(df_closed.iloc[last_idx]['close'])

    # 先用上一段强K震荡区间判断当前收盘是否突破，避免当前K的高低点把突破条件卡死。
    # 注释: 判断条件是否成立：len(df_closed) > STRONG_CHOP_LOOKBACK_BARS。
    if len(df_closed) > STRONG_CHOP_LOOKBACK_BARS:
        # 注释: 调用 len 并保存到 previous_start_idx。
        previous_start_idx = len(df_closed) - STRONG_CHOP_LOOKBACK_BARS - 1
        # 注释: 调用 len 并保存到 previous_end_idx。
        previous_end_idx = len(df_closed) - 1
        # 注释: 计算并保存 previous。
        previous = df_closed.iloc[previous_start_idx:previous_end_idx]
        # 注释: 调用 count_strong_hits 并保存到 long_hits, short_hits。
        long_hits, short_hits = count_strong_hits(previous_start_idx, previous_end_idx)
        # 注释: 判断条件是否成立：long_hits > 0 and short_hits > 0。
        if long_hits > 0 and short_hits > 0:
            # 注释: 调用 float 并保存到 zone_high。
            zone_high = float(previous['high'].max())
            # 注释: 调用 float 并保存到 zone_low。
            zone_low = float(previous['low'].min())
            # 注释: 判断条件是否成立：last_close > zone_high。
            if last_close > zone_high:
                # 注释: 返回计算结果：make_result(。
                return make_result(
                    # 注释: 说明当前代码行的作用：False, previous, long_hits, short_hits,。
                    False, previous, long_hits, short_hits,
                    # 注释: 计算并保存 resolved。
                    resolved=True, breakout_side='up', zone_source='previous'
                # 注释: 结束当前多行结构。
                )
            # 注释: 判断条件是否成立：last_close < zone_low。
            if last_close < zone_low:
                # 注释: 返回计算结果：make_result(。
                return make_result(
                    # 注释: 说明当前代码行的作用：False, previous, long_hits, short_hits,。
                    False, previous, long_hits, short_hits,
                    # 注释: 计算并保存 resolved。
                    resolved=True, breakout_side='down', zone_source='previous'
                # 注释: 结束当前多行结构。
                )
            # 注释: 返回计算结果：make_result(True, previous, long_hits, short_hits, zone_source='previous')。
            return make_result(True, previous, long_hits, short_hits, zone_source='previous')

    # 注释: 调用 df_closed.tail 并保存到 recent。
    recent = df_closed.tail(STRONG_CHOP_LOOKBACK_BARS)
    # 注释: 调用 len 并保存到 start_idx。
    start_idx = len(df_closed) - len(recent)
    # 注释: 调用 count_strong_hits 并保存到 long_hits, short_hits。
    long_hits, short_hits = count_strong_hits(start_idx, len(df_closed))
    # 注释: 判断条件是否成立：long_hits <= 0 or short_hits <= 0。
    if long_hits <= 0 or short_hits <= 0:
        # 注释: 返回包含当前处理结果的字典。
        return {'is_chop': False}
    # 注释: 返回计算结果：make_result(True, recent, long_hits, short_hits, zone_source='recent')。
    return make_result(True, recent, long_hits, short_hits, zone_source='recent')


# 注释: 定义 evaluate_adx_ema_context 函数，封装对应业务逻辑。
def evaluate_adx_ema_context(df, timeframe, now_dt=None):
    # 注释: 调用 get_closed_df 并保存到 df_closed。
    df_closed = get_closed_df(df, timeframe, now_dt=now_dt)
    # 注释: 初始化 base 字典。
    base = {
        # 注释: 设置 timeframe 字段的取值。
        'timeframe': timeframe,
        # 注释: 设置 signal_bar_time 字段的取值。
        'signal_bar_time': '',
        # 注释: 设置 direction 字段的取值。
        'direction': None,
        # 注释: 设置 status 字段的取值。
        'status': 'no_data',
        # 注释: 设置 can_open_long 字段的取值。
        'can_open_long': False,
        # 注释: 设置 can_open_short 字段的取值。
        'can_open_short': False,
        # 注释: 设置 long_trend 字段的取值。
        'long_trend': False,
        # 注释: 设置 short_trend 字段的取值。
        'short_trend': False,
        # 注释: 设置 pullback_long 字段的取值。
        'pullback_long': False,
        # 注释: 设置 pullback_short 字段的取值。
        'pullback_short': False,
        # 注释: 设置 close_long 字段的取值。
        'close_long': False,
        # 注释: 设置 close_short 字段的取值。
        'close_short': False,
        # 注释: 设置 is_oscillation 字段的取值。
        'is_oscillation': True,
        # 注释: 设置 summary 字段的取值。
        'summary': '',
        # 注释: 设置 details 字段的取值。
        'details': {}
    # 注释: 结束当前多行结构。
    }
    # 注释: 调用 max 并保存到 min_len。
    min_len = max(ADX_LENGTH + 2, EMA_TREND_LENGTH + EMA_SLOPE_LOOKBACK + 2, EMA_CROSS_LOOKBACK + 2)
    # 注释: 判断条件是否成立：len(df_closed) < min_len。
    if len(df_closed) < min_len:
        # 注释: 更新 base['summary'] 的值。
        base['summary'] = f"{timeframe}: 数据不足"
        # 注释: 返回计算结果：base。
        return base

    # 注释: 计算并保存 last。
    last = df_closed.iloc[-1]
    # 注释: 计算并保存 prev。
    prev = df_closed.iloc[-2]
    # 注释: 计算并保存 ema_ref。
    ema_ref = df_closed.iloc[-1 - EMA_SLOPE_LOOKBACK]
    # 注释: 更新 base['signal_bar_time'] 的值。
    base['signal_bar_time'] = format_bar_time(last.get('timestamp'))

    # 注释: 调用 price_to_float 并保存到 adx。
    adx = price_to_float(last.get('adx'))
    # 注释: 调用 price_to_float 并保存到 prev_adx。
    prev_adx = price_to_float(prev.get('adx'))
    # 注释: 调用 price_to_float 并保存到 plus_di。
    plus_di = price_to_float(last.get('plus_di'))
    # 注释: 调用 price_to_float 并保存到 minus_di。
    minus_di = price_to_float(last.get('minus_di'))
    # 注释: 调用 price_to_float 并保存到 ema20。
    ema20 = price_to_float(last.get('ema20'))
    # 注释: 调用 price_to_float 并保存到 ema20_ref。
    ema20_ref = price_to_float(ema_ref.get('ema20'))
    # 注释: 调用 count_ema_crosses 并保存到 up_cross, down_cross。
    up_cross, down_cross = count_ema_crosses(df_closed)
    # 注释: 说明当前代码行的作用：ema_clean = up_cross <= 1 and down_cross <= 1。
    ema_clean = up_cross <= 1 and down_cross <= 1
    # 注释: 计算并保存 ema_up。
    ema_up = ema20 > ema20_ref
    # 注释: 计算并保存 ema_down。
    ema_down = ema20 < ema20_ref
    # 注释: 计算并保存 adx_rising。
    adx_rising = adx > prev_adx
    # 注释: 计算并保存 adx_falling。
    adx_falling = adx < prev_adx
    # 注释: 计算并保存 extreme。
    extreme = adx > ADX_EXTREME
    # 注释: 调用 detect_strong_chop 并保存到 chop。
    chop = detect_strong_chop(df_closed)
    # 注释: 调用 recent_ema_persistence 并保存到 persistence。
    persistence = recent_ema_persistence(df_closed) if timeframe in ('15m', '1h', '4h') else {'direction': None}

    # 注释: 更新 base['details'] 的值。
    base['details'] = {
        # 注释: 设置 adx 字段的取值。
        'adx': adx,
        # 注释: 设置 prev_adx 字段的取值。
        'prev_adx': prev_adx,
        # 注释: 设置 plus_di 字段的取值。
        'plus_di': plus_di,
        # 注释: 设置 minus_di 字段的取值。
        'minus_di': minus_di,
        # 注释: 设置 ema20 字段的取值。
        'ema20': ema20,
        # 注释: 设置 ema20_ref 字段的取值。
        'ema20_ref': ema20_ref,
        # 注释: 设置 up_cross 字段的取值。
        'up_cross': up_cross,
        # 注释: 设置 down_cross 字段的取值。
        'down_cross': down_cross,
        # 注释: 设置 ema_clean 字段的取值。
        'ema_clean': ema_clean,
        # 注释: 设置 ema_persistence 字段的取值。
        'ema_persistence': persistence,
        # 注释: 设置 strong_chop 字段的取值。
        'strong_chop': chop,
        # 注释: 设置 extreme_adx 字段的取值。
        'extreme_adx': extreme
    # 注释: 结束当前多行结构。
    }

    # 注释: 判断条件是否成立：chop.get('is_chop')。
    if chop.get('is_chop'):
        # 注释: 更新 base['status'] 的值。
        base['status'] = 'strong_chop'
        # 注释: 更新 base['summary'] 的值。
        base['summary'] = f"{timeframe}: 强K交叉震荡 zone={chop.get('zone_low'):.4f}-{chop.get('zone_high'):.4f}"
        # 注释: 返回计算结果：base。
        return base
    # 注释: 判断条件是否成立：adx <= 20。
    if adx <= 20:
        # 注释: 更新 base['status'] 的值。
        base['status'] = 'range'
        # 注释: 更新 base['summary'] 的值。
        base['summary'] = f"{timeframe}: ADX={adx:.2f} 震荡，禁止开仓"
        # 注释: 返回计算结果：base。
        return base
    # 注释: 判断条件是否成立：adx <= ADX_NO_TRADE_MAX。
    if adx <= ADX_NO_TRADE_MAX:
        # 注释: 更新 base['status'] 的值。
        base['status'] = 'transition'
        # 注释: 更新 base['summary'] 的值。
        base['summary'] = f"{timeframe}: ADX={adx:.2f} 过渡，等待方向确认"
        # 注释: 返回计算结果：base。
        return base

    # 注释: 计算并保存 direction。
    direction = None
    # 注释: 计算并保存 status。
    status = 'unclear'
    # 注释: 计算并保存 can_open_long。
    can_open_long = False
    # 注释: 计算并保存 can_open_short。
    can_open_short = False
    # 注释: 判断条件是否成立：plus_di > minus_di and ema_up and ema_clean。
    if plus_di > minus_di and ema_up and ema_clean:
        # 注释: 计算并保存 direction。
        direction = 'long'
        # 注释: 判断条件是否成立：adx_rising。
        if adx_rising:
            # 注释: 计算并保存 status。
            status = 'long'
            # 注释: 计算并保存 can_open_long。
            can_open_long = True
        # 注释: 继续判断备选条件：adx_falling。
        elif adx_falling:
            # 注释: 计算并保存 status。
            status = 'long_weakening'
        # 注释: 当前面的条件都不成立时进入该分支。
        else:
            # 注释: 计算并保存 status。
            status = 'long_flat_adx'
    # 注释: 继续判断备选条件：minus_di > plus_di and ema_down and ema_clean。
    elif minus_di > plus_di and ema_down and ema_clean:
        # 注释: 计算并保存 direction。
        direction = 'short'
        # 注释: 判断条件是否成立：adx_rising。
        if adx_rising:
            # 注释: 计算并保存 status。
            status = 'short'
            # 注释: 计算并保存 can_open_short。
            can_open_short = True
        # 注释: 继续判断备选条件：adx_falling。
        elif adx_falling:
            # 注释: 计算并保存 status。
            status = 'short_weakening'
        # 注释: 当前面的条件都不成立时进入该分支。
        else:
            # 注释: 计算并保存 status。
            status = 'short_flat_adx'

    # 注释: 判断条件是否成立：direction is None and persistence.get('direction') and not persistence.get('broke…。
    if direction is None and persistence.get('direction') and not persistence.get('broken'):
        # 注释: 计算并保存 direction。
        direction = persistence['direction']
        # 注释: 计算并保存 status。
        status = f"ema20_{direction}_persistence"
        # 注释: 判断条件是否成立：direction == 'long'。
        if direction == 'long':
            # 注释: 计算并保存 can_open_long。
            can_open_long = True
        # 注释: 当前面的条件都不成立时进入该分支。
        else:
            # 注释: 计算并保存 can_open_short。
            can_open_short = True

    # 注释: 更新 base['direction'] 的值。
    base['direction'] = direction
    # 注释: 更新 base['status'] 的值。
    base['status'] = status
    # 注释: 更新 base['can_open_long'] 的值。
    base['can_open_long'] = can_open_long
    # 注释: 更新 base['can_open_short'] 的值。
    base['can_open_short'] = can_open_short
    # 注释: 说明当前代码行的作用：base['long_trend'] = direction == 'long' and can_open_long。
    base['long_trend'] = direction == 'long' and can_open_long
    # 注释: 说明当前代码行的作用：base['short_trend'] = direction == 'short' and can_open_short。
    base['short_trend'] = direction == 'short' and can_open_short
    # 注释: 说明当前代码行的作用：base['pullback_long'] = direction == 'long' and status in ('long_weakening', 'lon…。
    base['pullback_long'] = direction == 'long' and status in ('long_weakening', 'long_flat_adx')
    # 注释: 说明当前代码行的作用：base['pullback_short'] = direction == 'short' and status in ('short_weakening', '…。
    base['pullback_short'] = direction == 'short' and status in ('short_weakening', 'short_flat_adx')
    # 注释: 更新 base['is_oscillation'] 的值。
    base['is_oscillation'] = direction is None
    # 注释: 更新 base['summary'] 的值。
    base['summary'] = (
        # 注释: 拼接或传入文本内容。
        f"{timeframe}: status={status}, dir={direction}, ADX={adx:.2f}, "
        # 注释: 拼接或传入文本内容。
        f"+DI={plus_di:.2f}, -DI={minus_di:.2f}, EMA clean={ema_clean}, "
        # 注释: 拼接或传入文本内容。
        f"cross={up_cross}/{down_cross}, extreme={extreme}"
    # 注释: 结束当前多行结构。
    )
    # 注释: 返回计算结果：base。
    return base


# 注释: 定义 context_allows_side 函数，封装对应业务逻辑。
def context_allows_side(local_state, background_state, side):
    # 注释: 判断条件是否成立：not local_state or not background_state。
    if not local_state or not background_state:
        # 注释: 返回计算结果：False, '缺少趋势状态'。
        return False, '缺少趋势状态'
    # 注释: 说明当前代码行的作用：opposite = 'short' if side == 'long' else 'long'。
    opposite = 'short' if side == 'long' else 'long'
    # 注释: 判断条件是否成立：local_state.get('is_oscillation')。
    if local_state.get('is_oscillation'):
        # 注释: 返回计算结果：False, f"{local_state.get('timeframe')} 本级别震荡/趋势不明"。
        return False, f"{local_state.get('timeframe')} 本级别震荡/趋势不明"
    # 注释: 判断条件是否成立：local_state.get('direction') == opposite。
    if local_state.get('direction') == opposite:
        # 注释: 返回计算结果：False, f"{local_state.get('timeframe')} 本级别方向相反"。
        return False, f"{local_state.get('timeframe')} 本级别方向相反"
    # 注释: 判断条件是否成立：background_state.get('direction') == opposite。
    if background_state.get('direction') == opposite:
        # 注释: 返回计算结果：False, f"{background_state.get('timeframe')} 背景趋势相反"。
        return False, f"{background_state.get('timeframe')} 背景趋势相反"
    # 注释: 判断条件是否成立：background_state.get('status') in ('range', 'transition', 'strong_chop', 'unclear…。
    if background_state.get('status') in ('range', 'transition', 'strong_chop', 'unclear', 'no_data'):
        # 注释: 返回计算结果：False, f"{background_state.get('timeframe')} 背景趋势不允许开仓: {background_state.get('st…。
        return False, f"{background_state.get('timeframe')} 背景趋势不允许开仓: {background_state.get('status')}"

    # 注释: 说明当前代码行的作用：local_open_key = 'can_open_long' if side == 'long' else 'can_open_short'。
    local_open_key = 'can_open_long' if side == 'long' else 'can_open_short'
    # 注释: 判断条件是否成立：not local_state.get(local_open_key) and local_state.get('direction') != side。
    if not local_state.get(local_open_key) and local_state.get('direction') != side:
        # 注释: 返回计算结果：False, f"{local_state.get('timeframe')} 本级别未给出{side}方向"。
        return False, f"{local_state.get('timeframe')} 本级别未给出{side}方向"

    # 注释: 说明当前代码行的作用：bg_same_side = background_state.get('direction') == side。
    bg_same_side = background_state.get('direction') == side
    # 注释: 说明当前代码行的作用：bg_open_key = 'can_open_long' if side == 'long' else 'can_open_short'。
    bg_open_key = 'can_open_long' if side == 'long' else 'can_open_short'
    # 注释: 调用 background_state.get 并保存到 bg_soft_allow。
    bg_soft_allow = background_state.get('status') in (
        # 注释: 拼接或传入文本内容。
        f'{side}_weakening',
        # 注释: 拼接或传入文本内容。
        f'{side}_flat_adx',
        # 注释: 拼接或传入文本内容。
        f'ema20_{side}_persistence'
    # 注释: 结束当前多行结构。
    )
    # 注释: 判断条件是否成立：not bg_same_side or (not background_state.get(bg_open_key) and not bg_soft_allow)。
    if not bg_same_side or (not background_state.get(bg_open_key) and not bg_soft_allow):
        # 注释: 返回计算结果：False, f"{background_state.get('timeframe')} 背景未确认{side}"。
        return False, f"{background_state.get('timeframe')} 背景未确认{side}"
    # 注释: 返回计算结果：True, 'ok'。
    return True, 'ok'


# 注释: 定义 detect_entry_trigger 函数，封装对应业务逻辑。
def detect_entry_trigger(df, timeframe, side, now_dt=None):
    # 注释: 调用 get_closed_df 并保存到 df_closed。
    df_closed = get_closed_df(df, timeframe, now_dt=now_dt)
    # 注释: 判断条件是否成立：len(df_closed) < max(ADX_LENGTH + 3, SYNTHETIC_STRONG_MAX_BARS + 2)。
    if len(df_closed) < max(ADX_LENGTH + 3, SYNTHETIC_STRONG_MAX_BARS + 2):
        # 注释: 返回固定结果 None。
        return None

    # 注释: 计算并保存 last。
    last = df_closed.iloc[-1]
    # 注释: 调用 get_price_tick 并保存到 tick。
    tick = get_price_tick(last.get('close'))
    # 注释: 调用 latest_large_strong 并保存到 large。
    large = latest_large_strong(df_closed)
    # 注释: 判断条件是否成立：large。
    if large:
        # 注释: 返回包含当前处理结果的字典。
        return {
            # 注释: 设置 blocked 字段的取值。
            'blocked': True,
            # 注释: 设置 reason 字段的取值。
            'reason': f"最新{timeframe}为大强{large['direction']}线，过滤不开仓",
            # 注释: 设置 large 字段的取值。
            'large': large
        # 注释: 结束当前多行结构。
        }

    # 注释: 调用 latest_effective_strong 并保存到 strong。
    strong = latest_effective_strong(df_closed, side)
    # 注释: 判断条件是否成立：strong。
    if strong:
        # 注释: 判断条件是否成立：side == 'long'。
        if side == 'long':
            # 注释: 计算并保存 entry_level。
            entry_level = strong['high'] + tick
            # 注释: 计算并保存 stop。
            stop = strong['low'] - tick
        # 注释: 当前面的条件都不成立时进入该分支。
        else:
            # 注释: 计算并保存 entry_level。
            entry_level = strong['low'] - tick
            # 注释: 计算并保存 stop。
            stop = strong['high'] + tick
        # 注释: 返回包含当前处理结果的字典。
        return {
            # 注释: 设置 blocked 字段的取值。
            'blocked': False,
            # 注释: 设置 type 字段的取值。
            'type': 'strong_candle',
            # 注释: 设置 side 字段的取值。
            'side': side,
            # 注释: 设置 timeframe 字段的取值。
            'timeframe': timeframe,
            # 注释: 设置 entry_level 字段的取值。
            'entry_level': precision_price(entry_level),
            # 注释: 设置 stop 字段的取值。
            'stop': precision_price(stop),
            # 注释: 设置 trigger 字段的取值。
            'trigger': strong,
            # 注释: 设置 reason 字段的取值。
            'reason': f"{timeframe}最新{strong['kind']}强{'阳' if side == 'long' else '阴'}线"
        # 注释: 结束当前多行结构。
        }

    # 注释: 调用 find_previous_opposite_strong 并保存到 previous。
    previous = find_previous_opposite_strong(df_closed, side)
    # 注释: 判断条件是否成立：previous is None。
    if previous is None:
        # 注释: 返回固定结果 None。
        return None
    # 注释: 调用 price_to_float 并保存到 atr。
    atr = price_to_float(last.get('atr'))
    # 注释: 调用 price_to_float 并保存到 close。
    close = price_to_float(last.get('close'))
    # 注释: 调用 price_to_float 并保存到 high。
    high = price_to_float(last.get('high'))
    # 注释: 调用 price_to_float 并保存到 low。
    low = price_to_float(last.get('low'))
    # 注释: 判断条件是否成立：side == 'long'。
    if side == 'long':
        # 注释: 计算并保存 level。
        level = previous['high']
        # 注释: 计算并保存 broke。
        broke = close > level and high > level
        # 注释: 计算并保存 stop。
        stop = close - ENTRY_ATR_STOP_MULTIPLIER * atr
        # 注释: 计算并保存 entry_level。
        entry_level = level + tick
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 计算并保存 level。
        level = previous['low']
        # 注释: 计算并保存 broke。
        broke = close < level and low < level
        # 注释: 计算并保存 stop。
        stop = close + ENTRY_ATR_STOP_MULTIPLIER * atr
        # 注释: 计算并保存 entry_level。
        entry_level = level - tick
    # 注释: 判断条件是否成立：not broke。
    if not broke:
        # 注释: 返回固定结果 None。
        return None
    # 注释: 返回包含当前处理结果的字典。
    return {
        # 注释: 设置 blocked 字段的取值。
        'blocked': False,
        # 注释: 设置 type 字段的取值。
        'type': 'opposite_strong_break',
        # 注释: 设置 side 字段的取值。
        'side': side,
        # 注释: 设置 timeframe 字段的取值。
        'timeframe': timeframe,
        # 注释: 设置 entry_level 字段的取值。
        'entry_level': precision_price(entry_level),
        # 注释: 设置 stop 字段的取值。
        'stop': precision_price(stop),
        # 注释: 设置 trigger 字段的取值。
        'trigger': previous,
        # 注释: 设置 reason 字段的取值。
        'reason': f"{timeframe}收盘突破前强{'阴' if side == 'long' else '阳'}线关键位"
    # 注释: 结束当前多行结构。
    }


# 注释: 定义 zone_distance 函数，封装对应业务逻辑。
def zone_distance(zone, price):
    # 注释: 判断条件是否成立：zone['lower'] <= price <= zone['upper']。
    if zone['lower'] <= price <= zone['upper']:
        # 注释: 返回计算结果：0.0。
        return 0.0
    # 注释: 返回计算结果：min(abs(price - zone['lower']), abs(price - zone['upper']))。
    return min(abs(price - zone['lower']), abs(price - zone['upper']))


# 注释: 定义 add_or_merge_zone 函数，封装对应业务逻辑。
def add_or_merge_zone(zones, zone_type, price, atr, source_idx):
    # 注释: 计算并保存 merge_distance。
    merge_distance = SUP_RES_MERGE_ATR * atr
    # 注释: 计算并保存 buffer。
    buffer = SUP_RES_ZONE_BUFFER_ATR * atr
    # 注释: 计算并保存 matched。
    matched = None
    # 注释: 遍历 [z for z in zones if z['type'] == zone_type]，逐项处理 zone。
    for zone in [z for z in zones if z['type'] == zone_type]:
        # 注释: 判断条件是否成立：zone_distance(zone, price) <= merge_distance。
        if zone_distance(zone, price) <= merge_distance:
            # 注释: 计算并保存 matched。
            matched = zone
            # 注释: 结束当前循环。
            break
    # 注释: 判断条件是否成立：matched。
    if matched:
        # 注释: 更新 matched['lower'] 的值。
        matched['lower'] = min(matched['lower'], price) - buffer
        # 注释: 更新 matched['upper'] 的值。
        matched['upper'] = max(matched['upper'], price) + buffer
        # 注释: 更新 matched['touches'] 的值。
        matched['touches'] = 0
        # 注释: 更新 matched['created_idx'] 的值。
        matched['created_idx'] = source_idx
        # 注释: 更新 matched['valid'] 的值。
        matched['valid'] = True
        # 注释: 返回计算结果：matched。
        return matched

    # 注释: 初始化 zone 字典。
    zone = {
        # 注释: 设置 type 字段的取值。
        'type': zone_type,
        # 注释: 设置 lower 字段的取值。
        'lower': price - buffer,
        # 注释: 设置 upper 字段的取值。
        'upper': price + buffer,
        # 注释: 设置 touches 字段的取值。
        'touches': 0,
        # 注释: 设置 created_idx 字段的取值。
        'created_idx': source_idx,
        # 注释: 设置 valid 字段的取值。
        'valid': True
    # 注释: 结束当前多行结构。
    }
    # 注释: 调用 zones.append 执行对应处理。
    zones.append(zone)
    # 注释: 返回计算结果：zone。
    return zone


# 注释: 定义 is_swing_low 函数，封装对应业务逻辑。
def is_swing_low(df_closed, idx, window):
    # 注释: 计算并保存 current。
    current = df_closed.iloc[idx]['low']
    # 注释: 遍历 range(1, window + 1)，逐项处理 offset。
    for offset in range(1, window + 1):
        # 注释: 判断条件是否成立：current >= df_closed.iloc[idx - offset]['low'] or current >= df_closed.iloc[idx +…。
        if current >= df_closed.iloc[idx - offset]['low'] or current >= df_closed.iloc[idx + offset]['low']:
            # 注释: 返回固定结果 False。
            return False
    # 注释: 返回固定结果 True。
    return True


# 注释: 定义 is_swing_high 函数，封装对应业务逻辑。
def is_swing_high(df_closed, idx, window):
    # 注释: 计算并保存 current。
    current = df_closed.iloc[idx]['high']
    # 注释: 遍历 range(1, window + 1)，逐项处理 offset。
    for offset in range(1, window + 1):
        # 注释: 判断条件是否成立：current <= df_closed.iloc[idx - offset]['high'] or current <= df_closed.iloc[idx…。
        if current <= df_closed.iloc[idx - offset]['high'] or current <= df_closed.iloc[idx + offset]['high']:
            # 注释: 返回固定结果 False。
            return False
    # 注释: 返回固定结果 True。
    return True


# 注释: 定义 rebuild_zone_touches 函数，封装对应业务逻辑。
def rebuild_zone_touches(zones, df_closed):
    # 注释: 初始化 active 列表。
    active = []
    # 注释: 遍历 zones，逐项处理 zone。
    for zone in zones:
        # 注释: 调用 dict 并保存到 zone。
        zone = dict(zone)
        # 注释: 更新 zone['touches'] 的值。
        zone['touches'] = 0
        # 注释: 更新 zone['valid'] 的值。
        zone['valid'] = True
        # 注释: 调用 max 并保存到 start_idx。
        start_idx = max(0, int(zone.get('created_idx', 0)))
        # 注释: 遍历 range(start_idx, len(df_closed))，逐项处理 idx。
        for idx in range(start_idx, len(df_closed)):
            # 注释: 计算并保存 row。
            row = df_closed.iloc[idx]
            # 注释: 调用 price_to_float 并保存到 close。
            close = price_to_float(row['close'])
            # 注释: 判断条件是否成立：zone['type'] == 'support'。
            if zone['type'] == 'support':
                # 注释: 判断条件是否成立：close < zone['lower']。
                if close < zone['lower']:
                    # 注释: 更新 zone['valid'] 的值。
                    zone['valid'] = False
                    # 注释: 结束当前循环。
                    break
                # 注释: 判断条件是否成立：row['low'] <= zone['upper'] and close >= zone['lower']。
                if row['low'] <= zone['upper'] and close >= zone['lower']:
                    # 注释: 按 += 更新 zone['touches'] 的值。
                    zone['touches'] += 1
            # 注释: 当前面的条件都不成立时进入该分支。
            else:
                # 注释: 判断条件是否成立：close > zone['upper']。
                if close > zone['upper']:
                    # 注释: 更新 zone['valid'] 的值。
                    zone['valid'] = False
                    # 注释: 结束当前循环。
                    break
                # 注释: 判断条件是否成立：row['high'] >= zone['lower'] and close <= zone['upper']。
                if row['high'] >= zone['lower'] and close <= zone['upper']:
                    # 注释: 按 += 更新 zone['touches'] 的值。
                    zone['touches'] += 1
        # 注释: 判断条件是否成立：zone['valid'] and zone['touches'] >= SUP_RES_VALID_TOUCHES。
        if zone['valid'] and zone['touches'] >= SUP_RES_VALID_TOUCHES:
            # 注释: 调用 active.append 执行对应处理。
            active.append(zone)
    # 注释: 返回计算结果：active。
    return active


# 注释: 定义 recently_broken_zones 函数，封装对应业务逻辑。
def recently_broken_zones(zones, df_closed):
    # 注释: 初始化 broken 字典。
    broken = {'broken_support': [], 'broken_resistance': []}
    # 注释: 遍历 zones，逐项处理 raw_zone。
    for raw_zone in zones:
        # 注释: 调用 dict 并保存到 zone。
        zone = dict(raw_zone)
        # 注释: 计算并保存 touches。
        touches = 0
        # 注释: 计算并保存 invalid_idx。
        invalid_idx = None
        # 注释: 调用 max 并保存到 start_idx。
        start_idx = max(0, int(zone.get('created_idx', 0)))
        # 注释: 遍历 range(start_idx, len(df_closed))，逐项处理 idx。
        for idx in range(start_idx, len(df_closed)):
            # 注释: 计算并保存 row。
            row = df_closed.iloc[idx]
            # 注释: 调用 price_to_float 并保存到 close。
            close = price_to_float(row['close'])
            # 注释: 判断条件是否成立：zone['type'] == 'support'。
            if zone['type'] == 'support':
                # 注释: 判断条件是否成立：close < zone['lower']。
                if close < zone['lower']:
                    # 注释: 计算并保存 invalid_idx。
                    invalid_idx = idx
                    # 注释: 结束当前循环。
                    break
                # 注释: 判断条件是否成立：row['low'] <= zone['upper'] and close >= zone['lower']。
                if row['low'] <= zone['upper'] and close >= zone['lower']:
                    # 注释: 按 += 更新 touches 的值。
                    touches += 1
            # 注释: 当前面的条件都不成立时进入该分支。
            else:
                # 注释: 判断条件是否成立：close > zone['upper']。
                if close > zone['upper']:
                    # 注释: 计算并保存 invalid_idx。
                    invalid_idx = idx
                    # 注释: 结束当前循环。
                    break
                # 注释: 判断条件是否成立：row['high'] >= zone['lower'] and close <= zone['upper']。
                if row['high'] >= zone['lower'] and close <= zone['upper']:
                    # 注释: 按 += 更新 touches 的值。
                    touches += 1
        # 注释: 判断条件是否成立：touches < SUP_RES_VALID_TOUCHES or invalid_idx is None。
        if touches < SUP_RES_VALID_TOUCHES or invalid_idx is None:
            # 注释: 跳过本次循环剩余逻辑，进入下一轮。
            continue
        # 注释: 判断条件是否成立：invalid_idx < len(df_closed) - 3。
        if invalid_idx < len(df_closed) - 3:
            # 注释: 跳过本次循环剩余逻辑，进入下一轮。
            continue
        # 注释: 更新 zone['touches'] 的值。
        zone['touches'] = touches
        # 注释: 更新 zone['invalidated_idx'] 的值。
        zone['invalidated_idx'] = invalid_idx
        # 注释: 判断条件是否成立：zone['type'] == 'support'。
        if zone['type'] == 'support':
            # 注释: 调用 broken['broken_support'].append 执行对应处理。
            broken['broken_support'].append(zone)
        # 注释: 当前面的条件都不成立时进入该分支。
        else:
            # 注释: 调用 broken['broken_resistance'].append 执行对应处理。
            broken['broken_resistance'].append(zone)
    # 注释: 返回计算结果：broken。
    return broken


# 注释: 定义 build_support_resistance_zones 函数，封装对应业务逻辑。
def build_support_resistance_zones(df_4h, now_dt=None):
    # 注释: 调用 get_closed_df 并保存到 df_closed。
    df_closed = get_closed_df(df_4h, '4h', now_dt=now_dt)
    # 注释: 判断条件是否成立：len(df_closed) > SUP_RES_LOOKBACK_4H。
    if len(df_closed) > SUP_RES_LOOKBACK_4H:
        # 注释: 调用 df_closed.tail 并保存到 df_closed。
        df_closed = df_closed.tail(SUP_RES_LOOKBACK_4H).reset_index(drop=True)
    # 注释: 判断条件是否成立：len(df_closed) < SUP_RES_SWING_WINDOW * 2 + 20。
    if len(df_closed) < SUP_RES_SWING_WINDOW * 2 + 20:
        # 注释: 返回包含当前处理结果的字典。
        return {'support': [], 'resistance': []}

    # 注释: 初始化 zones 列表。
    zones = []
    # 注释: 遍历 range(SUP_RES_SWING_WINDOW, len(df_closed) - SUP_RES_SWING_WINDOW)，逐项处理 idx。
    for idx in range(SUP_RES_SWING_WINDOW, len(df_closed) - SUP_RES_SWING_WINDOW):
        # 注释: 调用 price_to_float 并保存到 atr。
        atr = price_to_float(df_closed.iloc[idx].get('atr'))
        # 注释: 判断条件是否成立：atr <= 0。
        if atr <= 0:
            # 注释: 跳过本次循环剩余逻辑，进入下一轮。
            continue
        # 注释: 判断条件是否成立：is_swing_low(df_closed, idx, SUP_RES_SWING_WINDOW)。
        if is_swing_low(df_closed, idx, SUP_RES_SWING_WINDOW):
            # 注释: 调用 add_or_merge_zone 执行对应处理。
            add_or_merge_zone(zones, 'support', price_to_float(df_closed.iloc[idx]['low']), atr, idx)
        # 注释: 判断条件是否成立：is_swing_high(df_closed, idx, SUP_RES_SWING_WINDOW)。
        if is_swing_high(df_closed, idx, SUP_RES_SWING_WINDOW):
            # 注释: 调用 add_or_merge_zone 执行对应处理。
            add_or_merge_zone(zones, 'resistance', price_to_float(df_closed.iloc[idx]['high']), atr, idx)

    # 注释: 调用 rebuild_zone_touches 并保存到 active。
    active = rebuild_zone_touches(zones, df_closed)
    # 注释: 调用 recently_broken_zones 并保存到 broken。
    broken = recently_broken_zones(zones, df_closed)
    # 注释: 说明当前代码行的作用：support = sorted([z for z in active if z['type'] == 'support'], key=lambda z: z['…。
    support = sorted([z for z in active if z['type'] == 'support'], key=lambda z: z['upper'], reverse=True)
    # 注释: 说明当前代码行的作用：resistance = sorted([z for z in active if z['type'] == 'resistance'], key=lambda…。
    resistance = sorted([z for z in active if z['type'] == 'resistance'], key=lambda z: z['lower'])
    # 注释: 返回包含当前处理结果的字典。
    return {
        # 注释: 设置 support 字段的取值。
        'support': support,
        # 注释: 设置 resistance 字段的取值。
        'resistance': resistance,
        # 注释: 设置 broken_support 字段的取值。
        'broken_support': broken['broken_support'],
        # 注释: 设置 broken_resistance 字段的取值。
        'broken_resistance': broken['broken_resistance']
    # 注释: 结束当前多行结构。
    }


# 注释: 定义 nearest_opposite_zone 函数，封装对应业务逻辑。
def nearest_opposite_zone(zones, side, price):
    # 注释: 判断条件是否成立：side == 'long'。
    if side == 'long':
        # 注释: 初始化 candidates 列表。
        candidates = [z for z in zones.get('resistance', []) if z['lower'] > price]
        # 注释: 返回计算结果：min(candidates, key=lambda z: z['lower']) if candidates else None。
        return min(candidates, key=lambda z: z['lower']) if candidates else None
    # 注释: 初始化 candidates 列表。
    candidates = [z for z in zones.get('support', []) if z['upper'] < price]
    # 注释: 返回计算结果：max(candidates, key=lambda z: z['upper']) if candidates else None。
    return max(candidates, key=lambda z: z['upper']) if candidates else None


# 注释: 定义 target_from_zones_or_rr 函数，封装对应业务逻辑。
def target_from_zones_or_rr(zones, side, entry, stop):
    # 注释: 调用 nearest_opposite_zone 并保存到 opposite。
    opposite = nearest_opposite_zone(zones, side, entry)
    # 注释: 判断条件是否成立：opposite。
    if opposite:
        # 注释: 返回计算结果：opposite['lower'] if side == 'long' else opposite['upper'], opposite。
        return opposite['lower'] if side == 'long' else opposite['upper'], opposite
    # 注释: 调用 abs 并保存到 risk。
    risk = abs(entry - stop)
    # 注释: 判断条件是否成立：side == 'long'。
    if side == 'long':
        # 注释: 返回计算结果：entry + 2 * risk, None。
        return entry + 2 * risk, None
    # 注释: 返回计算结果：entry - 2 * risk, None。
    return entry - 2 * risk, None


# 注释: 定义 expected_profit_ok 函数，封装对应业务逻辑。
def expected_profit_ok(side, entry, stop, target, atr):
    # 注释: 判断条件是否成立：entry <= 0 or stop <= 0 or target <= 0。
    if entry <= 0 or stop <= 0 or target <= 0:
        # 注释: 返回计算结果：False, '价格数据无效'。
        return False, '价格数据无效'
    # 注释: 判断条件是否成立：side == 'long'。
    if side == 'long':
        # 注释: 计算并保存 profit_distance。
        profit_distance = target - entry
        # 注释: 计算并保存 risk。
        risk = entry - stop
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 计算并保存 profit_distance。
        profit_distance = entry - target
        # 注释: 计算并保存 risk。
        risk = stop - entry
    # 注释: 判断条件是否成立：profit_distance <= 0 or risk <= 0。
    if profit_distance <= 0 or risk <= 0:
        # 注释: 返回计算结果：False, '盈亏方向无效'。
        return False, '盈亏方向无效'
    # 注释: 计算并保存 fee_rate。
    fee_rate = 0.0004
    # 注释: 计算并保存 min_fee_price。
    min_fee_price = entry * fee_rate * MIN_EXPECTED_PROFIT_FEE_MULTIPLIER
    # 注释: 计算并保存 min_atr_price。
    min_atr_price = atr * MIN_EXPECTED_PROFIT_ATR if atr and atr > 0 else 0
    # 注释: 调用 max 并保存到 min_required。
    min_required = max(min_fee_price, min_atr_price)
    # 注释: 判断条件是否成立：profit_distance < min_required。
    if profit_distance < min_required:
        # 注释: 返回计算结果：False, f"预期空间过小 profit={profit_distance:.4f}, required={min_required:.4f}"。
        return False, f"预期空间过小 profit={profit_distance:.4f}, required={min_required:.4f}"
    # 注释: 返回计算结果：True, 'ok'。
    return True, 'ok'


# 注释: 定义 find_1h_sr_confirmation 函数，封装对应业务逻辑。
def find_1h_sr_confirmation(df_1h, zone, side, now_dt=None):
    # 注释: 调用 get_closed_df 并保存到 df_closed。
    df_closed = get_closed_df(df_1h, '1h', now_dt=now_dt)
    # 注释: 判断条件是否成立：len(df_closed) < 3。
    if len(df_closed) < 3:
        # 注释: 返回固定结果 None。
        return None
    # 注释: 调用 df_closed.tail 并保存到 recent。
    recent = df_closed.tail(SR_CONFIRM_LOOKBACK_1H)
    # 注释: 计算并保存 touched。
    touched = False
    # 注释: 遍历 recent.iterrows()，逐项处理 _, row。
    for _, row in recent.iterrows():
        # 注释: 调用 candle_parts 并保存到 open_price, high, low, close, _, _。
        open_price, high, low, close, _, _ = candle_parts(row)
        # 注释: 调用 abs 并保存到 body。
        body = abs(close - open_price)
        # 注释: 判断条件是否成立：side == 'long'。
        if side == 'long':
            # 注释: 判断条件是否成立：low <= zone['upper'] and close >= zone['lower']。
            if low <= zone['upper'] and close >= zone['lower']:
                # 注释: 计算并保存 touched。
                touched = True
            # 注释: 调用 min 并保存到 lower_wick。
            lower_wick = min(open_price, close) - low
            # 注释: 说明当前代码行的作用：confirm = touched and close > open_price and close > zone['upper'] and lower_wick…。
            confirm = touched and close > open_price and close > zone['upper'] and lower_wick <= body * 0.5
            # 注释: 判断条件是否成立：confirm。
            if confirm:
                # 注释: 返回包含当前处理结果的字典。
                return {'high': high, 'low': low, 'timestamp': row.get('timestamp'), 'bar_time': format_bar_time(row.get('timestamp')), 'zone': zone}
        # 注释: 当前面的条件都不成立时进入该分支。
        else:
            # 注释: 判断条件是否成立：high >= zone['lower'] and close <= zone['upper']。
            if high >= zone['lower'] and close <= zone['upper']:
                # 注释: 计算并保存 touched。
                touched = True
            # 注释: 计算并保存 upper_wick。
            upper_wick = high - max(open_price, close)
            # 注释: 说明当前代码行的作用：confirm = touched and close < open_price and close < zone['lower'] and upper_wick…。
            confirm = touched and close < open_price and close < zone['lower'] and upper_wick <= body * 0.5
            # 注释: 判断条件是否成立：confirm。
            if confirm:
                # 注释: 返回包含当前处理结果的字典。
                return {'high': high, 'low': low, 'timestamp': row.get('timestamp'), 'bar_time': format_bar_time(row.get('timestamp')), 'zone': zone}
    # 注释: 返回固定结果 None。
    return None


# 注释: 定义 sr_15m_entry_ready 函数，封装对应业务逻辑。
def sr_15m_entry_ready(df_15m, confirm, side, now_dt=None):
    # 注释: 调用 get_closed_df 并保存到 df_closed。
    df_closed = get_closed_df(df_15m, '15m', now_dt=now_dt)
    # 注释: 判断条件是否成立：len(df_closed) == 0 or confirm is None。
    if len(df_closed) == 0 or confirm is None:
        # 注释: 返回固定结果 False。
        return False
    # 注释: 说明当前代码行的作用：observed = df_closed[df_closed['timestamp'] >= confirm['timestamp']].tail(SR_ENTR…。
    observed = df_closed[df_closed['timestamp'] >= confirm['timestamp']].tail(SR_ENTRY_LOOKBACK_15M)
    # 注释: 判断条件是否成立：len(observed) == 0。
    if len(observed) == 0:
        # 注释: 返回固定结果 False。
        return False
    # 注释: 判断条件是否成立：side == 'long'。
    if side == 'long':
        # 注释: 判断条件是否成立：bool((observed['low'] < confirm['low']).any())。
        if bool((observed['low'] < confirm['low']).any()):
            # 注释: 返回固定结果 False。
            return False
        # 注释: 返回计算结果：price_to_float(observed.iloc[-1]['close']) > confirm['high']。
        return price_to_float(observed.iloc[-1]['close']) > confirm['high']
    # 注释: 判断条件是否成立：bool((observed['high'] > confirm['high']).any())。
    if bool((observed['high'] > confirm['high']).any()):
        # 注释: 返回固定结果 False。
        return False
    # 注释: 返回计算结果：price_to_float(observed.iloc[-1]['close']) < confirm['low']。
    return price_to_float(observed.iloc[-1]['close']) < confirm['low']


# 注释: 定义 build_sr_rebound_candidates 函数，封装对应业务逻辑。
def build_sr_rebound_candidates(context, side):
    # 注释: 计算并保存 zones。
    zones = context['zones']
    # 注释: 调用 get_closed_df 并保存到 df_4h_closed。
    df_4h_closed = get_closed_df(context['dfs']['4h'], '4h', now_dt=context['now_dt'])
    # 注释: 判断条件是否成立：len(df_4h_closed) == 0。
    if len(df_4h_closed) == 0:
        # 注释: 返回包含当前处理结果的列表。
        return []
    # 注释: 调用 price_to_float 并保存到 atr_4h。
    atr_4h = price_to_float(df_4h_closed.iloc[-1].get('atr'))
    # 注释: 判断条件是否成立：atr_4h <= 0。
    if atr_4h <= 0:
        # 注释: 返回包含当前处理结果的列表。
        return []

    # 注释: 说明当前代码行的作用：zone_list = zones.get('support' if side == 'long' else 'resistance', [])。
    zone_list = zones.get('support' if side == 'long' else 'resistance', [])
    # 注释: 初始化 candidates 列表。
    candidates = []
    # 注释: 遍历 zone_list[:5]，逐项处理 zone。
    for zone in zone_list[:5]:
        # 注释: 调用 find_1h_sr_confirmation 并保存到 confirm。
        confirm = find_1h_sr_confirmation(context['dfs']['1h'], zone, side, now_dt=context['now_dt'])
        # 注释: 判断条件是否成立：not sr_15m_entry_ready(context['dfs']['15m'], confirm, side, now_dt=context['now_…。
        if not sr_15m_entry_ready(context['dfs']['15m'], confirm, side, now_dt=context['now_dt']):
            # 注释: 跳过本次循环剩余逻辑，进入下一轮。
            continue
        # 注释: 调用 price_to_float 并保存到 entry_ref。
        entry_ref = price_to_float(get_closed_df(context['dfs']['15m'], '15m', context['now_dt']).iloc[-1]['close'])
        # 注释: 调用 get_price_tick 并保存到 tick。
        tick = get_price_tick(entry_ref)
        # 注释: 判断条件是否成立：side == 'long'。
        if side == 'long':
            # 注释: 调用 min 并保存到 stop。
            stop = min(zone['lower'] - SUP_RES_STOP_BUFFER_ATR * atr_4h, confirm['low'] - tick)
        # 注释: 当前面的条件都不成立时进入该分支。
        else:
            # 注释: 调用 max 并保存到 stop。
            stop = max(zone['upper'] + SUP_RES_STOP_BUFFER_ATR * atr_4h, confirm['high'] + tick)
        # 注释: 调用 target_from_zones_or_rr 并保存到 target, target_zone。
        target, target_zone = target_from_zones_or_rr(zones, side, entry_ref, stop)
        # 注释: 调用 expected_profit_ok 并保存到 ok, reason。
        ok, reason = expected_profit_ok(side, entry_ref, stop, target, atr_4h)
        # 注释: 判断条件是否成立：not ok。
        if not ok:
            # 注释: 写入运行日志，记录当前处理进度。
            logging.info(f"跳过SR反弹{side}: {reason}")
            # 注释: 跳过本次循环剩余逻辑，进入下一轮。
            continue
        # 注释: 调用 candidates.append 执行对应处理。
        candidates.append({
            # 注释: 设置 module 字段的取值。
            'module': 'sr_rebound',
            # 注释: 设置 strategy_tf 字段的取值。
            'strategy_tf': '15m',
            # 注释: 设置 trigger_tf 字段的取值。
            'trigger_tf': '15m',
            # 注释: 设置 exit_tf 字段的取值。
            'exit_tf': '5m',
            # 注释: 设置 side 字段的取值。
            'side': side,
            # 注释: 设置 entry_level 字段的取值。
            'entry_level': entry_ref,
            # 注释: 设置 stop 字段的取值。
            'stop': precision_price(stop),
            # 注释: 设置 target 字段的取值。
            'target': precision_price(target),
            # 注释: 设置 target_zone 字段的取值。
            'target_zone': target_zone,
            # 注释: 设置 reason 字段的取值。
            'reason': f"4H有效{'支撑' if side == 'long' else '阻力'} + 1H确认 + 15M入场",
            # 注释: 设置 confirm 字段的取值。
            'confirm': confirm,
            # 注释: 设置 state_summary 字段的取值。
            'state_summary': f"SR {side}: zone={zone['lower']:.4f}-{zone['upper']:.4f}, confirm={confirm['bar_time']}"
        # 注释: 结束当前多行结构。
        })
    # 注释: 返回计算结果：candidates。
    return candidates


# 注释: 定义 build_sr_breakout_candidates 函数，封装对应业务逻辑。
def build_sr_breakout_candidates(context, side):
    # 注释: 计算并保存 zones。
    zones = context['zones']
    # 注释: 调用 get_closed_df 并保存到 df_4h_closed。
    df_4h_closed = get_closed_df(context['dfs']['4h'], '4h', now_dt=context['now_dt'])
    # 注释: 判断条件是否成立：len(df_4h_closed) < 3。
    if len(df_4h_closed) < 3:
        # 注释: 返回包含当前处理结果的列表。
        return []
    # 注释: 调用 price_to_float 并保存到 atr_4h。
    atr_4h = price_to_float(df_4h_closed.iloc[-1].get('atr'))
    # 注释: 判断条件是否成立：atr_4h <= 0。
    if atr_4h <= 0:
        # 注释: 返回包含当前处理结果的列表。
        return []
    # 注释: 调用 df_4h_closed.tail 并保存到 recent3。
    recent3 = df_4h_closed.tail(3)
    # 注释: 初始化 candidates 列表。
    candidates = []
    # 注释: 判断条件是否成立：side == 'long'。
    if side == 'long':
        # 注释: 遍历 zones.get('broken_resistance', [])[:5]，逐项处理 zone。
        for zone in zones.get('broken_resistance', [])[:5]:
            # 注释: 判断条件是否成立：recent3.iloc[0]['close'] > zone['upper'] and bool((recent3.iloc[1:]['close'] >= z…。
            if recent3.iloc[0]['close'] > zone['upper'] and bool((recent3.iloc[1:]['close'] >= zone['upper']).all()):
                # 注释: 调用 price_to_float 并保存到 entry_ref。
                entry_ref = price_to_float(recent3.iloc[-1]['close'])
                # 注释: 计算并保存 stop。
                stop = zone['upper'] - SUP_RES_STOP_BUFFER_ATR * atr_4h
                # 注释: 调用 target_from_zones_or_rr 并保存到 target, target_zone。
                target, target_zone = target_from_zones_or_rr(zones, side, entry_ref, stop)
                # 注释: 初始化 candidates.append({'module': 'sr_breakout', 'strategy_tf': '4h', 'trigger_tf': '4… 字典。
                candidates.append({'module': 'sr_breakout', 'strategy_tf': '4h', 'trigger_tf': '4h', 'exit_tf': '1h', 'side': side, 'entry_level': entry_ref, 'stop': precision_price(stop), 'target': precision_price(target), 'target_zone': target_zone, 'reason': "4H有效阻力突破并连续2根收盘守住", 'state_summary': f"SR breakout long: previous_res={zone['lower']:.4f}-{zone['upper']:.4f}"})
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 遍历 zones.get('broken_support', [])[:5]，逐项处理 zone。
        for zone in zones.get('broken_support', [])[:5]:
            # 注释: 判断条件是否成立：recent3.iloc[0]['close'] < zone['lower'] and bool((recent3.iloc[1:]['close'] <= z…。
            if recent3.iloc[0]['close'] < zone['lower'] and bool((recent3.iloc[1:]['close'] <= zone['lower']).all()):
                # 注释: 调用 price_to_float 并保存到 entry_ref。
                entry_ref = price_to_float(recent3.iloc[-1]['close'])
                # 注释: 计算并保存 stop。
                stop = zone['lower'] + SUP_RES_STOP_BUFFER_ATR * atr_4h
                # 注释: 调用 target_from_zones_or_rr 并保存到 target, target_zone。
                target, target_zone = target_from_zones_or_rr(zones, side, entry_ref, stop)
                # 注释: 初始化 candidates.append({'module': 'sr_breakout', 'strategy_tf': '4h', 'trigger_tf': '4… 字典。
                candidates.append({'module': 'sr_breakout', 'strategy_tf': '4h', 'trigger_tf': '4h', 'exit_tf': '1h', 'side': side, 'entry_level': entry_ref, 'stop': precision_price(stop), 'target': precision_price(target), 'target_zone': target_zone, 'reason': "4H有效支撑跌破并连续2根收盘压住", 'state_summary': f"SR breakout short: previous_sup={zone['lower']:.4f}-{zone['upper']:.4f}"})
    # 注释: 返回计算结果：candidates。
    return candidates


# 注释: 定义 make_empty_state 函数，封装对应业务逻辑。
def make_empty_state(timeframe, summary=''):
    # 注释: 返回包含当前处理结果的字典。
    return {'timeframe': timeframe, 'signal_bar_time': '', 'long_trend': False, 'short_trend': False, 'pullback_long': False, 'pullback_short': False, 'close_long': False, 'close_short': False, 'summary': summary, 'details': {}}


# 注释: 定义 make_state_summary 函数，封装对应业务逻辑。
def make_state_summary(context, timeframe, side, summary):
    # 注释: 调用 dict 并保存到 state。
    state = dict(context['trend_states'].get(timeframe, make_empty_state(timeframe)))
    # 注释: 更新 state['summary'] 的值。
    state['summary'] = summary or state.get('summary', '')
    # 注释: 判断条件是否成立：side == 'long'。
    if side == 'long':
        # 注释: 更新 state['long_trend'] 的值。
        state['long_trend'] = True
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 更新 state['short_trend'] 的值。
        state['short_trend'] = True
    # 注释: 返回计算结果：state。
    return state


# 注释: 定义 candidate_priority 函数，封装对应业务逻辑。
def candidate_priority(candidate):
    # 注释: 初始化 tf_rank 字典。
    tf_rank = {'4h': 0, '1h': 1, '15m': 2}
    # 注释: 初始化 module_rank 字典。
    module_rank = {'sr_breakout': 0, 'sr_rebound': 1, 'trend_trigger': 2}
    # 注释: 返回计算结果：(tf_rank.get(candidate.get('strategy_tf'), 9), module_rank.get(candidate.get('mod…。
    return (tf_rank.get(candidate.get('strategy_tf'), 9), module_rank.get(candidate.get('module'), 9))


# 注释: 定义 candidate_entry_price_still_valid 函数，封装对应业务逻辑。
def candidate_entry_price_still_valid(candidate, curr_price):
    # 注释: 调用 candidate.get 并保存到 side。
    side = candidate.get('side')
    # 注释: 调用 price_to_float 并保存到 entry_level。
    entry_level = price_to_float(candidate.get('entry_level'))
    # 注释: 判断条件是否成立：entry_level <= 0。
    if entry_level <= 0:
        # 注释: 返回计算结果：False, f"候选入场价无效: module={candidate.get('module')}, entry_level={candidate.get('e…。
        return False, f"候选入场价无效: module={candidate.get('module')}, entry_level={candidate.get('entry_level')}"
    # 注释: 判断条件是否成立：side == 'long' and curr_price < entry_level。
    if side == 'long' and curr_price < entry_level:
        # 注释: 返回计算结果：False, f"多单触发价已失效: module={candidate.get('module')}, current={curr_price}, entry_…。
        return False, f"多单触发价已失效: module={candidate.get('module')}, current={curr_price}, entry_level={entry_level}"
    # 注释: 判断条件是否成立：side == 'short' and curr_price > entry_level。
    if side == 'short' and curr_price > entry_level:
        # 注释: 返回计算结果：False, f"空单触发价已失效: module={candidate.get('module')}, current={curr_price}, entry_…。
        return False, f"空单触发价已失效: module={candidate.get('module')}, current={curr_price}, entry_level={entry_level}"
    # 注释: 返回计算结果：True, 'ok'。
    return True, 'ok'


# 注释: 定义 build_trend_trigger_candidates 函数，封装对应业务逻辑。
def build_trend_trigger_candidates(context):
    # 注释: 初始化 candidates 列表。
    candidates = []
    # 注释: 计算并保存 zones。
    zones = context['zones']
    # 注释: 遍历 STRATEGY_TIMEFRAMES，逐项处理 strategy_tf。
    for strategy_tf in STRATEGY_TIMEFRAMES:
        # 注释: 调用 context['trend_states'].get 并保存到 local_state。
        local_state = context['trend_states'].get(strategy_tf)
        # 注释: 计算并保存 background_tf。
        background_tf = STRATEGY_BACKGROUND_TF[strategy_tf]
        # 注释: 调用 context['trend_states'].get 并保存到 background_state。
        background_state = context['trend_states'].get(background_tf)
        # 注释: 计算并保存 trigger_tf。
        trigger_tf = STRATEGY_TRIGGER_TF[strategy_tf]
        # 注释: 计算并保存 trigger_df。
        trigger_df = context['dfs'][trigger_tf]

        # 注释: 遍历 ('long', 'short')，逐项处理 side。
        for side in ('long', 'short'):
            # 注释: 调用 context_allows_side 并保存到 allowed, deny_reason。
            allowed, deny_reason = context_allows_side(local_state, background_state, side)
            # 注释: 判断条件是否成立：not allowed。
            if not allowed:
                # 注释: 写入运行日志，记录当前处理进度。
                logging.info(f"跳过{strategy_tf}->{trigger_tf} {side}: {deny_reason}")
                # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                continue
            # 注释: 判断条件是否成立：strategy_tf == '15m'。
            if strategy_tf == '15m':
                # 注释: 调用 detect_strong_chop 并保存到 trigger_chop。
                trigger_chop = detect_strong_chop(get_closed_df(trigger_df, trigger_tf, context['now_dt']))
                # 注释: 判断条件是否成立：trigger_chop.get('is_chop')。
                if trigger_chop.get('is_chop'):
                    # 注释: 写入运行日志，记录当前处理进度。
                    logging.info(f"跳过15m策略{side}: 5m强K交叉震荡未突破")
                    # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                    continue

            # 注释: 调用 detect_entry_trigger 并保存到 trigger。
            trigger = detect_entry_trigger(trigger_df, trigger_tf, side, now_dt=context['now_dt'])
            # 注释: 判断条件是否成立：not trigger。
            if not trigger:
                # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                continue
            # 注释: 判断条件是否成立：trigger.get('blocked')。
            if trigger.get('blocked'):
                # 注释: 写入运行日志，记录当前处理进度。
                logging.info(trigger.get('reason'))
                # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                continue

            # 注释: 调用 price_to_float 并保存到 entry_ref。
            entry_ref = price_to_float(get_closed_df(trigger_df, trigger_tf, context['now_dt']).iloc[-1]['close'])
            # 注释: 调用 price_to_float 并保存到 stop。
            stop = price_to_float(trigger['stop'])
            # 注释: 判断条件是否成立：background_state.get('details', {}).get('extreme_adx')。
            if background_state.get('details', {}).get('extreme_adx'):
                # 注释: 调用 get_closed_df 并保存到 bg_closed。
                bg_closed = get_closed_df(context['dfs'][background_tf], background_tf, context['now_dt'])
                # 注释: 调用 price_to_float 并保存到 atr。
                atr = price_to_float(bg_closed.iloc[-1].get('atr')) if len(bg_closed) else 0.0
                # 注释: 判断条件是否成立：atr > 0。
                if atr > 0:
                    # 注释: 判断条件是否成立：side == 'long'。
                    if side == 'long':
                        # 注释: 调用 max 并保存到 stop。
                        stop = max(stop, entry_ref - EXTREME_ADX_STOP_BUFFER_ATR * atr)
                    # 注释: 当前面的条件都不成立时进入该分支。
                    else:
                        # 注释: 调用 min 并保存到 stop。
                        stop = min(stop, entry_ref + EXTREME_ADX_STOP_BUFFER_ATR * atr)
            # 注释: 调用 target_from_zones_or_rr 并保存到 target, target_zone。
            target, target_zone = target_from_zones_or_rr(zones, side, entry_ref, stop)
            # 注释: 调用 price_to_float 并保存到 trigger_atr。
            trigger_atr = price_to_float(get_closed_df(trigger_df, trigger_tf, context['now_dt']).iloc[-1].get('atr'))
            # 注释: 调用 expected_profit_ok 并保存到 ok, reason。
            ok, reason = expected_profit_ok(side, entry_ref, stop, target, trigger_atr)
            # 注释: 判断条件是否成立：not ok。
            if not ok:
                # 注释: 写入运行日志，记录当前处理进度。
                logging.info(f"跳过趋势触发{strategy_tf}->{trigger_tf} {side}: {reason}")
                # 注释: 跳过本次循环剩余逻辑，进入下一轮。
                continue

            # 注释: 调用 candidates.append 执行对应处理。
            candidates.append({
                # 注释: 设置 module 字段的取值。
                'module': 'trend_trigger',
                # 注释: 设置 strategy_tf 字段的取值。
                'strategy_tf': strategy_tf,
                # 注释: 设置 trigger_tf 字段的取值。
                'trigger_tf': trigger_tf,
                # 注释: 设置 exit_tf 字段的取值。
                'exit_tf': STRATEGY_EXIT_TF[strategy_tf],
                # 注释: 设置 side 字段的取值。
                'side': side,
                # 注释: 设置 entry_level 字段的取值。
                'entry_level': trigger['entry_level'],
                # 注释: 设置 stop 字段的取值。
                'stop': precision_price(stop),
                # 注释: 设置 target 字段的取值。
                'target': precision_price(target),
                # 注释: 设置 target_zone 字段的取值。
                'target_zone': target_zone,
                # 注释: 设置 reason 字段的取值。
                'reason': f"{strategy_tf}趋势 + {trigger['reason']}",
                # 注释: 设置 state_summary 字段的取值。
                'state_summary': f"{strategy_tf}/{background_tf}/{trigger_tf}: {trigger['reason']}; bg={background_state.get('status')}",
                # 注释: 设置 trigger 字段的取值。
                'trigger': trigger
            # 注释: 结束当前多行结构。
            })
    # 注释: 返回计算结果：candidates。
    return candidates


# 注释: 定义 build_entry_candidates 函数，封装对应业务逻辑。
def build_entry_candidates(context):
    # 注释: 调用 build_trend_trigger_candidates 并保存到 candidates。
    candidates = build_trend_trigger_candidates(context)
    # 注释: 遍历 ('long', 'short')，逐项处理 side。
    for side in ('long', 'short'):
        # 注释: 调用 context_allows_side 并保存到 allowed, deny_reason。
        allowed, deny_reason = context_allows_side(context['trend_states'].get('15m'), context['trend_states'].get('1h'), side)
        # 注释: 判断条件是否成立：allowed。
        if allowed:
            # 注释: 调用 candidates.extend 执行对应处理。
            candidates.extend(build_sr_rebound_candidates(context, side))
        # 注释: 当前面的条件都不成立时进入该分支。
        else:
            # 注释: 写入运行日志，记录当前处理进度。
            logging.info(f"跳过SR反弹{side}: {deny_reason}")

        # 注释: 调用 context_allows_side 并保存到 allowed, deny_reason。
        allowed, deny_reason = context_allows_side(context['trend_states'].get('4h'), context['trend_states'].get('1d'), side)
        # 注释: 判断条件是否成立：allowed。
        if allowed:
            # 注释: 调用 candidates.extend 执行对应处理。
            candidates.extend(build_sr_breakout_candidates(context, side))
        # 注释: 当前面的条件都不成立时进入该分支。
        else:
            # 注释: 写入运行日志，记录当前处理进度。
            logging.info(f"跳过SR突破{side}: {deny_reason}")

    # 注释: 计算并保存 candidates.sort(key。
    candidates.sort(key=candidate_priority)
    # 注释: 返回计算结果：candidates。
    return candidates


# 注释: 定义 build_context 函数，封装对应业务逻辑。
def build_context(dfs, now_dt):
    # 注释: 初始化 trend_states 字典。
    trend_states = {
        # 注释: 计算并保存 timeframe: evaluate_adx_ema_context(df, timeframe, now_dt。
        timeframe: evaluate_adx_ema_context(df, timeframe, now_dt=now_dt)
        # 注释: 说明当前代码行的作用：for timeframe, df in dfs.items()。
        for timeframe, df in dfs.items()
        # 注释: 说明当前代码行的作用：if timeframe in ('15m', '1h', '4h', '1d')。
        if timeframe in ('15m', '1h', '4h', '1d')
    # 注释: 结束当前多行结构。
    }
    # 注释: 返回包含当前处理结果的字典。
    return {'dfs': dfs, 'now_dt': now_dt, 'trend_states': trend_states, 'zones': build_support_resistance_zones(dfs['4h'], now_dt=now_dt)}


# 注释: 定义 is_in_post_exit_cooldown 函数，封装对应业务逻辑。
def is_in_post_exit_cooldown(now_dt):
    # 注释: 调用 parse_bar_time 并保存到 last_exit_dt。
    last_exit_dt = parse_bar_time(trade_state.get('last_exit_time', ''))
    # 注释: 判断条件是否成立：last_exit_dt is None。
    if last_exit_dt is None:
        # 注释: 返回固定结果 False。
        return False
    # 注释: 判断条件是否成立：now_dt.tzinfo is None。
    if now_dt.tzinfo is None:
        # 注释: 调用 now_dt.replace 并保存到 now_dt。
        now_dt = now_dt.replace(tzinfo=EXCHANGE_TZ)
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 调用 now_dt.astimezone 并保存到 now_dt。
        now_dt = now_dt.astimezone(EXCHANGE_TZ)
    # 注释: 返回计算结果：(now_dt - last_exit_dt).total_seconds() < POST_EXIT_COOLDOWN_SECONDS。
    return (now_dt - last_exit_dt).total_seconds() < POST_EXIT_COOLDOWN_SECONDS


# 注释: 定义 update_stop_if_tighter 函数，封装对应业务逻辑。
def update_stop_if_tighter(side, new_stop, reason, curr_price, signal_bar_15m=''):
    # 注释: 调用 price_to_float 并保存到 current_stop。
    current_stop = price_to_float(trade_state.get('stop_loss_price'))
    # 注释: 判断条件是否成立：new_stop <= 0。
    if new_stop <= 0:
        # 注释: 返回固定结果 False。
        return False
    # 注释: 判断条件是否成立：side == 'long'。
    if side == 'long':
        # 注释: 判断条件是否成立：new_stop <= current_stop or new_stop >= curr_price。
        if new_stop <= current_stop or new_stop >= curr_price:
            # 注释: 返回固定结果 False。
            return False
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 判断条件是否成立：(current_stop > 0 and new_stop >= current_stop) or new_stop <= curr_price。
        if (current_stop > 0 and new_stop >= current_stop) or new_stop <= curr_price:
            # 注释: 返回固定结果 False。
            return False
    # 注释: 调用 precision_price 并保存到 new_stop。
    new_stop = precision_price(new_stop)
    # 注释: 判断条件是否成立：refresh_protective_stop_order(new_stop)。
    if refresh_protective_stop_order(new_stop):
        # 注释: 更新 trade_state['stop_loss_price'] 的值。
        trade_state['stop_loss_price'] = new_stop
        # 注释: 写入运行日志，记录当前处理进度。
        logging.info(f"{reason}: 已收紧服务端止损到 {new_stop}")
        # 注释: 返回固定结果 True。
        return True
    # 注释: 计算并保存 handle_stop_order_refresh_failure(reason, curr_price, signal_bar_15m。
    handle_stop_order_refresh_failure(reason, curr_price, signal_bar_15m=signal_bar_15m, trigger_label=reason)
    # 注释: 返回固定结果 False。
    return False


# 注释: 定义 close_partial_position 函数，封装对应业务逻辑。
def close_partial_position(reason, ratio=0.5, curr_price=None, signal_bar_15m=''):
    """第一目标位半仓止盈，并将剩余仓位止损推到成本附近。"""
    # 注释: 判断条件是否成立：trade_state.get('partial_taken') or not trade_state.get('has_position')。
    if trade_state.get('partial_taken') or not trade_state.get('has_position'):
        # 注释: 返回固定结果 False。
        return False
    # 注释: 调用 trade_state.get 并保存到 side。
    side = trade_state.get('side')
    # 注释: 调用 price_to_float 并保存到 total_amount。
    total_amount = price_to_float(trade_state.get('amount'))
    # 注释: 判断条件是否成立：total_amount <= 0。
    if total_amount <= 0:
        # 注释: 返回固定结果 False。
        return False
    # 注释: 判断条件是否成立：curr_price is None。
    if curr_price is None:
        # 注释: 调用 get_latest_price 并保存到 curr_price。
        curr_price = get_latest_price()

    # 注释: 计算并保存 close_amount。
    close_amount = total_amount * ratio
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 调用 float 并保存到 close_amount。
        close_amount = float(exchange.amount_to_precision(SYMBOL, close_amount))
    # 注释: 捕获异常分支：except Exception。
    except Exception:
        # 注释: 调用 float 并保存到 close_amount。
        close_amount = float(close_amount)
    # 注释: 判断条件是否成立：close_amount <= 0 or close_amount >= total_amount。
    if close_amount <= 0 or close_amount >= total_amount:
        # 注释: 返回固定结果 False。
        return False

    # 注释: 说明当前代码行的作用：close_side = 'sell' if side == 'long' else 'buy'。
    close_side = 'sell' if side == 'long' else 'buy'
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 计算并保存 cancel_protective_stop_order(silent。
        cancel_protective_stop_order(silent=True)
        # 注释: 调用 exchange.create_market_order 并保存到 order。
        order = exchange.create_market_order(SYMBOL, close_side, close_amount)
        # 注释: 调用 order.get 并保存到 actual_close_price。
        actual_close_price = order.get('average', curr_price) or curr_price
        # 注释: 调用 get_trading_fee_rate 并保存到 taker_fee_rate。
        taker_fee_rate = get_trading_fee_rate()
        # 注释: 调用 float 并保存到 partial_fee。
        partial_fee = float(actual_close_price) * close_amount * taker_fee_rate
        # 注释: 调用 price_to_float 并保存到 updated_open_fee。
        updated_open_fee = price_to_float(trade_state.get('open_fee')) + partial_fee
        # 注释: 调用 max 并保存到 remaining_amount_raw。
        remaining_amount_raw = max(total_amount - close_amount, 0.0)
        # 注释: 计算并保存 remaining_amount。
        remaining_amount = remaining_amount_raw
        # 注释: 开始执行可能抛出异常的逻辑。
        try:
            # 注释: 调用 float 并保存到 remaining_amount。
            remaining_amount = float(exchange.amount_to_precision(SYMBOL, remaining_amount))
        # 注释: 捕获异常分支：except Exception。
        except Exception:
            # 注释: 调用 float 并保存到 remaining_amount。
            remaining_amount = float(remaining_amount)

        # 注释: 调用 get_position_risk 并保存到 position_after_partial。
        position_after_partial = get_position_risk(side=side)
        # 注释: 判断条件是否成立：position_after_partial and not position_after_partial.get('fetch_failed')。
        if position_after_partial and not position_after_partial.get('fetch_failed'):
            # 注释: 调用 abs 并保存到 exchange_remaining。
            exchange_remaining = abs(price_to_float(position_after_partial.get('position_amt')))
            # 注释: 判断条件是否成立：exchange_remaining > POSITION_AMT_EPSILON。
            if exchange_remaining > POSITION_AMT_EPSILON:
                # 注释: 计算并保存 remaining_amount。
                remaining_amount = exchange_remaining
        # 注释: 继续判断备选条件：position_after_partial is None and remaining_amount <= POSITION_AMT_EPSILON。
        elif position_after_partial is None and remaining_amount <= POSITION_AMT_EPSILON:
            # 注释: 更新 trade_state['amount'] 的值。
            trade_state['amount'] = 0
            # 注释: 更新 trade_state['open_fee'] 的值。
            trade_state['open_fee'] = updated_open_fee
            # 注释: 更新 trade_state['partial_taken'] 的值。
            trade_state['partial_taken'] = True
            # 注释: 调用 reset_trade_state_after_external_close 执行对应处理。
            reset_trade_state_after_external_close(
                # 注释: 计算并保存 signal_bar_15m。
                signal_bar_15m=signal_bar_15m,
                # 注释: 计算并保存 reason。
                reason="半仓止盈后交易所已无剩余有效仓位",
                # 注释: 初始化 external_context 字典。
                external_context={'external_close_order_id': extract_order_id(order)}
            # 注释: 结束当前多行结构。
            )
            # 注释: 返回固定结果 True。
            return True
        # 注释: 继续判断备选条件：remaining_amount <= POSITION_AMT_EPSILON and remaining_amount_raw > POSITION_AMT_…。
        elif remaining_amount <= POSITION_AMT_EPSILON and remaining_amount_raw > POSITION_AMT_EPSILON:
            # 注释: 写入警告日志，提示需要关注的非致命问题。
            logging.warning(
                # 注释: 拼接或传入文本内容。
                f"半仓后剩余数量被交易所精度压成0，改用原始剩余数量: "
                # 注释: 拼接或传入文本内容。
                f"raw={remaining_amount_raw}, close_amount={close_amount}, total={total_amount}"
            # 注释: 结束当前多行结构。
            )
            # 注释: 计算并保存 remaining_amount。
            remaining_amount = remaining_amount_raw

        # 注释: 判断条件是否成立：remaining_amount <= POSITION_AMT_EPSILON。
        if remaining_amount <= POSITION_AMT_EPSILON:
            # 注释: 更新 trade_state['amount'] 的值。
            trade_state['amount'] = 0
            # 注释: 更新 trade_state['open_fee'] 的值。
            trade_state['open_fee'] = updated_open_fee
            # 注释: 更新 trade_state['partial_taken'] 的值。
            trade_state['partial_taken'] = True
            # 注释: 调用 reset_trade_state_after_external_close 执行对应处理。
            reset_trade_state_after_external_close(
                # 注释: 计算并保存 signal_bar_15m。
                signal_bar_15m=signal_bar_15m,
                # 注释: 计算并保存 reason。
                reason="半仓止盈后剩余数量低于有效持仓阈值",
                # 注释: 初始化 external_context 字典。
                external_context={'external_close_order_id': extract_order_id(order)}
            # 注释: 结束当前多行结构。
            )
            # 注释: 返回固定结果 True。
            return True
        # 注释: 更新 trade_state['amount'] 的值。
        trade_state['amount'] = remaining_amount
        # 注释: 更新 trade_state['open_fee'] 的值。
        trade_state['open_fee'] = updated_open_fee
        # 注释: 更新 trade_state['partial_taken'] 的值。
        trade_state['partial_taken'] = True

        # 注释: 调用 price_to_float 并保存到 entry。
        entry = price_to_float(trade_state.get('entry_price'))
        # 注释: 计算并保存 breakeven_stop。
        breakeven_stop = entry
        # 注释: 判断条件是否成立：side == 'long'。
        if side == 'long':
            # 注释: 调用 min 并保存到 breakeven_stop。
            breakeven_stop = min(entry, curr_price - get_price_tick(curr_price))
        # 注释: 当前面的条件都不成立时进入该分支。
        else:
            # 注释: 调用 max 并保存到 breakeven_stop。
            breakeven_stop = max(entry, curr_price + get_price_tick(curr_price))
        # 注释: 调用 precision_price 并保存到 breakeven_stop。
        breakeven_stop = precision_price(breakeven_stop)
        # 注释: 判断条件是否成立：stop_price_is_still_valid(curr_price, breakeven_stop, side) and refresh_protectiv…。
        if stop_price_is_still_valid(curr_price, breakeven_stop, side) and refresh_protective_stop_order(breakeven_stop):
            # 注释: 更新 trade_state['stop_loss_price'] 的值。
            trade_state['stop_loss_price'] = breakeven_stop
        # 注释: 当前面的条件都不成立时进入该分支。
        else:
            # 注释: 调用 refresh_protective_stop_order 执行对应处理。
            refresh_protective_stop_order(trade_state['stop_loss_price'])

        # 注释: 发送邮件通知当前交易事件。
        send_msg(
            # 注释: 拼接或传入文本内容。
            "ETH交易: 第一目标半仓止盈",
            # 注释: 拼接或传入文本内容。
            f"原因: {reason}\n方向: {side}\n平仓数量: {close_amount}\n剩余数量: {remaining_amount}\n"
            # 注释: 拼接或传入文本内容。
            f"平仓价: {actual_close_price}\n剩余止损: {trade_state.get('stop_loss_price')}\n15M信号时间: {signal_bar_15m}"
        # 注释: 结束当前多行结构。
        )
        # 注释: 写入运行日志，记录当前处理进度。
        logging.info(
            # 注释: 拼接或传入文本内容。
            f"半仓止盈完成: reason={reason}, close_amount={close_amount}, "
            # 注释: 拼接或传入文本内容。
            f"remaining={remaining_amount}, stop={trade_state.get('stop_loss_price')}"
        # 注释: 结束当前多行结构。
        )
        # 注释: 返回固定结果 True。
        return True
    # 注释: 捕获异常分支：except Exception as e。
    except Exception as e:
        # 注释: 写入错误日志，便于排查运行异常。
        logging.error(f"半仓止盈失败: {e}")
        # 注释: 写入错误日志，便于排查运行异常。
        logging.error(traceback.format_exc())
        # 注释: 调用 refresh_protective_stop_order 执行对应处理。
        refresh_protective_stop_order(trade_state.get('stop_loss_price'))
        # 注释: 返回固定结果 False。
        return False


# 注释: 定义 apply_new_exit_rules 函数，封装对应业务逻辑。
def apply_new_exit_rules(context, signal_bar_15m=''):
    # 注释: 判断条件是否成立：not trade_state.get('has_position')。
    if not trade_state.get('has_position'):
        # 注释: 直接结束当前函数。
        return
    # 注释: 调用 trade_state.get 并保存到 side。
    side = trade_state.get('side')
    # 注释: 调用 get_latest_price 并保存到 curr_price。
    curr_price = get_latest_price()
    # 注释: 调用 trade_state.get 并保存到 exit_tf。
    exit_tf = trade_state.get('entry_exit_tf') or STRATEGY_EXIT_TF.get(trade_state.get('entry_strategy_tf'), '15m')
    # 注释: 调用 context['dfs'].get 并保存到 df_exit。
    df_exit = context['dfs'].get(exit_tf)
    # 注释: 判断条件是否成立：df_exit is not None。
    if df_exit is not None:
        # 注释: 调用 get_closed_df 并保存到 df_exit_closed。
        df_exit_closed = get_closed_df(df_exit, exit_tf, context['now_dt'])
        # 注释: 调用 latest_large_strong 并保存到 large。
        large = latest_large_strong(df_exit_closed)
        # 注释: 判断条件是否成立：large。
        if large:
            # 注释: 判断条件是否成立：side == 'long'。
            if side == 'long':
                # 注释: 说明当前代码行的作用：proposed = large['low'] if large['direction'] == 'long' else max(large['low'], cu…。
                proposed = large['low'] if large['direction'] == 'long' else max(large['low'], curr_price - 0.3 * large['atr'])
            # 注释: 当前面的条件都不成立时进入该分支。
            else:
                # 注释: 说明当前代码行的作用：proposed = large['high'] if large['direction'] == 'short' else min(large['high'],…。
                proposed = large['high'] if large['direction'] == 'short' else min(large['high'], curr_price + 0.3 * large['atr'])
            # 注释: 计算并保存 update_stop_if_tighter(side, proposed, f"{exit_tf}出现大强线，缩紧平仓价", curr_price, signa…。
            update_stop_if_tighter(side, proposed, f"{exit_tf}出现大强线，缩紧平仓价", curr_price, signal_bar_15m=signal_bar_15m)
            # 注释: 直接结束当前函数。
            return

        # 注释: 说明当前代码行的作用：opposite = 'short' if side == 'long' else 'long'。
        opposite = 'short' if side == 'long' else 'long'
        # 注释: 调用 latest_effective_strong 并保存到 strong_opposite。
        strong_opposite = latest_effective_strong(df_exit_closed, opposite)
        # 注释: 判断条件是否成立：strong_opposite。
        if strong_opposite:
            # 注释: 说明当前代码行的作用：proposed = strong_opposite['low'] if side == 'long' else strong_opposite['high']。
            proposed = strong_opposite['low'] if side == 'long' else strong_opposite['high']
            # 注释: 计算并保存 update_stop_if_tighter(side, proposed, f"{exit_tf}出现反向强K，缩紧止盈", curr_price, signa…。
            update_stop_if_tighter(side, proposed, f"{exit_tf}出现反向强K，缩紧止盈", curr_price, signal_bar_15m=signal_bar_15m)

    # 注释: 调用 price_to_float 并保存到 target。
    target = price_to_float(trade_state.get('entry_sr_target'))
    # 注释: 调用 price_to_float 并保存到 entry。
    entry = price_to_float(trade_state.get('entry_price'))
    # 注释: 调用 price_to_float 并保存到 risk。
    risk = price_to_float(trade_state.get('entry_initial_risk'))
    # 注释: 判断条件是否成立：target <= 0 and entry > 0 and risk > 0。
    if target <= 0 and entry > 0 and risk > 0:
        # 注释: 说明当前代码行的作用：target = entry + 2 * risk if side == 'long' else entry - 2 * risk。
        target = entry + 2 * risk if side == 'long' else entry - 2 * risk
    # 注释: 判断条件是否成立：target > 0。
    if target > 0:
        # 注释: 判断条件是否成立：side == 'long' and curr_price >= target。
        if side == 'long' and curr_price >= target:
            # 注释: 判断条件是否成立：close_partial_position("达到第一目标位，平仓50%并推保护止损", curr_price=curr_price, signal_bar_1…。
            if close_partial_position("达到第一目标位，平仓50%并推保护止损", curr_price=curr_price, signal_bar_15m=signal_bar_15m):
                # 注释: 直接结束当前函数。
                return
            # 注释: 计算并保存 update_stop_if_tighter(side, max(entry, target - 0.25 * risk), "达到第一目标位，剩余仓位止损推至成…。
            update_stop_if_tighter(side, max(entry, target - 0.25 * risk), "达到第一目标位，剩余仓位止损推至成本/目标附近", curr_price, signal_bar_15m=signal_bar_15m)
        # 注释: 继续判断备选条件：side == 'short' and curr_price <= target。
        elif side == 'short' and curr_price <= target:
            # 注释: 判断条件是否成立：close_partial_position("达到第一目标位，平仓50%并推保护止损", curr_price=curr_price, signal_bar_1…。
            if close_partial_position("达到第一目标位，平仓50%并推保护止损", curr_price=curr_price, signal_bar_15m=signal_bar_15m):
                # 注释: 直接结束当前函数。
                return
            # 注释: 计算并保存 update_stop_if_tighter(side, min(entry, target + 0.25 * risk), "达到第一目标位，剩余仓位止损推至成…。
            update_stop_if_tighter(side, min(entry, target + 0.25 * risk), "达到第一目标位，剩余仓位止损推至成本/目标附近", curr_price, signal_bar_15m=signal_bar_15m)

    # 注释: 调用 get_closed_df 并保存到 df_4h_closed。
    df_4h_closed = get_closed_df(context['dfs']['4h'], '4h', context['now_dt'])
    # 注释: 判断条件是否成立：len(df_4h_closed) == 0。
    if len(df_4h_closed) == 0:
        # 注释: 直接结束当前函数。
        return
    # 注释: 调用 price_to_float 并保存到 atr_4h。
    atr_4h = price_to_float(df_4h_closed.iloc[-1].get('atr'))
    # 注释: 计算并保存 near_buffer。
    near_buffer = SUP_RES_NEAR_ATR * atr_4h
    # 注释: 调用 context.get 并保存到 zones。
    zones = context.get('zones', {})
    # 注释: 判断条件是否成立：side == 'long'。
    if side == 'long':
        # 注释: 调用 nearest_opposite_zone 并保存到 resistance。
        resistance = nearest_opposite_zone(zones, side, curr_price - near_buffer)
        # 注释: 判断条件是否成立：resistance and curr_price >= resistance['lower'] - near_buffer。
        if resistance and curr_price >= resistance['lower'] - near_buffer:
            # 注释: 更新 update_stop_if_tighter(side, max(trade_state['stop_loss_price'], resistance['lowe… 的值。
            update_stop_if_tighter(side, max(trade_state['stop_loss_price'], resistance['lower'] - 0.1 * atr_4h), "靠近有效阻力区，缩紧止盈", curr_price, signal_bar_15m=signal_bar_15m)
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 调用 nearest_opposite_zone 并保存到 support。
        support = nearest_opposite_zone(zones, side, curr_price + near_buffer)
        # 注释: 判断条件是否成立：support and curr_price <= support['upper'] + near_buffer。
        if support and curr_price <= support['upper'] + near_buffer:
            # 注释: 更新 update_stop_if_tighter(side, min(trade_state['stop_loss_price'], support['upper']… 的值。
            update_stop_if_tighter(side, min(trade_state['stop_loss_price'], support['upper'] + 0.1 * atr_4h), "靠近有效支撑区，缩紧止盈", curr_price, signal_bar_15m=signal_bar_15m)


# 注释: 定义 monitor_position_new 函数，封装对应业务逻辑。
def monitor_position_new(context, signal_bar_15m='', allow_strategy_close=False):
    """新版持仓管理：只保留仓位同步、保护止损和新策略出场规则。"""
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 调用 trade_state.get 并保存到 side。
        side = trade_state.get('side')
        # 注释: 判断条件是否成立：side not in ('long', 'short')。
        if side not in ('long', 'short'):
            # 注释: 直接结束当前函数。
            return

        # 注释: 调用 get_position_risk 并保存到 position_risk。
        position_risk = get_position_risk(side=side)
        # 注释: 判断条件是否成立：position_risk and position_risk.get('fetch_failed')。
        if position_risk and position_risk.get('fetch_failed'):
            # 注释: 写入警告日志，提示需要关注的非致命问题。
            logging.warning("本轮无法获取交易所仓位风险信息，跳过持仓管理，避免误判")
            # 注释: 直接结束当前函数。
            return
        # 注释: 判断条件是否成立：position_risk is None。
        if position_risk is None:
            # 注释: 更新 trade_state['position_miss_count'] 的值。
            trade_state['position_miss_count'] = int(trade_state.get('position_miss_count', 0) or 0) + 1
            # 注释: 判断条件是否成立：trade_state['position_miss_count'] < EXTERNAL_CLOSE_CONFIRM_MISS_COUNT。
            if trade_state['position_miss_count'] < EXTERNAL_CLOSE_CONFIRM_MISS_COUNT:
                # 注释: 写入警告日志，提示需要关注的非致命问题。
                logging.warning(
                    # 注释: 拼接或传入文本内容。
                    f"本轮未查到交易所持仓，先不重置（第{trade_state['position_miss_count']}/{EXTERNAL_CLOSE_CONFIRM_MISS_COUNT}次）"
                # 注释: 结束当前多行结构。
                )
                # 注释: 直接结束当前函数。
                return

            # 注释: 调用 has_open_position_on_exchange 并保存到 fallback_check。
            fallback_check = has_open_position_on_exchange(side=side)
            # 注释: 判断条件是否成立：fallback_check.get('fetch_failed')。
            if fallback_check.get('fetch_failed'):
                # 注释: 写入警告日志，提示需要关注的非致命问题。
                logging.warning("二次确认仓位失败，本轮不重置本地状态，避免误判")
                # 注释: 直接结束当前函数。
                return
            # 注释: 判断条件是否成立：fallback_check.get('has_position')。
            if fallback_check.get('has_position'):
                # 注释: 更新 trade_state['position_miss_count'] 的值。
                trade_state['position_miss_count'] = 0
                # 注释: 写入警告日志，提示需要关注的非致命问题。
                logging.warning("fetch_positions_risk 返回空，但 fetch_positions 仍有仓位，本轮忽略外部平仓判定")
                # 注释: 直接结束当前函数。
                return

            # 注释: 初始化 external_close_context 字典。
            external_close_context = {
                # 注释: 设置 stop_order_id_before_cancel 字段的取值。
                'stop_order_id_before_cancel': trade_state.get('stop_order_id', ''),
                # 注释: 设置 stop_order_price_before_cancel 字段的取值。
                'stop_order_price_before_cancel': trade_state.get('stop_order_price', 0.0)
            # 注释: 结束当前多行结构。
            }
            # 注释: 计算并保存 cancel_protective_stop_order(silent。
            cancel_protective_stop_order(silent=True)
            # 注释: 调用 reset_trade_state_after_external_close 执行对应处理。
            reset_trade_state_after_external_close(
                # 注释: 计算并保存 signal_bar_15m。
                signal_bar_15m=signal_bar_15m,
                # 注释: 计算并保存 reason。
                reason="检测到交易所实际已无仓位，可能是服务端止损或人工操作已成交，本地状态已重置",
                # 注释: 计算并保存 external_context。
                external_context=external_close_context
            # 注释: 结束当前多行结构。
            )
            # 注释: 直接结束当前函数。
            return

        # 注释: 更新 trade_state['position_miss_count'] 的值。
        trade_state['position_miss_count'] = 0
        # 注释: 更新 trade_state['liquidation_price'] 的值。
        trade_state['liquidation_price'] = position_risk.get('liquidation_price') or 0.0
        # 注释: 更新 trade_state['close_cond_4h'] 的值。
        trade_state['close_cond_4h'] = context['trend_states'].get('4h', {}).get('summary', '')
        # 注释: 更新 trade_state['close_cond_1h'] 的值。
        trade_state['close_cond_1h'] = context['trend_states'].get('1h', {}).get('summary', '')
        # 注释: 更新 trade_state['close_cond_15m'] 的值。
        trade_state['close_cond_15m'] = context['trend_states'].get('15m', {}).get('summary', '')

        # 注释: 调用 get_latest_price 并保存到 curr_price。
        curr_price = get_latest_price()
        # 注释: 调用 price_to_float 并保存到 stop_loss。
        stop_loss = price_to_float(trade_state.get('stop_loss_price'))
        # 注释: 判断条件是否成立：side == 'long'。
        if side == 'long':
            # 注释: 判断条件是否成立：curr_price > price_to_float(trade_state.get('highest_price'))。
            if curr_price > price_to_float(trade_state.get('highest_price')):
                # 注释: 更新 trade_state['highest_price'] 的值。
                trade_state['highest_price'] = curr_price
            # 注释: 判断条件是否成立：stop_loss > 0 and curr_price <= stop_loss。
            if stop_loss > 0 and curr_price <= stop_loss:
                # 注释: 计算并保存 close_position("触发保护止损", curr_price, signal_bar_15m。
                close_position("触发保护止损", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="保护止损")
                # 注释: 直接结束当前函数。
                return
        # 注释: 当前面的条件都不成立时进入该分支。
        else:
            # 注释: 判断条件是否成立：curr_price < price_to_float(trade_state.get('lowest_price')) or price_to_float(tr…。
            if curr_price < price_to_float(trade_state.get('lowest_price')) or price_to_float(trade_state.get('lowest_price')) <= 0:
                # 注释: 更新 trade_state['lowest_price'] 的值。
                trade_state['lowest_price'] = curr_price
            # 注释: 判断条件是否成立：stop_loss > 0 and curr_price >= stop_loss。
            if stop_loss > 0 and curr_price >= stop_loss:
                # 注释: 计算并保存 close_position("触发保护止损", curr_price, signal_bar_15m。
                close_position("触发保护止损", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="保护止损")
                # 注释: 直接结束当前函数。
                return

        # 注释: 计算并保存 apply_new_exit_rules(context, signal_bar_15m。
        apply_new_exit_rules(context, signal_bar_15m=signal_bar_15m)
    # 注释: 捕获异常分支：except Exception as e。
    except Exception as e:
        # 注释: 写入错误日志，便于排查运行异常。
        logging.error(f"新版持仓管理异常: {e}")
        # 注释: 写入错误日志，便于排查运行异常。
        logging.error(traceback.format_exc())


# 注释: 定义 states_for_candidate 函数，封装对应业务逻辑。
def states_for_candidate(context, candidate):
    # 注释: 调用 make_empty_state 并保存到 state_4h。
    state_4h = make_empty_state('4h')
    # 注释: 调用 make_empty_state 并保存到 state_1h。
    state_1h = make_empty_state('1h')
    # 注释: 调用 make_empty_state 并保存到 state_15m。
    state_15m = make_empty_state('15m')
    # 注释: 计算并保存 side。
    side = candidate['side']
    # 注释: 计算并保存 strategy_tf。
    strategy_tf = candidate['strategy_tf']
    # 注释: 调用 candidate.get 并保存到 summary。
    summary = candidate.get('state_summary') or candidate.get('reason', '')
    # 注释: 判断条件是否成立：strategy_tf == '4h'。
    if strategy_tf == '4h':
        # 注释: 调用 make_state_summary 并保存到 state_4h。
        state_4h = make_state_summary(context, '4h', side, summary)
        # 注释: 调用 make_state_summary 并保存到 state_1h。
        state_1h = make_state_summary(context, '1h', side, f"触发周期: {candidate.get('trigger_tf')}")
    # 注释: 继续判断备选条件：strategy_tf == '1h'。
    elif strategy_tf == '1h':
        # 注释: 调用 make_state_summary 并保存到 state_4h。
        state_4h = make_state_summary(context, '4h', side, f"背景: {context['trend_states'].get('4h', {}).get('summary', '')}")
        # 注释: 调用 make_state_summary 并保存到 state_1h。
        state_1h = make_state_summary(context, '1h', side, summary)
        # 注释: 调用 make_state_summary 并保存到 state_15m。
        state_15m = make_state_summary(context, '15m', side, f"触发周期: {candidate.get('trigger_tf')}")
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 调用 make_state_summary 并保存到 state_1h。
        state_1h = make_state_summary(context, '1h', side, f"背景: {context['trend_states'].get('1h', {}).get('summary', '')}")
        # 注释: 调用 make_state_summary 并保存到 state_15m。
        state_15m = make_state_summary(context, '15m', side, summary)
    # 注释: 返回计算结果：state_4h, state_1h, state_15m。
    return state_4h, state_1h, state_15m


# 注释: 定义 run_strategy 函数，封装对应业务逻辑。
def run_strategy():
    """新版策略主循环：5M节拍扫描，15m/1h/4h按权重产生候选信号。"""
    # 注释: 声明使用全局变量：trade_state。
    global trade_state

    # 注释: 调用 concurrent.futures.ThreadPoolExecutor 并保存到 executor。
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 初始化 futures 字典。
        futures = {
            # 注释: 设置 5m 字段的取值。
            '5m': executor.submit(fetch_df, SYMBOL, '5m', 220),
            # 注释: 设置 15m 字段的取值。
            '15m': executor.submit(fetch_df, SYMBOL, '15m', 220),
            # 注释: 设置 1h 字段的取值。
            '1h': executor.submit(fetch_df, SYMBOL, '1h', 220),
            # 注释: 设置 4h 字段的取值。
            '4h': executor.submit(fetch_df, SYMBOL, '4h', SUP_RES_LOOKBACK_4H),
            # 注释: 设置 1d 字段的取值。
            '1d': executor.submit(fetch_df, SYMBOL, '1d', 140),
        # 注释: 结束当前多行结构。
        }
        # 注释: 初始化 dfs 字典。
        dfs = {
            # 注释: 计算并保存 timeframe: future.result(timeout。
            timeframe: future.result(timeout=FETCH_DF_TASK_TIMEOUT_SECONDS)
            # 注释: 说明当前代码行的作用：for timeframe, future in futures.items()。
            for timeframe, future in futures.items()
        # 注释: 结束当前多行结构。
        }
    # 注释: 捕获异常分支：except concurrent.futures.TimeoutError。
    except concurrent.futures.TimeoutError:
        # 注释: 写入错误日志，便于排查运行异常。
        logging.error(f"抓取K线任务超时，已跳过本轮策略计算: timeout={FETCH_DF_TASK_TIMEOUT_SECONDS}s")
        # 注释: 计算并保存 executor.shutdown(wait。
        executor.shutdown(wait=False, cancel_futures=True)
        # 注释: 直接结束当前函数。
        return
    # 注释: 捕获异常分支：except Exception。
    except Exception:
        # 注释: 计算并保存 executor.shutdown(wait。
        executor.shutdown(wait=False, cancel_futures=True)
        # 注释: 说明当前代码行的作用：raise。
        raise
    # 注释: 当前面的条件都不成立时进入该分支。
    else:
        # 注释: 计算并保存 executor.shutdown(wait。
        executor.shutdown(wait=True)

    # 注释: 判断条件是否成立：any(df is None for df in dfs.values())。
    if any(df is None for df in dfs.values()):
        # 注释: 直接结束当前函数。
        return

    # 注释: 调用 get_server_now_dt 并保存到 server_now_dt。
    server_now_dt = get_server_now_dt()
    # 注释: 调用 get_closed_bar_time 并保存到 signal_bar_5m。
    signal_bar_5m = get_closed_bar_time(dfs['5m'], '5m', now_dt=server_now_dt)
    # 注释: 调用 get_closed_bar_time 并保存到 signal_bar_15m。
    signal_bar_15m = get_closed_bar_time(dfs['15m'], '15m', now_dt=server_now_dt)
    # 注释: 调用 validate_signal_bar 并保存到 signal_guard_5m。
    signal_guard_5m = validate_signal_bar('5m', signal_bar_5m, now_dt=server_now_dt)
    # 注释: 调用 validate_signal_bar 并保存到 signal_guard_15m。
    signal_guard_15m = validate_signal_bar('15m', signal_bar_15m, now_dt=server_now_dt)

    # 注释: 判断条件是否成立：not signal_guard_5m['valid']。
    if not signal_guard_5m['valid']:
        # 注释: 写入警告日志，提示需要关注的非致命问题。
        logging.warning(f"跳过异常5M信号K线: signal={signal_bar_5m}, reason={signal_guard_5m.get('reason')}")
        # 注释: 直接结束当前函数。
        return

    # 注释: 调用 bool 并保存到 is_new_signal_5m。
    is_new_signal_5m = bool(signal_guard_5m['is_new'])
    # 注释: 调用 bool 并保存到 is_new_signal_15m。
    is_new_signal_15m = bool(signal_guard_15m.get('valid') and signal_guard_15m.get('is_new'))
    # 注释: 调用 build_context 并保存到 context。
    context = build_context(dfs, server_now_dt)

    # 注释: 判断条件是否成立：trade_state['has_position']。
    if trade_state['has_position']:
        # 注释: 调用 monitor_position_new 执行对应处理。
        monitor_position_new(
            # 注释: 说明当前代码行的作用：context,。
            context,
            # 注释: 计算并保存 signal_bar_15m。
            signal_bar_15m=signal_bar_15m if signal_guard_15m.get('valid') else '',
            # 注释: 计算并保存 allow_strategy_close。
            allow_strategy_close=is_new_signal_5m
        # 注释: 结束当前多行结构。
        )
        # 注释: 判断条件是否成立：is_new_signal_5m。
        if is_new_signal_5m:
            # 注释: 更新 trade_state['last_processed_bar_5m'] 的值。
            trade_state['last_processed_bar_5m'] = signal_bar_5m
        # 注释: 判断条件是否成立：is_new_signal_15m。
        if is_new_signal_15m:
            # 注释: 更新 trade_state['last_processed_bar_15m'] 的值。
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        # 注释: 直接结束当前函数。
        return

    # 注释: 判断条件是否成立：not is_new_signal_5m。
    if not is_new_signal_5m:
        # 注释: 直接结束当前函数。
        return

    # 注释: 判断条件是否成立：is_in_post_exit_cooldown(server_now_dt)。
    if is_in_post_exit_cooldown(server_now_dt):
        # 注释: 写入运行日志，记录当前处理进度。
        logging.info("平仓后冷却中：至少等待4根5M K线后再开新仓")
        # 注释: 更新 trade_state['last_processed_bar_5m'] 的值。
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        # 注释: 判断条件是否成立：is_new_signal_15m。
        if is_new_signal_15m:
            # 注释: 更新 trade_state['last_processed_bar_15m'] 的值。
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        # 注释: 直接结束当前函数。
        return

    # 注释: 调用 build_entry_candidates 并保存到 candidates。
    candidates = build_entry_candidates(context)
    # 注释: 判断条件是否成立：not candidates。
    if not candidates:
        # 注释: 更新 trade_state['last_processed_bar_5m'] 的值。
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        # 注释: 判断条件是否成立：is_new_signal_15m。
        if is_new_signal_15m:
            # 注释: 更新 trade_state['last_processed_bar_15m'] 的值。
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        # 注释: 直接结束当前函数。
        return

    # 注释: 调用 candidate_priority 并保存到 best_priority。
    best_priority = candidate_priority(candidates[0])
    # 注释: 说明当前代码行的作用：top_candidates = [candidate for candidate in candidates if candidate_priority(can…。
    top_candidates = [candidate for candidate in candidates if candidate_priority(candidate) == best_priority]
    # 注释: 初始化 top_sides 字典。
    top_sides = {candidate['side'] for candidate in top_candidates}
    # 注释: 判断条件是否成立：len(top_sides) > 1。
    if len(top_sides) > 1:
        # 注释: 写入运行日志，记录当前处理进度。
        logging.info(f"同权重候选多空冲突，跳过本轮: {top_candidates}")
        # 注释: 更新 trade_state['last_processed_bar_5m'] 的值。
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        # 注释: 判断条件是否成立：is_new_signal_15m。
        if is_new_signal_15m:
            # 注释: 更新 trade_state['last_processed_bar_15m'] 的值。
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        # 注释: 直接结束当前函数。
        return

    # 注释: 计算并保存 candidate。
    candidate = top_candidates[0]
    # 注释: 判断条件是否成立：candidate['strategy_tf'] == '15m' and signal_bar_15m in (trade_state.get('last_en…。
    if candidate['strategy_tf'] == '15m' and signal_bar_15m in (trade_state.get('last_entry_bar_15m'), trade_state.get('last_exit_bar_15m')):
        # 注释: 写入运行日志，记录当前处理进度。
        logging.info(f"同一15M K线内已交易/刚平仓，跳过15M策略开仓: {signal_bar_15m}")
        # 注释: 更新 trade_state['last_processed_bar_5m'] 的值。
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        # 注释: 判断条件是否成立：is_new_signal_15m。
        if is_new_signal_15m:
            # 注释: 更新 trade_state['last_processed_bar_15m'] 的值。
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        # 注释: 直接结束当前函数。
        return

    # 注释: 调用 get_latest_price 并保存到 curr_price。
    curr_price = get_latest_price()
    # 注释: 调用 candidate_entry_price_still_valid 并保存到 entry_still_valid, entry_invalid_reason。
    entry_still_valid, entry_invalid_reason = candidate_entry_price_still_valid(candidate, curr_price)
    # 注释: 判断条件是否成立：not entry_still_valid。
    if not entry_still_valid:
        # 注释: 写入运行日志，记录当前处理进度。
        logging.info(entry_invalid_reason)
        # 注释: 更新 trade_state['last_processed_bar_5m'] 的值。
        trade_state['last_processed_bar_5m'] = signal_bar_5m
        # 注释: 判断条件是否成立：is_new_signal_15m。
        if is_new_signal_15m:
            # 注释: 更新 trade_state['last_processed_bar_15m'] 的值。
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        # 注释: 直接结束当前函数。
        return

    # 注释: 调用 states_for_candidate 并保存到 state_4h, state_1h, state_15m。
    state_4h, state_1h, state_15m = states_for_candidate(context, candidate)
    # 注释: 初始化 entry_meta 字典。
    entry_meta = {
        # 注释: 设置 strategy_tf 字段的取值。
        'strategy_tf': candidate.get('strategy_tf'),
        # 注释: 设置 exit_tf 字段的取值。
        'exit_tf': candidate.get('exit_tf'),
        # 注释: 设置 sr_target 字段的取值。
        'sr_target': candidate.get('target', 0.0)
    # 注释: 结束当前多行结构。
    }
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 调用 open_order 并保存到 opened。
        opened = open_order(
            # 注释: 说明当前代码行的作用：candidate['side'],。
            candidate['side'],
            # 注释: 说明当前代码行的作用：curr_price,。
            curr_price,
            # 注释: 说明当前代码行的作用：candidate['stop'],。
            candidate['stop'],
            # 注释: 说明当前代码行的作用：state_4h,。
            state_4h,
            # 注释: 说明当前代码行的作用：state_1h,。
            state_1h,
            # 注释: 说明当前代码行的作用：state_15m,。
            state_15m,
            # 注释: 说明当前代码行的作用：signal_bar_15m if signal_guard_15m.get('valid') else '',。
            signal_bar_15m if signal_guard_15m.get('valid') else '',
            # 注释: 调用 candidate.get 并保存到 entry_reason。
            entry_reason=candidate.get('reason', candidate.get('module', 'new_strategy')),
            # 注释: 计算并保存 entry_trigger_tf。
            entry_trigger_tf=f"{candidate.get('strategy_tf')}->{candidate.get('trigger_tf')}",
            # 注释: 计算并保存 entry_meta。
            entry_meta=entry_meta
        # 注释: 结束当前多行结构。
        )
        # 注释: 判断条件是否成立：opened。
        if opened:
            # 注释: 写入运行日志，记录当前处理进度。
            logging.info(
                # 注释: 拼接或传入文本内容。
                f"新版策略开仓成功: side={candidate['side']}, module={candidate['module']}, "
                # 注释: 拼接或传入文本内容。
                f"strategy_tf={candidate['strategy_tf']}, trigger_tf={candidate['trigger_tf']}, "
                # 注释: 拼接或传入文本内容。
                f"target={candidate.get('target')}"
            # 注释: 结束当前多行结构。
            )
        # 注释: 当前面的条件都不成立时进入该分支。
        else:
            # 注释: 写入警告日志，提示需要关注的非致命问题。
            logging.warning(f"新版策略开仓未成功: {candidate}")
    # 注释: 捕获异常分支：except Exception。
    except Exception:
        # 注释: 写入错误日志，便于排查运行异常。
        logging.error(f"新版策略开仓失败:\n{traceback.format_exc()}")

    # 注释: 更新 trade_state['last_processed_bar_5m'] 的值。
    trade_state['last_processed_bar_5m'] = signal_bar_5m
    # 注释: 判断条件是否成立：is_new_signal_15m。
    if is_new_signal_15m:
        # 注释: 更新 trade_state['last_processed_bar_15m'] 的值。
        trade_state['last_processed_bar_15m'] = signal_bar_15m


# ==========================================
# 4. 程序入口
# ==========================================
if __name__ == '__main__':  # Python标准写法，判断当前文件是否作为主程序被直接运行
    # 注释: 开始执行可能抛出异常的逻辑。
    try:
        # 注释: 调用 get_server_time_str 并保存到 current_time_str。
        current_time_str = get_server_time_str()
        # 注释: 向控制台输出当前运行信息。
        print("网络连通成功！服务器时间:", current_time_str)
        # 注释: 调用 exchange.fetch_balance 并保存到 balance_after。
        balance_after = exchange.fetch_balance({'type': 'future'})
        # 注释: 调用 float 并保存到 final_usdt。
        final_usdt = float(balance_after['total']['USDT'])
        logging.info(f"🚀 自动化交易策略系统启动，初始金额：{final_usdt}")  # 记录系统成功启动的信息到日志
    # 注释: 捕获异常分支：except Exception as e。
    except Exception as e:
        # 注释: 写入错误日志，便于排查运行异常。
        logging.error(f"启动自检失败，但主循环会继续尝试运行: {e}")
    #exchange.load_markets()
    while True:  # 开启一个无限循环，让策略持续运行不中断
        try:  # 尝试在主循环中运行策略，捕获最高层级的异常
            # 注释: 调用 maybe_log_heartbeat 执行对应处理。
            maybe_log_heartbeat()
            run_strategy()  # 调用核心策略函数
        except Exception as e:  # 捕获策略运行中未被内部函数捕获的意外崩溃
            # 注释: 向控制台输出当前运行信息。
            print(traceback.format_exc())
            logging.error(f"系统运行报错: {e}")  # 将意外报错记录到日志，防止程序直接闪退
            # 注释: 写入错误日志，便于排查运行异常。
            logging.error("主循环将继续运行，休眠后自动进入下一轮")
            # 注释: 暂停执行一段时间，控制轮询、重试或主循环节奏。
            time.sleep(MAIN_LOOP_ERROR_SLEEP_SECONDS)
            # 注释: 跳过本次循环剩余逻辑，进入下一轮。
            continue
        # 每1秒执行一次循环
        time.sleep(MAIN_LOOP_SLEEP_SECONDS)  # 当前循环执行完毕后，挂起休眠1秒，减轻CPU和API请求压力，然后再进行下一次循环
