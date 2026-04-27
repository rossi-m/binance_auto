"""
环境：
python version>=3.12 必须的
该脚本在东京服务器linux跑的,在国内挂了vpn也会有一堆问题的。
当前主要库版本：
python==3.12.9
ccxt==4.5.33
pandas_ta==0.4.71b0
说明:
该脚本是币安合约交易自动脚本（只交易ETH）

策略：
该脚本使用了MACD,EMA,BOLL，RSI,ATR，交易量作为指标，15分钟，1小时，4小时多周期共振和长影响趋势判断作为执行策略
"""
import ccxt  # 导入ccxt库，用于连接加密货币交易所API
import pandas as pd  # 导入pandas库，用于数据处理和分析
import pandas_ta as ta  # 导入pandas_ta库，用于计算技术指标（如EMA, MACD, BOLL等）
import time  # 导入time库，用于控制循环执行的时间间隔
import logging  # 导入logging库，用于记录程序运行日志
import smtplib  # 导入smtplib库，用于发送邮件通知
from email.mime.text import MIMEText  # 导入MIMEText，用于构建纯文本格式的邮件内容
from email.header import Header  # 导入Header，用于设置邮件头信息（如主题等）
import os,sys  # 导入os和sys模块，用于处理系统级操作和路径（目前未深入使用）
import concurrent.futures  # 导入concurrent.futures模块，用于并行执行多个任务
import csv  # 导入csv库，用于将交易记录写入文件
import datetime  # 导入datetime库，用于获取和格式化时间
import traceback  # 导入traceback模块，用于在异常时打印详细的调用栈信息
import sqlite3

BAR_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'  # 定义统一的K线时间格式字符串，用于日志记录和CSV输出
TIMEFRAME_SECONDS = {  # 定义各时间周期对应的秒数，用于计算K线是否已收盘
    '15m': 15 * 60,    # 15分钟 = 900秒
    '1h': 60 * 60,     # 1小时 = 3600秒
    '4h': 4 * 60 * 60  # 4小时 = 14400秒
}
EXCHANGE_TZ = datetime.timezone(datetime.timedelta(hours=8))  # 定义交易所时区为东八区（北京时间），用于时间戳转换
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_ENV_PATH = os.path.join(BASE_DIR, '.env.local')
STATS_DB_PATH = os.path.join(BASE_DIR, 'trade_stats.db')


def load_local_env(env_path=LOCAL_ENV_PATH):
    """从本地 .env.local 读取环境变量，避免把密钥写进代码仓库。"""
    if not os.path.exists(env_path):
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
RETRACE_THRESHOLD = 0.4  # 动态回撤止盈的基准档位，中等浮盈时允许回撤40%
DYNAMIC_RETRACE_MIN_PROFIT_ATR = 1.2  # 最大浮盈不足 1.2 * 1H ATR 时，不启用回撤锁盈
DYNAMIC_RETRACE_TREND_BONUS = 0.05  # 4H 和 1H 趋势同向时，给趋势单多一点回撤空间
DYNAMIC_RETRACE_REVERSAL_PENALTY = 0.10  # 出现 close_long/close_short 转弱信号时，收紧回撤阈值
DYNAMIC_RETRACE_MIN_RATIO = 0.15  # 再强的单子也至少保留 15% 的回撤缓冲，避免过紧
DYNAMIC_RETRACE_MAX_RATIO = 0.60  # 再早期的单子也不允许超过 60% 的利润回吐
DYNAMIC_RETRACE_STOP_STEP_ATR_RATIO = 0.15  # 新保护止损至少移动 0.15 * ATR，才值得重挂服务端止损单
DYNAMIC_RETRACE_STOP_STEP_PRICE_RATIO = 0.0005  # 再叠加一层按价格比例的最小步长，避免微小抖动频繁更新
SHADOW_REVERSAL_TIGHTEN_BUFFER_RATIO = 0.0003  # 影线反转只做保护时，止损与现价保留最小缓冲，避免刚更新就被立即触发
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
OSCILLATION_THRESHOLD_4H = 0.033  # 4H 布林带宽比低于 3.7% 视为震荡
OSCILLATION_THRESHOLD_1H = 0.020  # 1H 单独收紧到 2%，避免过滤范围过大
SHADOW_REVERSAL_LOOKBACK_BARS = 4  # 长影线反转信号允许向前追踪的已收盘K线数
SHADOW_REVERSAL_CONFIRM_MAX_OFFSET_BARS = 3  # 长影线反转确认最多只允许引用 3 根以内的参考K，避免信号过期
REQUIRE_15M_CONFIRM_FOR_1H_CLOSE = True  # 1H 平仓信号需要 15M 同向确认，避免单根 1H 转弱就直接离场
EXTREME_EXIT_LOOKBACK_BARS_1H = 3  # 1H 极值平仓允许向前追踪的参考K数量，给“后续几根确认”留窗口
EXCHANGE_HTTP_TIMEOUT_MS = 10000  # 单次交易所HTTP请求最多等待10秒，避免底层请求长时间挂起
FETCH_DF_TASK_TIMEOUT_SECONDS = 15  # 单个周期抓取任务最多等待15秒，超过就跳过本轮
FETCH_DF_SLOW_LOG_SECONDS = 5  # 单次抓K线+算指标超过5秒就记慢查询日志
MAIN_LOOP_SLEEP_SECONDS = 1  # 主循环正常节奏
MAIN_LOOP_ERROR_SLEEP_SECONDS = 5  # 主循环遇到顶层异常后，先休息5秒再继续
HEARTBEAT_INTERVAL_SECONDS = 15 * 60  # 每15分钟输出一次心跳日志，方便判断进程是否还活着

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
    'shadow_stop_mode': '',       # 记录是否启用了影线收紧止损，如 "4H 长上影收紧止损"
    'liquidation_price': 0.0,  # 记录当前仓位从交易所返回的真实强平价
    'stop_order_id': '',  # 记录服务端 STOP_MARKET 止损单的订单ID，便于后续撤单和替换
    'stop_order_price': 0.0,  # 记录当前服务端止损单对应的触发价格
    'stop_order_refresh_fail_count': 0,  # 连续几次更新服务端止损单失败，达到阈值后才保护性平仓
    'last_stop_order_refresh_error': '',  # 最近一次服务端止损更新失败的原始错误文本
    'entry_signal_bar_15m': '',   # 记录入场所对应的15M已收盘信号K线时间
    'last_entry_bar_15m': '',     # 最近一次入场所对应的15M已收盘信号K线时间（防止同根K线重复开仓）
    'last_exit_bar_15m': '',      # 最近一次平仓所对应的15M已收盘信号K线时间（防止同根K线平仓后立即重开）
    'last_processed_bar_15m': '', # 最近一次已处理过的15M已收盘信号K线时间（核心去重字段，防止同一根K线重复执行策略逻辑）
    'last_shadow_adjust_bar_15m': '',  # 最近一次因影线调整止损的15M信号时间（防止同根K线重复收紧止损）
    'position_miss_count': 0  # 连续几轮未在交易所查到仓位，用于避免误判“外部平仓”
}

# 初始化币安合约 API
exchange = ccxt.binance({  # 实例化ccxt的binance对象，用于调用币安API
    'apiKey': API_KEY,  # 传入API公钥
    'secret': SECRET_KEY,  # 传入API私钥
    'options': {'defaultType': 'future'},  # 设置默认交易类型为U本位合约 (future)
    'enableRateLimit': True,  # 开启内置的速率限制功能，防止请求频率过高被封IP
    'timeout': EXCHANGE_HTTP_TIMEOUT_MS,  # 给交易所HTTP请求设置硬超时，避免网络卡死时无限等待
})
exchange.enable_demo_trading(True)  # 开启模拟交易模式（测试网），不会产生真实交易

# 日志配置
logging.basicConfig(  # 配置日志记录的全局基本设置
    level=logging.INFO,  # 设置日志输出级别为INFO，过滤掉DEBUG级别的日志
    format='%(asctime)s - %(levelname)s - %(message)s'  # 设置日志输出格式：时间 - 级别 - 具体信息
)

runtime_state = {
    'last_heartbeat_ts': 0.0
}


# ==========================================
# 2. 功能模块
# ==========================================

def send_msg(subject, content):  # 定义发送邮件通知的函数，接收邮件主题和内容两个参数
    """发送邮件通知"""  # 函数的文档字符串
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        logging.warning("未配置邮件通知环境变量，已跳过邮件发送。")
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


TRADE_CSV_HEADERS = [
    '建仓时间', '趋势方向', '4H条件', '1H条件', '15M条件', '入场原因',
    '平仓时间', '平仓原因', '点数盈亏', '手续费', '净利润(USDT)', '是否盈利',
    '入场15M信号时间', '平仓15M信号时间', '平仓触发周期', '持仓秒数',
    '开仓订单ID', '平仓订单ID'
]
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


def fetch_df(symbol, timeframe, limit=100):  # 定义获取K线数据并计算指标的函数
    """获取K线并计算技术指标"""  # 函数的文档字符串
    start_ts = time.monotonic()
    try:  # 尝试执行获取和计算数据的代码块
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)  # 调用API获取指定交易对、时间周期和数量的K线数据
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])  # 将K线数据转换为pandas DataFrame，并指定列名
        # 将 timestamp (毫秒) 转换为 datetime 格式
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        # 如果你想调整时区到本地时间（比如北京时间 东八区），可以加上下面这行：
        df['timestamp'] = df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai')
       
        
        # 指标计算，只能通过计算来，没有提供具体指标的接口
        df['ema20'] = ta.ema(df['close'], length=20)  # 计算收盘价的20周期指数移动平均线 (EMA20)，并新增一列
        df['ema50'] = ta.ema(df['close'], length=50)  # 计算收盘价的50周期指数移动平均线 (EMA50)，并新增一列
        
        # BOLL (默认 20, 2)
        #返回列明：'BBL_20_2.0_2.0', 'BBM_20_2.0_2.0', 'BBU_20_2.0_2.0', 'BBB_20_2.0_2.0','BBP_20_2.0_2.0']
        #前3个下面有说明，重点说明后面2个：
        # BBB_20_2.0_2.0：表示布林带的宽度，用来判断震荡幅度。计算公式：(上轨-下轨)/中轨*100
        # BBP_20_2.0_2.0：布林带百分比，表示收盘价在布林带通道内的相对位置。计算公式(收盘价 - 下轨) / (上轨 - 下轨)，值>1价格突破上轨；值<0价格跌破了下轨；值=0.5价格正好在中轨
        boll = ta.bbands(df['close'], length=20, std=2)  # 计算20周期、2倍标准差的布林带 (Bollinger Bands)
        
        df = pd.concat([df, boll], axis=1)  # 将计算出的布林带数据按列拼接到原始DataFrame中
        # BOLL 列名根据 pandas_ta 版本不同可能会带有多个下划线
        # 根据你打印出的结果进行修改：
        df.rename(columns={  # 重命名布林带的列名，使其更易读
            'BBL_20_2.0_2.0': 'boll_dn',  # 将下轨重命名为 'boll_dn'
            'BBM_20_2.0_2.0': 'boll_mid',  # 将中轨重命名为 'boll_mid'
            'BBU_20_2.0_2.0': 'boll_up'  # 将上轨重命名为 'boll_up'
        }, inplace=True)  # inplace=True 表示直接在原DataFrame上修改
        
        # RSI 14
        df['rsi'] = ta.rsi(df['close'], length=14)  # 计算收盘价的14周期相对强弱指数 (RSI14)，并新增一列
        
        # ATR 14
        df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)  # 计算14周期的平均真实波幅 (ATR)，需要最高、最低、收盘价，新增一列
        
        # MACD (默认 12, 26, 9)
        macd = ta.macd(df['close'])  # 计算默认参数(12, 26, 9)的平滑异同移动平均线 (MACD)
        df = pd.concat([df, macd], axis=1)  # 将MACD的计算结果拼接到原始DataFrame中
        df.rename(columns={  # 重命名MACD相关的列名，方便后续逻辑调用
            'MACD_12_26_9': 'macd',  # DIFF值（快线）
            'MACDh_12_26_9': 'macd_hist',  # MACD值（红绿柱子）
            'MACDs_12_26_9': 'macd_signal'  # DEA值（慢线）
        }, inplace=True)  # 直接在原DataFrame上修改
        
        elapsed = time.monotonic() - start_ts
        if elapsed >= FETCH_DF_SLOW_LOG_SECONDS:
            logging.warning(f"获取数据较慢 ({timeframe}): {elapsed:.2f}s")
        return df  # 返回计算好所有指标的DataFrame
    except Exception as e:  # 捕获获取数据或计算指标过程中的异常
        logging.error(f"获取数据失败 ({timeframe}): {e}")  # 记录错误日志，包含时间周期和具体错误信息
        return None  # 如果发生错误，返回None


def maybe_log_heartbeat():
    """定期输出心跳日志，确认主循环仍然存活且没有卡死"""
    now_ts = time.monotonic()
    last_heartbeat_ts = runtime_state.get('last_heartbeat_ts', 0.0)
    if now_ts - last_heartbeat_ts < HEARTBEAT_INTERVAL_SECONDS:
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
    # Binance 的全平仓条件单使用 closePosition 即可，额外传 reduceOnly 会报 -1106。
    params = {
        'stopPrice': stop_price,
        'closePosition': True,
        'workingType': STOP_WORKING_TYPE
    }
    return exchange.create_order(SYMBOL, 'STOP_MARKET', stop_side, None, None, params)


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
    if not close_position:
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


def fetch_open_close_position_orders(side=None):
    """读取当前仓位方向上的全平仓条件单，便于做撤单确认和冲突排查。"""
    try:
        open_orders = exchange.fetch_open_orders(SYMBOL)
    except Exception as e:
        logging.warning(f"查询未成交条件单失败: {e}")
        return None

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
    sync_result = sync_protective_stop_order_state(side=side, silent=True) if side else None
    stop_order_ids = []
    if sync_result is not None:
        stop_order_ids = [extract_order_id(order) for order in sync_result['orders'] if extract_order_id(order)]

    local_stop_order_id = str(trade_state.get('stop_order_id', '') or '')
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
    sync_result = sync_protective_stop_order_state(side=side, silent=True)
    previous_stop_order_ids = []
    if sync_result is not None:
        previous_stop_order_ids = [extract_order_id(order) for order in sync_result['orders'] if extract_order_id(order)]
    elif trade_state.get('stop_order_id', ''):
        previous_stop_order_ids = [trade_state.get('stop_order_id', '')]

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
        f"影线止损模式: {trade_state.get('shadow_stop_mode', '')}\n"
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
        'shadow_stop_mode': '',
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
        'last_shadow_adjust_bar_15m': '',
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


def format_shadow_focus_for_mail(state, side_dir):
    """只保留影线反转真正关键的字段，避免把 body/full_range 等噪音带进邮件。"""
    shadow = state.get('details', {}).get('shadow', {})
    if side_dir == 'long':
        candidate = shadow.get('lower_shadow_candidate') or {}
        parts = [
            f"反转多={format_mail_bool(state.get('lower_shadow_reversal_long'))}",
            f"实体中点触达={format_mail_bool(shadow.get('lower_shadow_mid_hit'))}",
            f"近{SHADOW_REVERSAL_CONFIRM_MAX_OFFSET_BARS}根内={format_mail_bool(shadow.get('lower_shadow_offset_ok'))}",
            f"当前阳线={format_mail_bool(shadow.get('lower_shadow_bullish_confirm'))}",
            f"当前非空头={format_mail_bool(not bool(normalize_mail_value(shadow.get('current_short_logic_active', False))))}"
        ]
    else:
        candidate = shadow.get('upper_shadow_candidate') or {}
        parts = [
            f"反转空={format_mail_bool(state.get('upper_shadow_reversal_short'))}",
            f"实体中点触达={format_mail_bool(shadow.get('upper_shadow_mid_hit'))}",
            f"近{SHADOW_REVERSAL_CONFIRM_MAX_OFFSET_BARS}根内={format_mail_bool(shadow.get('upper_shadow_offset_ok'))}",
            f"当前阴线={format_mail_bool(shadow.get('upper_shadow_bearish_confirm'))}",
            f"当前非多头={format_mail_bool(not bool(normalize_mail_value(shadow.get('current_long_logic_active', False))))}"
        ]

    if candidate.get('bar_time'):
        parts.insert(1, f"参考K={candidate.get('bar_time')}")
    if candidate.get('offset') is not None:
        parts.append(f"距今={candidate.get('offset')}根")
    return '，'.join(parts)


def format_shadow_tighten_focus_for_mail(state, side_dir):
    """平仓时只保留影线收紧止损的核心判断。"""
    shadow = state.get('details', {}).get('shadow', {})
    if side_dir == 'long':
        resistance_details = shadow.get('prev_upper_resistance') or {}
        resistance_hit = any(bool(normalize_mail_value(v)) for v in resistance_details.values())
        return '，'.join([
            f"收紧多止损={format_mail_bool(state.get('upper_shadow_tighten_long'))}",
            f"前一根长上影={format_mail_bool(shadow.get('prev_upper_shadow'))}",
            f"前一根压力位={format_mail_bool(resistance_hit)}"
        ])

    support_details = shadow.get('prev_lower_support') or {}
    support_hit = any(bool(normalize_mail_value(v)) for v in support_details.values())
    return '，'.join([
        f"收紧空止损={format_mail_bool(state.get('lower_shadow_tighten_short'))}",
        f"前一根长下影={format_mail_bool(shadow.get('prev_lower_shadow'))}",
        f"前一根支撑位={format_mail_bool(support_hit)}"
    ])


def format_entry_condition_for_mail(state, side_dir, entry_reason=''):
    """按开仓方向和开仓原因，只展示当前最相关的一组条件。"""
    if not isinstance(state, dict):
        return '无状态数据'

    details = state.get('details', {})
    trend_4h_label_map = {
        'gate': '门槛',
        'ema_side': 'EMA方向',
        'boll_side': 'BOLL方向',
        'candle_side': 'K线方向',
        'structure_ok': '结构',
        'volume_expand': '放量',
        'rsi_zone': 'RSI区间',
        'macd_momentum': 'MACD动能',
        'price_progress': '价格推进'
    }
    pullback_4h_label_map = {
        'ema_or_boll': 'EMA/BOLL',
        'c1_ema': 'EMA',
        'c2_boll': 'BOLL',
        'c3_vol': '放量',
        'c4_rsi': 'RSI',
        'c5_rsi_drop': 'RSI回落'
    }
    small_tf_labels = ['量能', 'RSI', 'MACD', 'EMA', 'BOLL']

    if side_dir == 'long':
        if entry_reason == 'shadow_reversal_long' and state.get('lower_shadow_reversal_long'):
            return f"影线反转多:{format_shadow_focus_for_mail(state, 'long')}"
        if state.get('long_trend'):
            return f"多头:{format_mail_checks(details.get('lt'), label_map=trend_4h_label_map, default_labels=small_tf_labels)}"
        if state.get('pullback_long'):
            return f"回调多:{format_mail_checks(details.get('pl'), label_map=pullback_4h_label_map, default_labels=small_tf_labels)}"
        return ""

    if entry_reason == 'shadow_reversal_short' and state.get('upper_shadow_reversal_short'):
        return f"影线反转空:{format_shadow_focus_for_mail(state, 'short')}"
    if state.get('short_trend'):
        return f"空头:{format_mail_checks(details.get('st'), label_map=trend_4h_label_map, default_labels=small_tf_labels)}"
    if state.get('pullback_short'):
        return f"回调空:{format_mail_checks(details.get('ps'), label_map=pullback_4h_label_map, default_labels=small_tf_labels)}"
    return ""


def format_condition_snapshot_for_mail(timeframe, state):
    """把某个周期压缩成简洁快照，便于写入通知和复盘。"""
    if not isinstance(state, dict):
        return f"{timeframe}: 无状态数据"

    details = state.get('details', {})
    parts = [f"{timeframe}信号时间={state.get('signal_bar_time', '')}"]

    if state.get('lower_shadow_reversal_long'):
        parts.append(f"影线反转多:{format_shadow_focus_for_mail(state, 'long')}")
    if state.get('upper_shadow_reversal_short'):
        parts.append(f"影线反转空:{format_shadow_focus_for_mail(state, 'short')}")
    if state.get('upper_shadow_tighten_long'):
        parts.append(f"长上影收紧止损:{format_shadow_tighten_focus_for_mail(state, 'long')}")
    if state.get('lower_shadow_tighten_short'):
        parts.append(f"长下影收紧止损:{format_shadow_tighten_focus_for_mail(state, 'short')}")
    if state.get('close_long'):
        parts.append(f"close_long:{format_mail_checks(details.get('close_long_checks'))}")
    if state.get('close_short'):
        parts.append(f"close_short:{format_mail_checks(details.get('close_short_checks'))}")

    if len(parts) == 1:
        parts.append("无关键平仓条件")
    return ' | '.join(parts)


def safe_ratio(numerator, denominator):
    """安全除法，避免被0除"""
    # 这里把分母最小值钉死为 1e-9，防止 full_range = 0 时出现除0报错
    return numerator / max(denominator, 1e-9)


def is_long_upper_shadow(row):
    """根据用户定义判断是否为长上影线"""
    # full_range 表示这根K线从最低到最高一共波动了多少
    full_range = row['high'] - row['low']
    # body 表示实体长度，只是拿来辅助观察，当前规则里不直接参与判定
    body = abs(row['close'] - row['open'])
    # upper_shadow = 最高价减去实体顶部，得到上影线长度
    upper_shadow = row['high'] - max(row['open'], row['close'])
    # 从当前K线上直接读取 ATR，用来过滤掉“波动太小”的噪音影线
    atr = row.get('atr', float('nan'))
    # 如果没有有效 ATR，或者这根K线没有真实波动，就直接判定不是长上影线
    if pd.isna(full_range) or pd.isna(atr) or full_range <= 0:
        return False, {'a1': False, 'a2': False, 'a3': False, 'upper_shadow': upper_shadow, 'body': body, 'full_range': full_range}
    # a1：上影线至少占整根K线一半，说明上方抛压很明显
    a1 = safe_ratio(upper_shadow, full_range) >= 0.5
    # a2：实体顶部靠近低位，说明冲高之后被压回来了
    #    阳线时 close 是实体顶部，用 |close - low|；阴线时 open 是实体顶部，用 |open - low|
    if row['close'] >= row['open']:  # 阳线
        a2 = safe_ratio(abs(row['close'] - row['low']), full_range) <= 0.35
    else:  # 阴线
        a2 = safe_ratio(abs(row['open'] - row['low']), full_range) <= 0.35
    # a3：整根K线的波动不能太小，防止把细小抖动误判成强信号
    a3 = full_range >= 0.6 * atr
    # 三个子条件里满足两个，就认为这根K线是长上影线
    return sum([a1, a2, a3]) >= 2, {
        'a1': a1,
        'a2': a2,
        'a3': a3,
        'upper_shadow': upper_shadow,
        'body': body,
        'full_range': full_range
    }


def is_long_lower_shadow(row):
    """根据长上影线规则镜像判断长下影线"""
    # full_range 表示这根K线从最低到最高一共波动了多少
    full_range = row['high'] - row['low']
    # body 表示实体长度，只是拿来辅助观察，当前规则里不直接参与判定
    body = abs(row['close'] - row['open'])
    # lower_shadow = 实体底部减去最低价，得到下影线长度
    lower_shadow = min(row['open'], row['close']) - row['low']
    # 从当前K线上直接读取 ATR，用来过滤掉“波动太小”的噪音影线
    atr = row.get('atr', float('nan'))
    # 如果没有有效 ATR，或者这根K线没有真实波动，就直接判定不是长下影线
    if pd.isna(full_range) or pd.isna(atr) or full_range <= 0:
        return False, {'b1': False, 'b2': False, 'b3': False, 'lower_shadow': lower_shadow, 'body': body, 'full_range': full_range}
    # b1：下影线至少占整根K线一半，说明下方承接很明显
    b1 = safe_ratio(lower_shadow, full_range) >= 0.5
    # b2：实体底部靠近高位，说明下探之后又被拉回来了
    #    阳线时 open 是实体底部，用 |high - open|；阴线时 close 是实体底部，用 |high - close|
    if row['close'] >= row['open']:  # 阳线
        b2 = safe_ratio(abs(row['high'] - row['open']), full_range) <= 0.35
    else:  # 阴线
        b2 = safe_ratio(abs(row['high'] - row['close']), full_range) <= 0.35
    # b3：整根K线波动不能太小
    b3 = full_range >= 0.6 * atr
    # 三个子条件里满足两个，就认为这根K线是长下影线
    return sum([b1, b2, b3]) >= 2, {
        'b1': b1,
        'b2': b2,
        'b3': b3,
        'lower_shadow': lower_shadow,
        'body': body,
        'full_range': full_range
    }


def is_upper_shadow_at_resistance(last, prev, prev2, tol, is_above):
    """判断长上影线是否位于压力区"""
    # 高点碰到或接近布林上轨，视为打到第一层压力
    near_boll = is_above(last['high'], last['boll_up'])
    
    # 高点接近最近两根K线前高，也视为打到局部压力位
    break_prev_high = tol(last['high'],max(prev['high'], prev2['high']))
    # 只要这三类压力位命中任意一个，就把当前位置视为“压力区”
    return any([near_boll, break_prev_high]), {
        'near_boll_up': near_boll,
        
        'break_prev_high': break_prev_high
    }


def is_lower_shadow_at_support(last, prev, prev2, tol, is_below):
    """判断长下影线是否位于支撑区"""
    # 低点碰到或接近布林下轨，视为打到第一层支撑
    near_boll = is_below(last['low'], last['boll_dn'])
    # 低点碰到或接近 EMA50，视为打到第二层支撑
    
    # 低点接近最近两根K线前低，也视为打到局部支撑位
    break_prev_low = tol(last['low'] ,min(prev['low'], prev2['low']))
    # 只要这三类支撑位命中任意一个，就把当前位置视为“支撑区”
    return any([near_boll, break_prev_low]), {
        'near_boll_dn': near_boll,
        
        'break_prev_low': break_prev_low
    }


def pick_state_signal(state_4h, state_1h, key):
    """优先返回4H，其次1H的信号状态"""
    # 先检查 4H，因为大周期信号权重更高
    if state_4h.get(key):
        return '4H', state_4h
    # 4H 没有时，再看 1H
    if state_1h.get(key):
        return '1H', state_1h
    # 两个周期都没有这个信号，就返回空结果
    return '', None


def clamp(value, min_value, max_value):
    """将数值限制在指定范围内"""
    return max(min_value, min(value, max_value))


def calculate_dynamic_retrace_plan(side, entry_price, extreme_price, curr_price, atr_1h, state_4h, state_1h, state_15m):
    """根据最大浮盈、ATR 和多周期状态计算动态回撤止盈方案"""
    # 如果 1H ATR 不存在、是 NaN，或者小于等于 0，就无法把浮盈换算成 ATR 倍数，直接放弃动态回撤计算。
    if atr_1h is None or pd.isna(atr_1h) or atr_1h <= 0:
        # 返回 None 表示本轮不启用动态回撤方案。
        return None
    # 如果入场价、历史极值价、当前价里有任意一个缺失，就无法继续计算利润与回撤。
    if entry_price is None or extreme_price is None or curr_price is None:
        # 关键价格数据不完整时，同样不生成方案。
        return None

    # 多单的最大浮盈 = 持仓期间最高价 - 入场价。
    if side == 'long':
        # 对多单来说，价格越涨，浮盈越大，所以用最高价减入场价。
        max_profit = extreme_price - entry_price
    else:
        # 对空单来说，价格越跌，浮盈越大，所以用入场价减最低价。
        max_profit = entry_price - extreme_price

    # 如果历史上根本没有跑出正浮盈，就没必要做“利润回撤保护”。
    if max_profit <= 0:
        # 连最大浮盈都不为正时，直接跳过。
        return None

    # 把最大浮盈换算成 ATR 倍数，便于不同波动环境下用统一尺度判断利润深度。
    profit_atr = max_profit / atr_1h
    # 如果最大浮盈还不到最小启用门槛，就继续给单子空间，不做动态回撤锁盈。
    if profit_atr < DYNAMIC_RETRACE_MIN_PROFIT_ATR:
        # 利润太浅，不值得启动该机制。
        return None

    # 浮盈越深，回撤阈值越紧；这样前期给空间，后期主动锁利润。
    # 当利润还比较浅时，允许更大的利润回吐，避免太早被洗出去。
    if profit_atr < 2.0:
        # 基础回撤比例再放宽 15%。
        retrace_threshold = RETRACE_THRESHOLD + 0.15
    # 当利润进入中等区间时，直接使用默认回撤比例。
    elif profit_atr < 3.5:
        # 保持基准回撤阈值。
        retrace_threshold = RETRACE_THRESHOLD
    # 当利润已经比较深时，开始适度收紧回撤空间。
    elif profit_atr < 5.0:
        # 比默认值再少给 10% 的利润回吐空间。
        retrace_threshold = RETRACE_THRESHOLD - 0.10
    else:
        # 当利润非常深时，进一步收紧，优先锁住大部分已得利润。
        retrace_threshold = RETRACE_THRESHOLD - 0.20

    # 多空分支分别计算，因为趋势确认、转弱信号和保护价方向都不同。
    if side == 'long':
        # 多单在 4H + 1H 共振走强时放宽一点，出现转弱信号时立刻收紧。
        # 如果 4H 和 1H 都还是明显多头，就给趋势单多一点回撤空间，避免顺势单太早被洗掉。
        if state_4h.get('long_trend') and state_1h.get('long_trend'):
            # 在当前回撤阈值上叠加趋势奖励。
            retrace_threshold += DYNAMIC_RETRACE_TREND_BONUS
        # 只要任一周期出现 close_long，说明多头可能转弱，要尽快收紧保护。
        if state_4h.get('close_long') or state_1h.get('close_long') or state_15m.get('close_long'):
            # 在当前回撤阈值上扣掉转弱惩罚。
            retrace_threshold -= DYNAMIC_RETRACE_REVERSAL_PENALTY
        # 把最终回撤阈值限制在预设上下限之间，避免过松或过紧。
        retrace_threshold = clamp(retrace_threshold, DYNAMIC_RETRACE_MIN_RATIO, DYNAMIC_RETRACE_MAX_RATIO)
        # 当前回撤比例 = (最高浮盈点 - 当前价) / 最大浮盈，用来表示利润已经吐回了多少。
        retrace_ratio = (extreme_price - curr_price) / max_profit
        # 多单保护价 = 历史最高浮盈点 - 允许回吐的利润。
        # 也就是在最高价下方留出一段允许回撤的距离，低于这个位置就该止盈离场。
        trail_price = extreme_price - max_profit * retrace_threshold
    else:
        # 空单和多单完全对称：趋势强则放宽，转弱则收紧。
        # 如果 4H 和 1H 都还是明显空头，就给空单多一点回撤空间。
        if state_4h.get('short_trend') and state_1h.get('short_trend'):
            # 在当前回撤阈值上叠加趋势奖励。
            retrace_threshold += DYNAMIC_RETRACE_TREND_BONUS
        # 只要任一周期出现 close_short，说明空头可能转弱，要尽快收紧保护。
        if state_4h.get('close_short') or state_1h.get('close_short') or state_15m.get('close_short'):
            # 在当前回撤阈值上扣掉转弱惩罚。
            retrace_threshold -= DYNAMIC_RETRACE_REVERSAL_PENALTY
        # 同样把最终回撤阈值限制在合理区间，避免参数失控。
        retrace_threshold = clamp(retrace_threshold, DYNAMIC_RETRACE_MIN_RATIO, DYNAMIC_RETRACE_MAX_RATIO)
        # 当前回撤比例 = (当前价 - 历史最低价) / 最大浮盈，用来表示空单利润已经回吐了多少。
        retrace_ratio = (curr_price - extreme_price) / max_profit
        # 空单保护价 = 历史最低浮盈点 + 允许回吐的利润。
        # 也就是在最低价上方留出一段允许反弹的距离，反弹到这里就该止盈离场。
        trail_price = extreme_price + max_profit * retrace_threshold

    # 返回完整的动态回撤方案，供外层逻辑决定是否更新止损或直接止盈。
    return {
        # 这笔持仓曾经达到过的最大浮盈点数。
        'max_profit': max_profit,
        # 最大浮盈折算成多少倍 1H ATR。
        'profit_atr': profit_atr,
        # 当前允许利润回吐的比例阈值。
        'retrace_threshold': retrace_threshold,
        # 按当前价格计算，利润已经回吐了多少比例。
        'retrace_ratio': retrace_ratio,
        # 动态保护价；价格触及它时就应执行动态回撤止盈。
        'trail_price': trail_price
    }


def should_refresh_dynamic_stop(side, current_stop, new_stop, atr_1h, entry_price):
    """判断动态回撤新止损是否足够有意义，避免频繁重挂服务端止损单"""
    if new_stop is None or atr_1h is None or pd.isna(atr_1h) or atr_1h <= 0 or entry_price <= 0:
        return False

    min_step = max(
        atr_1h * DYNAMIC_RETRACE_STOP_STEP_ATR_RATIO,
        entry_price * DYNAMIC_RETRACE_STOP_STEP_PRICE_RATIO
    )

    # 只有当新的保护位比旧止损“明显更好”时，才去更新服务端止损单。
    if side == 'long':
        return new_stop > current_stop + min_step
    return new_stop < current_stop - min_step


def tighten_stop_on_reversal_warning(side, reversal_tf, reversal_state, curr_price, signal_bar_15m=''):
    """影线反转已出现但还不满足反手开仓时，只收紧保护止损，不直接平仓。"""
    if not reversal_state:
        return False

    shadow_details = (reversal_state.get('details') or {}).get('shadow') or {}
    if side == 'long':
        candidate = shadow_details.get('upper_shadow_candidate') or {}
        raw_protect_ref = candidate.get('body_low')
        if raw_protect_ref is None or pd.isna(raw_protect_ref):
            return False
        capped_protect_ref = min(raw_protect_ref, curr_price * (1 - SHADOW_REVERSAL_TIGHTEN_BUFFER_RATIO))
        new_sl = max(trade_state['stop_loss_price'], capped_protect_ref)
        shadow_mode = f"{reversal_tf} 长上影反转预警收紧止损"
        notify_subject = "ETH交易: 多单反转预警收紧止损"
        notify_body = f"{reversal_tf} 长上影反转预警触发，新的多单止损价: {new_sl:.4f}"
        close_reason = f"{reversal_tf} 长上影反转预警收紧止损"
    else:
        candidate = shadow_details.get('lower_shadow_candidate') or {}
        raw_protect_ref = candidate.get('body_high')
        if raw_protect_ref is None or pd.isna(raw_protect_ref):
            return False
        capped_protect_ref = max(raw_protect_ref, curr_price * (1 + SHADOW_REVERSAL_TIGHTEN_BUFFER_RATIO))
        new_sl = min(trade_state['stop_loss_price'], capped_protect_ref)
        shadow_mode = f"{reversal_tf} 长下影反转预警收紧止损"
        notify_subject = "ETH交易: 空单反转预警收紧止损"
        notify_body = f"{reversal_tf} 长下影反转预警触发，新的空单止损价: {new_sl:.4f}"
        close_reason = f"{reversal_tf} 长下影反转预警收紧止损"

    if side == 'long':
        if new_sl <= trade_state['stop_loss_price']:
            return False
    elif new_sl >= trade_state['stop_loss_price']:
        return False

    if not refresh_protective_stop_order(new_sl):
        return handle_stop_order_refresh_failure(
            "服务端止损更新失败，连续5次后主动平仓",
            curr_price,
            signal_bar_15m=signal_bar_15m,
            trigger_label="服务端止损更新失败，连续5次后主动平仓"
        )

    trade_state['stop_loss_price'] = new_sl
    trade_state['shadow_stop_mode'] = shadow_mode
    trade_state['last_shadow_adjust_bar_15m'] = signal_bar_15m
    trade_state['stop_order_price'] = new_sl
    logging.info(f"{shadow_mode}: 新止损价={new_sl:.4f}")
    order_id_lines = format_order_id_lines(
        open_order_id=trade_state.get('open_order_id', ''),
        stop_order_id=trade_state.get('stop_order_id', '')
    )
    order_id_suffix = f"\n{order_id_lines}" if order_id_lines else ''
    send_msg(notify_subject, f"{notify_body}{order_id_suffix}")

    if side == 'long' and curr_price <= trade_state['stop_loss_price']:
        close_position(close_reason, curr_price, signal_bar_15m=signal_bar_15m, trigger_label=close_reason)
        return True
    if side == 'short' and curr_price >= trade_state['stop_loss_price']:
        close_position(close_reason, curr_price, signal_bar_15m=signal_bar_15m, trigger_label=close_reason)
        return True
    return False


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
                # 这里特意新增“入场原因”一列，方便后面区分趋势单和影线反转单
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

def evaluate_trend(df, timeframe, time_factor, is_4h=False, now_dt=None):  # 定义评估趋势状态的函数，接收数据、时间系数和是否为4H周期的标识
    """根据交易规则评估趋势状态，返回满足的条件字典"""  # 函数的文档字符串
    if df is None or len(df) < 4:  # 检查传入的数据是否为空，或行数是否少于4行（无法对比前两根已收盘K线）
        return {}  # 数据不足时返回空字典
    #最后一根已收盘K线”在 DataFrame 里的 iloc 索引
    last_idx = get_last_closed_index(df, timeframe, now_dt=now_dt)
    if last_idx is None or last_idx < 3:
        return {}

    last = df.iloc[last_idx]  # 获取最近一根已收盘K线
    prev = df.iloc[last_idx - 1]  # 获取倒数第二根已收盘K线
    prev2 = df.iloc[last_idx - 2]  # 获取倒数第三根已收盘K线
    prev3 = df.iloc[last_idx - 3]  # 再往前取一根K线，专门给“上一根影线是否在前高/前低附近”做参考

    # --- 容错计算 ---
    def tol(val, target):  # 定义内部辅助函数：计算是否在允许的容错范围内
        if pd.isna(val) or pd.isna(target) or target == 0 or pd.isna(last['atr']) or pd.isna(last['boll_mid']) or last['boll_mid'] == 0:
            return False
        diff_rate = abs(val - target) / target  # 计算实际值与目标值的绝对偏差率
        dynamic_threshold = 0.3 * (last['atr'] / last['boll_mid']) * time_factor  # 根据公式：0.3 * (ATR / 中轨) * 时间系数，计算动态阈值
        return diff_rate <= dynamic_threshold  # 判断偏差率是否小于等于动态阈值，返回布尔结果

    def is_above(val, target):  # 定义内部辅助函数：判断某值是否在目标之上（含容错）
        return val > target or tol(val, target)  # 如果严格大于目标值，或者在容错范围内，均视为在上方

    def is_below(val, target):  # 定义内部辅助函数：判断某值是否在目标之下（含容错）
        return val < target or tol(val, target)  # 如果严格小于目标值，或者在容错范围内，均视为在下方

    def find_recent_shadow_candidate(direction):
        """寻找最近几根已收盘K线里仍可用于影线反转的参考K。"""
        max_offset = min(SHADOW_REVERSAL_LOOKBACK_BARS, last_idx - 2)
        if max_offset < 1:
            return None
        for offset in range(1, max_offset + 1):
            candidate = df.iloc[last_idx - offset]
            candidate_prev = df.iloc[last_idx - offset - 1]
            candidate_prev2 = df.iloc[last_idx - offset - 2]

            if direction == 'upper':
                is_shadow, shadow_details = is_long_upper_shadow(candidate)
                at_key_level, level_details = is_upper_shadow_at_resistance(candidate, candidate_prev, candidate_prev2, tol, is_above)
                stop_ref = candidate['high'] + 0.1 * candidate['atr'] if is_shadow else None
            else:
                is_shadow, shadow_details = is_long_lower_shadow(candidate)
                at_key_level, level_details = is_lower_shadow_at_support(candidate, candidate_prev, candidate_prev2, tol, is_below)
                stop_ref = candidate['low'] - 0.1 * candidate['atr'] if is_shadow else None

            if not (is_shadow and at_key_level):
                continue

            body_high = max(candidate['open'], candidate['close'])
            body_low = min(candidate['open'], candidate['close'])
            return {
                'offset': offset, #第几根是长影线
                'bar_time': format_bar_time(candidate['timestamp']),
                'body_high': body_high,
                'body_low': body_low,
                'body_mid': (body_high + body_low) / 2,
                'stop_ref': stop_ref, #zh
                'shadow_details': shadow_details,
                'level_details': level_details
            }

        return None

    def find_recent_extreme_exit_reference(direction):
        """寻找最近几根 1H 极值K，以及其后是否已经出现实体确认。"""
        max_offset = min(EXTREME_EXIT_LOOKBACK_BARS_1H, last_idx)
        if max_offset < 1:
            return None

        for offset in range(1, max_offset + 1):
            candidate_idx = last_idx - offset
            candidate = df.iloc[candidate_idx]

            if direction == 'short':
                extreme_hit = candidate['close'] <= candidate['boll_dn'] or candidate['rsi'] < 30
                body_threshold = max(candidate['open'], candidate['close'])
                confirm_cmp = lambda bar_close: bar_close > body_threshold
            else:
                extreme_hit = candidate['close'] >= candidate['boll_up'] or candidate['rsi'] > 72
                body_threshold = min(candidate['open'], candidate['close'])
                confirm_cmp = lambda bar_close: bar_close < body_threshold

            if not extreme_hit:
                continue

            confirm_bar_time = ''
            confirm_close = None
            confirmed = False
            for follow_idx in range(candidate_idx + 1, last_idx + 1):
                follow_bar = df.iloc[follow_idx]
                if confirm_cmp(follow_bar['close']):
                    confirmed = True
                    confirm_bar_time = format_bar_time(follow_bar['timestamp'])
                    confirm_close = follow_bar['close']
                    break

            return {
                'offset': offset,
                'bar_time': format_bar_time(candidate['timestamp']),
                'body_threshold': body_threshold,
                'confirmed': confirmed,
                'confirm_bar_time': confirm_bar_time,
                'confirm_close': confirm_close
            }

        return None

    res = {  # 初始化存放各个趋势判断结果的字典
        'pullback_short': False, # 标记是否为回调空头结构，初始为False
        'long_trend': False,     # 标记是否为多头趋势，初始为False
        'pullback_long': False,  # 标记是否为回调多头结构，初始为False
        'short_trend': False,    # 标记是否为空头趋势，初始为False
        'close_short': False,    # 标记是否满足强劲空头平仓条件，初始为False
        'close_long': False,     # 标记是否满足强劲多头平仓条件，初始为False
        'is_oscillation': False, # 标记当前周期是否被识别为震荡行情
        'signal_bar_time': format_bar_time(last['timestamp']), # 当前这次评估对应的已收盘信号K线时间
        'long_upper_shadow': False, # 最新已收盘K线本身是否是长上影线
        'long_lower_shadow': False, # 最新已收盘K线本身是否是长下影线
        'upper_shadow_filter_long': False, # 是否要因为长上影线而过滤掉趋势做多
        'lower_shadow_filter_short': False, # 是否要因为长下影线而过滤掉趋势做空
        'upper_shadow_reversal_short': False, # 是否出现“上一根长上影 + 当前阴线确认”的开空反转信号
        'lower_shadow_reversal_long': False, # 是否出现“上一根长下影 + 当前阳线确认”的开多反转信号
        'upper_shadow_tighten_long': False, # 持有多单时，是否需要因为长上影线收紧止损
        'lower_shadow_tighten_short': False, # 持有空单时，是否需要因为长下影线收紧止损
        'shadow_short_stop_ref': None, # 影线反转开空时的参考止损价
        'shadow_long_stop_ref': None, # 影线反转开多时的参考止损价
        'shadow_short_trigger_tf': '', # 开空影线信号来自哪个周期：4H 或 1H
        'shadow_long_trigger_tf': '', # 开多影线信号来自哪个周期：4H 或 1H
        'shadow_upper_reference_bar': '', # 当前开空影线反转引用的长上影K线时间
        'shadow_lower_reference_bar': '', # 当前开多影线反转引用的长下影K线时间
        'upper_shadow_long_protect_ref': None, # 多单因为长上影收紧止损时的新保护位
        'lower_shadow_short_protect_ref': None # 空单因为长下影收紧止损时的新保护位
    }
    boll_band_width_ratio = safe_ratio(last['boll_up'] - last['boll_dn'], last['boll_dn'])
    res['boll_band_width_ratio'] = boll_band_width_ratio
    res['oscillation_threshold'] = None

    if is_4h:  # 如果当前评估的是4小时级别数据，执行以下特定逻辑
        # 震荡过滤：(UP - DN) / DN < 0.04 不开仓 (这里仅计算指标，在run_strategy拦截)
        res['oscillation_threshold'] = OSCILLATION_THRESHOLD_4H
        res['is_oscillation'] = boll_band_width_ratio < OSCILLATION_THRESHOLD_4H  # 判断布林带上下轨间距率是否小于4H阈值，若是则为震荡

        # 4H 四类趋势都额外加入 RSI14 条件，c1(EMA)和c2(布林中轨)同质化高，合并为满足一个即可
        # A. 空头转多头 -> 回调趋势还是空头 (3个条件满足2个)
        ps_c1 = (last['open'] < min(last['ema20'], last['ema50']) and  # 条件1：当前开盘价低于EMA20和EMA50的最小值，且...
                 prev['close'] < min(prev['ema20'], prev['ema50']) and last['close'] < last['open'])  # 上一根收盘价也低于EMA极小值，且当前为阴线（收盘<开盘）
        ps_c2 = (last['open'] < last['boll_mid'] and  # 条件2：当前开盘价低于布林中轨，且...
                 prev['close'] < prev['boll_mid'] and last['close'] < last['open'])  # 上一根收盘价也低于布林中轨，且当前为阴线
        ps_c_ema_boll = ps_c1 or ps_c2  # EMA和布林中轨满足一个即可
        ps_c3 = last['volume'] > prev['volume']  # 条件3：当前K线成交量大于上一根K线成交量
        ps_c4 = last['rsi'] > 40  # 条件4：最新收盘蜡烛图 RSI14 >35
        ps_c5 = last['rsi']-prev['rsi']<-4
        res['pullback_short'] = ps_c_ema_boll and sum([ps_c3, ps_c4,ps_c5]) >= 2  # 3个条件满足至少2个，则判定为回调空头结构

        # A. 空头转多头 -> 多头趋势：方向门槛 + 质量打分
        lt_gate_ema = last['close'] > last['ema20'] and last['ema20'] >= last['ema50']
        lt_gate_boll = last['close'] > last['boll_mid']
        lt_gate_candle = last['close'] > last['open']
        lt_score_structure_ok = lt_gate_ema or lt_gate_boll
        lt_gate = lt_score_structure_ok and lt_gate_candle

        lt_score_volume_expand = last['volume'] > prev['volume']
        lt_score_rsi_zone = 40 <= last['rsi'] <= 68
        lt_score_macd_momentum = (last['macd'] > last['macd_signal']) and (abs(last['macd_hist']) >= abs(prev['macd_hist']))
        lt_score_price_progress = last['close'] > prev['close']
        lt_score_rsi_val = last['rsi']-prev['rsi']>4
        lt_score = sum([
            lt_score_rsi_val,
            lt_score_volume_expand,
            lt_score_rsi_zone,
            lt_score_macd_momentum,
            lt_score_price_progress
        ])
        lt_score_threshold = 3
        res['long_trend'] = lt_gate and lt_score >= lt_score_threshold

        # B. 多头转空头 -> 回调趋势还是多头 (3个条件满足2个，有容错)
        pl_c1 = (is_above(last['open'], min(last['ema20'], last['ema50'])) and  # 条件1：当前开盘价在EMA20/50最小值之上（含容错），且...
                 is_above(prev['close'], min(prev['ema20'], prev['ema50'])) and last['close'] > last['open'])  # 上一根收盘价在EMA极小值之上（含容错），且当前为阳线
        pl_c2 = (is_above(last['open'], last['boll_mid']) and  # 条件2：当前开盘价在布林中轨之上（含容错），且...
                 is_above(prev['close'], prev['boll_mid']) and last['close'] > last['open'])  # 上一根收盘价在布林中轨之上（含容错），且当前为阳线
        pl_c_ema_boll = pl_c1 or pl_c2  # EMA和布林中轨满足一个即可
        pl_c3 = last['volume'] > prev['volume']  # 条件3：当前成交量大于上一根成交量
        pl_c4 = last['rsi'] < 68  # 条件4：最新收盘蜡烛图 RSI14 < 70
        pl_c5 = last['rsi']-prev['rsi']>4
        res['pullback_long'] = pl_c_ema_boll and sum([pl_c3, pl_c4, pl_c5]) >= 2  # 满足至少2个条件，即判定为回调多头结构

        # B. 多头转空头 -> 空头趋势：方向门槛 + 质量打分
        st_gate_ema = last['close'] < last['ema20'] and last['ema20'] <= last['ema50']
        st_gate_boll = last['close'] < last['boll_mid']
        st_gate_candle = last['close'] < last['open']
        st_score_structure_ok = st_gate_ema or st_gate_boll
        st_gate = st_score_structure_ok and st_gate_candle

        st_score_rsi_val = last['rsi']-prev['rsi']<-4
        st_score_volume_expand = last['volume'] > prev['volume']
        st_score_rsi_zone = 32 <= last['rsi'] <= 70
        st_score_macd_momentum = (last['macd'] < last['macd_signal']) and (abs(last['macd_hist']) >= abs(prev['macd_hist']))
        st_score_price_progress = last['close'] < prev['close']
        st_score = sum([
            st_score_rsi_val,
            st_score_volume_expand,
            st_score_rsi_zone,
            st_score_macd_momentum,
            st_score_price_progress
        ])
        st_score_threshold = 3
        res['short_trend'] = st_gate and st_score >= st_score_threshold

        # 空头平仓：改为“上一根严重超卖 + 当前RSI拐头回升 + 收回布林带内”
        cs_c1 = prev['rsi'] < 32
        cs_c2 = last['rsi']-prev['rsi']>4
        cs_c3 = last['close'] > last['boll_dn']
        res['close_short'] = all([cs_c1, cs_c2, cs_c3])
        close_short_checks = {
            'prev_rsi_lt_30': cs_c1,
            'rsi_rebound': cs_c2,
            'back_inside_boll_dn': cs_c3
        }

        # 多头平仓：改为“上一根严重超买 + 当前RSI拐头回落 + 收回布林带内”
        cl_c1 = prev['rsi'] > 70
        cl_c2 = last['rsi']-prev['rsi']<-4
        cl_c3 = last['close'] < last['boll_up']
        res['close_long'] = all([cl_c1, cl_c2, cl_c3])
        close_long_checks = {
            'prev_rsi_gt_75': cl_c1,
            'rsi_pullback': cl_c2,
            'back_inside_boll_up': cl_c3
        }

        # 记录 4H 级别的具体条件判断结果
        res['details'] = {
            'ps': {'ema_or_boll': ps_c_ema_boll, 'c1_ema': ps_c1, 'c2_boll': ps_c2, 'c3_vol': ps_c3, 'c4_rsi': ps_c4, 'c5_rsi_val': ps_c5},
            'lt': {
                'gate': lt_gate,
                'score': lt_score,
                'threshold': lt_score_threshold,
                'ema_side': lt_gate_ema,
                'boll_side': lt_gate_boll,
                'candle_side': lt_gate_candle,
                'rsi_val': lt_score_rsi_val,
                'volume_expand': lt_score_volume_expand,
                'rsi_zone': lt_score_rsi_zone,
                'macd_momentum': lt_score_macd_momentum,
                'price_progress': lt_score_price_progress
            },
            'pl': {'ema_or_boll': pl_c_ema_boll, 'c1_ema': pl_c1, 'c2_boll': pl_c2, 'c3_vol': pl_c3, 'c4_rsi': pl_c4, 'c5_rsi_val': pl_c5},
            'st': {
                'gate': st_gate,
                'score': st_score,
                'threshold': st_score_threshold,
                'ema_side': st_gate_ema,
                'boll_side': st_gate_boll,
                'candle_side': st_gate_candle,
                'rsi_val': st_score_rsi_val,
                'volume_expand': st_score_volume_expand,
                'rsi_zone': st_score_rsi_zone,
                'macd_momentum': st_score_macd_momentum,
                'price_progress': st_score_price_progress
            },
            'close_long_checks': close_long_checks,
            'close_short_checks': close_short_checks
        }

    else:  # 如果当前评估的是15分钟或1小时级别数据，执行以下逻辑
        # 15分钟，1小时级别逻辑  # 注释说明这是针对小级别的数据逻辑
        if timeframe == '1h':
            res['oscillation_threshold'] = OSCILLATION_THRESHOLD_1H
            res['is_oscillation'] = boll_band_width_ratio < OSCILLATION_THRESHOLD_1H
 
        # 回调趋势还是空头 (满足3个条件)
        ps_c1 = last['volume'] > prev['volume']  # 条件1：当前K线成交量相比上一根有所上升
        ps_c2 = last['rsi'] >= 35 and last['rsi']-prev['rsi']<-4  # 条件2：当前RSI大于35
        ps_c3 = (prev['close'] < min(prev['ema20'], prev['ema50']) and  # 条件3：上一根收盘价低于EMA20/50的极小值，且...
                 last['open'] < min(last['ema20'], last['ema50']) and last['close'] < last['open'])  # 当前开盘价低于EMA极小值，并且上一根是阴线
        ps_c4 = (prev['close'] < prev['boll_mid'] and  # 条件4：上一根收盘价低于布林中轨，且...
                 last['open'] < last['boll_mid'] and last['close'] < last['open'])  # 当前开盘价低于布林中轨，并且上一根是阴线
        ps_c5 = (last['macd'] < last['macd_signal']) and (abs(last['macd_hist']) > abs(prev['macd_hist']))
        ps_ema_or_boll = ps_c3 or ps_c4  # EMA 与 BOLL 至少满足一个
        res['pullback_short'] = ps_ema_or_boll and sum([ps_c1, ps_c2,ps_c5]) >= 2  # 上述4个条件满足至少3个，且 EMA/BOLL 至少命中一个

        # 多头趋势 (满足3个条件)
        lt_c1 = last['volume'] > prev['volume']  # 条件1：当前K线成交量相比上一根有所上升
        lt_c2 = 35<=last['rsi'] <= 65 and last['rsi']-prev['rsi']>4 # 条件2：当前RSI小于65
        lt_c3 = (prev['close'] > min(prev['ema20'], prev['ema50']) and  # 条件3：上一根收盘价高于EMA20/50的极小值，且...
                 last['open'] > min(last['ema20'], last['ema50']) and last['close'] > last['open'])  # 当前开盘价高于EMA极小值，并且上一根是阳线
        lt_c4 = (prev['close'] > prev['boll_mid'] and  # 条件4：上一根收盘价高于布林中轨，且...
                 last['open'] > last['boll_mid'] and last['close'] > last['open'])  # 当前开盘价高于布林中轨，并且上一根是阳线
        lt_c5 = (last['macd'] > last['macd_signal']) and (abs(last['macd_hist']) > abs(prev['macd_hist']))  # 条件5：MACD为金叉状态（快线>慢线），且当前MACD值比上一个大（动能向上）
        lt_ema_or_boll = lt_c3 or lt_c4  # EMA 与 BOLL 至少满足一个
        res['long_trend'] = lt_ema_or_boll and sum([lt_c1, lt_c2, lt_c5]) >= 2 # 上述5个条件满足至少3个，且 EMA/BOLL 至少命中一个

        # 回调趋势还是多头 (满足3个条件)
        pl_c1 = last['volume'] > prev['volume']  # 条件1：当前K线成交量相比上一根有所上升
        pl_c2 = 35 <= last['rsi'] <= 65 and last['rsi']-prev['rsi']>4  # 条件2：当前RSI的值在40到65之间
        pl_c3 = (last['macd'] > last['macd_signal']) and (abs(last['macd_hist']) > abs(prev['macd_hist']))  # 条件3：MACD是金叉，且当前MACD值比上一个大
        # pl_c4 = prev['close'] > prev['ema20'] and prev['close'] > prev['ema50']  # 条件4：上一根的收盘价在EMA20和EMA50的上方
        # pl_c5 = prev['close'] > prev['boll_mid']  # 条件5：上一根的收盘价在布林带中轨的上方
        pl_c4 = last['open']>min(last['ema20'], last['ema50']) and prev['close']>min(prev['ema20'], prev['ema50']) and last['close'] > last['open']
        pl_c5 = last['open']>last['boll_mid'] and  prev['close']>prev['boll_mid'] and last['close'] > last['open'] 

        pl_ema_or_boll = pl_c4 or pl_c5  # EMA 与 BOLL 至少满足一个
        res['pullback_long'] = pl_ema_or_boll and sum([pl_c1, pl_c2, pl_c3]) >= 2  # 满足至少3个条件，且 EMA/BOLL 至少命中一个

        # 空头趋势 (满足3个条件)
        st_c1 = last['volume'] > prev['volume']  # 条件1：当前K线成交量相比上一根有所上升
        st_c2 = 35 <= last['rsi'] <= 65 and last['rsi']-prev['rsi']<=-4  # 条件2：当前RSI的值在40到60之间
        st_c3 = (last['macd'] < last['macd_signal']) and (abs(last['macd_hist']) > abs(prev['macd_hist']))  # 条件3：MACD是死叉（快线<慢线），并且当前MACD值大于上一个
        # st_c4 = prev['close'] < prev['ema20'] and prev['close'] < prev['ema50']  # 条件4：上一根的收盘价在EMA20和EMA50的下方
        # st_c5 = prev['close'] < prev['boll_mid']  # 条件5：上一根的收盘价在布林带中轨的下方
        st_c4 = last['open']<max(last['ema20'], last['ema50']) and prev['close']< max(prev['ema20'], prev['ema50']) and last['close'] < last['open']  # 上一根收盘价在EMA极大值之下（含容错），且当前为阴线
        st_c5 = last['open']<last['boll_mid'] and prev['close'] <prev['boll_mid'] and last['close'] < last['open']  # 上一根收盘价在布林中轨之下（含容错），且当前为阴线
        st_ema_or_boll = st_c4 or st_c5  # EMA 与 BOLL 至少满足一个
        res['short_trend'] = st_ema_or_boll and sum([st_c1, st_c2, st_c3]) >= 2  # 满足至少3个条件，且 EMA/BOLL 至少命中一个

        if timeframe == '1h':
            # 1H 空头平仓：最近几根先出现严重超卖/贴下轨，后续任一收盘站上参考K实体上沿，再叠加 MACD 动能衰减
            short_exit_ref = find_recent_extreme_exit_reference('short')
            cs_c1 = short_exit_ref is not None
            cs_c2 = bool(short_exit_ref and short_exit_ref['confirmed'])
            cs_c3 = abs(last['macd_hist']) < abs(prev['macd_hist'])
            res['close_short'] = all([cs_c1, cs_c2, cs_c3])
            close_short_checks = {
                'recent_touch_boll_dn_or_rsi_lt_30': cs_c1,
                'close_back_above_ref_body_high': cs_c2,
                'macd_hist_weakening': cs_c3
            }
            if short_exit_ref:
                close_short_checks.update({
                    'ref_bar_time': short_exit_ref['bar_time'],
                    'ref_offset': short_exit_ref['offset'],
                    'ref_body_high': short_exit_ref['body_threshold'],
                    'confirm_bar_time': short_exit_ref['confirm_bar_time'] or '无'
                })

            # 1H 多头平仓：最近几根先出现严重超买/贴上轨，后续任一收盘跌破参考K实体下沿，再叠加 MACD 动能衰减
            long_exit_ref = find_recent_extreme_exit_reference('long')
            cl_c1 = long_exit_ref is not None
            cl_c2 = bool(long_exit_ref and long_exit_ref['confirmed'])
            cl_c3 = abs(last['macd_hist']) < abs(prev['macd_hist'])
            res['close_long'] = all([cl_c1, cl_c2, cl_c3])
            close_long_checks = {
                'recent_touch_boll_up_or_rsi_gt_72': cl_c1,
                'close_back_below_ref_body_low': cl_c2,
                'macd_hist_weakening': cl_c3
            }
            if long_exit_ref:
                close_long_checks.update({
                    'ref_bar_time': long_exit_ref['bar_time'],
                    'ref_offset': long_exit_ref['offset'],
                    'ref_body_low': long_exit_ref['body_threshold'],
                    'confirm_bar_time': long_exit_ref['confirm_bar_time'] or '无'
                })
        else:
            # 15M 维持原有更灵敏的平仓条件
            cs_c1 = last['volume'] < prev['volume']  # 条件1：最新已收盘K线缩量
            cs_c2 = last['rsi'] < 30  # 条件2：最新已收盘K线RSI小于30（严重超卖）
            cs_c3 = last['close'] <= last['boll_dn'] or tol(last['close'], last['boll_dn'])  # 条件3：最新已收盘K线收盘价接近或跌破布林带下轨
            res['close_short'] = sum([cs_c1, cs_c2, cs_c3]) >= 2  # 满足至少2个条件，触发小级别强劲空头平仓信号
            close_short_checks = {
                'volume_shrink': cs_c1,
                'rsi_lt_30': cs_c2,
                'touch_or_break_boll_dn': cs_c3
            }

            # 15M 维持原有更灵敏的平仓条件
            cl_c1 = last['volume'] < prev['volume']  # 条件1：最新已收盘K线缩量
            cl_c2 = last['rsi'] > 75  # 条件2：最新已收盘K线RSI大于75（严重超买）
            cl_c3 = last['close'] >= last['boll_up'] or tol(last['close'], last['boll_up'])  # 条件3：最新已收盘K线收盘价接近或突破布林带上轨
            res['close_long'] = sum([cl_c1, cl_c2, cl_c3]) >= 2  # 满足至少2个条件，触发小级别强劲多头平仓信号
            close_long_checks = {
                'volume_shrink': cl_c1,
                'rsi_gt_75': cl_c2,
                'touch_or_break_boll_up': cl_c3
            }

        # 记录 1H / 15M 级别的具体条件判断结果
        res['details'] = {
            'ps': [ps_c1, ps_c2, ps_c3, ps_c4],
            'lt': [lt_c1, lt_c2, lt_c3, lt_c4, lt_c5],
            'pl': [pl_c1, pl_c2, pl_c3, pl_c4, pl_c5],
            'st': [st_c1, st_c2, st_c3, st_c4, st_c5],
            'close_long_checks': close_long_checks,
            'close_short_checks': close_short_checks
        }

    # 先给影线细节预留一个空字典，只有在 4H / 1H 上才会真正填充内容
    res['details']['shadow'] = {}
    #包括了，长影线止损，长影线确认反转，判断是否是长影线，长影线是否达到支撑或者压力位置。
    if timeframe in ('4h', '1h'):
        # 先判断最新一根K线是不是长上影线
        last_upper_shadow, last_upper_shadow_details = is_long_upper_shadow(last)
        # 再判断最新一根K线是不是长下影线
        last_lower_shadow, last_lower_shadow_details = is_long_lower_shadow(last)
        # 上一根是否为长上影线，给“确认反转”逻辑用
        prev_upper_shadow, prev_upper_shadow_details = is_long_upper_shadow(prev)
        # 上一根是否为长下影线，给“确认反转”逻辑用
        prev_lower_shadow, prev_lower_shadow_details = is_long_lower_shadow(prev)

        # 判断最新一根长上影线是不是出现在压力位
        last_upper_resistance, last_upper_resistance_details = is_upper_shadow_at_resistance(last, prev, prev2, tol, is_above)
        # 判断最新一根长下影线是不是出现在支撑位
        last_lower_support, last_lower_support_details = is_lower_shadow_at_support(last, prev, prev2, tol, is_below)
        # 判断上一根长上影线是不是出现在压力位，供“下一根确认反转”使用
        prev_upper_resistance, prev_upper_resistance_details = is_upper_shadow_at_resistance(prev, prev2, prev3, tol, is_above)
        # 判断上一根长下影线是不是出现在支撑位，供“下一根确认反转”使用
        prev_lower_support, prev_lower_support_details = is_lower_shadow_at_support(prev, prev2, prev3, tol, is_below)

        # 把“最新一根本身是不是长影线”直接记录下来，别的地方可以直接读取
        res['long_upper_shadow'] = last_upper_shadow
        res['long_lower_shadow'] = last_lower_shadow

        # 多单过滤器：当前这根如果是压力区长上影，而且收盘没站回强势区，就先别追多
        res['upper_shadow_filter_long'] = (
            last_upper_shadow and
            last_upper_resistance and
            (last['close'] <= max(last['ema20'], last['boll_mid']) or last['close'] < last['open'])
        )
        # 空单过滤器：当前这根如果是支撑区长下影，而且收盘回到偏强位置，就先别追空
        res['lower_shadow_filter_short'] = (
            last_lower_shadow and
            last_lower_support and
            (last['close'] >= min(last['ema20'], last['boll_mid']) or last['close'] > last['open'])
        )

        # 成交量是否放大只作为辅助观察，不作为硬条件，但我会记到 details 里
        upper_reversal_volume_ok = last['volume'] > prev['volume']
        lower_reversal_volume_ok = last['volume'] > prev['volume']
        upper_shadow_candidate = find_recent_shadow_candidate('upper')
        lower_shadow_candidate = find_recent_shadow_candidate('lower')
        upper_shadow_mid_hit = upper_shadow_candidate is not None and last['close'] <= upper_shadow_candidate['body_mid']
        upper_shadow_body_low_hit = upper_shadow_candidate is not None and last['close'] <= upper_shadow_candidate['body_low']
        lower_shadow_mid_hit = lower_shadow_candidate is not None and last['close'] >= lower_shadow_candidate['body_mid']
        lower_shadow_body_high_hit = lower_shadow_candidate is not None and last['close'] >= lower_shadow_candidate['body_high']
        upper_shadow_offset_ok = upper_shadow_candidate is not None and upper_shadow_candidate.get('offset', 99) <= SHADOW_REVERSAL_CONFIRM_MAX_OFFSET_BARS
        lower_shadow_offset_ok = lower_shadow_candidate is not None and lower_shadow_candidate.get('offset', 99) <= SHADOW_REVERSAL_CONFIRM_MAX_OFFSET_BARS
        upper_shadow_bearish_confirm = last['close'] < last['open']
        lower_shadow_bullish_confirm = last['close'] > last['open']
        current_long_logic_active = res['long_trend'] or res['pullback_long']
        current_short_logic_active = res['short_trend'] or res['pullback_short']

        # 开空反转：最近 3 根内先出现压力区长上影，当前这根要重新跌到实体中点下方且本身为阴线，且当前未回到多头逻辑。
        res['upper_shadow_reversal_short'] = (
            upper_shadow_candidate is not None and
            upper_shadow_offset_ok and
            upper_shadow_mid_hit and
            upper_shadow_bearish_confirm and
            not current_long_logic_active
        )
        # 开多反转：最近 3 根内先出现支撑区长下影，当前这根要重新涨到实体中点上方且本身为阳线，且当前未回到空头逻辑。
        res['lower_shadow_reversal_long'] = (
            lower_shadow_candidate is not None and
            lower_shadow_offset_ok and
            lower_shadow_mid_hit and
            lower_shadow_bullish_confirm and
            not current_short_logic_active
        )

        # 多单收紧止损：上一根先出现高位长上影，这一根又没创新高，视为多头变弱
        res['upper_shadow_tighten_long'] = (
            prev_upper_shadow and
            prev_upper_resistance and
            (prev['rsi'] >= 70 or prev['close'] >= prev['boll_up'] or tol(prev['close'], prev['boll_up'])) and
            last['high'] <= prev['high']
        )
        # 空单收紧止损：上一根先出现低位长下影，这一根又没创新低，视为空头变弱
        res['lower_shadow_tighten_short'] = (
            prev_lower_shadow and
            prev_lower_support and
            (prev['rsi'] <= 30 or prev['close'] <= prev['boll_dn'] or tol(prev['close'], prev['boll_dn'])) and
            last['low'] >= prev['low']
        )

        # 如果最近几根存在可用的长上影参考K，就预先算出“反转开空”的止损参考位
        if upper_shadow_candidate is not None:
            res['shadow_short_stop_ref'] = upper_shadow_candidate['stop_ref']
            res['shadow_short_trigger_tf'] = timeframe
            res['shadow_upper_reference_bar'] = upper_shadow_candidate['bar_time']
        # 如果最近几根存在可用的长下影参考K，就预先算出“反转开多”的止损参考位
        if lower_shadow_candidate is not None:
            res['shadow_long_stop_ref'] = lower_shadow_candidate['stop_ref']
            res['shadow_long_trigger_tf'] = timeframe
            res['shadow_lower_reference_bar'] = lower_shadow_candidate['bar_time']
        # 多单收紧止损时，保护位 = 上一根长上影实体底部 - 0.1ATR
        if res['upper_shadow_tighten_long']:
            res['upper_shadow_long_protect_ref'] = min(prev['open'], prev['close']) - 0.1 * prev['atr']
        # 空单收紧止损时，保护位 = 上一根长下影实体顶部 + 0.1ATR
        if res['lower_shadow_tighten_short']:
            res['lower_shadow_short_protect_ref'] = max(prev['open'], prev['close']) + 0.1 * prev['atr']

        # 把所有影线子条件塞进 details，方便你后面看日志和 CSV 复盘
        res['details']['shadow'] = {
            'last_upper_shadow': last_upper_shadow,
            'last_lower_shadow': last_lower_shadow,
            'prev_upper_shadow': prev_upper_shadow,
            'prev_lower_shadow': prev_lower_shadow,
            'last_upper_shadow_details': last_upper_shadow_details,
            'last_lower_shadow_details': last_lower_shadow_details,
            'prev_upper_shadow_details': prev_upper_shadow_details,
            'prev_lower_shadow_details': prev_lower_shadow_details,
            'last_upper_resistance': last_upper_resistance_details,
            'last_lower_support': last_lower_support_details,
            'prev_upper_resistance': prev_upper_resistance_details,
            'prev_lower_support': prev_lower_support_details,
            'upper_shadow_candidate': upper_shadow_candidate,
            'lower_shadow_candidate': lower_shadow_candidate,
            'upper_reversal_volume_ok': upper_reversal_volume_ok,
            'lower_reversal_volume_ok': lower_reversal_volume_ok,
            'upper_shadow_mid_hit': upper_shadow_mid_hit,
            'upper_shadow_body_low_hit': upper_shadow_body_low_hit,
            'lower_shadow_mid_hit': lower_shadow_mid_hit,
            'lower_shadow_body_high_hit': lower_shadow_body_high_hit,
            'upper_shadow_offset_ok': upper_shadow_offset_ok,
            'lower_shadow_offset_ok': lower_shadow_offset_ok,
            'upper_shadow_bearish_confirm': upper_shadow_bearish_confirm,
            'lower_shadow_bullish_confirm': lower_shadow_bullish_confirm,
            'current_long_logic_active': current_long_logic_active,
            'current_short_logic_active': current_short_logic_active,
            'upper_shadow_filter_long': res['upper_shadow_filter_long'],
            'lower_shadow_filter_short': res['lower_shadow_filter_short'],
            'upper_shadow_reversal_short': res['upper_shadow_reversal_short'],
            'lower_shadow_reversal_long': res['lower_shadow_reversal_long'],
            'upper_shadow_tighten_long': res['upper_shadow_tighten_long'],
            'lower_shadow_tighten_short': res['lower_shadow_tighten_short']
        }

    return res  # 返回包含所有趋势和信号状态的字典

def open_order(side, price, sl_price, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='trend', entry_trigger_tf=''):  # 定义执行开仓指令的函数，接收方向、当前价格、止损价格和各级别状态
    """执行开仓指令"""  # 函数的文档字符串
    global trade_state  # 声明trade_state为全局变量，以便修改它的状态
    amount = calculate_amount(price)  # 调用calculate_amount函数，计算本次下单的数量
    amount = float(amount)
    if amount == 0:
        return False  # 如果计算出的下单数量为0（可能余额不足或出错），则直接返回，不开仓
    open_order_id = ''

    # 开仓前先用“保守估算强平价”预校验长影线止损，防止止损本来就落到强平外面
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

    try:  # 尝试执行开仓流程
        # 获取开仓前的可用余额，用于后续平仓时计算精确净利润
        balance_before = exchange.fetch_balance({'type': 'future'})
        initial_usdt = float(balance_before['total']['USDT'])
        
        # 设置杠杆
        exchange.set_leverage(LEVERAGE, SYMBOL)  # 调用交易所API设置当前交易对的杠杆倍数

        # 市价单开仓
        order_side = 'buy' if side == 'long' else 'sell'  # 判断下单方向：做多为'buy'，做空为'sell'
        order = exchange.create_market_order(SYMBOL, order_side, amount)  # 调用API发送市价开仓订单
        open_order_id = extract_order_id(order)

        # 记录实际成交均价 (如有滑点以实际为准)
        actual_price = order.get('average', price)  # 尝试从订单结果中获取实际成交均价，如果没有则使用传入的理论价格
        if actual_price is None or actual_price == 0: actual_price = price  # 防止获取到的average为None，作为保底

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
                f"原因: 真实强平价过近，长影线止损无安全空间\n方向: {side}\n"
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
            'close_cond_4h': '',
            'close_cond_1h': '',
            'close_cond_15m': '',
            'entry_reason': entry_reason,  # 记录这笔单到底是趋势单还是影线反转单
            'entry_trigger_tf': entry_trigger_tf_display,  # 记录真正触发本次开仓的周期
            'shadow_stop_mode': '',  # 开仓时先清空，后面如果发生“影线收紧止损”再写入
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
            'last_shadow_adjust_bar_15m': '',  # 新开一笔单后，先把“最近一次影线调止损的15M时间”清空
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
        msg = (f"🚀 【已开仓】\n方向: {side}\n入场价: {actual_price}\n"  # 构建通知邮件的内容字符串，包含方向和入场价
               f"止损价: {actual_safe_stop}\n强平价: {actual_liquidation_price}\n数量: {amount}\n"
               f"杠杆: {LEVERAGE}x\n开仓前账户资金(USDT): {initial_usdt:.4f}\n入场原因: {entry_reason}\n触发周期: {entry_trigger_tf_display}\n15M信号时间: {signal_bar_15m}\n"
               f"开仓条件明细:\n{open_condition_details}"
               f"{order_id_suffix}")  # 继续构建字符串，包含止损价、数量和杠杆倍数
        send_msg(f"ETH交易: 开仓 {side}", msg)  # 调用发邮件函数，发送开仓通知
        logging.info(
            f"开仓成功: {side} at {actual_price}, SL: {actual_safe_stop}, "
            f"liq={actual_liquidation_price}, reason={entry_reason}, "
            f"open_order_id={open_order_id}, stop_order_id={trade_state.get('stop_order_id', '')}, stop_meta={actual_stop_meta}"
        )  # 在系统日志中记录开仓成功的详细信息
        return True

    except Exception as e:  # 捕获开仓过程中的所有异常
        error_msg = f"开仓失败: {e}"
        order_id_lines = format_order_id_lines(open_order_id=open_order_id)
        if order_id_lines:
            error_msg = f"{error_msg}\n{order_id_lines}"
        logging.error(error_msg)
        logging.error(traceback.format_exc())
        # 可以加上邮件通知
        send_msg("ETH交易: ⚠️开仓失败警告", error_msg)
        return False


def monitor_position(state_4h, state_1h, state_15m, atr_1h, atr_4h, signal_bar_15m='', allow_strategy_close=False):  # 定义持仓监控函数，接收三个时间级别的状态评估结果和ATR值
    """持仓监控逻辑"""  # 函数的文档字符串
    global trade_state  # 声明全局变量trade_state，以便读取和更新状态
    try:  # 尝试执行监控逻辑
        # 先同步一次交易所真实仓位，防止服务端止损已经成交但本地状态还以为有仓位
        #获取仓位的标记价格和强平价格
        position_risk = get_position_risk(side=trade_state['side'])
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

            fallback_check = has_open_position_on_exchange(side=trade_state['side'])
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
        trade_state['close_cond_4h'] = format_condition_snapshot_for_mail('4H', state_4h)
        trade_state['close_cond_1h'] = format_condition_snapshot_for_mail('1H', state_1h)
        trade_state['close_cond_15m'] = format_condition_snapshot_for_mail('15M', state_15m)
        entry_allowed = not state_4h.get('is_oscillation', False) and not state_1h.get('is_oscillation', False)
        upper_shadow_filter_long = state_4h.get('upper_shadow_filter_long') or state_1h.get('upper_shadow_filter_long')
        lower_shadow_filter_short = state_4h.get('lower_shadow_filter_short') or state_1h.get('lower_shadow_filter_short')
        reversal_long_tf, reversal_long_state = pick_state_signal(state_4h, state_1h, 'lower_shadow_reversal_long')
        reversal_short_tf, reversal_short_state = pick_state_signal(state_4h, state_1h, 'upper_shadow_reversal_short')
        reversal_conflict = reversal_long_state is not None and reversal_short_state is not None
        long_cond_4h = state_4h.get('long_trend')
        long_cond_1h = state_1h.get('long_trend') or state_1h.get('pullback_long')
        long_cond_15m = state_15m.get('long_trend')
        short_cond_4h = state_4h.get('short_trend')
        short_cond_1h = state_1h.get('short_trend') or state_1h.get('pullback_short')
        short_cond_15m = state_15m.get('short_trend')
        trend_long_ready = entry_allowed and long_cond_4h and long_cond_1h and long_cond_15m and not upper_shadow_filter_long
        trend_short_ready = entry_allowed and short_cond_4h and short_cond_1h and short_cond_15m and not lower_shadow_filter_short

        curr_price = get_latest_price()  # 提取最新的成交价格
        entry = trade_state['entry_price']  # 读取当前持仓的入场价格
        sl_price = trade_state['stop_loss_price']  # 读取当前设置的止损价格
        # 这里只允许在新的15M信号K线上调一次影线止损，避免同一根15M重复抬止损/压止损
        can_adjust_shadow_stop = (
            allow_strategy_close and
            signal_bar_15m and
            signal_bar_15m != trade_state.get('last_shadow_adjust_bar_15m', '')
        )
        # --- A. 多单监控 ---
        if trade_state['side'] == 'long':  # 如果当前持仓方向为做多
            # 更新最高价
            if curr_price > trade_state['highest_price']:  # 判断当前价格是否高于记录的历史最高价
                trade_state['highest_price'] = curr_price  # 如果是，则更新历史最高价为当前价格

            # 1. 动态止损
            if curr_price <= sl_price:  # 判断当前价格是否跌破或等于止损价
                close_position("触发ATR止损", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="ATR止损")  # 如果触发止损，调用平仓函数并传入原因
                return  # 平仓后退出监控函数

            # 2. 动态回撤止盈：先算出理论保护价，再决定是直接平仓还是只抬高服务端止损。
            dynamic_retrace = calculate_dynamic_retrace_plan(
                side='long',
                entry_price=entry,
                extreme_price=trade_state['highest_price'],
                curr_price=curr_price,
                atr_1h=atr_1h,
                state_4h=state_4h,
                state_1h=state_1h,
                state_15m=state_15m
            )
            if dynamic_retrace:
                dynamic_stop = dynamic_retrace['trail_price']
                # 价格已经回撤到保护位，说明这段利润不再继续放。
                if curr_price <= dynamic_stop:
                    close_position(
                        f"动态回撤止盈(曾获利:{dynamic_retrace['max_profit']:.2f}, 回撤阈值:{dynamic_retrace['retrace_threshold']:.0%})",
                        curr_price,
                        signal_bar_15m=signal_bar_15m,
                        trigger_label="动态回撤止盈"
                    )
                    return

                # 价格还没碰到保护位，就仅把保护止损往更有利的方向推进。
                if should_refresh_dynamic_stop('long', trade_state['stop_loss_price'], dynamic_stop, atr_1h, entry):
                    if refresh_protective_stop_order(dynamic_stop):
                        trade_state['stop_loss_price'] = dynamic_stop
                        logging.info(
                            f"多单动态回撤收紧止损: stop={dynamic_stop:.4f}, "
                            f"max_profit={dynamic_retrace['max_profit']:.4f}, "
                            f"profit_atr={dynamic_retrace['profit_atr']:.2f}, "
                            f"threshold={dynamic_retrace['retrace_threshold']:.0%}"
                        )
                    else:
                        if handle_stop_order_refresh_failure(
                            "动态回撤保护止损更新失败，连续5次后主动平仓",
                            curr_price,
                            signal_bar_15m=signal_bar_15m,
                            trigger_label="动态回撤保护止损更新失败，连续5次后主动平仓"
                        ):
                            return
                        return

            # 3. 影线收紧止损、确认反转和平仓条件
            if allow_strategy_close:
                if can_adjust_shadow_stop:
                    # 先看 4H / 1H 有没有“多单该因为长上影线收紧止损”的信号，4H 优先
                    tighten_tf, tighten_state = pick_state_signal(state_4h, state_1h, 'upper_shadow_tighten_long')
                    if tighten_state is not None:
                        # protect_ref 就是新的保护价，也就是我们希望把止损抬到的位置
                        protect_ref = tighten_state.get('upper_shadow_long_protect_ref')
                        if protect_ref is not None and not pd.isna(protect_ref):
                            # 多单止损只能往上抬，不能往下放宽，所以这里取 max
                            new_sl = max(trade_state['stop_loss_price'], protect_ref)
                            if new_sl > trade_state['stop_loss_price']:
                                if not refresh_protective_stop_order(new_sl):
                                    if handle_stop_order_refresh_failure(
                                        "服务端止损更新失败，连续5次后主动平仓",
                                        curr_price,
                                        signal_bar_15m=signal_bar_15m,
                                        trigger_label="服务端止损更新失败，连续5次后主动平仓"
                                    ):
                                        return
                                    return
                                # 更新全局止损价
                                trade_state['stop_loss_price'] = new_sl
                                # 记录这次止损是因为哪个周期的长上影线收紧的
                                trade_state['shadow_stop_mode'] = f"{tighten_tf} 长上影收紧止损"
                                # 记住这根15M已经处理过收紧止损，后面同一根15M不再重复执行
                                trade_state['last_shadow_adjust_bar_15m'] = signal_bar_15m
                                trade_state['stop_order_price'] = new_sl
                                logging.info(f"多单收紧止损: {tighten_tf}, 新止损价={new_sl:.4f}")
                                order_id_lines = format_order_id_lines(
                                    open_order_id=trade_state.get('open_order_id', ''),
                                    stop_order_id=trade_state.get('stop_order_id', '')
                                )
                                order_id_suffix = f"\n{order_id_lines}" if order_id_lines else ''
                                send_msg(
                                    "ETH交易: 多单收紧止损",
                                    f"{tighten_tf} 长上影信号触发，新的多单止损价: {new_sl:.4f}"
                                    f"{order_id_suffix}"
                                )
                                # 如果当前价格已经跌到新止损下方，就直接按“收紧止损”原因平仓
                                if curr_price <= trade_state['stop_loss_price']:
                                    close_position(f"{tighten_tf} 长上影收紧止损", curr_price, signal_bar_15m=signal_bar_15m, trigger_label=f"{tighten_tf} 长上影收紧止损")
                                    return

                # 如果已经满足完整的反手做空条件，直接平多并反手开空
                if entry_allowed and not reversal_conflict and reversal_short_state is not None and short_cond_15m and not lower_shadow_filter_short:
                    if close_position(f"{reversal_short_tf} 长上影确认反转平多并开空", curr_price, signal_bar_15m=signal_bar_15m, trigger_label=f"{reversal_short_tf} 长上影确认反转平多并开空"):
                        try:
                            reverse_price = get_latest_price()
                            reverse_sl = reversal_short_state.get('shadow_short_stop_ref')
                            if reverse_sl is None or pd.isna(reverse_sl):
                                reverse_sl = reverse_price + atr_4h
                            if open_order('short', reverse_price, reverse_sl, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='shadow_reversal_short', entry_trigger_tf=reversal_short_tf):
                                logging.info(f"{reversal_short_tf} 影线反转触发，已完成多转空")
                        except Exception:
                            logging.error(f"多转空开仓失败:\n{traceback.format_exc()}")
                    return

                # 如果还不满足完整的开空条件，就保留原来的“先平多”风控动作
                if reversal_short_state is not None:
                    if tighten_stop_on_reversal_warning(
                        'long',
                        reversal_short_tf,
                        reversal_short_state,
                        curr_price,
                        signal_bar_15m=signal_bar_15m
                    ):
                        return
                    logging.info(f"{reversal_short_tf} 长上影反转预警已出现，但未满足反手开空条件，本轮仅收紧或继续持有多单")
                    return

                # 如果满足完整的趋势做空条件，也直接平多并反手开空
                if trend_short_ready and not trend_long_ready:
                    if close_position("趋势空头条件成立，反手平多并开空", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="趋势反手开空"):
                        try:
                            reverse_price = get_latest_price()
                            reverse_sl = reverse_price + atr_4h
                            if open_order('short', reverse_price, reverse_sl, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='trend_short', entry_trigger_tf='4H+1H+15M'):
                                logging.info("趋势空头条件成立，已完成多转空")
                        except Exception:
                            logging.error(f"趋势多转空开仓失败:\n{traceback.format_exc()}")
                    return

                # 如果影线没有触发平仓，再退回原来的 close_long 逻辑，4H / 1H 各自独立可平仓
                if state_4h.get('close_long'):
                    logging.info(f"4H close_long 触发明细: {format_condition_snapshot_for_mail('4H', state_4h)}")
                    close_position("4H close_long 策略平仓", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="4H close_long")
                    return
                if state_1h.get('close_long'):
                    if REQUIRE_15M_CONFIRM_FOR_1H_CLOSE and not state_15m.get('close_long'):
                        logging.info(
                            f"1H close_long 已触发，但 15M close_long 未确认，本轮先不平多: "
                            f"{format_condition_snapshot_for_mail('1H', state_1h)}"
                        )
                    else:
                        logging.info(f"1H close_long 触发明细: {format_condition_snapshot_for_mail('1H', state_1h)}")
                        close_position("1H close_long 策略平仓", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="1H close_long")
                        return

           
        # --- B. 空单监控 ---
        elif trade_state['side'] == 'short':  # 如果当前持仓方向为做空
            # 更新最低价
            if curr_price < trade_state['lowest_price']:  # 判断当前价格是否低于记录的历史最低价
                trade_state['lowest_price'] = curr_price  # 如果是，则更新历史最低价为当前价格

            # 1. 动态止损
            if curr_price >= sl_price:  # 判断当前价格是否上涨触及或超过止损价
                close_position("触发ATR止损", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="ATR止损")  # 如果触发止损，调用平仓函数并传入原因
                return  # 平仓后退出监控函数
            # 2. 动态回撤止盈：空单逻辑和多单镜像，对应的是“压低”保护止损。
            dynamic_retrace = calculate_dynamic_retrace_plan(
                side='short',
                entry_price=entry,
                extreme_price=trade_state['lowest_price'],
                curr_price=curr_price,
                atr_1h=atr_1h,
                state_4h=state_4h,
                state_1h=state_1h,
                state_15m=state_15m
            )
            if dynamic_retrace:
                dynamic_stop = dynamic_retrace['trail_price']
                # 空单价格反弹回保护位，说明利润开始明显回吐。
                if curr_price >= dynamic_stop:
                    close_position(
                        f"动态回撤止盈(曾获利:{dynamic_retrace['max_profit']:.2f}, 回撤阈值:{dynamic_retrace['retrace_threshold']:.0%})",
                        curr_price,
                        signal_bar_15m=signal_bar_15m,
                        trigger_label="动态回撤止盈"
                    )
                    return

                # 空单未被打到保护位时，只更新更低的保护止损，不放宽旧止损。
                if should_refresh_dynamic_stop('short', trade_state['stop_loss_price'], dynamic_stop, atr_1h, entry):
                    if refresh_protective_stop_order(dynamic_stop):
                        trade_state['stop_loss_price'] = dynamic_stop
                        logging.info(
                            f"空单动态回撤收紧止损: stop={dynamic_stop:.4f}, "
                            f"max_profit={dynamic_retrace['max_profit']:.4f}, "
                            f"profit_atr={dynamic_retrace['profit_atr']:.2f}, "
                            f"threshold={dynamic_retrace['retrace_threshold']:.0%}"
                        )
                    else:
                        if handle_stop_order_refresh_failure(
                            "动态回撤保护止损更新失败，连续5次后主动平仓",
                            curr_price,
                            signal_bar_15m=signal_bar_15m,
                            trigger_label="动态回撤保护止损更新失败，连续5次后主动平仓"
                        ):
                            return
                        return

            # 3. 影线收紧止损、确认反转和平仓条件
            if allow_strategy_close:
                if can_adjust_shadow_stop:
                    # 先看 4H / 1H 有没有“空单该因为长下影线收紧止损”的信号，4H 优先
                    tighten_tf, tighten_state = pick_state_signal(state_4h, state_1h, 'lower_shadow_tighten_short')
                    if tighten_state is not None:
                        # protect_ref 就是新的保护价，也就是我们希望把空单止损压到的位置
                        protect_ref = tighten_state.get('lower_shadow_short_protect_ref')
                        if protect_ref is not None and not pd.isna(protect_ref):
                            # 空单止损只能往下压，不能重新放宽，所以这里取 min
                            new_sl = min(trade_state['stop_loss_price'], protect_ref)
                            if new_sl < trade_state['stop_loss_price']:
                                if not refresh_protective_stop_order(new_sl):
                                    if handle_stop_order_refresh_failure(
                                        "服务端止损更新失败，连续5次后主动平仓",
                                        curr_price,
                                        signal_bar_15m=signal_bar_15m,
                                        trigger_label="服务端止损更新失败，连续5次后主动平仓"
                                    ):
                                        return
                                    return
                                # 更新全局止损价
                                trade_state['stop_loss_price'] = new_sl
                                # 记录这次止损是因为哪个周期的长下影线收紧的
                                trade_state['shadow_stop_mode'] = f"{tighten_tf} 长下影收紧止损"
                                # 记住这根15M已经处理过收紧止损，后面同一根15M不再重复执行
                                trade_state['last_shadow_adjust_bar_15m'] = signal_bar_15m
                                trade_state['stop_order_price'] = new_sl
                                logging.info(f"空单收紧止损: {tighten_tf}, 新止损价={new_sl:.4f}")
                                order_id_lines = format_order_id_lines(
                                    open_order_id=trade_state.get('open_order_id', ''),
                                    stop_order_id=trade_state.get('stop_order_id', '')
                                )
                                order_id_suffix = f"\n{order_id_lines}" if order_id_lines else ''
                                send_msg(
                                    "ETH交易: 空单收紧止损",
                                    f"{tighten_tf} 长下影信号触发，新的空单止损价: {new_sl:.4f}"
                                    f"{order_id_suffix}"
                                )
                                # 如果当前价格已经反弹到新止损上方，就直接按“收紧止损”原因平仓
                                if curr_price >= trade_state['stop_loss_price']:
                                    close_position(f"{tighten_tf} 长下影收紧止损", curr_price, signal_bar_15m=signal_bar_15m, trigger_label=f"{tighten_tf} 长下影收紧止损")
                                    return

                # 如果已经满足完整的反手做多条件，直接平空并反手开多
                if entry_allowed and not reversal_conflict and reversal_long_state is not None and long_cond_15m and not upper_shadow_filter_long:
                    if close_position(f"{reversal_long_tf} 长下影确认反转平空并开多", curr_price, signal_bar_15m=signal_bar_15m, trigger_label=f"{reversal_long_tf} 长下影确认反转平空并开多"):
                        try:
                            reverse_price = get_latest_price()
                            reverse_sl = reversal_long_state.get('shadow_long_stop_ref')
                            if reverse_sl is None or pd.isna(reverse_sl):
                                reverse_sl = reverse_price - atr_4h
                            if open_order('long', reverse_price, reverse_sl, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='shadow_reversal_long', entry_trigger_tf=reversal_long_tf):
                                logging.info(f"{reversal_long_tf} 影线反转触发，已完成空转多")
                        except Exception:
                            logging.error(f"空转多开仓失败:\n{traceback.format_exc()}")
                    return

                # 如果还不满足完整的开多条件，就保留原来的“先平空”风控动作
                if reversal_long_state is not None:
                    if tighten_stop_on_reversal_warning(
                        'short',
                        reversal_long_tf,
                        reversal_long_state,
                        curr_price,
                        signal_bar_15m=signal_bar_15m
                    ):
                        return
                    logging.info(f"{reversal_long_tf} 长下影反转预警已出现，但未满足反手开多条件，本轮仅收紧或继续持有空单")
                    return

                # 如果满足完整的趋势做多条件，也直接平空并反手开多
                if trend_long_ready and not trend_short_ready:
                    if close_position("趋势多头条件成立，反手平空并开多", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="趋势反手开多"):
                        try:
                            reverse_price = get_latest_price()
                            reverse_sl = reverse_price - atr_4h
                            if open_order('long', reverse_price, reverse_sl, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='trend_long', entry_trigger_tf='4H+1H+15M'):
                                logging.info("趋势多头条件成立，已完成空转多")
                        except Exception:
                            logging.error(f"趋势空转多开仓失败:\n{traceback.format_exc()}")
                    return

                # 如果影线没有触发平仓，再退回原来的 close_short 逻辑，4H / 1H 各自独立可平仓
                if state_4h.get('close_short'):
                    logging.info(f"4H close_short 触发明细: {format_condition_snapshot_for_mail('4H', state_4h)}")
                    close_position("4H close_short 策略平仓", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="4H close_short")
                    return
                if state_1h.get('close_short'):
                    if REQUIRE_15M_CONFIRM_FOR_1H_CLOSE and not state_15m.get('close_short'):
                        logging.info(
                            f"1H close_short 已触发，但 15M close_short 未确认，本轮先不平空: "
                            f"{format_condition_snapshot_for_mail('1H', state_1h)}"
                        )
                    else:
                        logging.info(f"1H close_short 触发明细: {format_condition_snapshot_for_mail('1H', state_1h)}")
                        close_position("1H close_short 策略平仓", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="1H close_short")
                        return

           
    except Exception as e:  # 捕获监控过程中可能出现的行情获取或计算异常
        logging.error(f"监控异常: {e}")  # 记录异常信息到日志中


def close_position(reason, curr_price=None, signal_bar_15m='', trigger_label=''):  # 定义平仓函数，接收平仓原因和当前触发价格
    """执行平仓指令"""  # 函数的文档字符串
    global trade_state  # 声明全局变量，以便在平仓后重置状态
    # 平仓方向与持仓方向相反
    side = 'sell' if trade_state['side'] == 'long' else 'buy'  # 确定平仓的订单方向：多单平仓为'sell'，空单平仓为'buy'
    close_order_id = ''
    try:  
        if curr_price is None:
            curr_price = get_latest_price()
        # 在主动市价平仓前先撤掉旧的服务端止损单，避免平仓后残留条件单
        cancel_protective_stop_order(silent=True)
        # 尝试执行平仓交易流程
        order = exchange.create_market_order(SYMBOL, side, trade_state['amount'])  # 调用API发送市价单，按照持仓数量全部平仓
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
        if trade_state['side'] == 'long':  # 如果原持仓是多单
            pnl_points = actual_close_price - trade_state['entry_price']  # 盈亏 = 现价 - 开仓价
        else:  # 如果原持仓是空单
            pnl_points = trade_state['entry_price'] - actual_close_price  # 盈亏 = 开仓价 - 现价

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
        msg = (f"🏁 【已平仓】\n原因: {reason}\n入场价: {trade_state['entry_price']}\n"  # 构建平仓通知邮件的文本内容
               f"出场价: {actual_close_price}\n点数盈亏: {pnl_points:.2f}\n手续费: {fee_cost:.4f}\n净利润(USDT): {net_pnl_usdt:.2f}\n"
               f"平仓后账户资金(USDT): {final_usdt:.4f}\n15M信号时间: {exit_signal_bar_15m}\n触发来源: {trigger_label or reason}\n"
               f"平仓条件明细:\n{close_condition_details}"
               f"{order_id_suffix}")  # 拼接出场价格和最终点数盈亏信息
        send_msg(f"ETH交易: 平仓通知", msg)  # 发送带有平仓结果的邮件通知

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
            'shadow_stop_mode': '',
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
            'last_shadow_adjust_bar_15m': '',
            'position_miss_count': 0
        })  # 平仓成功后，将全局持仓状态重置
        logging.info(
            f"清仓成功: {reason}, PnL: {pnl_points:.2f}, Net USDT: {net_pnl_usdt:.2f}, "
            f"open_order_id={open_order_id}, close_order_id={close_order_id}"
        )  # 记录清仓成功的日志，包含原因和点数盈亏
        return True
    except Exception as e:  # 捕获调用API平仓时可能发生的异常
        logging.error(f"清仓失败: {e}")  # 将清仓失败的报错信息写入日志
        return False


def run_strategy():  # 定义策略运行的主函数，负责统筹数据获取、状态判断和下单
    
    global trade_state  # 声明全局变量，判断是否需要执行监控还是开仓
    # 1. 获取各个级别数据
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
    try:
        future_4h = executor.submit(fetch_df, SYMBOL, '4h', 100)
        future_1h = executor.submit(fetch_df, SYMBOL, '1h', 100) # 获取1小时级别的最近100根K线及指标数据
        future_15m = executor.submit(fetch_df, SYMBOL, '15m', 100) # 获取15分钟级别的最近100根K线及指标数据
        df_4h  = future_4h.result(timeout=FETCH_DF_TASK_TIMEOUT_SECONDS)
        df_1h  = future_1h.result(timeout=FETCH_DF_TASK_TIMEOUT_SECONDS)
        df_15m = future_15m.result(timeout=FETCH_DF_TASK_TIMEOUT_SECONDS)
    except concurrent.futures.TimeoutError:
        logging.error(
            f"抓取K线任务超时，已跳过本轮策略计算: timeout={FETCH_DF_TASK_TIMEOUT_SECONDS}s"
        )
        executor.shutdown(wait=False, cancel_futures=True)
        return
    except Exception:
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)

    if df_4h is None or df_1h is None or df_15m is None:  # 判断是否任何一个级别的数据获取失败
        return  # 若获取失败则直接退出本次循环，等待下一次
  
    # 2. 评估各个级别的状态
    server_now_dt = get_server_now_dt()
    state_4h = evaluate_trend(df_4h, '4h', time_factor=1.0, is_4h=True, now_dt=server_now_dt)  # 调用评估函数，计算4H级别的状态，时间系数设为1.0
    state_1h = evaluate_trend(df_1h, '1h', time_factor=1.3, is_4h=False, now_dt=server_now_dt)  # 调用评估函数，计算1H级别的状态，时间系数设为1.3
    state_15m = evaluate_trend(df_15m, '15m', time_factor=1.8, is_4h=False, now_dt=server_now_dt)  # 调用评估函数，计算15M级别的状态，时间系数设为1.8
    signal_bar_15m = get_closed_bar_time(df_15m, '15m', now_dt=server_now_dt)
    is_new_signal_bar = signal_bar_15m and signal_bar_15m != trade_state['last_processed_bar_15m']
    last_closed_idx_4h = get_last_closed_index(df_4h, '4h', now_dt=server_now_dt)
    last_closed_idx_1h = get_last_closed_index(df_1h, '1h', now_dt=server_now_dt)
    if last_closed_idx_4h is None or last_closed_idx_1h is None:
        return
    atr_4h = df_4h.iloc[last_closed_idx_4h]['atr']  # 从4小时级别最近一根已收盘K线获取ATR值，用于后续计算止损
    atr_1h = df_1h.iloc[last_closed_idx_1h]['atr']
    atr_1h = atr_1h * 1.5
    # 3. 检查是否有持仓
    if trade_state['has_position']:  # 判断当前是否已经有仓位在手
        monitor_position(state_4h, state_1h, state_15m, atr_1h, atr_4h, signal_bar_15m=signal_bar_15m, allow_strategy_close=is_new_signal_bar)  # 如果有持仓，进入监控持仓和止盈止损逻辑
        if is_new_signal_bar:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        return  # 监控结束后直接返回，不需要执行开仓逻辑
    if not is_new_signal_bar:
        return
    # 4. 入场逻辑判断
    # 震荡行情过滤
    if state_4h.get('is_oscillation', False) or state_1h.get('is_oscillation', False):  # 4H 或 1H 任一震荡都先跳过开仓
        logging.info(
            "跳过震荡行情: "
            f"4H宽度比={state_4h.get('boll_band_width_ratio', float('nan')):.4f}, 阈值={state_4h.get('oscillation_threshold')}; "
            f"1H宽度比={state_1h.get('boll_band_width_ratio', float('nan')):.4f}, 阈值={state_1h.get('oscillation_threshold')}"
        )
        trade_state['last_processed_bar_15m'] = signal_bar_15m
        return  # 如果是震荡行情，直接返回不开仓
    if signal_bar_15m == trade_state['last_entry_bar_15m'] or signal_bar_15m == trade_state['last_exit_bar_15m']:
        logging.info(f"跳过重复15M信号K线: {signal_bar_15m}")
        trade_state['last_processed_bar_15m'] = signal_bar_15m
        return
    upper_shadow_filter_long = state_4h.get('upper_shadow_filter_long') or state_1h.get('upper_shadow_filter_long')  # 4H 或 1H 任一出现压制型长上影线，就过滤趋势做多
    lower_shadow_filter_short = state_4h.get('lower_shadow_filter_short') or state_1h.get('lower_shadow_filter_short')  # 4H 或 1H 任一出现支撑型长下影线，就过滤趋势做空
    reversal_long_tf, reversal_long_state = pick_state_signal(state_4h, state_1h, 'lower_shadow_reversal_long')  # 查找是否存在“长下影 + 阳线确认”的开多反转信号
    reversal_short_tf, reversal_short_state = pick_state_signal(state_4h, state_1h, 'upper_shadow_reversal_short')  # 查找是否存在“长上影 + 阴线确认”的开空反转信号
    reversal_conflict = reversal_long_state is not None and reversal_short_state is not None  # 如果同一轮里多空反转都出现，就视为信号冲突

    # --- 做多入场逻辑 ---
    long_cond_4h = state_4h.get('long_trend')  # 判断4H级别是否确认处于多头趋势
    long_cond_1h = state_1h.get('long_trend') or state_1h.get('pullback_long')  # 判断1H级别是否处于多头趋势或回调多头结构
    long_cond_15m = state_15m.get('long_trend')  # 判断15M级别是否确认处于多头趋势（入场点确认）
    trend_long_ready = long_cond_4h and long_cond_1h and long_cond_15m and not upper_shadow_filter_long  # 原趋势做多成立，但会被长上影过滤器拦下来

    # --- 做空入场逻辑 ---
    short_cond_4h = state_4h.get('short_trend')  # 判断4H级别是否确认处于空头趋势
    short_cond_1h = state_1h.get('short_trend') or state_1h.get('pullback_short')  # 判断1H级别是否处于空头趋势或回调空头结构
    short_cond_15m = state_15m.get('short_trend')  # 判断15M级别是否确认处于空头趋势（入场点确认）
    trend_short_ready = short_cond_4h and short_cond_1h and short_cond_15m and not lower_shadow_filter_short  # 原趋势做空成立，但会被长下影过滤器拦下来

    if reversal_conflict:
        logging.info("检测到多空影线确认反转同时出现，跳过影线反转开仓，继续等待趋势分支")

    if not reversal_conflict and reversal_long_state is not None and (long_cond_15m or reversal_long_state.get('details', {}).get('shadow', {}).get('lower_shadow_body_high_hit')) and not upper_shadow_filter_long:
        try:
            # 真正下单时，还是以当前最新成交价作为入场价
            curr_price = get_latest_price()
            # 影线反转单优先使用影线结构给出的止损位
            sl_price = reversal_long_state.get('shadow_long_stop_ref')
            # 如果影线止损位拿不到，就退回 ATR 止损作为兜底
            if sl_price is None or pd.isna(sl_price):
                sl_price = curr_price - atr_4h
            # entry_reason 会被写进状态、通知和 CSV，后面一看就知道是影线反转多单
            open_order('long', curr_price, sl_price, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='shadow_reversal_long', entry_trigger_tf=reversal_long_tf)
            logging.info(f"{reversal_long_tf} 长下影确认反转多单开仓成功")
        except Exception:
            logging.error(f"影线反转多单开仓失败:\n{traceback.format_exc()}")
        trade_state['last_processed_bar_15m'] = signal_bar_15m
        return

    if not reversal_conflict and reversal_short_state is not None and (short_cond_15m or reversal_short_state.get('details', {}).get('shadow', {}).get('upper_shadow_body_low_hit')) and not lower_shadow_filter_short:
        try:
            # 真正下单时，还是以当前最新成交价作为入场价
            curr_price = get_latest_price()
            # 影线反转单优先使用影线结构给出的止损位
            sl_price = reversal_short_state.get('shadow_short_stop_ref')
            # 如果影线止损位拿不到，就退回 ATR 止损作为兜底
            if sl_price is None or pd.isna(sl_price):
                sl_price = curr_price + atr_4h
            # entry_reason 会被写进状态、通知和 CSV，后面一看就知道是影线反转空单
            open_order('short', curr_price, sl_price, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='shadow_reversal_short', entry_trigger_tf=reversal_short_tf)
            logging.info(f"{reversal_short_tf} 长上影确认反转空单开仓成功")
        except Exception:
            logging.error(f"影线反转空单开仓失败:\n{traceback.format_exc()}")
        trade_state['last_processed_bar_15m'] = signal_bar_15m
        return

    if trend_long_ready and trend_short_ready:
        logging.info("趋势多空条件同时成立，当前信号冲突，跳过本轮开仓")
        trade_state['last_processed_bar_15m'] = signal_bar_15m
        return

    if trend_long_ready:  # 如果三个级别的做多条件同时满足（共振）
        try:
            curr_price = get_latest_price()
            sl_price = curr_price - atr_4h  # 根据公式：入场价 - 1.3 * 1H_ATR，计算多单底线止损价
            open_order('long', curr_price, sl_price, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='trend_long', entry_trigger_tf='4H+1H+15M')  # 调用开仓函数，执行做多操作
            logging.info("多单开仓成功")
        except Exception:
            logging.error(f"多单开仓失败:\n{traceback.format_exc()}")
        trade_state['last_processed_bar_15m'] = signal_bar_15m
        return  # 开仓后返回结束本次逻辑

    if trend_short_ready:  # 如果三个级别的做空条件同时满足（共振）
        try:
            curr_price = get_latest_price()
            sl_price = curr_price + atr_4h  # 根据公式：入场价 + 1.3 * 1H_ATR，计算空单底线止损价
            open_order('short', curr_price, sl_price, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='trend_short', entry_trigger_tf='4H+1H+15M')  # 调用开仓函数，执行做空操作
            logging.info("空单开仓成功")
        except Exception:
            logging.error(f"空单开仓失败:\n{traceback.format_exc()}")
        trade_state['last_processed_bar_15m'] = signal_bar_15m
        return  # 开仓后返回结束本次逻辑
    trade_state['last_processed_bar_15m'] = signal_bar_15m


# ==========================================
# 4. 程序入口
# ==========================================
if __name__ == '__main__':  # Python标准写法，判断当前文件是否作为主程序被直接运行
    try:
        current_time_str = get_server_time_str()
        print("网络连通成功！服务器时间:", current_time_str)
        balance_after = exchange.fetch_balance({'type': 'future'})
        final_usdt = float(balance_after['total']['USDT'])
        logging.info(f"🚀 自动化交易策略系统启动，初始金额：{final_usdt}")  # 记录系统成功启动的信息到日志
    except Exception as e:
        logging.error(f"启动自检失败，但主循环会继续尝试运行: {e}")
    #exchange.load_markets()
    while True:  # 开启一个无限循环，让策略持续运行不中断
        try:  # 尝试在主循环中运行策略，捕获最高层级的异常
            maybe_log_heartbeat()
            run_strategy()  # 调用核心策略函数
        except Exception as e:  # 捕获策略运行中未被内部函数捕获的意外崩溃
            print(traceback.format_exc())
            logging.error(f"系统运行报错: {e}")  # 将意外报错记录到日志，防止程序直接闪退
            logging.error("主循环将继续运行，休眠后自动进入下一轮")
            time.sleep(MAIN_LOOP_ERROR_SLEEP_SECONDS)
            continue
        # 每1秒执行一次循环
        time.sleep(MAIN_LOOP_SLEEP_SECONDS)  # 当前循环执行完毕后，挂起休眠1秒，减轻CPU和API请求压力，然后再进行下一次循环

