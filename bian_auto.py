"""
环境：
python version>=3.12 必须的
阿里云东京服务器linux
说明:
该脚本是币安合约交易自动脚本（只交易ETH）
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

BAR_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'  # 定义统一的K线时间格式字符串，用于日志记录和CSV输出
TIMEFRAME_SECONDS = {  # 定义各时间周期对应的秒数，用于计算K线是否已收盘
    '15m': 15 * 60,    # 15分钟 = 900秒
    '1h': 60 * 60,     # 1小时 = 3600秒
    '4h': 4 * 60 * 60  # 4小时 = 14400秒
}
EXCHANGE_TZ = datetime.timezone(datetime.timedelta(hours=8))  # 定义交易所时区为东八区（北京时间），用于时间戳转换
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_ENV_PATH = os.path.join(BASE_DIR, '.env.local')

print("local_env_path:", LOCAL_ENV_PATH)
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
MARGIN_RATE = 0.7  # 设置每次开仓使用的保证金比例，使用余额的70%
RETRACE_THRESHOLD = 0.6  # 设置利润回撤阈值：利润从最高点回调60%时触发清仓
STOP_WORKING_TYPE = 'MARK_PRICE'  # 服务端条件止损按标记价格触发，避免只看最新成交价带来的偏差
LIQUIDATION_SAFE_BUFFER_RATIO = 0.003  # 止损价和强平价之间至少保留 0.3% 的价格缓冲
ESTIMATED_LIQUIDATION_GUARD_RATIO = 0.8  # 用于开仓前估算强平距离，取 0.8 / 杠杆，故意保守一点

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
    'initial_balance': 0.0, # 记录开仓前的账户USDT总余额
    'open_fee': 0.0,   # 记录开仓时产生的手续费
    'open_order_id': '',  # 记录开仓市价单的订单ID
    'close_order_id': '',  # 记录最近一次平仓市价单的订单ID
    'entry_reason': '', # 记录本次开仓来源
    'shadow_stop_mode': '',       # 记录是否启用了影线收紧止损，如 "4H 长上影收紧止损"
    'liquidation_price': 0.0,  # 记录当前仓位从交易所返回的真实强平价
    'stop_order_id': '',  # 记录服务端 STOP_MARKET 止损单的订单ID，便于后续撤单和替换
    'stop_order_price': 0.0,  # 记录当前服务端止损单对应的触发价格
    'entry_signal_bar_15m': '',   # 记录入场所对应的15M已收盘信号K线时间
    'last_entry_bar_15m': '',     # 最近一次入场所对应的15M已收盘信号K线时间（防止同根K线重复开仓）
    'last_exit_bar_15m': '',      # 最近一次平仓所对应的15M已收盘信号K线时间（防止同根K线平仓后立即重开）
    'last_processed_bar_15m': '', # 最近一次已处理过的15M已收盘信号K线时间（核心去重字段，防止同一根K线重复执行策略逻辑）
    'last_shadow_adjust_bar_15m': ''  # 最近一次因影线调整止损的15M信号时间（防止同根K线重复收紧止损）
}

# 初始化币安合约 API
exchange = ccxt.binance({  # 实例化ccxt的binance对象，用于调用币安API
    'apiKey': API_KEY,  # 传入API公钥
    'secret': SECRET_KEY,  # 传入API私钥
    'options': {'defaultType': 'future'},  # 设置默认交易类型为U本位合约 (future)
    'enableRateLimit': True,  # 开启内置的速率限制功能，防止请求频率过高被封IP
})
exchange.enable_demo_trading(True)  # 开启模拟交易模式（测试网），不会产生真实交易

# 日志配置
logging.basicConfig(  # 配置日志记录的全局基本设置
    level=logging.INFO,  # 设置日志输出级别为INFO，过滤掉DEBUG级别的日志
    format='%(asctime)s - %(levelname)s - %(message)s'  # 设置日志输出格式：时间 - 级别 - 具体信息
)


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
        
        return df  # 返回计算好所有指标的DataFrame
    except Exception as e:  # 捕获获取数据或计算指标过程中的异常
        logging.error(f"获取数据失败 ({timeframe}): {e}")  # 记录错误日志，包含时间周期和具体错误信息
        return None  # 如果发生错误，返回None


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


def get_position_risk(side=None):
    """获取当前合约仓位风险信息，包括强平价和标记价格"""
    try:
        positions = exchange.fetch_positions_risk([SYMBOL])
        for pos in positions:
            info = pos.get('info', {})

            position_amt = float(info.get('positionAmt', 0) or 0)
            if abs(position_amt) <= 1e-12:
                continue
            pos_side = 'long' if position_amt > 0 else 'short'
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


def cancel_protective_stop_order(silent=False):
    """撤销当前记录的服务端止损单，平仓或替换止损时要先撤旧单"""
    stop_order_id = trade_state.get('stop_order_id', '')
    if not stop_order_id:
        return True
    try:
        exchange.cancel_order(stop_order_id, SYMBOL)
        if not silent:
            logging.info(f"已撤销旧服务端止损单: {stop_order_id}")
        return True
    except Exception as e:
        if not silent:
            logging.warning(f"撤销服务端止损单失败({stop_order_id}): {e}")
        return False
    finally:
        trade_state['stop_order_id'] = ''
        trade_state['stop_order_price'] = 0.0


def refresh_protective_stop_order(stop_price):
    """更新服务端止损单：先撤旧单，再按新的止损价重挂"""
    if not trade_state.get('has_position') or not trade_state.get('side'):
        return True
    cancel_protective_stop_order(silent=True)
    try:
        stop_order = place_protective_stop_order(trade_state['side'], stop_price)
        trade_state['stop_order_id'] = extract_order_id(stop_order)
        trade_state['stop_order_price'] = float(stop_price)
        logging.info(f"已更新服务端止损单: id={trade_state['stop_order_id']}, stop={stop_price}")
        return True
    except Exception as e:
        logging.error(f"重挂服务端止损单失败: {e}")
        return False


def reset_trade_state_after_external_close(signal_bar_15m='', reason='检测到交易所仓位已关闭'):
    """当服务端止损或人工操作已把仓位关掉时，重置本地状态，避免下个循环误操作"""
    post_exit_processed_bar_15m = signal_bar_15m or trade_state.get('last_processed_bar_15m', '')
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
        'entry_reason': '',
        'shadow_stop_mode': '',
        'initial_balance': 0.0,
        'open_fee': 0.0,
        'open_order_id': '',
        'close_order_id': '',
        'liquidation_price': 0.0,
        'stop_order_id': '',
        'stop_order_price': 0.0,
        'entry_signal_bar_15m': '',
        'last_exit_bar_15m': signal_bar_15m,
        'last_processed_bar_15m': post_exit_processed_bar_15m,
        'last_shadow_adjust_bar_15m': ''
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
    # a2：收盘靠近低位，说明冲高之后被压回来了
    a2 = safe_ratio(abs(row['close'] - row['low']), full_range) <= 0.35
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
    # b2：收盘靠近高位，说明下探之后又被拉回来了
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


def log_trade_to_csv(entry_time, side, cond_4h, cond_1h, cond_15m, entry_reason, exit_time, close_reason, pnl_points, fee_cost, net_pnl_usdt, is_profit, entry_signal_bar_15m='', exit_signal_bar_15m='', exit_trigger='', holding_seconds=0, open_order_id='', close_order_id=''):
    """将交易记录写入CSV文件，按月分表"""
    # 根据平仓时间生成当月的 CSV 文件名，例如 trades_log_2024-05.csv
    month_str = exit_time[:7]  # 提取 'YYYY-MM' 部分
    filename = f'trades_log_{month_str}.csv'
    
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

    res = {  # 初始化存放各个趋势判断结果的字典
        'pullback_short': False, # 标记是否为回调空头结构，初始为False
        'long_trend': False,     # 标记是否为多头趋势，初始为False
        'pullback_long': False,  # 标记是否为回调多头结构，初始为False
        'short_trend': False,    # 标记是否为空头趋势，初始为False
        'close_short': False,    # 标记是否满足强劲空头平仓条件，初始为False
        'close_long': False,     # 标记是否满足强劲多头平仓条件，初始为False
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
        'upper_shadow_long_protect_ref': None, # 多单因为长上影收紧止损时的新保护位
        'lower_shadow_short_protect_ref': None # 空单因为长下影收紧止损时的新保护位
    }

    if is_4h:  # 如果当前评估的是4小时级别数据，执行以下特定逻辑
        # 震荡过滤：(UP - DN) / DN < 0.04 不开仓 (这里仅计算指标，在run_strategy拦截)
        res['is_oscillation'] = (last['boll_up'] - last['boll_dn']) / last['boll_dn'] < 0.04  # 判断布林带上下轨间距率是否小于0.04，若是则为震荡

        #下面c1,c2应该满足其中之一，c3应该必须满足
        # A. 空头转多头 -> 回调趋势还是空头 (满足任意一个)，
        ps_c1 = (last['open'] < min(last['ema20'], last['ema50']) and  # 条件1：当前开盘价低于EMA20和EMA50的最小值，且...
                 prev['close'] < min(prev['ema20'], prev['ema50']) and last['close'] < last['open'])  # 上一根收盘价也低于EMA极小值，且当前为阴线（收盘<开盘）
        ps_c2 = (last['open'] < last['boll_mid'] and  # 条件2：当前开盘价低于布林中轨，且...
                 prev['close'] < prev['boll_mid'] and last['close'] < last['open'])  # 上一根收盘价也低于布林中轨，且当前为阴线
        ps_c3 = last['volume'] > prev['volume']  # 条件3：当前K线成交量大于上一根K线成交量
        res['pullback_short'] = sum([ps_c1, ps_c2, ps_c3]) >= 2  # 只要上述3个条件满足任意2个，则判定为回调空头结构

        # A. 空头转多头 -> 多头趋势 (满足2个点)，这里其实有点疑问
        lt_c1 = (last['open'] > min(last['ema20'], last['ema50']) and  # 条件1：当前开盘价高于EMA20和EMA50的最小值，且...
                 prev['close'] > min(prev['ema20'], prev['ema50']) and last['close'] > last['open'])  # 上一根收盘价也高于EMA极小值，且当前为阳线（收盘>开盘）
        lt_c2 = (last['open'] > last['boll_mid'] and  # 条件2：当前开盘价高于布林中轨，且...
                 prev['close'] > prev['boll_mid'] and last['close'] > last['open'])  # 上一根收盘价也高于布林中轨，且当前为阳线
        lt_c3 = last['volume'] > prev['volume']  # 条件3：当前成交量大于上一个成交量
        res['long_trend'] = sum([lt_c1, lt_c2, lt_c3]) >= 2  # 上述3个条件中满足至少2个，则判定为多头趋势
        
        # B. 多头转空头 -> 回调趋势还是多头 (满足任意一个，有容错)
        pl_c1 = (is_above(last['open'], min(last['ema20'], last['ema50'])) and  # 条件1：当前开盘价在EMA20/50最小值之上（含容错），且...
                 is_above(prev['close'], min(prev['ema20'], prev['ema50'])) and last['close'] > last['open'])  # 上一根收盘价在EMA极小值之上（含容错），且当前为阳线
        pl_c2 = (is_above(last['open'], last['boll_mid']) and  # 条件2：当前开盘价在布林中轨之上（含容错），且...
                 is_above(prev['close'], prev['boll_mid']) and last['close'] > last['open'])  # 上一根收盘价在布林中轨之上（含容错），且当前为阳线
        pl_c3 = last['volume'] > prev['volume']  # 条件3：当前成交量大于上一根成交量
        res['pullback_long'] = sum([pl_c1, pl_c2, pl_c3]) >= 2  # 满足任意1个条件，即判定为回调多头结构

        # B. 多头转空头 -> 空头趋势 (满足2个点，有容错)
        st_c1 = (is_below(last['open'], max(last['ema20'], last['ema50'])) and  # 条件1：当前开盘价在EMA20/50最大值之下（含容错），且...
                 is_below(prev['close'], max(prev['ema20'], prev['ema50'])) and last['close'] < last['open'])  # 上一根收盘价在EMA极大值之下（含容错），且当前为阴线
        st_c2 = (is_below(last['open'], last['boll_mid']) and  # 条件2：当前开盘价在布林中轨之下（含容错），且...
                 is_below(prev['close'], prev['boll_mid']) and last['close'] < last['open'])  # 上一根收盘价在布林中轨之下（含容错），且当前为阴线
        st_c3 = last['volume'] > prev['volume']  # 条件3：当前成交量大于上一根成交量
        res['short_trend'] = sum([st_c1, st_c2, st_c3]) >= 2  # 满足至少2个条件，即判定为空头趋势

        # 空头平仓 (满足2个条件)
        cs_c1 = last['volume'] < prev['volume']  # 条件1：最新已收盘K线缩量
        cs_c2 = last['rsi'] < 30  # 条件2：最新已收盘K线RSI处于超卖区域
        cs_c3 = last['close'] <= last['boll_dn'] or tol(last['close'], last['boll_dn'])  # 条件3：最新已收盘K线收盘价接近或跌破布林下轨
        res['close_short'] = sum([cs_c1, cs_c2, cs_c3]) >= 2  # 满足至少2个条件，即触发强劲空头平仓信号

        # 多头平仓 (满足2个条件)
        cl_c1 = last['volume'] < prev['volume']  # 条件1：最新已收盘K线缩量
        cl_c2 = last['rsi'] > 75  # 条件2：最新已收盘K线RSI处于超买区域
        cl_c3 = last['close'] >= last['boll_up'] or tol(last['close'], last['boll_up'])  # 条件3：最新已收盘K线收盘价接近或突破布林上轨
        res['close_long'] = sum([cl_c1, cl_c2, cl_c3]) >= 2  # 满足至少2个条件，即触发强劲多头平仓信号

        # 记录 4H 级别的具体条件判断结果
        res['details'] = {
            'ps': [ps_c1, ps_c2, ps_c3],
            'lt': [lt_c1, lt_c2, lt_c3],
            'pl': [pl_c1, pl_c2, pl_c3],
            'st': [st_c1, st_c2, st_c3]
        }

    else:  # 如果当前评估的是15分钟或1小时级别数据，执行以下逻辑
        # 15分钟，1小时级别逻辑  # 注释说明这是针对小级别的数据逻辑

        # 回调趋势还是空头 (满足3个条件)
        ps_c1 = last['volume'] > prev['volume']  # 条件1：当前K线成交量相比上一根有所上升
        ps_c2 = last['rsi'] > 40  # 条件2：当前RSI大于40
        ps_c3 = (prev['close'] < min(prev['ema20'], prev['ema50']) and  # 条件3：上一根收盘价低于EMA20/50的极小值，且...
                 last['open'] < min(last['ema20'], last['ema50']) and prev['close'] < prev['open'])  # 当前开盘价低于EMA极小值，并且上一根是阴线
        ps_c4 = (prev['close'] < prev['boll_mid'] and  # 条件4：上一根收盘价低于布林中轨，且...
                 last['open'] < last['boll_mid'] and prev['close'] < prev['open'])  # 当前开盘价低于布林中轨，并且上一根是阴线
        res['pullback_short'] = sum([ps_c1, ps_c2, ps_c3, ps_c4]) >= 3  # 上述4个条件满足至少3个，判定为回调空头结构

        # 多头趋势 (满足3个条件)
        lt_c1 = last['volume'] > prev['volume']  # 条件1：当前K线成交量相比上一根有所上升
        lt_c2 = last['rsi'] < 60  # 条件2：当前RSI小于60
        lt_c3 = (prev['close'] > min(prev['ema20'], prev['ema50']) and  # 条件3：上一根收盘价高于EMA20/50的极小值，且...
                 last['open'] > min(last['ema20'], last['ema50']) and prev['close'] > prev['open'])  # 当前开盘价高于EMA极小值，并且上一根是阳线
        lt_c4 = (prev['close'] > prev['boll_mid'] and  # 条件4：上一根收盘价高于布林中轨，且...
                 last['open'] > last['boll_mid'] and prev['close'] > prev['open'])  # 当前开盘价高于布林中轨，并且上一根是阳线
        lt_c5 = (last['macd'] > last['macd_signal']) and (abs(last['macd_hist']) > abs(prev['macd_hist']))  # 条件5：MACD为金叉状态（快线>慢线），且当前MACD值比上一个大（动能向上）
        res['long_trend'] = sum([lt_c1, lt_c2, lt_c3, lt_c4, lt_c5]) >= 3  # 上述5个条件满足至少3个，判定为多头趋势

        # 回调趋势还是多头 (满足3个条件)
        pl_c1 = last['volume'] > prev['volume']  # 条件1：当前K线成交量相比上一根有所上升
        pl_c2 = 40 <= last['rsi'] <= 60  # 条件2：当前RSI的值在40到60之间
        pl_c3 = (last['macd'] > last['macd_signal']) and (abs(last['macd_hist']) > abs(prev['macd_hist']))  # 条件3：MACD是金叉，且当前MACD值比上一个大
        pl_c4 = prev['close'] > prev['ema20'] and prev['close'] > prev['ema50']  # 条件4：上一根的收盘价在EMA20和EMA50的上方
        pl_c5 = prev['close'] > prev['boll_mid']  # 条件5：上一根的收盘价在布林带中轨的上方
        res['pullback_long'] = sum([pl_c1, pl_c2, pl_c3, pl_c4, pl_c5]) >= 3  # 满足至少3个条件，判定为回调多头结构

        # 空头趋势 (满足3个条件)
        st_c1 = last['volume'] > prev['volume']  # 条件1：当前K线成交量相比上一根有所上升
        st_c2 = 40 <= last['rsi'] <= 60  # 条件2：当前RSI的值在40到60之间
        st_c3 = (last['macd'] < last['macd_signal']) and (abs(last['macd_hist']) > abs(prev['macd_hist']))  # 条件3：MACD是死叉（快线<慢线），并且当前MACD值大于上一个
        st_c4 = prev['close'] < prev['ema20'] and prev['close'] < prev['ema50']  # 条件4：上一根的收盘价在EMA20和EMA50的下方
        st_c5 = prev['close'] < prev['boll_mid']  # 条件5：上一根的收盘价在布林带中轨的下方
        res['short_trend'] = sum([st_c1, st_c2, st_c3, st_c4, st_c5]) >= 3  # 满足至少3个条件，判定为空头趋势

        # 空头平仓 (满足2个条件)
        cs_c1 = last['volume'] < prev['volume']  # 条件1：最新已收盘K线缩量
        cs_c2 = last['rsi'] < 30  # 条件2：最新已收盘K线RSI小于30（严重超卖）
        cs_c3 = last['close'] <= last['boll_dn'] or tol(last['close'], last['boll_dn'])  # 条件3：最新已收盘K线收盘价接近或跌破布林带下轨
        res['close_short'] = sum([cs_c1, cs_c2, cs_c3]) >= 2  # 满足至少2个条件，触发小级别强劲空头平仓信号

        # 多头平仓 (满足2个条件)
        cl_c1 = last['volume'] < prev['volume']  # 条件1：最新已收盘K线缩量
        cl_c2 = last['rsi'] > 75  # 条件2：最新已收盘K线RSI大于75（严重超买）
        cl_c3 = last['close'] >= last['boll_up'] or tol(last['close'], last['boll_up'])  # 条件3：最新已收盘K线收盘价接近或突破布林带上轨
        res['close_long'] = sum([cl_c1, cl_c2, cl_c3]) >= 2  # 满足至少2个条件，触发小级别强劲多头平仓信号

        # 记录 1H / 15M 级别的具体条件判断结果
        res['details'] = {
            'ps': [ps_c1, ps_c2, ps_c3, ps_c4],
            'lt': [lt_c1, lt_c2, lt_c3, lt_c4, lt_c5],
            'pl': [pl_c1, pl_c2, pl_c3, pl_c4, pl_c5],
            'st': [st_c1, st_c2, st_c3, st_c4, st_c5]
        }

    # 先给影线细节预留一个空字典，只有在 4H / 1H 上才会真正填充内容
    res['details']['shadow'] = {}
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

        # 开空反转：上一根先出现压力区长上影，这一根再用阴线跌破上一根实体低点来确认
        res['upper_shadow_reversal_short'] = (
            prev_upper_shadow and
            prev_upper_resistance and
            last['close'] < min(prev['open'], prev['close']) and
            last['close'] < last['open']
        )
        # 开多反转：上一根先出现支撑区长下影，这一根再用阳线突破上一根实体高点来确认
        res['lower_shadow_reversal_long'] = (
            prev_lower_shadow and
            prev_lower_support and
            last['close'] > max(prev['open'], prev['close']) and
            last['close'] > last['open']
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

        # 如果上一根是长上影线，就预先算出“反转开空”的止损参考位
        if prev_upper_shadow:
            res['shadow_short_stop_ref'] = prev['high'] + 0.1 * prev['atr']
            res['shadow_short_trigger_tf'] = timeframe
        # 如果上一根是长下影线，就预先算出“反转开多”的止损参考位
        if prev_lower_shadow:
            res['shadow_long_stop_ref'] = prev['low'] - 0.1 * prev['atr']
            res['shadow_long_trigger_tf'] = timeframe
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
            'upper_reversal_volume_ok': upper_reversal_volume_ok,
            'lower_reversal_volume_ok': lower_reversal_volume_ok,
            'upper_shadow_filter_long': res['upper_shadow_filter_long'],
            'lower_shadow_filter_short': res['lower_shadow_filter_short'],
            'upper_shadow_reversal_short': res['upper_shadow_reversal_short'],
            'lower_shadow_reversal_long': res['lower_shadow_reversal_long'],
            'upper_shadow_tighten_long': res['upper_shadow_tighten_long'],
            'lower_shadow_tighten_short': res['lower_shadow_tighten_short']
        }

    return res  # 返回包含所有趋势和信号状态的字典

def open_order(side, price, sl_price, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='trend'):  # 定义执行开仓指令的函数，接收方向、当前价格、止损价格和各级别状态
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
            # shadow_desc 里面放的是这个周期的影线细节，写进日志后方便你直接看条件有没有成立
            shadow_desc = state.get('details', {}).get('shadow', {})
            if side_dir == 'long':
                return f"多头:{state['details']['lt']} | 回调多:{state['details']['pl']} | 影线:{shadow_desc}"
            return f"空头:{state['details']['st']} | 回调空:{state['details']['ps']} | 影线:{shadow_desc}"
                
        # 这三段字符串后面会一起写进 trade_state 和 CSV，方便复盘每次进场的上下文
        cond_4h_str = f"原因:{entry_reason} | {format_cond(state_4h, side)}"
        cond_1h_str = f"原因:{entry_reason} | {format_cond(state_1h, side)}"
        cond_15m_str = f"原因:{entry_reason} | {format_cond(state_15m, side)}"
        
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
            'entry_reason': entry_reason,  # 记录这笔单到底是趋势单还是影线反转单
            'shadow_stop_mode': '',  # 开仓时先清空，后面如果发生“影线收紧止损”再写入
            'initial_balance': initial_usdt,
            'open_fee': open_fee,
            'open_order_id': open_order_id,
            'close_order_id': '',
            'liquidation_price': actual_liquidation_price or 0.0,
            'stop_order_id': '',
            'stop_order_price': 0.0,
            'entry_signal_bar_15m': signal_bar_15m,
            'last_entry_bar_15m': signal_bar_15m,
            'last_shadow_adjust_bar_15m': ''  # 新开一笔单后，先把“最近一次影线调止损的15M时间”清空
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
               f"杠杆: {LEVERAGE}x\n入场原因: {entry_reason}\n15M信号时间: {signal_bar_15m}"
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


def monitor_position(state_4h, state_1h, state_15m, atr_1h, signal_bar_15m='', allow_strategy_close=False):  # 定义持仓监控函数，接收三个时间级别的状态评估结果和1H的ATR值
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
            cancel_protective_stop_order(silent=True)
            reset_trade_state_after_external_close(signal_bar_15m=signal_bar_15m, reason="检测到交易所实际已无仓位，可能是服务端止损或人工操作已成交，本地状态已重置")
            return
        trade_state['liquidation_price'] = position_risk.get('liquidation_price') or 0.0

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


             # 2. 利润回调止盈
            max_profit = trade_state['highest_price'] - entry  # 计算开仓以来的最大理论利润（最高价 - 入场价）
            # 只有当最大利润大于 1H ATR 时，才激活利润回撤清仓逻辑
            if max_profit > atr_1h:
                retrace_val = (trade_state['highest_price'] - curr_price) / max_profit  # 计算利润回撤比例：(最高价-当前价)/最大利润
                if retrace_val >= RETRACE_THRESHOLD:  # 判断回撤比例是否达到或超过预设的阈值 (60%)
                    close_position(f"利润回调清仓(曾获利:{max_profit:.2f})", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="利润回撤")  # 如果达到阈值，触发平仓，并记录曾获利点数
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
                                # 更新全局止损价
                                trade_state['stop_loss_price'] = new_sl
                                # 记录这次止损是因为哪个周期的长上影线收紧的
                                trade_state['shadow_stop_mode'] = f"{tighten_tf} 长上影收紧止损"
                                # 记住这根15M已经处理过收紧止损，后面同一根15M不再重复执行
                                trade_state['last_shadow_adjust_bar_15m'] = signal_bar_15m
                                trade_state['stop_order_price'] = new_sl
                                logging.info(f"多单收紧止损: {tighten_tf}, 新止损价={new_sl:.4f}")
                                if not refresh_protective_stop_order(new_sl):
                                    close_position("服务端止损更新失败，主动平仓", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="服务端止损更新失败")
                                    return
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

                # 再看有没有真正的“长上影 + 确认阴线”反转平多信号
                reversal_tf, reversal_state = pick_state_signal(state_4h, state_1h, 'upper_shadow_reversal_short')
                if reversal_state is not None:
                    close_position(f"{reversal_tf} 长上影确认反转平多", curr_price, signal_bar_15m=signal_bar_15m, trigger_label=f"{reversal_tf} 长上影确认反转平多")
                    return

                # 如果影线没有触发平仓，再退回原来的 close_long 逻辑，4H / 1H 各自独立可平仓
                if state_4h.get('close_long'):
                    close_position("4H close_long 策略平仓", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="4H close_long")
                    return
                if state_1h.get('close_long'):
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

             # 2. 利润回调止盈
            max_profit = entry - trade_state['lowest_price']  # 计算开仓以来的最大理论利润（入场价 - 最低价）
            # 只有当最大利润大于 倍的 4H ATR 时，才激活利润回撤清仓逻辑
            if max_profit >  atr_1h:
                retrace_val = (curr_price - trade_state['lowest_price']) / max_profit  # 计算利润回撤比例：(当前价-最低价)/最大利润
                if retrace_val >= RETRACE_THRESHOLD:  # 判断回撤比例是否达到或超过预设的阈值 (60%)
                    close_position(f"利润回调清仓(曾获利:{max_profit:.2f})", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="利润回撤")  # 如果达到阈值，触发平仓，并记录曾获利点数
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
                                # 更新全局止损价
                                trade_state['stop_loss_price'] = new_sl
                                # 记录这次止损是因为哪个周期的长下影线收紧的
                                trade_state['shadow_stop_mode'] = f"{tighten_tf} 长下影收紧止损"
                                # 记住这根15M已经处理过收紧止损，后面同一根15M不再重复执行
                                trade_state['last_shadow_adjust_bar_15m'] = signal_bar_15m
                                trade_state['stop_order_price'] = new_sl
                                logging.info(f"空单收紧止损: {tighten_tf}, 新止损价={new_sl:.4f}")
                                if not refresh_protective_stop_order(new_sl):
                                    close_position("服务端止损更新失败，主动平仓", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="服务端止损更新失败")
                                    return
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

                # 再看有没有真正的“长下影 + 确认阳线”反转平空信号
                reversal_tf, reversal_state = pick_state_signal(state_4h, state_1h, 'lower_shadow_reversal_long')
                if reversal_state is not None:
                    close_position(f"{reversal_tf} 长下影确认反转平空", curr_price, signal_bar_15m=signal_bar_15m, trigger_label=f"{reversal_tf} 长下影确认反转平空")
                    return

                # 如果影线没有触发平仓，再退回原来的 close_short 逻辑，4H / 1H 各自独立可平仓
                if state_4h.get('close_short'):
                    close_position("4H close_short 策略平仓", curr_price, signal_bar_15m=signal_bar_15m, trigger_label="4H close_short")
                    return
                if state_1h.get('close_short'):
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
        order_id_suffix = f"\n{order_id_lines}" if order_id_lines else ''
        msg = (f"🏁 【已平仓】\n原因: {reason}\n入场价: {trade_state['entry_price']}\n"  # 构建平仓通知邮件的文本内容
               f"出场价: {actual_close_price}\n点数盈亏: {pnl_points:.2f}\n手续费: {fee_cost:.4f}\n净利润(USDT): {net_pnl_usdt:.2f}\n15M信号时间: {exit_signal_bar_15m}\n触发来源: {trigger_label or reason}"
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
            'entry_reason': '',
            'shadow_stop_mode': '',
            'initial_balance': 0.0,
            'open_fee': 0.0,
            'open_order_id': '',
            'close_order_id': '',
            'liquidation_price': 0.0,
            'stop_order_id': '',
            'stop_order_price': 0.0,
            'entry_signal_bar_15m': '',
            'last_exit_bar_15m': exit_signal_bar_15m,
            'last_processed_bar_15m': post_exit_processed_bar_15m,
            'last_shadow_adjust_bar_15m': ''
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
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        future_4h = executor.submit(fetch_df, SYMBOL, '4h', 100)
        future_1h = executor.submit(fetch_df, SYMBOL, '1h', 100) # 获取1小时级别的最近100根K线及指标数据
        future_15m = executor.submit(fetch_df, SYMBOL, '15m', 100) # 获取15分钟级别的最近100根K线及指标数据
        df_4h  = future_4h.result()
        df_1h  = future_1h.result()
        df_15m = future_15m.result()

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
        monitor_position(state_4h, state_1h, state_15m, atr_1h, signal_bar_15m=signal_bar_15m, allow_strategy_close=is_new_signal_bar)  # 如果有持仓，进入监控持仓和止盈止损逻辑
        if is_new_signal_bar:
            trade_state['last_processed_bar_15m'] = signal_bar_15m
        return  # 监控结束后直接返回，不需要执行开仓逻辑
    if not is_new_signal_bar:
        return
    # 4. 入场逻辑判断
    # 震荡行情过滤
    if state_4h.get('is_oscillation', False):  # 检查4H级别的评估结果是否标记为震荡行情
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

    if not reversal_conflict and reversal_long_state is not None and long_cond_15m and not upper_shadow_filter_long:
        try:
            # 真正下单时，还是以当前最新成交价作为入场价
            curr_price = get_latest_price()
            # 影线反转单优先使用影线结构给出的止损位
            sl_price = reversal_long_state.get('shadow_long_stop_ref')
            # 如果影线止损位拿不到，就退回 ATR 止损作为兜底
            if sl_price is None or pd.isna(sl_price):
                sl_price = curr_price - atr_4h
            # entry_reason 会被写进状态、通知和 CSV，后面一看就知道是影线反转多单
            open_order('long', curr_price, sl_price, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='shadow_reversal_long')
            logging.info(f"{reversal_long_tf} 长下影确认反转多单开仓成功")
        except Exception:
            logging.error(f"影线反转多单开仓失败:\n{traceback.format_exc()}")
        trade_state['last_processed_bar_15m'] = signal_bar_15m
        return

    if not reversal_conflict and reversal_short_state is not None and short_cond_15m and not lower_shadow_filter_short:
        try:
            # 真正下单时，还是以当前最新成交价作为入场价
            curr_price = get_latest_price()
            # 影线反转单优先使用影线结构给出的止损位
            sl_price = reversal_short_state.get('shadow_short_stop_ref')
            # 如果影线止损位拿不到，就退回 ATR 止损作为兜底
            if sl_price is None or pd.isna(sl_price):
                sl_price = curr_price + atr_4h
            # entry_reason 会被写进状态、通知和 CSV，后面一看就知道是影线反转空单
            open_order('short', curr_price, sl_price, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='shadow_reversal_short')
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
            open_order('long', curr_price, sl_price, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='trend_long')  # 调用开仓函数，执行做多操作
            logging.info("多单开仓成功")
        except Exception:
            logging.error(f"多单开仓失败:\n{traceback.format_exc()}")
        trade_state['last_processed_bar_15m'] = signal_bar_15m
        return  # 开仓后返回结束本次逻辑

    if trend_short_ready:  # 如果三个级别的做空条件同时满足（共振）
        try:
            curr_price = get_latest_price()
            sl_price = curr_price + atr_4h  # 根据公式：入场价 + 1.3 * 1H_ATR，计算空单底线止损价
            open_order('short', curr_price, sl_price, state_4h, state_1h, state_15m, signal_bar_15m, entry_reason='trend_short')  # 调用开仓函数，执行做空操作
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
    current_time_str = get_server_time_str()
    print("网络连通成功！服务器时间:", current_time_str)
    balance_after = exchange.fetch_balance({'type': 'future'})
    final_usdt = float(balance_after['total']['USDT'])
    logging.info(f"🚀 自动化交易策略系统启动，初始金额：{final_usdt}")  # 记录系统成功启动的信息到日志
    #exchange.load_markets()
    while True:  # 开启一个无限循环，让策略持续运行不中断
        
        try:  # 尝试在主循环中运行策略，捕获最高层级的异常
            #positions = exchange.fetch_positions([SYMBOL])
            run_strategy()  # 调用核心策略函数
        except Exception as e:  # 捕获策略运行中未被内部函数捕获的意外崩溃
            print(traceback.format_exc())
            logging.error(f"系统运行报错: {e}")  # 将意外报错记录到日志，防止程序直接闪退
            sys.exit(0)
        # 每1秒执行一次循环
        time.sleep(1)  # 当前循环执行完毕后，挂起休眠1秒，减轻CPU和API请求压力，然后再进行下一次循环
