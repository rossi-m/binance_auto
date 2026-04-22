"""
ETH 自动交易策略 Web 监控台 - Flask 后端
管理策略子进程、日志流、CSV 数据、以及邮件验证控制。
"""

import os
import sys
import csv
import time
import json
import random
import signal
import atexit
import datetime
import threading
import subprocess
import smtplib
from pathlib import Path
from email.mime.text import MIMEText
from email.header import Header
from collections import defaultdict
from flask import Flask, render_template, jsonify, request, Response

# ---------- 路径配置 ----------
# 本文件位于 trading_web/ 目录下，策略文件在项目根目录
WEB_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(WEB_DIR)

LOCAL_ENV_PATH = os.path.join(PROJECT_ROOT, '.env.local')
LOG_FILE = os.path.join(WEB_DIR, 'strategy_output.log')
STRATEGY_SCRIPT = os.path.join(PROJECT_ROOT, 'bian_auto.py')

# ---------- 环境变量加载 ----------

def load_local_env(env_path=LOCAL_ENV_PATH):
    if not os.path.exists(env_path):
        return
    with open(env_path, 'r', encoding='utf-8') as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, value = line.split('=', 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)

load_local_env()

SMTP_HOST = os.getenv('SMTP_HOST', 'smtp.qq.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '465'))
EMAIL_SENDER = os.getenv('EMAIL_SENDER', '').strip()
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD', '').strip()
EMAIL_RECEIVER = os.getenv('EMAIL_RECEIVER', EMAIL_SENDER).strip()

# ---------- 全局状态 ----------

app = Flask(__name__, template_folder='templates', static_folder='static')

strategy_proc = None          # subprocess.Popen 实例
strategy_lock = threading.Lock()
log_offset = 0                # 已从 LOG_FILE 读取的字节数
log_lock = threading.Lock()

verification_state = {        # 邮件验证码状态
    'code': None,
    'expires_at': 0,
    'lock': threading.Lock()
}

start_verification_state = {   # 启动策略邮件验证码状态
    'code': None,
    'expires_at': 0,
    'lock': threading.Lock()
}

# ---------- 工具函数 ----------

def get_current_csv_path():
    now = datetime.datetime.now()
    month_str = now.strftime('%Y-%m')
    return os.path.join(PROJECT_ROOT, f'trades_log_{month_str}.csv')

def list_all_csv_files():
    files = []
    for fname in os.listdir(PROJECT_ROOT):
        if fname.startswith('trades_log_') and fname.endswith('.csv'):
            files.append(os.path.join(PROJECT_ROOT, fname))
    return sorted(files)

def read_csv_rows(filepath):
    rows = []
    if not os.path.exists(filepath):
        return rows
    try:
        with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
    except Exception as e:
        print(f"读取 CSV 出错: {e}")
    return rows

def parse_pnl(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

def compute_stats():
    all_csv = list_all_csv_files()
    all_rows = []
    for fp in all_csv:
        all_rows.extend(read_csv_rows(fp))

    today_str = datetime.datetime.now().strftime('%Y-%m-%d')
    today_pnl = 0.0
    total_pnl = 0.0
    daily_map = defaultdict(float)

    for row in all_rows:
        pnl = parse_pnl(row.get('净利润(USDT)', '0'))
        total_pnl += pnl
        exit_time = row.get('平仓时间', '')
        if exit_time and len(exit_time) >= 10:
            day = exit_time[:10]
            daily_map[day] += pnl
            if day == today_str:
                today_pnl += pnl

    # 构建本月折线图数据
    current_month_prefix = datetime.datetime.now().strftime('%Y-%m')
    chart_days = sorted([d for d in daily_map if d.startswith(current_month_prefix)])
    daily_chart = [{'date': d, 'pnl': round(daily_map[d], 2)} for d in chart_days]

    return {
        'today_pnl': round(today_pnl, 2),
        'total_pnl': round(total_pnl, 2),
        'trade_count': len(all_rows),
        'today_trade_count': sum(
            1 for r in all_rows
            if r.get('平仓时间', '').startswith(today_str)
        ),
        'daily_chart': daily_chart
    }

def send_verification_email(code, subject='ETH策略暂停验证码'):
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        return False, '邮件未配置'
    try:
        msg = MIMEText(f'验证码: {code}\n有效期5分钟，请勿泄露。', 'plain', 'utf-8')
        msg['From'] = EMAIL_SENDER
        msg['To'] = EMAIL_RECEIVER
        msg['Subject'] = Header(subject, 'utf-8')
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as smtp:
            smtp.login(EMAIL_SENDER, EMAIL_PASSWORD)
            smtp.sendmail(EMAIL_SENDER, [EMAIL_RECEIVER], msg.as_string())
        return True, ''
    except Exception as e:
        return False, str(e)

# ---------- 进程管理 ----------

def _reader_thread(pipe, prefix):
    """从子进程管道读取并写入日志文件。"""
    try:
        for line in iter(pipe.readline, b''):
            text = line.decode('utf-8', errors='replace')
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(LOG_FILE, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] {prefix}: {text}")
    except Exception as e:
        print(f"读取线程出错 ({prefix}): {e}")
    finally:
        pipe.close()

def start_strategy():
    global strategy_proc, log_offset
    with strategy_lock:
        if strategy_proc is not None and strategy_proc.poll() is None:
            return False, '策略已在运行'

        # 重置日志偏移量
        with log_lock:
            log_offset = 0

        # 确保日志文件存在
        Path(LOG_FILE).touch()

        try:
            strategy_proc = subprocess.Popen(
                ['/home/ubuntu/.local/bin/python', STRATEGY_SCRIPT],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=PROJECT_ROOT,
                bufsize=1
            )
            # 启动读取线程
            threading.Thread(target=_reader_thread, args=(strategy_proc.stdout, 'OUT'), daemon=True).start()
            threading.Thread(target=_reader_thread, args=(strategy_proc.stderr, 'ERR'), daemon=True).start()
            return True, ''
        except Exception as e:
            strategy_proc = None
            return False, str(e)

def stop_strategy():
    global strategy_proc
    with strategy_lock:
        if strategy_proc is None:
            return False, '策略未运行'
        try:
            strategy_proc.terminate()
            try:
                strategy_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                strategy_proc.kill()
                strategy_proc.wait(timeout=5)
            strategy_proc = None
            return True, ''
        except Exception as e:
            return False, str(e)

def _cleanup_child():
    """父进程退出时强制杀掉策略子进程，防止孤儿进程。"""
    global strategy_proc
    if strategy_proc is not None and strategy_proc.poll() is None:
        print(f"[清理] 正在终止策略子进程 PID={strategy_proc.pid} ...")
        try:
            strategy_proc.kill()
            strategy_proc.wait(timeout=5)
        except Exception:
            pass
        strategy_proc = None

atexit.register(_cleanup_child)

def _signal_handler(signum, frame):
    """收到 SIGTERM/SIGINT 时先清理子进程再退出。"""
    _cleanup_child()
    sys.exit(0)

signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)

# ---------- 路由 ----------

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status')
def api_status():
    running = False
    pid = None
    with strategy_lock:
        if strategy_proc is not None and strategy_proc.poll() is None:
            running = True
            pid = strategy_proc.pid
    return jsonify({'running': running, 'pid': pid})

@app.route('/api/log-content')
def api_log_content():
    """读取日志文件最后N行，直接返回给前端展示。"""
    max_lines = int(request.args.get('lines', 15))
    try:
        if not os.path.exists(LOG_FILE):
            return jsonify({'lines': []})
        with open(LOG_FILE, 'r', encoding='utf-8', errors='replace') as f:
            all_lines = f.read().splitlines()
        # 取最后 max_lines 行
        recent = all_lines[-max_lines:] if len(all_lines) > max_lines else all_lines
        return jsonify({'lines': recent})
    except Exception as e:
        return jsonify({'lines': [f'[读取日志出错: {e}]']})

@app.route('/api/logs')
def api_logs():
    def event_stream():
        global log_offset
        while True:
            try:
                if not os.path.exists(LOG_FILE):
                    time.sleep(1)
                    continue
                current_size = os.path.getsize(LOG_FILE)
                with log_lock:
                    start = log_offset
                    if current_size < start:
                        start = 0
                    if current_size > start:
                        with open(LOG_FILE, 'r', encoding='utf-8', errors='replace') as f:
                            f.seek(start)
                            chunk = f.read(current_size - start)
                            log_offset = current_size
                        if chunk:
                            for line in chunk.splitlines():
                                yield f"data: {line}\n\n"
                time.sleep(1)
            except Exception as e:
                yield f"data: [日志流错误: {e}]\n\n"
                time.sleep(2)

    return Response(event_stream(), mimetype='text/event-stream')

@app.route('/api/trades')
def api_trades():
    filepath = get_current_csv_path()
    rows = read_csv_rows(filepath)
    # 最新在前
    rows.reverse()
    return jsonify({
        'trades': rows,
        'file': os.path.basename(filepath),
        'updated_at': datetime.datetime.now().isoformat()
    })

@app.route('/api/stats')
def api_stats():
    return jsonify(compute_stats())

@app.route('/api/start-request', methods=['POST'])
def api_start_request():
    with strategy_lock:
        if strategy_proc is not None and strategy_proc.poll() is None:
            return jsonify({'success': False, 'error': '策略已在运行'}), 400

    code = ''.join(random.choices('0123456789', k=6))
    with start_verification_state['lock']:
        start_verification_state['code'] = code
        start_verification_state['expires_at'] = time.time() + 300  # 5分钟

    ok, err = send_verification_email(code, 'ETH策略启动验证码')
    if ok:
        return jsonify({'success': True, 'message': '验证码已发送至邮箱'})
    return jsonify({'success': False, 'error': f'发送邮件失败: {err}'}), 500

@app.route('/api/start-verify', methods=['POST'])
def api_start_verify():
    data = request.get_json(force=True, silent=True) or {}
    user_code = str(data.get('code', '')).strip()

    with start_verification_state['lock']:
        expected = start_verification_state['code']
        expires = start_verification_state['expires_at']
        if expected is None:
            return jsonify({'success': False, 'error': '没有待验证的启动请求'}), 400
        if time.time() > expires:
            start_verification_state['code'] = None
            return jsonify({'success': False, 'error': '验证码已过期'}), 400
        if user_code != expected:
            return jsonify({'success': False, 'error': '验证码错误'}), 400
        start_verification_state['code'] = None

    ok, msg = start_strategy()
    if ok:
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': msg}), 400

@app.route('/api/pause-request', methods=['POST'])
def api_pause_request():
    with strategy_lock:
        if strategy_proc is None or strategy_proc.poll() is not None:
            return jsonify({'success': False, 'error': '策略未运行'}), 400

    code = ''.join(random.choices('0123456789', k=6))
    with verification_state['lock']:
        verification_state['code'] = code
        verification_state['expires_at'] = time.time() + 300  # 5分钟

    ok, err = send_verification_email(code, 'ETH策略暂停验证码')
    if ok:
        return jsonify({'success': True, 'message': '验证码已发送至邮箱'})
    return jsonify({'success': False, 'error': f'发送邮件失败: {err}'}), 500

@app.route('/api/pause-verify', methods=['POST'])
def api_pause_verify():
    data = request.get_json(force=True, silent=True) or {}
    user_code = str(data.get('code', '')).strip()

    with verification_state['lock']:
        expected = verification_state['code']
        expires = verification_state['expires_at']
        if expected is None:
            return jsonify({'success': False, 'error': '没有待验证的暂停请求'}), 400
        if time.time() > expires:
            verification_state['code'] = None
            return jsonify({'success': False, 'error': '验证码已过期'}), 400
        if user_code != expected:
            return jsonify({'success': False, 'error': '验证码错误'}), 400
        verification_state['code'] = None

    ok, msg = stop_strategy()
    if ok:
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': msg}), 500

# ---------- 入口 ----------

if __name__ == '__main__':
    # 启动前清理旧日志文件
    for _f in ['nohup.out', LOG_FILE]:
        if os.path.exists(_f):
            os.remove(_f)
            print(f"已删除: {_f}")

    Path(LOG_FILE).touch()
    print(f"监控台启动: http://0.0.0.0:5000")
    print(f"策略脚本: {STRATEGY_SCRIPT}")
    app.run(host='0.0.0.0', port=5000, threaded=True)
