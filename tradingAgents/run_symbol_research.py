#!/usr/bin/env python3
"""为一个手动持仓交易对生成 TradingAgents 报告和独立 bias 文件。"""

from __future__ import annotations

import argparse
import fcntl
import os
import re
import subprocess
import sys
from pathlib import Path


def compact_symbol(symbol: str) -> str:
    # CCXT 永续格式 BTC/USDT:USDT 的冒号后部分是结算币，不能重复拼进交易对。
    raw_symbol = str(symbol or "").upper().split(":", 1)[0]
    compact = re.sub(r"[^A-Z0-9]", "", raw_symbol)
    if compact.endswith("USD") and not compact.endswith("USDT"):
        compact = compact[:-3] + "USDT"
    if compact and not compact.endswith("USDT"):
        compact += "USDT"
    return compact


def tradingagents_ticker(symbol: str) -> str:
    compact = compact_symbol(symbol)
    base = compact[:-4] if compact.endswith("USDT") else compact
    return f"{base}-USD"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TradingAgents research for one Binance USD-M symbol.")
    parser.add_argument("--symbol", required=True, help="Binance U本位交易对，例如 BTCUSDT。")
    parser.add_argument("--hours", type=int, default=9, help="bias有效期和新闻回看小时数。")
    parser.add_argument("--research-dir", default=".ai_research")
    parser.add_argument("--outputs-dir", default="outputs")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbol = compact_symbol(args.symbol)
    if not symbol or symbol == "USDT":
        raise SystemExit(f"无效交易对: {args.symbol}")

    script_dir = Path(__file__).resolve().parent
    research_dir = (script_dir / args.research_dir).resolve()
    outputs_dir = (script_dir / args.outputs_dir).resolve()
    lock_dir = research_dir / "jobs"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{symbol}.lock"

    # flock 随进程退出自动释放；即使分析异常，也不会留下永久死锁文件。
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(f"{symbol} 的 TradingAgents 任务已在运行，本次跳过。")
            return 0

        lock_handle.seek(0)
        lock_handle.truncate()
        lock_handle.write(f"pid={os.getpid()}\n")
        lock_handle.flush()

        command = [
            sys.executable,
            str(script_dir / "ai_market_bias.py"),
            "--symbol", symbol,
            "--tradingagents-ticker", tradingagents_ticker(symbol),
            "--hours", str(max(1, args.hours)),
            "--research-dir", str(research_dir),
            "--outputs-dir", str(outputs_dir),
            "--run-tradingagents",
            "--asset-type", "crypto",
        ]
        print("启动逐交易对 TradingAgents 研究:", " ".join(command))
        completed = subprocess.run(command, cwd=script_dir, check=False)
        return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
