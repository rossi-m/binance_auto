#!/usr/bin/env python3
"""AI 研报守卫模块：根据外部 AI 生成的研报 bias 文件，控制交易信号的过滤和仓位管理。

核心功能：
  1. 加载并校验 AI 研报 bias JSON 的合法性和时效性
  2. 评估入场候选信号 — 当 AI 明确给出反向信号时，过滤掉同向开仓
  3. 评估持仓减仓 — 当 AI 反向信号或风险系数要求时，计算应减仓数量
  4. 审计日志 — 所有 AI 决策写入月度 JSONL 日志，便于复盘

模式说明（AI_RESEARCH_MODE）：
  - off:           不加载 bias，所有信号放行
  - log:           只记录日志，不做过滤/减仓
  - reduce:        仅启用减仓功能
  - filter:        仅启用信号过滤功能
  - filter_reduce: 同时启用过滤和减仓
  - manage:        同 filter_reduce（全功能）
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

# AI 研报模式：控制模块的行为级别（off=关闭, log=仅记录, reduce=减仓, filter=过滤, filter_reduce=过滤+减仓, manage=全功能）
AI_RESEARCH_MODES = {"off", "log", "reduce", "filter", "filter_reduce", "manage"}
# 投资组合立场：overweight=超配(看多), neutral=中性, underweight=低配(看空)
PORTFOLIO_STANCES = {"overweight", "neutral", "underweight"}
# 期货方向偏差：long=偏多, short=偏空, neutral=中性, mixed=混合
FUTURES_BIASES = {"long", "short", "neutral", "mixed"}


def _env_bool(value: Any, default: bool = False) -> bool:
    """将环境变量值解析为布尔值，支持 1/true/yes/on → True，0/false/no/off → False。"""
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"invalid boolean value: {value!r}")


def _env_float(environ: Mapping[str, str], name: str, default: float) -> float:
    """从环境变量中读取浮点数，校验必须是有限数值。"""
    raw = environ.get(name, str(default))
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return value


def _env_int(environ: Mapping[str, str], name: str, default: int) -> int:
    """从环境变量中读取整数。"""
    raw = environ.get(name, str(default))
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc


@dataclass(frozen=True)
class AiResearchConfig:
    """AI 研报守卫的运行时配置（不可变，从环境变量加载）。"""

    # 运行模式：off/log/reduce/filter/filter_reduce/manage
    mode: str = "off"
    # AI 研报 bias JSON 文件路径
    bias_file: str = "/home/ubuntu/binance/tradingAgents/.ai_research/latest_bias_ETHUSDT.json"
    # 信号过滤的最低置信度阈值（低于此值不拦截）
    filter_min_confidence: float = 0.75
    # 减仓触发的最低置信度阈值
    reduce_min_confidence: float = 0.75
    # 反向信号时保留的仓位比例（1.0=不减仓, 0.0=清仓）
    reverse_signal_retained_position_ratio: float = 0.75
    # 最小保留仓位比例（安全底线，防止减仓过多）
    min_retained_position_ratio: float = 0.75
    # 单次最大减仓比例（例如 0.25 表示单次最多减 25%）
    max_position_reduction: float = 0.25
    # 是否允许减仓
    allow_position_reduction: bool = False
    # 是否允许完全平仓（当前版本不支持）
    allow_full_close: bool = False
    # 是否允许反向开仓（当前版本不支持）
    allow_reverse: bool = False
    # bias 文件生成时间的未来容差（秒），防止时钟不同步导致误判"来自未来的文件"
    future_tolerance_seconds: int = 300

    @property
    def retained_ratio_floor(self) -> float:
        """保留仓位比例的下限 = max(最小保留比例, 1.0 - 最大减仓比例)。"""
        return max(self.min_retained_position_ratio, 1.0 - self.max_position_reduction)


def load_ai_research_config(environ: Mapping[str, str] | None = None) -> AiResearchConfig:
    """从环境变量加载 AI 研报配置，解析所有参数并做合法性校验。"""
    if environ is None:
        environ = os.environ
    mode = str(environ.get("AI_RESEARCH_MODE", "off")).strip().lower()
    if mode not in AI_RESEARCH_MODES:
        raise ValueError(f"AI_RESEARCH_MODE must be one of {sorted(AI_RESEARCH_MODES)}")

    config = AiResearchConfig(
        mode=mode,
        bias_file=str(
            environ.get(
                "AI_BIAS_FILE",
                "/home/ubuntu/binance/tradingAgents/.ai_research/latest_bias_ETHUSDT.json",
            )
        ).strip(),
        filter_min_confidence=_env_float(environ, "AI_FILTER_MIN_CONFIDENCE", 0.75),
        reduce_min_confidence=_env_float(environ, "AI_REDUCE_MIN_CONFIDENCE", 0.75),
        reverse_signal_retained_position_ratio=_env_float(
            environ, "AI_REVERSE_SIGNAL_RETAINED_POSITION_RATIO", 0.75
        ),
        min_retained_position_ratio=_env_float(environ, "AI_MIN_RETAINED_POSITION_RATIO", 0.75),
        max_position_reduction=_env_float(environ, "AI_MAX_POSITION_REDUCTION", 0.25),
        allow_position_reduction=_env_bool(environ.get("AI_ALLOW_POSITION_REDUCTION"), False),
        allow_full_close=_env_bool(environ.get("AI_ALLOW_FULL_CLOSE"), False),
        allow_reverse=_env_bool(environ.get("AI_ALLOW_REVERSE"), False),
        future_tolerance_seconds=_env_int(environ, "AI_BIAS_FUTURE_TOLERANCE_SECONDS", 300),
    )
    # 校验 bias 文件路径非空
    if not config.bias_file:
        raise ValueError("AI_BIAS_FILE must not be empty")
    # 校验所有比例类参数必须在 [0, 1] 区间
    for name, value in (
        ("AI_FILTER_MIN_CONFIDENCE", config.filter_min_confidence),
        ("AI_REDUCE_MIN_CONFIDENCE", config.reduce_min_confidence),
        ("AI_REVERSE_SIGNAL_RETAINED_POSITION_RATIO", config.reverse_signal_retained_position_ratio),
        ("AI_MIN_RETAINED_POSITION_RATIO", config.min_retained_position_ratio),
        ("AI_MAX_POSITION_REDUCTION", config.max_position_reduction),
    ):
        if not 0.0 <= value <= 1.0:
            raise ValueError(f"{name} must be between 0 and 1")
    # 校验未来容差不能为负
    if config.future_tolerance_seconds < 0:
        raise ValueError("AI_BIAS_FUTURE_TOLERANCE_SECONDS must be >= 0")
    # 当前版本不支持反向开仓和完全平仓
    if config.allow_reverse:
        raise ValueError("AI_ALLOW_REVERSE=1 is unsupported; automatic reversal is disabled")
    if config.allow_full_close:
        raise ValueError("AI_ALLOW_FULL_CLOSE=1 is unsupported in the current implementation")
    return config


def config_for_log(config: AiResearchConfig) -> dict[str, Any]:
    """将配置转为可序列化字典（含计算字段 retained_ratio_floor），用于日志输出。"""
    data = asdict(config)
    data["retained_ratio_floor"] = config.retained_ratio_floor
    return data


def _parse_aware_datetime(value: Any, field: str) -> datetime:
    """解析包含时区信息的 ISO 8601 时间字符串。"""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty ISO datetime")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be a valid ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed


def fail_open_guard(error: str, *, mode: str, symbol: str) -> dict[str, Any]:
    """构建「故障放行」结果：当 AI bias 加载失败时，不拦截任何信号（fail-open 策略）。

    所有 allow_* 字段为 True，risk_multiplier=1.0，确保 AI 故障不影响正常交易。
    """
    return {
        "valid": False,
        "fail_open": True,
        "error": error,
        "mode": mode,
        "symbol": symbol,
        "generated_at": "",
        "expires_at": "",
        "portfolio_stance": "neutral",
        "futures_bias": "neutral",
        "time_horizon_hours": 0,
        "explicit_long_signal": False,
        "explicit_short_signal": False,
        "confidence": 0.0,
        "allow_long": True,
        "allow_short": True,
        "risk_multiplier": 1.0,
        "reason": "",
        "risk_events": [],
        "news_used": [],
        "source_counts": {},
    }


def validate_ai_bias(
    raw: Any,
    config: AiResearchConfig,
    *,
    expected_symbol: str = "ETHUSDT",
    now: datetime | None = None,
) -> dict[str, Any]:
    """校验 AI 研报 bias 数据的完整性和时效性，返回标准化 guard 结果字典。

    校验项：
      1. 根必须为 dict，symbol 必须匹配
      2. generated_at / expires_at 必须为带时区的 ISO 时间
      3. generated_at 不能晚于 expires_at，且不能是"未来"时间
      4. expires_at 不能已过期
      5. 各字段类型和范围校验（portfolio_stance、futures_bias、confidence 等）
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must include a timezone")
    # 根类型和符号校验
    if not isinstance(raw, dict):
        raise ValueError("AI bias root must be an object")
    if raw.get("symbol") != expected_symbol:
        raise ValueError(f"symbol must equal {expected_symbol}")

    # 解析并校验时间字段
    generated_at = _parse_aware_datetime(raw.get("generated_at"), "generated_at")
    expires_at = _parse_aware_datetime(raw.get("expires_at"), "expires_at")
    if generated_at > expires_at:
        raise ValueError("generated_at must not be after expires_at")
    if (generated_at - now).total_seconds() > config.future_tolerance_seconds:
        raise ValueError("generated_at is too far in the future")
    if now >= expires_at:
        raise ValueError("AI bias is expired")

    # 校验立场和偏差字段
    portfolio_stance = raw.get("portfolio_stance")
    futures_bias = raw.get("futures_bias")
    if portfolio_stance not in PORTFOLIO_STANCES:
        raise ValueError(f"portfolio_stance must be one of {sorted(PORTFOLIO_STANCES)}")
    if futures_bias not in FUTURES_BIASES:
        raise ValueError(f"futures_bias must be one of {sorted(FUTURES_BIASES)}")

    # 校验时间范围（正整数）
    time_horizon_hours = raw.get("time_horizon_hours")
    if isinstance(time_horizon_hours, bool) or not isinstance(time_horizon_hours, int) or time_horizon_hours <= 0:
        raise ValueError("time_horizon_hours must be a positive integer")
    # 校验布尔字段
    for field in ("explicit_long_signal", "explicit_short_signal", "allow_long", "allow_short"):
        if not isinstance(raw.get(field), bool):
            raise ValueError(f"{field} must be boolean")

    # 校验 confidence / risk_multiplier 在 [0, 1] 区间
    confidence = raw.get("confidence")
    risk_multiplier = raw.get("risk_multiplier")
    for field, value in (("confidence", confidence), ("risk_multiplier", risk_multiplier)):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{field} must be numeric")
        if not math.isfinite(float(value)) or not 0.0 <= float(value) <= 1.0:
            raise ValueError(f"{field} must be between 0 and 1")

    # 校验数组和字典字段
    for field in ("risk_events", "news_used"):
        if not isinstance(raw.get(field, []), list):
            raise ValueError(f"{field} must be an array")
    if not isinstance(raw.get("source_counts", {}), dict):
        raise ValueError("source_counts must be an object")

    return {
        "valid": True,
        "fail_open": False,
        "error": "",
        "mode": config.mode,
        "symbol": expected_symbol,
        "generated_at": generated_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "portfolio_stance": portfolio_stance,
        "futures_bias": futures_bias,
        "time_horizon_hours": time_horizon_hours,
        "explicit_long_signal": raw["explicit_long_signal"],
        "explicit_short_signal": raw["explicit_short_signal"],
        "confidence": float(confidence),
        "allow_long": raw["allow_long"],
        "allow_short": raw["allow_short"],
        "risk_multiplier": float(risk_multiplier),
        "reason": str(raw.get("reason", ""))[:800],  # 截断过长文本
        "risk_events": raw.get("risk_events", [])[:10],      # 最多保留 10 条
        "news_used": raw.get("news_used", [])[:20],           # 最多保留 20 条
        "source_counts": raw.get("source_counts", {}),
    }


def load_ai_research_guard(
    config: AiResearchConfig,
    *,
    expected_symbol: str = "ETHUSDT",
    now: datetime | None = None,
) -> dict[str, Any]:
    """加载 AI 研报 bias 文件并进行校验。模式为 off 时直接返回 fail_open 结果。"""
    if config.mode == "off":
        return fail_open_guard("AI research mode is off", mode=config.mode, symbol=expected_symbol)
    try:
        raw = json.loads(Path(config.bias_file).read_text(encoding="utf-8"))
        return validate_ai_bias(raw, config, expected_symbol=expected_symbol, now=now)
    except Exception as exc:
        # 任何异常都 fail-open：不拦截信号，确保 AI 故障不影响交易
        return fail_open_guard(f"{type(exc).__name__}: {exc}", mode=config.mode, symbol=expected_symbol)


def opposite_explicit_signal(position_side: str, guard: Mapping[str, Any]) -> bool:
    """判断当前持仓方向是否与 AI 研报的明确反向信号冲突。

    做多仓位 + futures_bias=short + explicit_short_signal=True → 冲突
    做空仓位 + futures_bias=long + explicit_long_signal=True → 冲突
    """
    return bool(
        (position_side == "long" and guard.get("futures_bias") == "short" and guard.get("explicit_short_signal") is True)
        or (position_side == "short" and guard.get("futures_bias") == "long" and guard.get("explicit_long_signal") is True)
    )


def evaluate_entry_candidate(
    candidate_side: str,
    guard: Mapping[str, Any],
    config: AiResearchConfig,
) -> dict[str, Any]:
    """评估入场候选信号是否被 AI 研报拦截。

    硬过滤条件（hard_filter）：
      - AI 对未来方向的 bias 与候选人方向相反
      - AI 明确给出了反向的 explicit_*_signal
      - AI 禁止了该方向的交易（allow_*=False）
      - AI 置信度高于 filter_min_confidence 阈值

    只有 mode 为 filter/filter_reduce/manage 时才实际拦截，log 模式只记录不拦截。
    """
    if candidate_side not in {"long", "short"}:
        raise ValueError("candidate_side must be long or short")
    hard_filter = False
    # AI bias 有效且置信度高于阈值时，检查是否触发硬过滤
    if guard.get("valid") and float(guard.get("confidence", 0.0)) >= config.filter_min_confidence:
        if candidate_side == "long":
            # 做多被拦截：AI 看空 + 明确做空信号 + 禁止做多
            hard_filter = bool(
                guard.get("futures_bias") == "short"
                and guard.get("explicit_short_signal") is True
                and guard.get("allow_long") is False
            )
        else:
            # 做空被拦截：AI 看多 + 明确做多信号 + 禁止做空
            hard_filter = bool(
                guard.get("futures_bias") == "long"
                and guard.get("explicit_long_signal") is True
                and guard.get("allow_short") is False
            )
    # 仅在 filter/filter_reduce/manage 模式下实际拦截
    filter_enabled = config.mode in {"filter", "filter_reduce", "manage"}
    allowed = not (hard_filter and filter_enabled)
    return {
        "allowed": allowed,
        "actual_action": "filter" if not allowed else ("log_only" if config.mode == "log" else "allow"),
        "shadow_action": "would_filter" if hard_filter else "would_allow",  # 影子动作：不受 mode 影响
        "reason": "explicit opposite AI signal" if hard_filter else "AI did not block candidate",
    }


def evaluate_position_reduction(
    position_side: str,
    initial_amount: float,
    current_amount: float,
    guard: Mapping[str, Any],
    config: AiResearchConfig,
    *,
    last_reduce_generated_at: str = "",
) -> dict[str, Any]:
    """评估是否需要对当前持仓进行减仓。

    两种触发场景：
      1. opposite_explicit_signal — AI 给出明确反向信号
      2. risk_multiplier < current_ratio — AI 风险系数要求降低仓位

    安全约束：
      - 减仓后仓位不低于 retained_ratio_floor
      - 同一 generated_at 不重复减仓（防止同一条 bias 反复触发）
      - 只在 reduce/filter_reduce/manage 模式下生效
    """
    if position_side not in {"long", "short"}:
        raise ValueError("position_side must be long or short")
    initial_amount = float(initial_amount)
    current_amount = float(current_amount)
    if initial_amount <= 0 or current_amount <= 0:
        raise ValueError("position amounts must be positive")

    # 当前仓位占初始仓位的比例
    current_ratio = min(1.0, current_amount / initial_amount)
    confidence = float(guard.get("confidence", 0.0)) if guard.get("valid") else 0.0
    opposite = opposite_explicit_signal(position_side, guard)  # AI 是否给出明确反向信号
    risk_multiplier = float(guard.get("risk_multiplier", 1.0)) if guard.get("valid") else 1.0
    # risk_multiplier < current_ratio 表示 AI 认为当前仓位过重
    risk_requests_reduction = risk_multiplier < current_ratio - 1e-12
    threshold_met = confidence >= config.reduce_min_confidence  # 置信度达标
    triggered = bool(guard.get("valid") and threshold_met and (opposite or risk_requests_reduction))

    # 计算目标仓位比例：优先取 AI 建议的 risk_multiplier，反向信号可进一步降低
    requested_ratio = risk_multiplier
    trigger = "none"
    if opposite:
        requested_ratio = min(requested_ratio, config.reverse_signal_retained_position_ratio)
        trigger = "opposite_explicit_signal"
    elif risk_requests_reduction:
        trigger = "risk_multiplier"
    # 目标比例不能低于安全底线
    target_ratio = max(config.retained_ratio_floor, min(1.0, requested_ratio))
    target_amount = initial_amount * target_ratio
    reduce_amount = max(0.0, current_amount - target_amount) if triggered else 0.0

    # 去重：同一个 generated_at 已触发过减仓则跳过
    duplicate = bool(
        reduce_amount > 0
        and guard.get("generated_at")
        and guard.get("generated_at") == last_reduce_generated_at
    )
    reduction_enabled = config.mode in {"reduce", "filter_reduce", "manage"} and config.allow_position_reduction
    should_reduce = bool(reduce_amount > 0 and not duplicate and reduction_enabled)
    if duplicate:
        shadow_action = "duplicate_ignored"
    elif reduce_amount > 0:
        shadow_action = "would_reduce"
    else:
        shadow_action = "would_hold"
    return {
        "should_reduce": should_reduce,
        "actual_action": "reduce" if should_reduce else ("log_only" if config.mode == "log" else "hold"),
        "shadow_action": shadow_action,
        "trigger": trigger,
        "opposite_explicit_signal": opposite,
        "current_ratio": current_ratio,
        "target_ratio": target_ratio,
        "target_amount": target_amount,
        "reduce_amount": reduce_amount,
        "duplicate": duplicate,
        "reason": trigger if triggered else "reduction conditions not met",
    }


def audit_log_path(config: AiResearchConfig, observed_at: datetime | None = None) -> Path:
    """生成月度审计日志文件路径：{bias目录}/logs/ai_research_decisions_{YYYY-MM}.jsonl。"""
    observed_at = observed_at or datetime.now().astimezone()
    return Path(config.bias_file).parent / "logs" / f"ai_research_decisions_{observed_at:%Y-%m}.jsonl"


def append_ai_audit_event(
    config: AiResearchConfig,
    event: Mapping[str, Any],
    *,
    observed_at: datetime | None = None,
) -> Path | None:
    """将 AI 决策事件追加写入月度 JSONL 审计日志。

    每条日志包含：观察时间、guard 结果、决策动作、原因等，用于复盘 AI 研报的实际影响。
    """
    if config.mode == "off":
        return None
    observed_at = observed_at or datetime.now().astimezone()
    path = audit_log_path(config, observed_at)
    path.parent.mkdir(parents=True, exist_ok=True)
    event_data = dict(event)
    # 提取追溯原因：优先 guard error，其次 event reason，再次 guard reason
    guard = event_data.get("guard") if isinstance(event_data.get("guard"), Mapping) else {}
    source_generated_at = str(guard.get("generated_at", "") or "").strip()
    if guard and guard.get("valid") is False and guard.get("error"):
        trace_reason = str(guard.get("error"))
    else:
        trace_reason = str(
            event_data.get("reason")
            or guard.get("reason")
            or guard.get("error")
            or event_data.get("event")
            or "AI decision"
        )
    # 补全时间戳字段
    event_data["generated_at"] = str(
        event_data.get("generated_at") or source_generated_at or observed_at.isoformat()
    )
    event_data["reason"] = trace_reason
    event_data["source_generated_at"] = source_generated_at
    event_data["generated_at_source"] = "ai_bias" if source_generated_at else "decision_time_fallback"
    payload = {"observed_at": observed_at.isoformat(), **event_data}
    # 原子写入：追加一行 JSON + flush + fsync
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, default=str, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return path
