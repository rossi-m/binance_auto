#!/usr/bin/env python3
"""Pure AI research guard logic shared by the trading bot and tests."""

from __future__ import annotations

import json
import math
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


AI_RESEARCH_MODES = {"off", "log", "reduce", "filter", "filter_reduce", "manage"}
PORTFOLIO_STANCES = {"overweight", "neutral", "underweight"}
FUTURES_BIASES = {"long", "short", "neutral", "mixed"}


def _env_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"invalid boolean value: {value!r}")


def _env_float(environ: Mapping[str, str], name: str, default: float) -> float:
    raw = environ.get(name, str(default))
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return value


def _env_int(environ: Mapping[str, str], name: str, default: int) -> int:
    raw = environ.get(name, str(default))
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc


@dataclass(frozen=True)
class AiResearchConfig:
    mode: str = "off"
    bias_file: str = "/home/ubuntu/binance/tradingAgents/.ai_research/latest_bias_ETHUSDT.json"
    filter_min_confidence: float = 0.75
    reduce_min_confidence: float = 0.75
    reverse_signal_retained_position_ratio: float = 0.75
    min_retained_position_ratio: float = 0.75
    max_position_reduction: float = 0.25
    allow_position_reduction: bool = False
    allow_full_close: bool = False
    allow_reverse: bool = False
    future_tolerance_seconds: int = 300

    @property
    def retained_ratio_floor(self) -> float:
        return max(self.min_retained_position_ratio, 1.0 - self.max_position_reduction)


def load_ai_research_config(environ: Mapping[str, str] | None = None) -> AiResearchConfig:
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
    if not config.bias_file:
        raise ValueError("AI_BIAS_FILE must not be empty")
    for name, value in (
        ("AI_FILTER_MIN_CONFIDENCE", config.filter_min_confidence),
        ("AI_REDUCE_MIN_CONFIDENCE", config.reduce_min_confidence),
        ("AI_REVERSE_SIGNAL_RETAINED_POSITION_RATIO", config.reverse_signal_retained_position_ratio),
        ("AI_MIN_RETAINED_POSITION_RATIO", config.min_retained_position_ratio),
        ("AI_MAX_POSITION_REDUCTION", config.max_position_reduction),
    ):
        if not 0.0 <= value <= 1.0:
            raise ValueError(f"{name} must be between 0 and 1")
    if config.future_tolerance_seconds < 0:
        raise ValueError("AI_BIAS_FUTURE_TOLERANCE_SECONDS must be >= 0")
    if config.allow_reverse:
        raise ValueError("AI_ALLOW_REVERSE=1 is unsupported; automatic reversal is disabled")
    if config.allow_full_close:
        raise ValueError("AI_ALLOW_FULL_CLOSE=1 is unsupported in the current implementation")
    return config


def config_for_log(config: AiResearchConfig) -> dict[str, Any]:
    data = asdict(config)
    data["retained_ratio_floor"] = config.retained_ratio_floor
    return data


def _parse_aware_datetime(value: Any, field: str) -> datetime:
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
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must include a timezone")
    if not isinstance(raw, dict):
        raise ValueError("AI bias root must be an object")
    if raw.get("symbol") != expected_symbol:
        raise ValueError(f"symbol must equal {expected_symbol}")

    generated_at = _parse_aware_datetime(raw.get("generated_at"), "generated_at")
    expires_at = _parse_aware_datetime(raw.get("expires_at"), "expires_at")
    if generated_at > expires_at:
        raise ValueError("generated_at must not be after expires_at")
    if (generated_at - now).total_seconds() > config.future_tolerance_seconds:
        raise ValueError("generated_at is too far in the future")
    if now >= expires_at:
        raise ValueError("AI bias is expired")

    portfolio_stance = raw.get("portfolio_stance")
    futures_bias = raw.get("futures_bias")
    if portfolio_stance not in PORTFOLIO_STANCES:
        raise ValueError(f"portfolio_stance must be one of {sorted(PORTFOLIO_STANCES)}")
    if futures_bias not in FUTURES_BIASES:
        raise ValueError(f"futures_bias must be one of {sorted(FUTURES_BIASES)}")

    time_horizon_hours = raw.get("time_horizon_hours")
    if isinstance(time_horizon_hours, bool) or not isinstance(time_horizon_hours, int) or time_horizon_hours <= 0:
        raise ValueError("time_horizon_hours must be a positive integer")
    for field in ("explicit_long_signal", "explicit_short_signal", "allow_long", "allow_short"):
        if not isinstance(raw.get(field), bool):
            raise ValueError(f"{field} must be boolean")

    confidence = raw.get("confidence")
    risk_multiplier = raw.get("risk_multiplier")
    for field, value in (("confidence", confidence), ("risk_multiplier", risk_multiplier)):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{field} must be numeric")
        if not math.isfinite(float(value)) or not 0.0 <= float(value) <= 1.0:
            raise ValueError(f"{field} must be between 0 and 1")

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
        "reason": str(raw.get("reason", ""))[:800],
        "risk_events": raw.get("risk_events", [])[:10],
        "news_used": raw.get("news_used", [])[:20],
        "source_counts": raw.get("source_counts", {}),
    }


def load_ai_research_guard(
    config: AiResearchConfig,
    *,
    expected_symbol: str = "ETHUSDT",
    now: datetime | None = None,
) -> dict[str, Any]:
    if config.mode == "off":
        return fail_open_guard("AI research mode is off", mode=config.mode, symbol=expected_symbol)
    try:
        raw = json.loads(Path(config.bias_file).read_text(encoding="utf-8"))
        return validate_ai_bias(raw, config, expected_symbol=expected_symbol, now=now)
    except Exception as exc:
        return fail_open_guard(f"{type(exc).__name__}: {exc}", mode=config.mode, symbol=expected_symbol)


def opposite_explicit_signal(position_side: str, guard: Mapping[str, Any]) -> bool:
    return bool(
        (position_side == "long" and guard.get("futures_bias") == "short" and guard.get("explicit_short_signal") is True)
        or (position_side == "short" and guard.get("futures_bias") == "long" and guard.get("explicit_long_signal") is True)
    )


def evaluate_entry_candidate(
    candidate_side: str,
    guard: Mapping[str, Any],
    config: AiResearchConfig,
) -> dict[str, Any]:
    if candidate_side not in {"long", "short"}:
        raise ValueError("candidate_side must be long or short")
    hard_filter = False
    if guard.get("valid") and float(guard.get("confidence", 0.0)) >= config.filter_min_confidence:
        if candidate_side == "long":
            hard_filter = bool(
                guard.get("futures_bias") == "short"
                and guard.get("explicit_short_signal") is True
                and guard.get("allow_long") is False
            )
        else:
            hard_filter = bool(
                guard.get("futures_bias") == "long"
                and guard.get("explicit_long_signal") is True
                and guard.get("allow_short") is False
            )
    filter_enabled = config.mode in {"filter", "filter_reduce", "manage"}
    allowed = not (hard_filter and filter_enabled)
    return {
        "allowed": allowed,
        "actual_action": "filter" if not allowed else ("log_only" if config.mode == "log" else "allow"),
        "shadow_action": "would_filter" if hard_filter else "would_allow",
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
    if position_side not in {"long", "short"}:
        raise ValueError("position_side must be long or short")
    initial_amount = float(initial_amount)
    current_amount = float(current_amount)
    if initial_amount <= 0 or current_amount <= 0:
        raise ValueError("position amounts must be positive")

    current_ratio = min(1.0, current_amount / initial_amount)
    confidence = float(guard.get("confidence", 0.0)) if guard.get("valid") else 0.0
    opposite = opposite_explicit_signal(position_side, guard)
    risk_multiplier = float(guard.get("risk_multiplier", 1.0)) if guard.get("valid") else 1.0
    risk_requests_reduction = risk_multiplier < current_ratio - 1e-12
    threshold_met = confidence >= config.reduce_min_confidence
    triggered = bool(guard.get("valid") and threshold_met and (opposite or risk_requests_reduction))

    requested_ratio = risk_multiplier
    trigger = "none"
    if opposite:
        requested_ratio = min(requested_ratio, config.reverse_signal_retained_position_ratio)
        trigger = "opposite_explicit_signal"
    elif risk_requests_reduction:
        trigger = "risk_multiplier"
    target_ratio = max(config.retained_ratio_floor, min(1.0, requested_ratio))
    target_amount = initial_amount * target_ratio
    reduce_amount = max(0.0, current_amount - target_amount) if triggered else 0.0

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
    observed_at = observed_at or datetime.now().astimezone()
    return Path(config.bias_file).parent / "logs" / f"ai_research_decisions_{observed_at:%Y-%m}.jsonl"


def append_ai_audit_event(
    config: AiResearchConfig,
    event: Mapping[str, Any],
    *,
    observed_at: datetime | None = None,
) -> Path | None:
    if config.mode == "off":
        return None
    observed_at = observed_at or datetime.now().astimezone()
    path = audit_log_path(config, observed_at)
    path.parent.mkdir(parents=True, exist_ok=True)
    event_data = dict(event)
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
    event_data["generated_at"] = str(
        event_data.get("generated_at") or source_generated_at or observed_at.isoformat()
    )
    event_data["reason"] = trace_reason
    event_data["source_generated_at"] = source_generated_at
    event_data["generated_at_source"] = "ai_bias" if source_generated_at else "decision_time_fallback"
    payload = {"observed_at": observed_at.isoformat(), **event_data}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, default=str, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return path
