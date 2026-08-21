import unittest
from unittest import mock

import pandas as pd

import bian_new as strategy
import demo


class BacktestLiveParityTests(unittest.TestCase):
    def test_fixed_trend_delay_waits_fifteen_minutes_for_both_strategy_levels(self):
        for candidate in (
            {"module": "trend_trigger", "strategy_tf": "1h", "exit_tf": "15m"},
            {"module": "trend_trigger", "strategy_tf": "4h", "exit_tf": "1h"},
        ):
            with self.subTest(candidate=candidate):
                self.assertEqual(
                    demo.trend_entry_delay_ms(candidate, "fixed-15m"),
                    demo.TIMEFRAME_MS["15m"],
                )

    def test_exit_timeframe_trend_delay_uses_fifteen_minutes_or_one_hour(self):
        self.assertEqual(
            demo.trend_entry_delay_ms(
                {"module": "trend_trigger", "strategy_tf": "1h", "exit_tf": "15m"},
                "exit-timeframe",
            ),
            demo.TIMEFRAME_MS["15m"],
        )
        self.assertEqual(
            demo.trend_entry_delay_ms(
                {"module": "trend_trigger", "strategy_tf": "4h", "exit_tf": "1h"},
                "exit-timeframe",
            ),
            demo.TIMEFRAME_MS["1h"],
        )

    def test_sr_candidates_are_not_delayed(self):
        self.assertEqual(
            demo.trend_entry_delay_ms(
                {"module": "sr_rebound", "strategy_tf": "15m", "exit_tf": "15m"},
                "fixed-15m",
            ),
            0,
        )

    def test_delayed_entry_cross_check_uses_original_trigger(self):
        long_candidate = {"module": "trend_trigger", "side": "long", "entry_level": 100.0}
        self.assertTrue(demo.delayed_entry_price_crossed(long_candidate, 101.0))
        self.assertFalse(demo.delayed_entry_price_crossed(long_candidate, 99.0))

        short_candidate = {"module": "trend_trigger", "side": "short", "entry_level": 100.0}
        self.assertTrue(demo.delayed_entry_price_crossed(short_candidate, 99.0))

    def test_crossed_delayed_entry_rebuilds_entry_stop_and_atr_policy(self):
        original = {
            "module": "trend_trigger", "strategy_tf": "1h", "side": "long",
            "entry_level": 100.0, "stop": 90.0, "entry_buffer_regime": "clean_trend",
        }
        fresh = {
            "module": "trend_trigger", "strategy_tf": "1h", "side": "long",
            "entry_level": 108.0, "stop": 97.0, "entry_buffer_regime": "default_defensive",
        }
        delayed = {"candidate": original}
        context = {"trend_states": {}}
        with (
            mock.patch.object(strategy, "entry_context_allows_side", return_value=(True, "ok")),
            mock.patch.object(strategy, "build_trend_trigger_candidates", return_value=[fresh]),
        ):
            released, crossed, reason = demo.delayed_candidate_for_release(delayed, context, 105.0)

        self.assertTrue(crossed)
        self.assertEqual(released["entry_level"], 108.0)
        self.assertEqual(released["stop"], 97.0)
        self.assertEqual(released["entry_buffer_regime"], "default_defensive")
        self.assertIn("重建趋势候选", reason)

    def test_delayed_entry_is_dropped_when_current_trend_is_invalid(self):
        original = {
            "module": "trend_trigger", "strategy_tf": "1h", "side": "long",
            "entry_level": 100.0, "stop": 90.0,
        }
        with mock.patch.object(
            strategy,
            "entry_context_allows_side",
            return_value=(False, "4h background reversed"),
        ):
            released, crossed, reason = demo.delayed_candidate_for_release(
                {"candidate": original}, {"trend_states": {}}, 101.0
            )

        self.assertIsNone(released)
        self.assertTrue(crossed)
        self.assertIn("趋势已失效", reason)

    def test_backtest_candidate_selection_delegates_to_live_strategy(self):
        selected = {"side": "short"}
        with mock.patch.object(
            strategy,
            "select_entry_candidate",
            return_value=(selected, "ok", "ok"),
        ) as choose:
            candidate, reason = demo.choose_candidate_like_bian_new(
                [{"side": "short"}],
                {"dfs": {}, "zones": {}},
                100.0,
                "2026-08-19 12:00:00",
            )

        self.assertIs(candidate, selected)
        self.assertEqual(reason, "ok")
        choose.assert_called_once()

    def test_crossed_atr_stop_falls_back_to_executable_target_stop(self):
        selected = strategy.select_executable_stop_candidate(
            "short",
            [
                (2429.01, "ATR lock already crossed"),
                (2705.86, "first target"),
            ],
            current_stop=2777.96,
            curr_price=2496.0,
            tick=0.01,
        )

        self.assertEqual(selected, (2705.86, "first target"))

    def test_one_minute_bars_resample_to_strategy_five_minute_bar(self):
        timestamps = pd.date_range("2026-08-19 00:00:00", periods=5, freq="1min", tz="UTC")
        frame = pd.DataFrame({
            "timestamp_ms": timestamps.astype("int64") // 1_000_000,
            "timestamp": timestamps,
            "open": [100, 101, 102, 103, 104],
            "high": [102, 103, 104, 105, 106],
            "low": [99, 100, 101, 102, 103],
            "close": [101, 102, 103, 104, 105],
            "volume": [1, 2, 3, 4, 5],
        })

        result = demo.resample_from_5m(frame, "5m")

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["open"], 100)
        self.assertEqual(result.iloc[0]["high"], 106)
        self.assertEqual(result.iloc[0]["low"], 99)
        self.assertEqual(result.iloc[0]["close"], 105)
        self.assertEqual(result.iloc[0]["volume"], 15)

    def test_strategy_price_precision_uses_backtest_market_tick(self):
        previous_tick = demo.backtest_runtime["price_tick"]
        try:
            demo.backtest_runtime["price_tick"] = 0.1
            demo.patch_strategy_side_effects()
            self.assertEqual(strategy.get_price_tick(123.45), 0.1)
            self.assertEqual(strategy.precision_price(123.46), 123.5)
        finally:
            demo.backtest_runtime["price_tick"] = previous_tick

    def test_pending_cancel_checks_delegate_to_live_strategy(self):
        context = {"now_dt": pd.Timestamp("2026-08-19 12:00:00", tz="Asia/Shanghai")}
        with mock.patch.object(
            strategy,
            "pending_entry_cancel_decision",
            return_value=("pending_entry_trend_invalid", "趋势不再支持 pending 方向: trend changed", 0),
        ) as decision:
            reason = demo.pending_entry_cancel_reason_like_bian_new(context, 2500.0)

        self.assertEqual(reason, "趋势不再支持 pending 方向: trend changed")
        decision.assert_called_once_with(context, 2500.0)

    def test_pending_cancel_checks_use_live_timeout(self):
        now_dt = pd.Timestamp("2026-08-19 12:00:00", tz="Asia/Shanghai")
        context = {"now_dt": now_dt}
        with (
            mock.patch.object(strategy, "pending_entry_price_still_reasonable", return_value=(True, "ok")),
            mock.patch.object(strategy, "pending_entry_trend_still_valid", return_value=(True, "ok")),
            mock.patch.object(
                strategy,
                "pending_entry_age_seconds",
                return_value=strategy.PENDING_ENTRY_MAX_AGE_SECONDS + 1,
            ) as age_check,
        ):
            code, reason, age = strategy.pending_entry_cancel_decision(context, 2500.0)

        self.assertEqual(code, "pending_entry_expired")
        self.assertIn("pending 入场单超时", reason)
        self.assertEqual(age, strategy.PENDING_ENTRY_MAX_AGE_SECONDS + 1)
        age_check.assert_called_once_with(now_dt=now_dt)

    def test_backtest_amount_delegates_to_live_raw_amount_logic(self):
        expected = (1.25, {"mode": "normal_notional"})
        candidate = {"module": "trend_trigger"}
        with mock.patch.object(strategy, "calculate_candidate_amount_raw", return_value=expected) as calculate:
            result = demo.backtest_candidate_amount(candidate, 2000.0, 1900.0, 10000.0)

        self.assertEqual(result, expected)
        calculate.assert_called_once_with(
            2000.0,
            1900.0,
            candidate=candidate,
            account_equity=10000.0,
        )

    def test_fixed_risk_amount_targets_two_percent_stop_loss(self):
        amount, meta = strategy.calculate_candidate_amount_raw(
            3000.0,
            2970.0,
            candidate={"module": "trend_trigger"},
            account_equity=10000.0,
        )

        self.assertAlmostEqual(amount, 200.0 / 30.0)
        self.assertEqual(meta["mode"], "fixed_risk")
        self.assertAlmostEqual(meta["estimated_stop_loss"], 200.0)
        self.assertAlmostEqual(meta["estimated_risk_ratio"], 0.02)

    def test_fixed_risk_amount_keeps_sixty_percent_margin_cap(self):
        amount, meta = strategy.calculate_candidate_amount_raw(
            3000.0,
            2995.0,
            candidate={"module": "trend_trigger"},
            account_equity=10000.0,
        )

        self.assertAlmostEqual(amount, 20.0)
        self.assertEqual(meta["mode"], "fixed_risk_capped_by_margin")
        self.assertAlmostEqual(meta["estimated_stop_loss"], 100.0)
        self.assertAlmostEqual(meta["estimated_risk_ratio"], 0.01)

    def test_fixed_risk_preserves_sr_breakout_distance_filter(self):
        amount, meta = strategy.calculate_candidate_amount_raw(
            3000.0,
            2800.0,
            candidate={"module": "sr_breakout", "profit_check_atr": 100.0},
            account_equity=10000.0,
        )

        self.assertEqual(amount, 0)
        self.assertEqual(meta["mode"], "sr_breakout_risk_skip")

    def test_risk_per_trade_cli_ratio_parser(self):
        self.assertEqual(strategy.parse_risk_per_trade("0.02"), 0.02)
        self.assertEqual(strategy.parse_risk_per_trade(0.01), 0.01)
        for invalid in (None, "", "two", 0, -0.01, 1.01):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    strategy.parse_risk_per_trade(invalid)

    def test_pending_stop_is_always_on_untriggered_side_of_current_price(self):
        long_candidate = {"side": "long", "entry_level": 100.0, "target": 120.0}
        short_candidate = {"side": "short", "entry_level": 100.0, "target": 80.0}
        with (
            mock.patch.object(strategy, "get_price_tick", return_value=0.01),
            mock.patch.object(strategy, "precision_price", side_effect=lambda value: round(float(value), 2)),
        ):
            long_prices, long_reason = strategy.resolve_pending_entry_prices(long_candidate, 101.0)
            short_prices, short_reason = strategy.resolve_pending_entry_prices(short_candidate, 99.0)

        self.assertEqual(long_reason, "ok")
        self.assertGreater(long_prices["stop_price"], 101.0)
        self.assertEqual(short_reason, "ok")
        self.assertLess(short_prices["stop_price"], 99.0)

    def test_pending_order_can_be_created_before_original_trigger_crosses(self):
        candidate = {"module": "trend_trigger", "side": "long", "entry_level": 100.0, "target": 120.0}

        allowed, reason = strategy.candidate_entry_price_still_valid(candidate, 95.0)

        self.assertTrue(allowed, reason)

    def test_cli_arg_value_supports_equals_form(self):
        with mock.patch.object(strategy.sys, "argv", ["bian_new.py", "--risk-per-trade=0.03"]):
            self.assertEqual(strategy.cli_arg_value("--risk-per-trade"), "0.03")


if __name__ == "__main__":
    unittest.main()
