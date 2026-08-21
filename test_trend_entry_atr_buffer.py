import unittest
from unittest import mock

import pandas as pd

import bian_new as strategy


class TrendEntryAtrBufferTests(unittest.TestCase):
    @staticmethod
    def frame(atr):
        return pd.DataFrame([
            {
                "timestamp": pd.Timestamp("2024-01-01", tz="Asia/Shanghai") + pd.Timedelta(minutes=i),
                "open": 95.0,
                "high": 101.0,
                "low": 94.0,
                "close": 100.0,
                "atr": atr,
            }
            for i in range(25)
        ])

    def detect_previous_large(self, timeframe, atr, side):
        previous = {
            "high": 100.0,
            "low": 80.0,
            "direction": side,
        }
        with (
            mock.patch.object(strategy, "get_closed_df", return_value=self.frame(atr)),
            mock.patch.object(strategy, "latest_large_strong", return_value=None),
            mock.patch.object(strategy, "find_previous_large_strong", return_value=previous),
            mock.patch.object(strategy, "get_price_tick", return_value=0.01),
            mock.patch.object(strategy, "precision_price", side_effect=lambda value: round(float(value), 8)),
        ):
            return strategy.detect_entry_trigger(self.frame(atr), timeframe, side)

    @staticmethod
    def trend_state(**detail_overrides):
        details = {
            "adx": 35.0,
            "prev_adx": 32.0,
            "adx_rising": True,
            "adx_falling": False,
            "plus_di": 30.0,
            "minus_di": 20.0,
            "di_gap": 10.0,
            "ema_clean": True,
            "ema_cross_count": 1,
            "di_direction_flips": 0,
            "atr_ratio": 0.02,
            "atr_ratio_percentile": 0.50,
            "atr_ratio_sample_count": 100,
            "extreme_adx": False,
        }
        details.update(detail_overrides)
        return {"timeframe": "1h", "direction": "long", "details": details}

    def test_15m_trigger_uses_15m_atr_for_long_entry_buffer(self):
        trigger = self.detect_previous_large("15m", atr=5.0, side="long")

        self.assertEqual(trigger["timeframe"], "15m")
        self.assertEqual(trigger["entry_level"], 105.0)
        self.assertEqual(trigger["entry_buffer_atr"], 5.0)

    def test_1h_trigger_uses_1h_atr_for_short_entry_buffer(self):
        trigger = self.detect_previous_large("1h", atr=12.0, side="short")

        self.assertEqual(trigger["timeframe"], "1h")
        self.assertEqual(trigger["entry_level"], 68.0)
        self.assertEqual(trigger["entry_buffer_atr"], 12.0)

    def test_half_atr_multiplier_uses_half_of_trigger_timeframe_atr(self):
        with mock.patch.object(strategy, "ENTRY_TRIGGER_ATR_MULTIPLIER", 0.5):
            trigger = self.detect_previous_large("15m", atr=5.0, side="long")

        self.assertEqual(trigger["entry_level"], 102.5)
        self.assertEqual(trigger["entry_buffer_atr"], 2.5)

    def test_invalid_trigger_atr_does_not_fall_back_to_tick(self):
        with (
            mock.patch.object(strategy, "get_closed_df", return_value=self.frame(0.0)),
            mock.patch.object(strategy, "latest_large_strong", return_value=None),
        ):
            trigger = strategy.detect_entry_trigger(self.frame(0.0), "15m", "long")

        self.assertIsNone(trigger)

    def test_tick_mode_reproduces_previous_one_tick_entry_buffer(self):
        previous = {
            "high": 100.0,
            "low": 80.0,
            "direction": "long",
        }
        with (
            mock.patch.object(strategy, "ENTRY_TRIGGER_BUFFER_MODE", "tick"),
            mock.patch.object(strategy, "get_closed_df", return_value=self.frame(5.0)),
            mock.patch.object(strategy, "latest_large_strong", return_value=None),
            mock.patch.object(strategy, "find_previous_large_strong", return_value=previous),
            mock.patch.object(strategy, "get_price_tick", return_value=0.01),
            mock.patch.object(strategy, "precision_price", side_effect=lambda value: round(float(value), 8)),
        ):
            trigger = strategy.detect_entry_trigger(self.frame(5.0), "15m", "long")

        self.assertEqual(trigger["entry_level"], 100.01)

    def test_clean_trend_uses_half_atr(self):
        policy = strategy.select_adaptive_trend_entry_buffer(self.trend_state(), "long")
        previous = {"high": 100.0, "low": 80.0, "direction": "long"}
        with (
            mock.patch.object(strategy, "ENTRY_TRIGGER_BUFFER_MODE", "adaptive"),
            mock.patch.object(strategy, "get_closed_df", return_value=self.frame(5.0)),
            mock.patch.object(strategy, "latest_large_strong", return_value=None),
            mock.patch.object(strategy, "find_previous_large_strong", return_value=previous),
            mock.patch.object(strategy, "get_price_tick", return_value=0.01),
            mock.patch.object(strategy, "precision_price", side_effect=lambda value: round(float(value), 8)),
        ):
            trigger = strategy.detect_entry_trigger(
                self.frame(5.0),
                "15m",
                "long",
                entry_buffer_policy=policy,
            )

        self.assertEqual(policy["regime"], "clean_trend")
        self.assertEqual(trigger["entry_level"], 102.5)
        self.assertEqual(trigger["entry_buffer_mode"], "atr")
        self.assertEqual(trigger["entry_buffer_atr_multiplier"], 0.5)

    def test_extreme_clean_trend_uses_one_tick(self):
        state = self.trend_state(adx=48.0, prev_adx=45.0, di_gap=15.0, extreme_adx=True)
        policy = strategy.select_adaptive_trend_entry_buffer(state, "long")
        previous = {"high": 100.0, "low": 80.0, "direction": "long"}
        with (
            mock.patch.object(strategy, "ENTRY_TRIGGER_BUFFER_MODE", "adaptive"),
            mock.patch.object(strategy, "get_closed_df", return_value=self.frame(5.0)),
            mock.patch.object(strategy, "latest_large_strong", return_value=None),
            mock.patch.object(strategy, "find_previous_large_strong", return_value=previous),
            mock.patch.object(strategy, "get_price_tick", return_value=0.01),
            mock.patch.object(strategy, "precision_price", side_effect=lambda value: round(float(value), 8)),
        ):
            trigger = strategy.detect_entry_trigger(
                self.frame(5.0),
                "15m",
                "long",
                entry_buffer_policy=policy,
            )

        self.assertEqual(policy["regime"], "extreme_trend_breakout")
        self.assertEqual(trigger["entry_level"], 100.01)
        self.assertEqual(trigger["entry_buffer_mode"], "tick")
        self.assertEqual(trigger["entry_buffer_atr_multiplier"], 0.0)

    def test_falling_adx_with_high_volatility_uses_one_atr(self):
        state = self.trend_state(
            adx=32.0,
            prev_adx=35.0,
            adx_rising=False,
            adx_falling=True,
            atr_ratio_percentile=0.85,
        )

        policy = strategy.select_adaptive_trend_entry_buffer(state, "long")

        self.assertEqual(policy["regime"], "weakening_or_choppy")
        self.assertEqual(policy["mode"], "atr")
        self.assertEqual(policy["atr_multiplier"], 1.0)

    def test_falling_adx_with_repeated_direction_changes_uses_one_atr(self):
        state = self.trend_state(
            adx=32.0,
            prev_adx=35.0,
            adx_rising=False,
            adx_falling=True,
            di_direction_flips=2,
        )

        policy = strategy.select_adaptive_trend_entry_buffer(state, "long")

        self.assertEqual(policy["regime"], "weakening_or_choppy")
        self.assertEqual(policy["atr_multiplier"], 1.0)

    def test_unclear_regime_falls_back_to_one_atr(self):
        state = self.trend_state(
            adx=35.0,
            prev_adx=35.0,
            adx_rising=False,
            adx_falling=False,
            di_gap=4.0,
        )

        policy = strategy.select_adaptive_trend_entry_buffer(state, "long")

        self.assertEqual(policy["regime"], "default_defensive")
        self.assertEqual(policy["atr_multiplier"], 1.0)

    def test_reversed_di_cannot_select_clean_or_extreme_entry_buffer(self):
        state = self.trend_state(
            adx=48.0,
            prev_adx=45.0,
            plus_di=18.0,
            minus_di=35.0,
            di_gap=17.0,
            extreme_adx=True,
        )

        policy = strategy.select_adaptive_trend_entry_buffer(state, "long")

        self.assertEqual(policy["regime"], "default_defensive")
        self.assertEqual(policy["atr_multiplier"], 1.0)
        self.assertFalse(policy["metrics"]["di_same_side"])


if __name__ == "__main__":
    unittest.main()
