import unittest
from unittest import mock

import bian_new as strategy


class EntryDirectionFilterTests(unittest.TestCase):
    @staticmethod
    def state(
        timeframe,
        side="long",
        status=None,
        can_open=True,
        ema20=110.0,
        ema20_ref=100.0,
        plus_di=30.0,
        minus_di=18.0,
        ema_clean=True,
        ema_cross_count=0,
    ):
        status = status or side
        return {
            "timeframe": timeframe,
            "direction": side,
            "status": status,
            "can_open_long": can_open and side == "long",
            "can_open_short": can_open and side == "short",
            "is_oscillation": False,
            "details": {
                "adx": 35.0,
                "plus_di": plus_di,
                "minus_di": minus_di,
                "di_gap": abs(plus_di - minus_di),
                "ema20": ema20,
                "ema20_ref": ema20_ref,
                "ema_clean": ema_clean,
                "ema_cross_count": ema_cross_count,
                "up_cross": ema_cross_count,
                "down_cross": 0,
            },
        }

    def test_background_weakening_is_not_soft_allowed(self):
        local = self.state("1h")
        background = self.state("4h", status="long_weakening", can_open=False)

        allowed, reason = strategy.context_allows_side(local, background, "long")

        self.assertFalse(allowed)
        self.assertIn("禁止weakening软放行", reason)

    def test_flat_adx_is_allowed_when_ema_and_di_are_clean(self):
        local = self.state("1h")
        background = self.state("4h", status="long_flat_adx", can_open=False)

        allowed, reason = strategy.context_allows_side(local, background, "long")

        self.assertTrue(allowed, reason)

    def test_flat_adx_is_rejected_when_di_gap_is_too_small(self):
        local = self.state("1h")
        background = self.state(
            "4h",
            status="long_flat_adx",
            can_open=False,
            plus_di=24.0,
            minus_di=20.0,
        )

        allowed, reason = strategy.context_allows_side(local, background, "long")

        self.assertFalse(allowed)
        self.assertIn("背景DI差不足", reason)

    def test_ema_persistence_is_rejected_after_di_reverses(self):
        local = self.state("1h")
        background = self.state(
            "4h",
            status="ema20_long_persistence",
            can_open=True,
            plus_di=18.0,
            minus_di=30.0,
        )

        allowed, reason = strategy.context_allows_side(local, background, "long")

        self.assertFalse(allowed)
        self.assertIn("背景DI方向已经反转", reason)

    def test_local_ema_persistence_is_rejected_after_di_reverses(self):
        local = self.state(
            "1h",
            status="ema20_long_persistence",
            can_open=True,
            plus_di=18.0,
            minus_di=30.0,
        )
        background = self.state("4h")

        allowed, reason = strategy.context_allows_side(local, background, "long")

        self.assertFalse(allowed)
        self.assertIn("EMA持续性状态未通过严格校验", reason)

    def test_long_requires_daily_and_4h_ema_to_both_slope_up(self):
        context = {
            "trend_states": {
                "1h": self.state("1h"),
                "4h": self.state("4h", ema20=110.0, ema20_ref=100.0),
                "1d": self.state("1d", ema20=190.0, ema20_ref=200.0),
            }
        }

        allowed, reason = strategy.entry_context_allows_side(context, "1h", "4h", "long")

        self.assertFalse(allowed)
        self.assertIn("日线与4小时EMA20未同时向上", reason)

    def test_long_is_allowed_when_daily_and_4h_ema_both_slope_up(self):
        context = {
            "trend_states": {
                "1h": self.state("1h"),
                "4h": self.state("4h", ema20=110.0, ema20_ref=100.0),
                "1d": self.state("1d", ema20=210.0, ema20_ref=200.0),
            }
        }

        allowed, reason = strategy.entry_context_allows_side(context, "1h", "4h", "long")

        self.assertTrue(allowed, reason)

    def test_short_does_not_use_long_only_daily_4h_filter(self):
        context = {
            "trend_states": {
                "1h": self.state(
                    "1h", side="short", ema20=90.0, ema20_ref=100.0, plus_di=18.0, minus_di=30.0
                ),
                "4h": self.state(
                    "4h", side="short", ema20=90.0, ema20_ref=100.0, plus_di=18.0, minus_di=30.0
                ),
                "1d": self.state("1d", ema20=210.0, ema20_ref=200.0),
            }
        }

        allowed, reason = strategy.entry_context_allows_side(context, "1h", "4h", "short")

        self.assertTrue(allowed, reason)

    def test_legacy_profile_can_restore_weakening_soft_allow(self):
        local = self.state("1h")
        background = self.state("4h", status="long_weakening", can_open=False)
        with mock.patch.object(strategy, "ALLOW_BACKGROUND_WEAKENING_SOFT", True):
            allowed, reason = strategy.context_allows_side(local, background, "long")

        self.assertTrue(allowed, reason)


if __name__ == "__main__":
    unittest.main()
