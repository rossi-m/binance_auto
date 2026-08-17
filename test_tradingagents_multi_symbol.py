import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tradingAgents import ai_market_bias
from tradingAgents import ai_research_guard
from tradingAgents import analyze_eth_tradingagents
from tradingAgents import run_symbol_research


class TradingAgentsMultiSymbolTests(unittest.TestCase):
    def test_arbitrary_usd_crypto_symbol_has_no_base_whitelist(self):
        self.assertTrue(analyze_eth_tradingagents.is_crypto_symbol('PEPE-USD'))
        self.assertTrue(analyze_eth_tradingagents.is_crypto_symbol('SUI/USDT'))
        self.assertEqual(analyze_eth_tradingagents.crypto_base('1000SHIB-USDT'), '1000SHIB')
        self.assertEqual(analyze_eth_tradingagents.binance_usdt_symbol('PEPE-USD'), 'PEPEUSDT')
        self.assertEqual(analyze_eth_tradingagents.binance_usdt_symbol('PEPE/USDT:USDT'), 'PEPEUSDT')
        self.assertEqual(ai_market_bias.symbol_to_binance('PEPE/USDT:USDT'), 'PEPEUSDT')

    def test_news_terms_follow_requested_symbol(self):
        sol_terms = ai_market_bias.symbol_news_terms('SOLUSDT')
        self.assertIn('SOL', sol_terms)
        self.assertIn('solana', sol_terms)
        self.assertTrue(ai_market_bias.keyword_match('Solana network upgrade announced', symbol='SOLUSDT'))
        self.assertFalse(ai_market_bias.keyword_match('Ethereum staking update', symbol='SOLUSDT'))

    def test_symbol_runner_builds_symbol_specific_ticker(self):
        self.assertEqual(run_symbol_research.compact_symbol('PEPE/USDT:USDT'), 'PEPEUSDT')
        self.assertEqual(run_symbol_research.tradingagents_ticker('PEPEUSDT'), 'PEPE-USD')

    def test_guard_can_load_an_explicit_symbol_bias_path(self):
        now = datetime.now(timezone.utc)
        payload = {
            'symbol': 'BTCUSDT',
            'generated_at': (now - timedelta(minutes=1)).isoformat(),
            'expires_at': (now + timedelta(hours=2)).isoformat(),
            'portfolio_stance': 'neutral',
            'futures_bias': 'mixed',
            'time_horizon_hours': 2,
            'explicit_long_signal': False,
            'explicit_short_signal': False,
            'confidence': 0.5,
            'allow_long': True,
            'allow_short': True,
            'risk_multiplier': 1.0,
            'reason': 'test',
            'risk_events': [],
            'news_used': [],
            'source_counts': {},
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / 'latest_bias_BTCUSDT.json'
            path.write_text(json.dumps(payload), encoding='utf-8')
            config = ai_research_guard.AiResearchConfig(mode='log', bias_file='unused.json')
            guard = ai_research_guard.load_ai_research_guard(
                config,
                expected_symbol='BTCUSDT',
                now=now,
                bias_file=str(path),
            )
        self.assertTrue(guard['valid'])
        self.assertEqual(guard['symbol'], 'BTCUSDT')


if __name__ == '__main__':
    unittest.main()
