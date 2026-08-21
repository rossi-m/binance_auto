import copy
import csv
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import bian_new as bot


class ManualPositionTakeoverTests(unittest.TestCase):
    def setUp(self):
        self.trade_state_before = copy.deepcopy(bot.trade_state)
        self.manual_states_before = copy.deepcopy(bot.manual_position_states)
        self.manual_ai_jobs_before = bot.runtime_state.get('manual_ai_jobs', {})
        self.manual_ai_enabled_before = bot.MANUAL_AI_RESEARCH_ENABLED
        self.manual_states_loaded_before = bot.manual_position_states_loaded
        bot.manual_position_states_loaded = True
        self.persistence_patchers = [
            mock.patch.object(bot, 'load_persisted_manual_position_states', return_value={}),
            mock.patch.object(bot, 'persist_manual_position_state', return_value=True),
            mock.patch.object(bot, 'delete_persisted_manual_position_state', return_value=True),
            mock.patch.object(bot, 'log_manual_trade_to_csv', return_value=True),
        ]
        for patcher in self.persistence_patchers:
            patcher.start()
        # 单元测试默认不启动真实 TradingAgents 子进程；需要验证后台任务的用例会单独打开。
        bot.MANUAL_AI_RESEARCH_ENABLED = False
        bot.runtime_state['manual_ai_jobs'] = {}
        bot.manual_position_states.clear()
        bot.trade_state['has_position'] = False
        bot.trade_state['side'] = None
        bot.trade_state['pending_entry_order_id'] = ''
        bot.trade_state['last_exit_time'] = ''

    def tearDown(self):
        bot.trade_state.clear()
        bot.trade_state.update(self.trade_state_before)
        bot.manual_position_states.clear()
        bot.manual_position_states.update(self.manual_states_before)
        bot.runtime_state['manual_ai_jobs'] = self.manual_ai_jobs_before
        bot.MANUAL_AI_RESEARCH_ENABLED = self.manual_ai_enabled_before
        bot.manual_position_states_loaded = self.manual_states_loaded_before
        for patcher in reversed(self.persistence_patchers):
            patcher.stop()

    def test_symbol_normalization_and_position_key(self):
        self.assertEqual(bot.normalize_futures_symbol('BTCUSDT'), 'BTC/USDT')
        self.assertEqual(bot.normalize_futures_symbol('BTC/USDT:USDT'), 'BTC/USDT')
        self.assertEqual(bot.position_key('BTCUSDT', 'long'), 'BTC/USDT|LONG')

    def test_raw_hedge_short_position_is_parsed_correctly(self):
        position = bot.normalize_account_position({
            'symbol': 'SOLUSDT',
            'positionAmt': '2.5',
            'positionSide': 'SHORT',
            'entryPrice': '150',
            'markPrice': '145',
            'liquidationPrice': '190',
        })
        self.assertEqual(position['symbol'], 'SOL/USDT')
        self.assertEqual(position['side'], 'short')
        self.assertEqual(position['key'], 'SOL/USDT|SHORT')
        self.assertEqual(position['amount'], 2.5)

    def test_client_order_prefix_identifies_only_script_orders(self):
        manual_order = {'id': '1', 'info': {'clientAlgoId': 'MY_MANUAL_STOP'}}
        script_order = {'id': '2', 'info': {'clientAlgoId': 'TAM_123'}}
        self.assertFalse(bot.is_script_owned_protective_order(manual_order))
        self.assertTrue(bot.is_script_owned_protective_order(script_order))

    def test_profile_selection_uses_lower_expected_loss(self):
        long_position = {'side': 'long'}
        short_position = {'side': 'short'}
        candidates = [
            {'profile_name': '4h_1h', 'stop': 95.0},
            {'profile_name': '1h_15m', 'stop': 97.0},
        ]
        with mock.patch.object(bot, 'manual_profile_stop_candidate', side_effect=candidates):
            selected = bot.select_manual_position_profile(long_position, {}, None, 0, 0)
        self.assertEqual(selected['profile_name'], '1h_15m')

        short_candidates = [
            {'profile_name': '4h_1h', 'stop': 105.0},
            {'profile_name': '1h_15m', 'stop': 103.0},
        ]
        with mock.patch.object(bot, 'manual_profile_stop_candidate', side_effect=short_candidates):
            selected = bot.select_manual_position_profile(short_position, {}, None, 0, 0)
        self.assertEqual(selected['profile_name'], '1h_15m')

    def test_existing_one_atr_profit_immediately_uses_profit_lock(self):
        result = bot.profit_atr_lock_stop_for_position(
            side='long',
            entry=100.0,
            highest=112.0,
            lowest=100.0,
            atr=10.0,
        )
        self.assertIsNotNone(result)
        self.assertEqual(result['retain_ratio'], 0.30)
        self.assertAlmostEqual(result['stop'], 103.6)

    def test_manual_owned_stop_filter_preserves_user_order(self):
        state = {
            'symbol': 'BTC/USDT',
            'side': 'long',
            'position_side': 'BOTH',
            'stop_order_id': '',
        }
        user_order = {'id': 'user', 'type': 'STOP_MARKET', 'info': {'clientAlgoId': 'USER_STOP'}}
        script_order = {'id': 'script', 'type': 'STOP_MARKET', 'info': {'clientAlgoId': 'TAM_abc'}}
        with mock.patch.object(bot, 'fetch_open_protective_stop_orders', return_value=[user_order, script_order]):
            owned = bot.manual_owned_stop_orders(state)
        self.assertEqual([bot.extract_order_id(order) for order in owned], ['script'])

    def test_partial_close_refreshes_only_script_stop_quantity(self):
        state = {
            'symbol': 'BTC/USDT',
            'side': 'long',
            'position_side': 'BOTH',
            'amount': 2.0,
            'entry_price': 100.0,
            'highest_price': 105.0,
            'lowest_price': 100.0,
            'stop_loss_price': 95.0,
            'exit_tf': '15m',
            'miss_count': 0,
        }
        position = dict(state, amount=1.0, mark_price=104.0, liquidation_price=70.0)
        with (
            mock.patch.object(bot, 'fetch_manual_position_dfs', return_value={}),
            mock.patch.object(bot, 'build_manual_dynamic_stop', return_value=None),
            mock.patch.object(bot, 'apply_manual_ai_position_guard', return_value=False),
            mock.patch.object(bot, 'manual_owned_stop_orders', return_value=[{'id': 'own'}]),
            mock.patch.object(bot, 'refresh_manual_protective_stop_order', return_value=True) as refresh,
            mock.patch.object(bot, 'get_server_now_dt', return_value=None),
        ):
            bot.manage_manual_position(state, position)
        refresh.assert_called_once()
        self.assertTrue(refresh.call_args.kwargs['force_quantity_refresh'])
        self.assertEqual(state['amount'], 1.0)

    def test_full_close_sets_cooldown_only_for_eth(self):
        btc_state = {
            'key': 'BTC/USDT|BOTH|LONG', 'symbol': 'BTC/USDT', 'position_side': 'BOTH',
            'side': 'long', 'miss_count': bot.MANUAL_POSITION_MISS_CONFIRM_COUNT - 1,
        }
        bot.manual_position_states[btc_state['key']] = btc_state
        with (
            mock.patch.object(bot, 'confirm_manual_position_fully_closed', return_value=True),
            mock.patch.object(bot, 'cleanup_closed_position_exit_orders', return_value=True),
            mock.patch.object(bot, 'get_server_time_str', return_value='2026-08-17 12:00:00'),
        ):
            bot.manage_all_manual_positions([])
        self.assertEqual(bot.trade_state['last_exit_time'], '')

        eth_state = {
            'key': 'ETH/USDT|BOTH|LONG', 'symbol': 'ETH/USDT', 'position_side': 'BOTH',
            'side': 'long', 'miss_count': bot.MANUAL_POSITION_MISS_CONFIRM_COUNT - 1,
        }
        bot.manual_position_states[eth_state['key']] = eth_state
        with (
            mock.patch.object(bot, 'confirm_manual_position_fully_closed', return_value=True),
            mock.patch.object(bot, 'cleanup_closed_position_exit_orders', return_value=True),
            mock.patch.object(bot, 'get_server_time_str', return_value='2026-08-17 12:00:00'),
        ):
            bot.manage_all_manual_positions([])
        self.assertEqual(bot.trade_state['last_exit_time'], '2026-08-17 12:00:00')

    def test_manual_trade_settlement_uses_binance_realized_pnl_and_fees(self):
        detected_dt = datetime(2026, 8, 17, 12, 0, tzinfo=bot.EXCHANGE_TZ)
        state = {
            'key': 'BTC/USDT|BOTH|LONG',
            'symbol': 'BTC/USDT',
            'side': 'long',
            'position_side': 'BOTH',
            'detected_time': '2026-08-17 12:00:00',
            'initial_entry_price': 100.0,
            'entry_price': 100.0,
            'initial_amount': 1.0,
            'amount': 1.0,
            'fee_rate': 0.0004,
        }
        trades = [
            {
                'timestamp': int(detected_dt.timestamp() * 1000) - 1000,
                'side': 'buy', 'price': 100.0, 'amount': 1.0, 'order': 'open-1',
                'info': {'positionSide': 'BOTH', 'commission': '0.04', 'realizedPnl': '0'},
            },
            {
                'timestamp': int(detected_dt.timestamp() * 1000) + 600_000,
                'side': 'sell', 'price': 110.0, 'amount': 1.0, 'order': 'close-1',
                'info': {'positionSide': 'BOTH', 'commission': '0.044', 'realizedPnl': '10'},
            },
        ]
        with mock.patch.object(bot.exchange, 'fetch_my_trades', return_value=trades):
            settlement = bot.fetch_manual_trade_settlement(state, '2026-08-17 12:10:05')
        self.assertEqual(settlement['data_source'], 'Binance成交明细')
        self.assertEqual(settlement['exit_price'], 110.0)
        self.assertAlmostEqual(settlement['realized_pnl_usdt'], 10.0)
        self.assertAlmostEqual(settlement['fee_cost'], 0.084)
        self.assertAlmostEqual(settlement['net_pnl_usdt'], 9.916)
        self.assertEqual(settlement['open_order_ids'], ['open-1'])
        self.assertEqual(settlement['close_order_ids'], ['close-1'])

    def test_manual_position_pauses_eth_entry_scan(self):
        snapshot = {
            'fetch_failed': False,
            'positions': [{'key': 'BTC/USDT|BOTH|LONG', 'symbol': 'BTC/USDT', 'side': 'long'}],
        }
        with (
            mock.patch.object(bot, 'reconcile_orphan_strategy_entry_orders', return_value=True),
            mock.patch.object(bot, 'fetch_all_account_positions', return_value=snapshot),
            mock.patch.object(bot, 'manage_all_manual_positions', return_value=True),
            mock.patch.object(bot, 'check_empty_position_lightweight_5m_gate') as eth_gate,
        ):
            result = bot._run_strategy_impl(None)
        self.assertEqual(result, 'manual_positions_managed_entry_paused')
        eth_gate.assert_not_called()

    def test_local_eth_and_manual_btc_are_managed_simultaneously(self):
        bot.trade_state['has_position'] = True
        bot.trade_state['side'] = 'long'
        eth = {'key': 'ETH/USDT|BOTH|LONG', 'symbol': 'ETH/USDT', 'side': 'long'}
        btc = {'key': 'BTC/USDT|BOTH|LONG', 'symbol': 'BTC/USDT', 'side': 'long'}
        btc_state = {
            'key': btc['key'], 'symbol': btc['symbol'], 'side': 'long',
            'position_side': 'BOTH', 'miss_count': 0,
        }
        with mock.patch.object(bot, 'initialize_manual_position_state', return_value=btc_state) as initialize:
            active = bot.manage_all_manual_positions([eth, btc])
        self.assertTrue(active)
        initialize.assert_called_once_with(btc)
        self.assertIn(btc['key'], bot.manual_position_states)
        self.assertNotIn(eth['key'], bot.manual_position_states)

    def test_existing_position_cancels_pending_eth_entry(self):
        bot.trade_state['pending_entry_order_id'] = 'pending-eth'
        snapshot = {
            'fetch_failed': False,
            'positions': [{'key': 'BTC/USDT|BOTH|LONG', 'symbol': 'BTC/USDT', 'side': 'long'}],
        }
        with (
            mock.patch.object(bot, 'reconcile_orphan_strategy_entry_orders', return_value=True),
            mock.patch.object(bot, 'fetch_all_account_positions', return_value=snapshot),
            mock.patch.object(bot, 'sync_pending_entry_fill_if_needed', return_value=''),
            mock.patch.object(bot, 'cancel_pending_entry_order', return_value=True) as cancel_pending,
            mock.patch.object(bot, 'manage_all_manual_positions', return_value=True),
        ):
            result = bot._run_strategy_impl(None)
        self.assertEqual(result, 'manual_positions_managed_entry_paused')
        cancel_pending.assert_called_once()

    def test_strategy_entry_prefix_does_not_claim_manual_order(self):
        manual_order = {'id': '1', 'type': 'STOP', 'info': {'clientAlgoId': 'MY_ENTRY'}}
        strategy_order = {'id': '2', 'type': 'STOP', 'info': {'clientAlgoId': 'TAE_123'}}

        self.assertFalse(bot.is_script_owned_entry_order(manual_order))
        self.assertTrue(bot.is_script_owned_entry_order(strategy_order))

    def test_manual_same_side_position_is_not_treated_as_pending_fill(self):
        bot.trade_state.update({
            'pending_entry_order_id': 'pending-eth',
            'pending_entry_side': 'long',
            'pending_entry_amount': 1.0,
        })
        open_order = {'id': 'pending-eth', 'status': 'open', 'filled': 0.0}
        position = {'side': 'long', 'position_amt': 1.0, 'entry_price': 3000.0}
        with (
            mock.patch.object(bot, 'fetch_pending_entry_order', return_value=open_order),
            mock.patch.object(bot, 'get_position_risk', return_value=position),
            mock.patch.object(bot, 'cancel_pending_entry_order', return_value=True) as cancel_pending,
            mock.patch.object(bot, 'finalize_pending_entry_position') as finalize,
        ):
            result = bot.sync_pending_entry_fill_if_needed()

        self.assertEqual(result, 'pending_entry_external_position_detected')
        cancel_pending.assert_called_once()
        finalize.assert_not_called()

    def test_orphan_reconcile_cancels_only_owned_untracked_entry(self):
        orphan = {'id': 'orphan-1', 'type': 'STOP', 'info': {'clientAlgoId': 'TAE_orphan'}}
        with (
            mock.patch.object(bot, 'fetch_open_strategy_entry_orders', side_effect=[[orphan], []]),
            mock.patch.object(bot, 'cancel_conditional_orders', return_value=True) as cancel_orders,
        ):
            ok = bot.reconcile_orphan_strategy_entry_orders()

        self.assertTrue(ok)
        self.assertEqual(cancel_orders.call_args.args[0], [orphan])

    def test_strategy_entry_lookup_fails_closed_when_algo_query_fails(self):
        with (
            mock.patch.object(bot, 'fetch_raw_future_open_orders', return_value=[]),
            mock.patch.object(bot, 'fetch_raw_future_open_algo_orders', return_value=None),
        ):
            self.assertIsNone(bot.fetch_open_strategy_entry_orders())

    def test_strategy_entry_lookup_uses_both_native_order_sources(self):
        ordinary = {'id': 'ordinary', 'type': 'STOP', 'info': {'clientOrderId': 'TAE_regular'}}
        algo = {'id': 'algo', 'type': 'STOP', 'info': {'clientAlgoId': 'TAE_algo'}}
        manual = {'id': 'manual', 'type': 'STOP', 'info': {'clientAlgoId': 'MANUAL'}}
        with (
            mock.patch.object(bot, 'fetch_raw_future_open_orders', return_value=[ordinary]),
            mock.patch.object(bot, 'fetch_raw_future_open_algo_orders', return_value=[algo, manual]),
        ):
            orders = bot.fetch_open_strategy_entry_orders()

        self.assertEqual([bot.extract_order_id(order) for order in orders], ['ordinary', 'algo'])

    def test_strategy_stop_contains_owned_client_prefix(self):
        bot.trade_state['amount'] = 1.0
        with mock.patch.object(bot.exchange, 'create_order', return_value={'id': 'stop-1'}) as create_order:
            bot.place_protective_stop_order('long', 90.0)
        params = create_order.call_args.args[5]
        self.assertTrue(params['newClientOrderId'].startswith(bot.STRATEGY_STOP_CLIENT_PREFIX))

    def test_manual_ai_guard_uses_symbol_specific_bias_file(self):
        with mock.patch.object(bot, 'load_ai_research_guard', return_value={'valid': False}) as load_guard:
            bot.get_ai_guard_for_symbol('BTC/USDT')
        self.assertEqual(load_guard.call_args.kwargs['expected_symbol'], 'BTCUSDT')
        self.assertTrue(load_guard.call_args.kwargs['bias_file'].endswith('latest_bias_BTCUSDT.json'))

    def test_manual_ai_reduction_can_only_reduce_existing_position(self):
        state = {
            'key': 'BTC/USDT|BOTH|LONG',
            'symbol': 'BTC/USDT',
            'side': 'long',
            'position_side': 'BOTH',
            'amount': 2.0,
            'ai_initial_amount': 2.0,
            'ai_last_reduce_generated_at': '',
            'ai_partial_reduce_count': 0,
            'ai_partial_reduce_amount': 0.0,
            'stop_loss_price': 90.0,
        }
        guard = {'valid': True, 'generated_at': '2026-08-17T04:00:00+00:00'}
        decision = {
            'should_reduce': True,
            'reduce_amount': 0.5,
            'shadow_action': 'would_reduce',
            'actual_action': 'reduce',
        }
        with (
            mock.patch.object(bot, 'ensure_manual_ai_research_job', return_value=guard),
            mock.patch.object(bot, 'evaluate_position_reduction', return_value=decision),
            mock.patch.object(bot, 'write_ai_audit'),
            mock.patch.object(bot.exchange, 'amount_to_precision', return_value='0.5'),
            mock.patch.object(bot, 'manual_exchange_min_amount', return_value=0.001),
            mock.patch.object(bot.exchange, 'create_order', return_value={'id': 'reduce-1', 'filled': 0.5}) as create_order,
            mock.patch.object(bot, 'refresh_manual_protective_stop_order', return_value=True) as refresh_stop,
        ):
            reduced = bot.apply_manual_ai_position_guard(state, {}, 100.0)
        self.assertTrue(reduced)
        self.assertEqual(state['amount'], 1.5)
        self.assertEqual(state['ai_last_reduce_generated_at'], guard['generated_at'])
        self.assertEqual(create_order.call_args.args[2], 'sell')
        self.assertEqual(create_order.call_args.args[5], {'reduceOnly': True})
        refresh_stop.assert_called_once()

    def test_manual_ai_background_job_uses_actual_symbol(self):
        state = {}
        fake_process = mock.Mock(pid=1234)
        fake_process.poll.return_value = None
        with (
            mock.patch.object(bot, 'MANUAL_AI_RESEARCH_ENABLED', True),
            mock.patch.object(bot, 'get_ai_guard_for_symbol', return_value={'valid': False, 'error': 'missing'}),
            mock.patch.object(bot, 'reap_manual_ai_jobs'),
            mock.patch.object(bot.os, 'makedirs'),
            mock.patch('builtins.open', mock.mock_open()),
            mock.patch.object(bot.subprocess, 'Popen', return_value=fake_process) as popen,
        ):
            bot.ensure_manual_ai_research_job('SOL/USDT', state=state)
        command = popen.call_args.args[0]
        self.assertIn('SOLUSDT', command)
        self.assertEqual(state['ai_research_status'], 'running')

    def test_strategy_eth_position_also_schedules_symbol_research(self):
        position = {'side': 'long', 'position_amt': 1.0}
        guard = {'valid': False, 'generated_at': '', 'error': 'missing'}
        decision = {
            'should_reduce': False,
            'shadow_action': 'would_hold',
            'actual_action': 'hold',
            'target_ratio': 1.0,
        }
        with (
            mock.patch.object(bot, 'ensure_manual_ai_research_job', return_value=guard) as ensure_job,
            mock.patch.object(bot, 'evaluate_position_reduction', return_value=decision),
            mock.patch.object(bot, 'write_ai_audit'),
        ):
            result = bot.apply_ai_position_guard(position)
        self.assertIs(result, position)
        ensure_job.assert_called_once_with(bot.SYMBOL, state=bot.trade_state)

    def test_eth_and_btc_research_jobs_use_independent_processes(self):
        eth_process = mock.Mock(pid=1001)
        btc_process = mock.Mock(pid=1002)
        eth_process.poll.return_value = None
        btc_process.poll.return_value = None
        with (
            mock.patch.object(bot, 'MANUAL_AI_RESEARCH_ENABLED', True),
            mock.patch.object(bot, 'get_ai_guard_for_symbol', return_value={'valid': False, 'error': 'missing'}),
            mock.patch.object(bot, 'reap_manual_ai_jobs'),
            mock.patch.object(bot.os, 'makedirs'),
            mock.patch('builtins.open', mock.mock_open()),
            mock.patch.object(bot.subprocess, 'Popen', side_effect=[eth_process, btc_process]) as popen,
        ):
            bot.ensure_manual_ai_research_job('ETH/USDT', state={})
            bot.ensure_manual_ai_research_job('BTC/USDT', state={})
        commands = [call.args[0] for call in popen.call_args_list]
        self.assertIn('ETHUSDT', commands[0])
        self.assertIn('BTCUSDT', commands[1])
        self.assertEqual(set(bot.runtime_state['manual_ai_jobs']), {'ETHUSDT', 'BTCUSDT'})

    def test_invalid_bias_runs_immediately_then_returns_to_fixed_schedule(self):
        entry_time = '2026-08-17 10:00:00'
        invalid_guard = {'valid': False, 'generated_at': '', 'error': 'missing'}
        immediate_reason = bot.ai_research_refresh_reason(
            invalid_guard,
            state={'entry_time': entry_time},
            now_dt=datetime(2026, 8, 17, 10, 0, tzinfo=bot.EXCHANGE_TZ),
        )
        self.assertEqual(immediate_reason, 'invalid_bias_immediate')

        generated_at_ten = {
            'valid': True,
            'generated_at': '2026-08-17T10:05:00+08:00',
        }
        before_slot = bot.ai_research_refresh_reason(
            generated_at_ten,
            state={'entry_time': entry_time},
            now_dt=datetime(2026, 8, 17, 13, 49, tzinfo=bot.EXCHANGE_TZ),
        )
        at_slot = bot.ai_research_refresh_reason(
            generated_at_ten,
            state={'entry_time': entry_time},
            now_dt=datetime(2026, 8, 17, 13, 50, tzinfo=bot.EXCHANGE_TZ),
        )
        self.assertEqual(before_slot, '')
        self.assertEqual(at_slot, 'scheduled_20260817_1350')

    def test_position_does_not_backfill_schedule_before_it_was_opened(self):
        guard = {'valid': True, 'generated_at': '2026-08-17T13:00:00+08:00'}
        reason = bot.ai_research_refresh_reason(
            guard,
            state={'detected_time': '2026-08-17 14:00:00'},
            now_dt=datetime(2026, 8, 17, 14, 0, tzinfo=bot.EXCHANGE_TZ),
        )
        self.assertEqual(reason, '')

    def test_fixed_schedule_rolls_across_midnight(self):
        guard = {'valid': True, 'generated_at': '2026-08-17T20:57:00+08:00'}
        state = {'entry_time': '2026-08-17 10:00:00'}
        before_morning = bot.ai_research_refresh_reason(
            guard,
            state=state,
            now_dt=datetime(2026, 8, 18, 5, 49, tzinfo=bot.EXCHANGE_TZ),
        )
        morning_slot = bot.ai_research_refresh_reason(
            guard,
            state=state,
            now_dt=datetime(2026, 8, 18, 5, 50, tzinfo=bot.EXCHANGE_TZ),
        )
        self.assertEqual(before_morning, '')
        self.assertEqual(morning_slot, 'scheduled_20260818_0550')


class ManualTradeLedgerPersistenceTests(unittest.TestCase):
    def test_manual_trade_csv_and_sqlite_source_breakdown(self):
        state = {
            'key': 'SOL/USDT|BOTH|LONG',
            'manual_trade_id': 'manual-ledger-test',
            'symbol': 'SOL/USDT',
            'position_side': 'BOTH',
            'side': 'long',
            'detected_time': '2026-08-17 10:00:00',
            'initial_entry_price': 100.0,
            'entry_price': 100.0,
            'initial_amount': 2.0,
            'max_amount': 2.0,
            'amount': 2.0,
            'reduce_count': 0,
        }
        settlement = {
            'exit_time': '2026-08-17 11:00:00',
            'exit_price': 105.0,
            'pnl_points': 5.0,
            'realized_pnl_usdt': 10.0,
            'fee_cost': 0.2,
            'net_pnl_usdt': 9.8,
            'open_order_ids': ['open-1'],
            'close_order_ids': ['close-1'],
            'data_source': 'Binance成交明细',
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / 'trade_stats.db')
            with (
                mock.patch.object(bot, 'BASE_DIR', temp_dir),
                mock.patch.object(bot, 'STATS_DB_PATH', db_path),
            ):
                self.assertTrue(bot.persist_manual_position_state(state))
                self.assertEqual(
                    bot.load_persisted_manual_position_states()[state['key']]['manual_trade_id'],
                    'manual-ledger-test',
                )
                self.assertTrue(bot.log_manual_trade_to_csv(state, settlement, '人工平仓'))
                self.assertTrue(bot.log_manual_trade_to_csv(state, settlement, '人工平仓'))

                csv_path = Path(temp_dir) / 'manual_trades_log_2026-08.csv'
                with csv_path.open('r', newline='', encoding='utf-8-sig') as csv_file:
                    rows = list(csv.DictReader(csv_file))
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]['交易对'], 'SOL/USDT')
                self.assertEqual(rows[0]['净利润(USDT)'], '9.8')

                with sqlite3.connect(db_path) as conn:
                    daily = conn.execute(
                        "SELECT pnl, trade_count, manual_pnl, manual_trade_count FROM daily_pnl"
                    ).fetchone()
                self.assertEqual(daily, (9.8, 1, 9.8, 1))
                self.assertTrue(bot.delete_persisted_manual_position_state(state['key']))
                self.assertEqual(bot.load_persisted_manual_position_states(), {})


if __name__ == '__main__':
    unittest.main()
