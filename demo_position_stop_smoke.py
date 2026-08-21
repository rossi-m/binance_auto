"""Open and close one small Binance Demo position with a protective stop."""
import time

import bian_new as strategy


def fetch_native_open_orders():
    regular = strategy.fetch_raw_future_open_orders()
    algo = strategy.fetch_raw_future_open_algo_orders()
    if regular is None or algo is None:
        raise RuntimeError("Could not confirm all Binance Demo open-order sources")
    return [*(regular or []), *(algo or [])]


def wait_for_position(side, attempts=10):
    for _attempt in range(attempts):
        position = strategy.get_position_risk(side=side)
        if position and not position.get("fetch_failed"):
            return position
        time.sleep(0.5)
    return None


def wait_until_flat(attempts=10):
    for _attempt in range(attempts):
        snapshot = strategy.fetch_all_account_positions()
        if not snapshot.get("fetch_failed") and not snapshot.get("positions"):
            return True
        time.sleep(0.5)
    return False


def cancel_owned_test_stops():
    for order in fetch_native_open_orders():
        if strategy.is_script_owned_protective_order(order):
            if not strategy.cancel_conditional_order_exact(
                order,
                silent=False,
                reason="Binance Demo position smoke cleanup",
            ):
                raise RuntimeError(
                    f"Could not cancel owned Demo protective stop: {strategy.extract_order_id(order)}"
                )


def close_demo_long(exchange):
    position = strategy.get_position_risk(side="long")
    if not position or position.get("fetch_failed"):
        return None
    amount = abs(float(position.get("position_amt") or 0.0))
    if amount <= 0:
        return None
    return exchange.create_market_order(
        strategy.SYMBOL,
        "sell",
        amount,
        {"reduceOnly": True},
    )


def main():
    if strategy.BINANCE_TRADING_MODE != "demo":
        raise RuntimeError("This smoke test only runs with BINANCE_TRADING_MODE=demo")

    exchange = strategy.exchange
    exchange.load_markets()
    snapshot = strategy.fetch_all_account_positions()
    if snapshot.get("fetch_failed") or snapshot.get("positions"):
        raise RuntimeError(f"Demo account must have no positions before test: {snapshot}")
    initial_orders = fetch_native_open_orders()
    if initial_orders:
        raise RuntimeError(f"Demo account must have no ETH orders before test: {initial_orders}")

    ticker = exchange.fetch_ticker(strategy.SYMBOL)
    current = float(ticker["last"])
    market = exchange.market(strategy.SYMBOL)
    min_amount = float(market.get("limits", {}).get("amount", {}).get("min") or 0.001)
    min_cost = float(market.get("limits", {}).get("cost", {}).get("min") or 20.0)
    amount = float(exchange.amount_to_precision(
        strategy.SYMBOL,
        max(min_amount, min_cost * 1.1 / current),
    ))

    entry_order = None
    stop_order = None
    close_order = None
    try:
        exchange.set_leverage(strategy.LEVERAGE, strategy.SYMBOL)
        entry_order = exchange.create_market_order(strategy.SYMBOL, "buy", amount)
        position = wait_for_position("long")
        if not position:
            raise RuntimeError("Demo market entry did not produce a visible long position")

        position_amount = abs(float(position["position_amt"]))
        entry_price = float(position["entry_price"])
        stop_price = float(exchange.price_to_precision(strategy.SYMBOL, entry_price * 0.95))
        strategy.trade_state.update({
            "has_position": True,
            "side": "long",
            "amount": position_amount,
        })
        stop_order = strategy.place_protective_stop_order("long", stop_price)
        stop_id = strategy.extract_order_id(stop_order)
        owned_stops = [
            order for order in fetch_native_open_orders()
            if strategy.is_script_owned_protective_order(order, tracked_order_id=stop_id)
        ]
        if len(owned_stops) != 1:
            raise RuntimeError(f"Expected one owned protective stop, found {len(owned_stops)}")

        print({
            "mode": strategy.BINANCE_TRADING_MODE,
            "entry_order_id": strategy.extract_order_id(entry_order),
            "entry_price": entry_price,
            "position_amount": position_amount,
            "protective_stop_id": stop_id,
            "protective_stop_price": stop_price,
            "protective_stop_client_id": strategy.extract_order_client_id(owned_stops[0]),
        })
    finally:
        cancel_owned_test_stops()
        close_order = close_demo_long(exchange)
        strategy.trade_state.update({
            "has_position": False,
            "side": None,
            "amount": 0.0,
        })

    if not wait_until_flat():
        raise RuntimeError("Demo position cleanup did not reach a flat account")
    remaining_orders = fetch_native_open_orders()
    if remaining_orders:
        raise RuntimeError(f"Demo order cleanup left open orders: {remaining_orders}")
    print({
        "close_order_id": strategy.extract_order_id(close_order),
        "positions": [],
        "open_orders": [],
        "demo_position_stop_smoke": "ok",
    })


if __name__ == "__main__":
    main()
