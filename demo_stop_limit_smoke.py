"""Create, inspect, and cancel one isolated Binance Demo STOP_LIMIT order."""
import uuid

import bian_new as strategy


def main():
    if strategy.BINANCE_TRADING_MODE != "demo":
        raise RuntimeError("This smoke test only runs with BINANCE_TRADING_MODE=demo")

    exchange = strategy.exchange
    exchange.load_markets()
    snapshot = strategy.fetch_all_account_positions()
    if snapshot.get("fetch_failed") or snapshot.get("positions"):
        raise RuntimeError(f"Demo account must have no positions: {snapshot}")
    initial_orders = strategy.fetch_all_open_orders_for_symbol()
    if initial_orders is None:
        raise RuntimeError("Could not confirm Demo open ETH orders before the smoke test")
    if initial_orders:
        raise RuntimeError("Demo account must have no open ETH orders before the smoke test")

    ticker = exchange.fetch_ticker(strategy.SYMBOL)
    current = float(ticker["last"])
    market = exchange.market(strategy.SYMBOL)
    min_amount = float(market.get("limits", {}).get("amount", {}).get("min") or 0.001)
    min_cost = float(market.get("limits", {}).get("cost", {}).get("min") or 20.0)
    amount = float(exchange.amount_to_precision(strategy.SYMBOL, max(min_amount, min_cost * 1.1 / current)))
    trigger = float(exchange.price_to_precision(strategy.SYMBOL, current * 1.05))
    limit_price = float(exchange.price_to_precision(strategy.SYMBOL, current * 1.052))
    client_id = f"{strategy.STRATEGY_ENTRY_CLIENT_PREFIX}TEST_{uuid.uuid4().hex[:16]}"[:36]
    order = None

    try:
        order = exchange.create_order(
            strategy.SYMBOL,
            strategy.ENTRY_STOP_LIMIT_ORDER_TYPE,
            "buy",
            amount,
            limit_price,
            {
                "stopPrice": trigger,
                "timeInForce": "GTC",
                "workingType": strategy.STOP_WORKING_TYPE,
                "newClientOrderId": client_id,
            },
        )
        open_orders = strategy.fetch_all_open_orders_for_symbol() or []
        matched = [item for item in open_orders if strategy.extract_order_client_id(item) == client_id]
        if len(matched) != 1:
            raise RuntimeError(f"Expected one owned Demo order, found {len(matched)}")
        inspected = matched[0]
        print({
            "mode": strategy.BINANCE_TRADING_MODE,
            "order_id": strategy.extract_order_id(inspected),
            "client_id": strategy.extract_order_client_id(inspected),
            "type": inspected.get("type"),
            "side": inspected.get("side"),
            "trigger": strategy.extract_order_stop_price(inspected),
            "limit": inspected.get("price") or inspected.get("info", {}).get("price"),
            "amount": amount,
            "current": current,
        })
    finally:
        for item in strategy.fetch_all_open_orders_for_symbol() or []:
            if strategy.extract_order_client_id(item) == client_id:
                strategy.cancel_conditional_order_exact(
                    item,
                    silent=False,
                    reason="Binance Demo STOP_LIMIT smoke cleanup",
                )
        position = strategy.get_position_risk(side="long")
        if position and not position.get("fetch_failed"):
            exchange.create_market_order(
                strategy.SYMBOL,
                "sell",
                abs(float(position["position_amt"])),
                {"reduceOnly": True},
            )

    remaining = [
        item for item in (strategy.fetch_all_open_orders_for_symbol() or [])
        if strategy.extract_order_client_id(item) == client_id
    ]
    if remaining:
        raise RuntimeError(f"Demo cleanup failed: {remaining}")
    print("demo_stop_limit_smoke=ok")


if __name__ == "__main__":
    main()
