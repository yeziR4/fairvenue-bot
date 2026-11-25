"""Offline test of the live code path against a fake client. No network, no key.

Run: python test_bot.py
"""
import asyncio
import tempfile
import time
from decimal import Decimal as D
from pathlib import Path

import bot
from fairvenue_schema6 import AgentError

bot.HOME = Path(tempfile.mkdtemp())
(bot.HOME / "logs").mkdir()


def market(mark, lp_mid, extra_bids=(), extra_asks=(), age_ms=500):
    bids = sorted([(lp_mid - 20 - k, D("0.001")) for k in range(5)] + list(extra_bids), reverse=True)
    asks = sorted([(lp_mid + 20 + k, D("0.001")) for k in range(5)] + list(extra_asks))
    raw = {"bids": [{"price": str(p), "quantity": str(q)} for p, q in bids],
           "asks": [{"price": str(p), "quantity": str(q)} for p, q in asks],
           "mark_price": str(mark), "mark_observed_at_ms": str(market.t),
           "protocol_time_ms": str(market.t + age_ms),
           "admission": {"open": True, "reasons": []},
           "market_rules": {"tick_size": "0.1", "step_size": "0.001"},
           "fee_schedule": {"fixed_taker_rate": "0.00012"}, "trades": []}
    market.t += 10000
    return bot.Market(raw, time.monotonic())


market.t = 1_000_000


class FakeClient:
    """Fills IOC orders against the book handed to it, rests GTC orders."""

    def __init__(self):
        self.book = None
        self.position, self.entry, self.wallet = D(0), None, D(1000)
        self.orders, self.fills, self.sent = [], [], []
        self.fail_next = None
        self.pending = False
        self.n = 0

    def has_pending_request(self):
        return self.pending

    async def recover_original(self, timeout_ms=15000):
        self.pending = False
        return {"status": "accepted", "reason": None}

    def _fill(self, side, price, qty, role, order_id):
        sign = 1 if side == "BUY" else -1
        new = self.position + sign * qty
        if self.position == 0 or (self.position > 0) == (sign > 0):
            total = abs(self.position) * (self.entry or D(0)) + qty * price
            self.entry = total / abs(new)
        else:
            closed = min(qty, abs(self.position))
            self.wallet += closed * (price - self.entry) * (1 if self.position > 0 else -1)
            if abs(new) < D("1e-12"):
                self.entry = None
            elif (new > 0) != (self.position > 0):
                self.entry = price
        self.position = new
        self.wallet -= price * qty * D("0.00012")
        self.fills.append({"position": str(len(self.fills) + 1), "lineage": "5", "at_ms": "1",
                           "kind": "fill", "role": role, "side": side, "order_id": order_id,
                           "price": str(price), "quantity": str(qty), "fee": "0.01"})

    async def submit(self, action, lifetime_ms, timeout_ms=60000):
        self.sent.append(action)
        if self.fail_next:
            code, self.fail_next = self.fail_next, None
            if code == "outcome_unknown":
                self.pending = True
            raise AgentError(code)
        self.n += 1
        order_id = "0x" + f"{self.n:064x}"
        if action["type"] == "place":
            o = action["order"]
            price, qty = D(o["price"]), D(o["quantity"])
            assert price == (price / D("0.1")).to_integral_value() * D("0.1"), "off tick"
            assert qty == (qty / D("0.001")).to_integral_value() * D("0.001"), "off step"
            if o["time_in_force"] == "IOC":
                levels = self.book.asks if o["side"] == "BUY" else self.book.bids
                left = qty
                for p, size in levels:
                    ok = p <= price if o["side"] == "BUY" else p >= price
                    if not ok or left <= 0:
                        break
                    take = min(size, left)
                    self._fill(o["side"], p, take, "taker", order_id)
                    left -= take
            else:
                assert o["post_only"] and o["reduce_only"]
                self.orders.append({"order_id": order_id, "status": "resting", "side": o["side"],
                                    "price": o["price"], "remaining": o["quantity"]})
        elif action["type"] == "cancel":
            for o in self.orders:
                if o["order_id"] == action["order_id"]:
                    o["status"] = "canceled"
        elif action["type"] == "kill_all":
            for o in self.orders:
                o["status"] = "canceled"
        return {"ack": {"order_id": order_id}, "recovery": {"status": "accepted"}}

    async def account(self, with_context=True, timeout_ms=15000):
        return {"context": None, "view": {
            "account": {"wallet_balance": str(self.wallet), "position_quantity": str(self.position),
                        "position_entry_price": None if self.entry is None else str(self.entry)},
            "orders": [dict(o) for o in self.orders], "fills": list(self.fills)}}

    def close(self):
        pass


async def main():
    client = FakeClient()
    b = bot.Bot(dict(bot.DEFAULTS, exit_min_interval_s=0.0), "live", client)

    async def tick(m):
        client.book = m
        await b.step(m)
        return m

    # 1. mark jumps above the stale LP asks: buy up to the per-order cap
    await tick(market(D("60245"), D("60200")))
    assert client.position == D("0.005"), client.position
    assert b.position == D("0.005")
    # 2. LP caught up: nothing to take, so rest an exit ask beyond the mark
    await tick(market(D("60245"), D("60245")))
    assert b.quote and b.quote["side"] == "SELL" and b.quote["price"] == D("60251"), b.quote
    # 3. mark drifts a little: no requote below the move threshold
    sent = len(client.sent)
    await tick(market(D("60248"), D("60245")))
    assert len(client.sent) == sent
    # 4. mark moves a lot: cancel and re-place
    await tick(market(D("60262"), D("60248")))
    assert b.quote["price"] == D("60268"), b.quote
    # 5. the exit quote gets filled by someone else: bot sees it and goes flat
    client.orders[-1]["status"] = "filled"
    client._fill("SELL", D("60268"), D("0.005"), "maker", client.orders[-1]["order_id"])
    b.last_account = 0
    await tick(market(D("60262"), D("60262")))
    assert b.position == 0 and b.quote is None
    assert client.wallet > 1000, client.wallet
    # 6. mark drops below the stale bids: sell, then a rate limit is respected
    await tick(market(D("60200"), D("60262")))
    assert client.position == D("-0.005")
    client.fail_next = "rate_limited"
    sent = len(client.sent)
    await tick(market(D("60140"), D("60200")))
    assert client.position == D("-0.005") and len(client.sent) == sent + 1
    await tick(market(D("60080"), D("60140")))          # still backing off
    assert len(client.sent) == sent + 1
    b.cmd_backoff_until = 0
    # 7. unknown outcome blocks all signing until the exact lookup resolves it
    client.fail_next = "outcome_unknown"
    await tick(market(D("60020"), D("60080")))
    assert b.blocked
    sent = len(client.sent)
    await tick(market(D("59960"), D("60020")))          # this step only recovers
    assert not b.blocked and len(client.sent) == sent
    # 8. inventory cap: never beyond max_position
    for i in range(6):
        b.cmd_backoff_until = 0
        await tick(market(D("59900") - 60 * i, D("59960") - 60 * i))
    assert abs(client.position) <= D("0.010"), client.position
    # 9. day-loss guard: only reducing trades once the day is deep in the red
    b.day_start_equity = b.equity(client.book) + D("50")
    before = client.position
    b.cmd_backoff_until = 0
    await tick(market(D("59400"), D("59500")))
    assert abs(client.position) <= abs(before)
    # 10. shutdown cancels working orders
    await b.shutdown()
    assert client.sent[-1]["type"] == "kill_all"
    print("all offline checks passed; orders sent:", len(client.sent))


asyncio.run(main())
