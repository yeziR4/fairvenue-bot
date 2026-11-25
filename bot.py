#!/usr/bin/env python3
"""FairVenue TS2 native TESTNET hackathon bot (mock tokens only).

Strategy
  1. Stale-quote taker: the reference LP quotes around the *previous* oracle
     mark. When the fresh mark has moved past a resting quote by more than
     fee + edge, take it with a price-bounded LIMIT IOC.
  2. Passive exit quote: while holding inventory, rest one post-only GTC order
     on the reducing side a few USDC beyond the mark (maker volume, cheap exit).

Modes
  --paper   public data only, no key, simulated fills (default if no key file)
  (default) dry run with the key: signed reads only, orders are logged, not sent
  --live    signs and sends orders

Stop with Ctrl+C: the bot cancels its working orders before exiting.
"""
from __future__ import annotations

import argparse
import asyncio
import http.client
import json
import logging
import logging.handlers
import os
import signal
import ssl
import sys
import time
from collections import deque
from datetime import datetime, timezone
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from pathlib import Path

D = Decimal
HOME = Path(os.environ.get("FV_BOT_HOME", Path(__file__).resolve().parent))
PINS_SHA256 = "ba25da81e3ebc9f6679a184993dbc4a64fd089684f54e174e8ad654b725232d4"
SETTLEMENT = "0xfd5e17a1a722e0890669aa8c71a024ea4e4277bd"
API_HOST = "app.fairvenue.xyz"
NOT_PLACED = ("rejected", "invalid_request", "rate_limited", "admission_closed")
OPEN_STATUSES = ("pending", "resting", "partially_filled")

DEFAULTS = {
    # --- taker ---
    "taker_enabled": True,
    "edge_open": "5",          # USDC per BTC required beyond the taker fee to open/add
    "edge_reduce": "0",        # ... to reduce inventory
    "max_position": "0.010",   # BTC, absolute inventory cap
    "max_order_qty": "0.005",  # BTC per taker order
    "mark_max_age_s": 8.0,     # ignore a mark older than this
    "ioc_lifetime_ms": 20000,
    # --- passive exit quote ---
    "exit_quote_enabled": True,
    "exit_half_spread": "6",   # USDC beyond the mark
    "exit_requote_move": "8",  # requote when the wanted price moved this much
    "exit_min_interval_s": 6.0,
    "quote_lifetime_ms": 90000,
    # --- pacing and safety ---
    "max_commands_per_min": 30,
    "account_poll_s": 20.0,
    "poll_fast_s": 0.7,
    "poll_slow_s": 2.0,
    "max_day_loss": "5",       # USDC: below this day PnL only reducing trades
    "taker_fee_rate": None,    # None: read fixed_taker_rate from the market
}


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_day(ms: int | None = None) -> str:
    return datetime.fromtimestamp((ms or now_ms()) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def fmt(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def to_tick(value: Decimal, tick: Decimal, up: bool) -> Decimal:
    return (value / tick).to_integral_value(rounding=ROUND_CEILING if up else ROUND_FLOOR) * tick


# --------------------------------------------------------------------------- market


class Market:
    def __init__(self, raw: dict, seen: float):
        self.seen = seen
        self.bids = [(D(x["price"]), D(x["quantity"])) for x in raw["bids"]]
        self.asks = [(D(x["price"]), D(x["quantity"])) for x in raw["asks"]]
        self.mark = D(raw["mark_price"]) if raw.get("mark_price") else None
        self.mark_at = int(raw["mark_observed_at_ms"]) if raw.get("mark_observed_at_ms") else None
        self.protocol_ms = int(raw["protocol_time_ms"])
        self.open = bool(raw["admission"]["open"])
        self.reasons = list(raw["admission"]["reasons"])
        rules, fees = raw["market_rules"], raw["fee_schedule"]
        self.tick, self.step = D(rules["tick_size"]), D(rules["step_size"])
        self.taker_fee = D(fees["fixed_taker_rate"])
        self.last_trade = raw["trades"][0]["trade_id"] if raw.get("trades") else None

    def mark_age(self) -> float:
        """Seconds since the oracle observation, by the server's own clock."""
        if self.mark_at is None:
            return 1e9
        return (self.protocol_ms - self.mark_at) / 1000 + (time.monotonic() - self.seen)


class PublicFeed:
    """Paced public reads of POST /api/v1/info native_market. No credentials."""

    def __init__(self):
        self._conn = None
        self.backoff_until = 0.0

    def _fetch(self) -> dict:
        body = b'{"type":"native_market"}'
        headers = {"Content-Type": "application/json", "Accept": "application/json",
                   "Cache-Control": "no-store", "User-Agent": "fv-bot/0.1",
                   "X-FairVenue-Testnet-Deployment": SETTLEMENT}
        for attempt in (1, 2):
            try:
                if self._conn is None:
                    self._conn = http.client.HTTPSConnection(
                        API_HOST, timeout=6, context=ssl.create_default_context())
                self._conn.request("POST", "/api/v1/info", body=body, headers=headers)
                response = self._conn.getresponse()
                data = response.read(262144)
                if response.status == 429:
                    raise RuntimeError("rate_limited")
                if response.status != 200:
                    raise RuntimeError(f"http_{response.status}")
                return json.loads(data)
            except RuntimeError:
                raise
            except Exception:
                try:
                    if self._conn is not None:
                        self._conn.close()
                finally:
                    self._conn = None
                if attempt == 2:
                    raise RuntimeError("unreachable") from None
        raise RuntimeError("unreachable")

    async def read(self) -> Market | None:
        if time.monotonic() < self.backoff_until:
            return None
        try:
            raw = await asyncio.wait_for(asyncio.to_thread(self._fetch), 10)
            if raw.get("symbol") != "BTCUSDC" or raw.get("settlement") != SETTLEMENT:
                raise RuntimeError("wrong_market")
            return Market(raw, time.monotonic())
        except Exception as problem:  # noqa: BLE001 - data errors must never kill the loop
            code = str(problem) or type(problem).__name__
            self.backoff_until = time.monotonic() + (5.0 if "rate_limited" in code else 1.5)
            logging.warning("public read failed: %s", code[:60])
            return None


# --------------------------------------------------------------------------- strategy (pure)


def taker_signal(market: Market, position: Decimal, cfg: dict, fee: Decimal,
                 own: dict | None, allow_open: bool = True):
    """Return (side, limit_price, quantity, edge) or None.

    Walks the book from the top while a level is mispriced against the mark by
    more than fee + edge. A level holding our own resting order stops the walk.
    """
    if market.mark is None or market.mark_age() > cfg["mark_max_age_s"]:
        return None
    mark = market.mark
    cap, per_order = D(cfg["max_position"]), D(cfg["max_order_qty"])
    edge_open, edge_reduce = D(cfg["edge_open"]), D(cfg["edge_reduce"])
    best = None
    for side, levels in (("BUY", market.asks), ("SELL", market.bids)):
        signed = position if side == "BUY" else -position   # >0 means this side adds
        reducing = max(D(0), -signed)                         # qty that only reduces
        room = (cap - signed) if allow_open else reducing
        room = min(room, per_order)
        qty, limit, first_edge = D(0), None, None
        for price, size in levels:
            if own and own["side"] != side and price == own["price"]:
                break  # never trade against our own resting order
            edge = (mark - price * (1 + fee)) if side == "BUY" else (price * (1 - fee) - mark)
            need = edge_reduce if qty < reducing else edge_open
            if edge < need or qty >= room:
                break
            take = min(size, room - qty)
            if qty < reducing:
                take = min(take, reducing - qty) if edge < edge_open else take
            take = (take / market.step).to_integral_value(rounding=ROUND_FLOOR) * market.step
            if take <= 0:
                break
            qty += take
            limit = price
            first_edge = edge if first_edge is None else first_edge
        if qty > 0 and (best is None or first_edge > best[3]):
            best = (side, limit, qty, first_edge)
    return best


def exit_quote(market: Market, position: Decimal, cfg: dict):
    """Wanted passive reducing quote (side, price, qty) or None when flat."""
    if position == 0 or market.mark is None or not market.bids or not market.asks:
        return None
    half, tick = D(cfg["exit_half_spread"]), market.tick
    qty = min(abs(position), D(cfg["max_order_qty"]))
    if position > 0:
        price = to_tick(max(market.mark + half, market.bids[0][0] + tick), tick, up=True)
        return ("SELL", price, qty)
    price = to_tick(min(market.mark - half, market.asks[0][0] - tick), tick, up=False)
    return ("BUY", price, qty)


# --------------------------------------------------------------------------- bot


class Bot:
    def __init__(self, cfg: dict, mode: str, client=None):
        self.cfg, self.mode, self.client = cfg, mode, client
        self.feed = PublicFeed()
        self.position = D(0)
        self.entry = None
        self.wallet = None
        self.quote = None            # {"order_id","side","price","qty","expires_ms"}
        self.blocked = False         # an original is pending: no signing until recovered
        self.commands = deque()      # monotonic times of signed commands
        self.cmd_backoff_until = 0.0
        self.last_account = 0.0
        self.last_quote_cmd = 0.0
        self.last_status = 0.0
        self.last_taker_mark_at = None
        self.seen_fills = set()
        self.stop = asyncio.Event()
        self.day, self.day_start_equity = None, None
        self.paper_cash = D(0)
        self.stats = {"taker_orders": 0, "taker_vol": D(0), "maker_vol": D(0), "fills": 0}
        self.state_path = HOME / "logs" / f"state-{mode}.json"
        self._load_state()

    # ---- persistence
    def _load_state(self):
        try:
            state = json.loads(self.state_path.read_text())
            self.day, start = state.get("day"), state.get("day_start_equity")
            self.day_start_equity = D(start) if start is not None else None
            self.seen_fills = set(state.get("seen_fills", [])[-500:])
            if self.mode == "paper":
                self.position = D(state.get("paper_position", "0"))
                self.paper_cash = D(state.get("paper_cash", "0"))
        except Exception:  # noqa: BLE001
            pass

    def _save_state(self):
        state = {"day": self.day,
                 "day_start_equity": None if self.day_start_equity is None else str(self.day_start_equity),
                 "seen_fills": sorted(self.seen_fills)[-500:]}
        if self.mode == "paper":
            state.update(paper_position=str(self.position), paper_cash=str(self.paper_cash))
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state))
        tmp.replace(self.state_path)

    # ---- accounting
    def equity(self, market: Market):
        if self.mode == "paper":
            return D(1000) + self.paper_cash + self.position * (market.mark or D(0))
        if self.wallet is None:
            return None
        unrealized = D(0)
        if self.position != 0 and self.entry is not None and market.mark is not None:
            unrealized = self.position * (market.mark - self.entry)
        return self.wallet + unrealized

    def day_pnl(self, market: Market):
        equity = self.equity(market)
        if equity is None:
            return None
        today = utc_day()
        if self.day != today or self.day_start_equity is None:
            self.day, self.day_start_equity = today, equity
            self._save_state()
            logging.info("new UTC day %s: start equity %s", today, fmt(equity))
        return equity - self.day_start_equity

    # ---- signed plumbing
    def _budget_ok(self) -> bool:
        cutoff = time.monotonic() - 60
        while self.commands and self.commands[0] < cutoff:
            self.commands.popleft()
        return (len(self.commands) < self.cfg["max_commands_per_min"]
                and time.monotonic() >= self.cmd_backoff_until)

    async def _recover(self):
        """Exact-original lookup only. Never resends, reprices or resigns."""
        from fairvenue_schema6 import AgentError
        try:
            if not self.client.has_pending_request():
                self.blocked = False
                return
            result = await self.client.recover_original(timeout_ms=15000)
            logging.warning("recovery: %s (%s)", result["status"], result.get("reason"))
            self.blocked = self.client.has_pending_request()
        except AgentError as error:
            logging.warning("recovery failed: %s", error.code)
            self.blocked = True

    async def _signed(self, action: dict, lifetime_ms: int, label: str):
        """Returns (status, result). status: accepted | <not_placed reason> | unknown | failed."""
        from fairvenue_schema6 import AgentError
        if self.mode != "live":
            logging.info("[%s] would send %s", self.mode, label)
            return "dry", None
        self.commands.append(time.monotonic())
        try:
            result = await self.client.submit(action, lifetime_ms=lifetime_ms, timeout_ms=25000)
            status = result["recovery"]["status"]
            if status != "accepted":
                logging.info("%s -> %s", label, status)
                return status, result
            return "accepted", result
        except AgentError as error:
            code = error.code
            if code in NOT_PLACED:
                if code == "rate_limited":
                    self.cmd_backoff_until = time.monotonic() + 10
                elif code == "admission_closed":
                    self.cmd_backoff_until = time.monotonic() + 5
                logging.info("%s -> not placed: %s", label, code)
                return code, None
            pending = True
            try:
                pending = self.client.has_pending_request()
            except AgentError:
                pass
            if pending:
                self.blocked = True
                logging.warning("%s -> outcome unknown, recovering before any new order", label)
                return "unknown", None
            logging.warning("%s -> failed before admission: %s", label, code)
            self.cmd_backoff_until = time.monotonic() + 2
            return "failed", None

    async def refresh_account(self, market: Market | None = None):
        from fairvenue_schema6 import AgentError
        if self.mode == "paper":
            return
        try:
            data = await self.client.account(with_context=False, timeout_ms=15000)
        except AgentError as error:
            logging.warning("account read failed: %s", error.code)
            return
        self.last_account = time.monotonic()
        view = data["view"]
        account = view["account"]
        if account is None:
            logging.warning("account not funded yet")
            return
        self.wallet = D(account["wallet_balance"])
        self.position = D(account["position_quantity"])
        self.entry = D(account["position_entry_price"]) if account["position_entry_price"] else None
        self.last_raw_account = account
        for fill in view["fills"]:
            key = f'{fill["lineage"]}:{fill["position"]}'
            if key in self.seen_fills:
                continue
            self.seen_fills.add(key)
            notional = D(fill["price"]) * D(fill["quantity"])
            self.stats["fills"] += 1
            self.stats["taker_vol" if fill["role"] == "taker" else "maker_vol"] += notional
            with open(HOME / "logs" / "fills.jsonl", "a") as out:
                out.write(json.dumps({k: fill[k] for k in (
                    "at_ms", "kind", "role", "side", "price", "quantity", "fee", "order_id")}) + "\n")
            logging.info("FILL %s %s %s @ %s fee %s", fill["role"], fill["side"],
                         fill["quantity"], fill["price"], fill["fee"])
        if self.quote is not None:
            mine = next((o for o in view["orders"] if o["order_id"] == self.quote["order_id"]), None)
            if mine is None or mine["status"] not in OPEN_STATUSES:
                self.quote = None
            elif mine["status"] != "pending":
                self.quote["qty"] = D(mine["remaining"])
        self.open_orders = [o for o in view["orders"] if o["status"] in OPEN_STATUSES]
        self._save_state()

    # ---- actions
    async def cancel_quote(self, why: str) -> bool:
        if self.quote is None:
            return True
        if self.mode != "live":
            self.quote = None
            return True
        status, _ = await self._signed({"type": "cancel", "order_id": self.quote["order_id"]},
                                       15000, f"cancel quote ({why})")
        if status in ("accepted", "rejected"):  # rejected: already gone
            self.quote = None
            return True
        return False

    async def do_taker(self, market: Market, signal):
        side, limit, qty, edge = signal
        label = f"IOC {side} {fmt(qty)} @ {fmt(limit)} (mark {fmt(market.mark)}, edge {fmt(edge.quantize(D('0.1')))})"
        self.last_taker_mark_at = market.mark_at
        if self.mode == "paper":
            fee = market.taker_fee
            filled, cost = D(0), D(0)
            for price, size in (market.asks if side == "BUY" else market.bids):
                if (side == "BUY" and price > limit) or (side == "SELL" and price < limit) or filled >= qty:
                    break
                take = min(size, qty - filled)
                filled += take
                cost += take * price
            sign = 1 if side == "BUY" else -1
            self.position += sign * filled
            self.paper_cash -= sign * cost + cost * fee
            self.stats["taker_orders"] += 1
            self.stats["taker_vol"] += cost
            logging.info("[paper] %s filled %s", label, fmt(filled))
            self._save_state()
            return
        order = {"symbol": "BTCUSDC", "side": side, "order_type": "LIMIT", "time_in_force": "IOC",
                 "price": fmt(limit), "quantity": fmt(qty), "reduce_only": False, "post_only": False}
        status, _ = await self._signed({"type": "place", "order": order},
                                       self.cfg["ioc_lifetime_ms"], label)
        if status == "accepted":
            self.stats["taker_orders"] += 1
            logging.info("%s -> accepted", label)
            await asyncio.sleep(0.4)  # FairLag hold, then matching
            await self.refresh_account(market)

    async def manage_quote(self, market: Market, allow: bool):
        cfg = self.cfg
        wanted = exit_quote(market, self.position, cfg) if (allow and cfg["exit_quote_enabled"]) else None
        current = self.quote
        if current is not None:
            expiring = current["expires_ms"] - now_ms() < 15000
            stale = (wanted is None or wanted[0] != current["side"]
                     or abs(wanted[1] - current["price"]) >= D(cfg["exit_requote_move"])
                     or wanted[2] != current["qty"] or expiring)
            if not stale:
                return
            urgent = wanted is None or wanted[0] != current["side"]
            if not urgent and time.monotonic() - self.last_quote_cmd < cfg["exit_min_interval_s"]:
                return
            if not self._budget_ok():
                return
            self.last_quote_cmd = time.monotonic()
            if not await self.cancel_quote("requote"):
                return
            await self.refresh_account(market)  # the quote may have filled meanwhile
            wanted = exit_quote(market, self.position, cfg) if (allow and cfg["exit_quote_enabled"]) else None
        if wanted is None or self.blocked or not self._budget_ok():
            return
        side, price, qty = wanted
        label = f"post-only {side} {fmt(qty)} @ {fmt(price)}"
        if self.mode != "live":
            if current is None or (side, price, qty) != (current["side"], current["price"], current["qty"]):
                logging.info("[%s] would rest %s", self.mode, label)
            self.quote = {"order_id": "0x-dry", "side": side, "price": price, "qty": qty,
                          "expires_ms": now_ms() + cfg["quote_lifetime_ms"]}
            return
        order = {"symbol": "BTCUSDC", "side": side, "order_type": "LIMIT", "time_in_force": "GTC",
                 "price": fmt(price), "quantity": fmt(qty), "reduce_only": True, "post_only": True}
        self.last_quote_cmd = time.monotonic()
        status, result = await self._signed({"type": "place", "order": order},
                                            cfg["quote_lifetime_ms"], label)
        if status == "accepted":
            self.quote = {"order_id": result["ack"]["order_id"], "side": side, "price": price,
                          "qty": qty, "expires_ms": now_ms() + cfg["quote_lifetime_ms"]}
            logging.info("%s -> resting", label)

    def paper_quote_fill(self, market: Market):
        """Paper mode: our dry quote fills when the opposite best crosses it."""
        quote = self.quote
        if self.mode != "paper" or quote is None:
            return
        # Conservative: only a visible trade through our price would fill us; skip simulation.
        if quote["expires_ms"] < now_ms():
            self.quote = None

    # ---- main loop
    async def step(self, market: Market):
        cfg = self.cfg
        if self.blocked:
            await self._recover()
            return
        pnl = self.day_pnl(market)
        allow_open = pnl is None or pnl > -D(cfg["max_day_loss"])
        if self.mode != "paper" and time.monotonic() - self.last_account > cfg["account_poll_s"]:
            await self.refresh_account(market)
        if not market.open:
            return
        fee = D(cfg["taker_fee_rate"]) if cfg["taker_fee_rate"] else market.taker_fee
        signal = None
        if cfg["taker_enabled"] and market.mark_at != self.last_taker_mark_at:
            signal = taker_signal(market, self.position, cfg, fee, self.quote, allow_open)
            if signal is None and self.quote is not None:
                # Would there be a signal if our own quote were out of the way?
                blocked_by_own = taker_signal(market, self.position, cfg, fee, None, allow_open)
                if blocked_by_own and blocked_by_own[0] != self.quote["side"] and self._budget_ok():
                    await self.cancel_quote("in the way of a taker order")
                    return
        if signal is not None and self._budget_ok():
            if self.quote is not None and self.quote["side"] != signal[0]:
                own = self.quote["price"]
                crosses = signal[1] >= own if signal[0] == "BUY" else signal[1] <= own
                if crosses and not await self.cancel_quote("would self-match"):
                    return
            await self.do_taker(market, signal)
            return
        await self.manage_quote(market, allow=True)
        self.paper_quote_fill(market)

    def status(self, market: Market):
        if time.monotonic() - self.last_status < 60:
            return
        self.last_status = time.monotonic()
        equity, pnl = self.equity(market), self.day_pnl(market)
        logging.info(
            "STATUS mode=%s pos=%s entry=%s mark=%s equity=%s day_pnl=%s taker_orders=%d "
            "taker_vol=%s maker_vol=%s cmds/min=%d quote=%s",
            self.mode, fmt(self.position), None if self.entry is None else fmt(self.entry),
            None if market.mark is None else fmt(market.mark),
            None if equity is None else fmt(equity.quantize(D("0.0001"))),
            None if pnl is None else fmt(pnl.quantize(D("0.0001"))),
            self.stats["taker_orders"], fmt(self.stats["taker_vol"].quantize(D("1"))),
            fmt(self.stats["maker_vol"].quantize(D("1"))), len(self.commands),
            None if self.quote is None else f'{self.quote["side"]} {fmt(self.quote["qty"])}@{fmt(self.quote["price"])}')
        if equity is not None:
            with open(HOME / "logs" / f"equity-{self.mode}.csv", "a") as out:
                out.write(f"{now_ms()},{fmt(equity)},{fmt(self.position)},{fmt(market.mark or D(0))}\n")

    async def run(self):
        cfg = self.cfg
        if self.mode != "paper":
            await self._recover()
            await self.refresh_account()
            logging.info("account: wallet=%s position=%s entry=%s open_orders=%d",
                         self.wallet, fmt(self.position), self.entry,
                         len(getattr(self, "open_orders", [])))
            if self.mode == "live" and getattr(self, "open_orders", []) and not self.blocked:
                await self._signed({"type": "kill_all"}, 15000, "kill_all (startup cleanup)")
        last_mark_at, next_mark_due = None, 0.0
        while not self.stop.is_set():
            market = await self.feed.read()
            if market is not None:
                if market.mark_at != last_mark_at:
                    last_mark_at = market.mark_at
                    # The oracle ticks every 10 s: poll fast around the next expected tick.
                    next_mark_due = time.monotonic() + 10 - min(market.mark_age(), 10)
                try:
                    await self.step(market)
                except Exception:  # noqa: BLE001 - keep running, never leak upstream text
                    logging.exception("step failed")
                    await asyncio.sleep(2)
                self.status(market)
            near_tick = -0.3 <= time.monotonic() - next_mark_due <= 4.0
            delay = cfg["poll_fast_s"] if near_tick else cfg["poll_slow_s"]
            try:
                await asyncio.wait_for(self.stop.wait(), delay)
            except asyncio.TimeoutError:
                pass
        await self.shutdown()

    async def shutdown(self):
        logging.info("stopping")
        if self.mode == "live" and self.client is not None:
            for _ in range(3):
                if self.blocked:
                    await self._recover()
                    await asyncio.sleep(2)
                    continue
                status, _ = await self._signed({"type": "kill_all"}, 15000, "kill_all (shutdown)")
                if status == "accepted":
                    break
                await asyncio.sleep(2)
        self._save_state()
        if self.client is not None:
            try:
                self.client.close()
            except Exception:  # noqa: BLE001
                pass


def build_client(cfg: dict):
    from fairvenue_schema6 import NativeTestnetClient, load_native_testnet_installation
    private = HOME / "private"
    custody = private / "continuity" / "original.sqlite"
    installed = load_native_testnet_installation(str(private / "installation.json"),
                                                 expected_sha256=PINS_SHA256)
    return NativeTestnetClient.from_credentials(
        str(private / "agent-key.json"), policy=installed.policy,
        approved_api_url=installed.api_url, custody_path=str(custody),
        initialize=not custody.exists())


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--paper", action="store_true", help="public data only, simulated fills")
    group.add_argument("--live", action="store_true", help="sign and send orders")
    parser.add_argument("--config", default=str(HOME / "config.json"))
    args = parser.parse_args()

    cfg = dict(DEFAULTS)
    if os.path.exists(args.config):
        cfg.update(json.loads(Path(args.config).read_text()))
    (HOME / "logs").mkdir(exist_ok=True)
    key_present = (HOME / "private" / "agent-key.json").exists()
    mode = "live" if args.live else "paper" if (args.paper or not key_present) else "dry"
    handlers = [logging.StreamHandler(sys.stdout),
                logging.handlers.RotatingFileHandler(HOME / "logs" / f"bot-{mode}.log",
                                                     maxBytes=5_000_000, backupCount=5)]
    logging.basicConfig(level=logging.INFO, handlers=handlers,
                        format="%(asctime)sZ %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
    logging.Formatter.converter = time.gmtime
    logging.info("starting in %s mode (TESTNET, mock tokens)", mode.upper())

    client = build_client(cfg) if mode != "paper" else None
    bot = Bot(cfg, mode, client)

    async def runner():
        loop = asyncio.get_running_loop()
        for name in ("SIGINT", "SIGTERM"):
            try:
                loop.add_signal_handler(getattr(signal, name), bot.stop.set)
            except (NotImplementedError, AttributeError):
                pass
        await bot.run()

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
