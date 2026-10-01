"""Exact UP book: one initial snapshot, then level changes in arrival order."""
from decimal import Decimal, InvalidOperation

from collection_store import encode, record


def decimal(value, *, price=False):
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        raise ValueError("Invalid decimal") from None
    if not number.is_finite() or number < 0 or (price and number > 1):
        raise ValueError("Invalid price/size")
    return number


def exact(value):
    return format(value, "f")


class Book:
    def __init__(self, market, asset):
        self.market, self.asset = market, asset
        self.levels = {}
        self.ready = False
        self.snapshot_saved = False
        self.valid = False
        self.bad_since_ns = 0
        self.needs_snapshot = False

    def start_connection(self):
        # Retain the last saved state so a fresh server snapshot can be stored
        # as corrections, but reject deltas until that snapshot arrives.
        self.ready = False
        self.valid = False
        self.bad_since_ns = 0
        self.needs_snapshot = True

    def require_snapshot(self):
        self.needs_snapshot = True
        self.valid = False

    def best(self):
        bids = [p for (side, p), size in self.levels.items() if side == "bid" and size > 0]
        asks = [p for (side, p), size in self.levels.items() if side == "ask" and size > 0]
        return max(bids) if bids else None, min(asks) if asks else None

    def check(self, ctx, kind, details=None, expected=None):
        bid, ask = self.best()
        crossed = bid is not None and ask is not None and bid > ask
        mismatch = False
        if expected:
            # Venue uses 0/1 for empty sides. A populated reported side must
            # also be flagged when our reconstructed side is empty.
            mismatch = any(key in expected and decimal(expected[key], price=True) != (actual if actual is not None else empty)
                           for actual, key, empty in [(bid, "best_bid", Decimal(0)), (ask, "best_ask", Decimal(1))])
        # The venue emits BBO before the associated level changes, sometimes
        # splitting one matching-engine operation across several messages.
        # Mark each intermediate state, but let later consistent deltas recover.
        # An independent BBO notification must not poison the L2 state.
        if kind in {"snapshot", "delta"}:
            self.valid = self.ready and not self.needs_snapshot and not crossed and not mismatch
            if self.valid:
                self.bad_since_ns = 0
            elif self.ready and not self.bad_since_ns:
                self.bad_since_ns = ctx["recv_ts_ns"]
        resync = bool(self.bad_since_ns and ctx["recv_ts_ns"] - self.bad_since_ns > 2_000_000_000)
        return record("book_checks_v2", ctx, market=self.market, asset_id=self.asset, kind=kind,
                      best_bid=exact(bid) if bid is not None else "", best_ask=exact(ask) if ask is not None else "",
                      valid=int(self.valid), details=encode({**(details or {}), "crossed": crossed, "bbo_mismatch": mismatch,
                          "reported_bid": str((expected or {}).get("best_bid", "")),
                          "reported_ask": str((expected or {}).get("best_ask", "")), "resync_required": resync}))

    def process(self, msg, ctx):
        relevant = msg.get("asset_id") == self.asset or any(
            change.get("asset_id") == self.asset for change in msg.get("price_changes", []))
        # custom_feature_enabled also broadcasts market lifecycle events for
        # other assets. Preserve those in raw storage, without reconnecting.
        if relevant and msg.get("market") and msg["market"] != self.market:
            raise ValueError("Message belongs to another market")
        event = msg.get("event_type")
        if event == "book" and msg.get("asset_id") == self.asset:
            if msg.get("snapshot_levels_omitted"):
                raise ValueError("Raw book header has no levels; replay the saved initial snapshot and deltas")
            new = {}
            for side, key in [("bid", "bids"), ("ask", "asks")]:
                levels = msg.get(key, [])
                if isinstance(levels, str):
                    import json
                    levels = json.loads(levels)
                for level in levels:
                    price, size = decimal(level["price"], price=True), decimal(level["size"])
                    if size:
                        new[(side, price)] = new.get((side, price), Decimal(0)) + size
            initial = not self.snapshot_saved
            reason = "initial" if initial else "reconnect" if not self.ready else "resync" if self.needs_snapshot else "refresh"
            changed = (sorted(k for k in self.levels.keys() | new.keys()
                              if self.levels.get(k, 0) != new.get(k, 0)) if not initial else [])
            records = []
            if not initial:
                for index, (side, price) in enumerate(changed):
                    size = new.get((side, price), Decimal(0))
                    delta = size - self.levels.get((side, price), Decimal(0))
                    records.append(record("pricechange_v2", ctx, row_index=index, market=self.market, asset_id=self.asset,
                                          hash=str(msg.get("hash", "")), side=side, price=exact(price),
                                          size=exact(size), delta=exact(delta)))
            self.levels, self.ready, self.valid = new, True, True
            self.needs_snapshot = False
            check = self.check(ctx, "snapshot", {"changed_levels": len(changed), "reason": reason,
                                                "stored_as": "snapshot" if initial else "deltas"})
            if initial:
                sides = {s: [[exact(p), exact(v)] for (side, p), v in sorted(new.items()) if side == s] for s in ["bid", "ask"]}
                records.append(record("orderbook_v2", ctx, market=self.market, asset_id=self.asset, hash=str(msg.get("hash", "")),
                                      bids=encode(sides["bid"]), asks=encode(sides["ask"]), reason=reason, valid=int(self.valid)))
                self.snapshot_saved = True
            return [*records, check]
        if event == "price_change":
            changes = [pc for pc in msg.get("price_changes", []) if pc.get("asset_id") == self.asset]
            if not changes:
                return []
            if not self.ready:
                return [self.check(ctx, "before_snapshot", {"count": len(changes)})]
            # Validate the whole message before changing any state.
            parsed = []
            for index, pc in enumerate(changes):
                side = {"BUY": "bid", "SELL": "ask"}.get(pc["side"])
                if side is None:
                    raise ValueError("Invalid book side")
                for key in ("best_bid", "best_ask"):
                    if key in pc:
                        decimal(pc[key], price=True)
                parsed.append((index, pc, side, decimal(pc["price"], price=True), decimal(pc["size"])))
            records = []
            for index, pc, side, price, size in parsed:
                key = side, price
                delta = size - self.levels.get(key, Decimal(0))
                if size:
                    self.levels[key] = size
                else:
                    self.levels.pop(key, None)
                # Absolute size survives retries and permits idempotent replay.
                if delta:
                    records.append(record("pricechange_v2", ctx, row_index=index, market=self.market, asset_id=self.asset,
                                          hash=str(pc.get("hash", "")), side=side, price=exact(price), size=exact(size), delta=exact(delta)))
            records.append(self.check(ctx, "delta", expected=changes[-1]))
            return records
        if msg.get("asset_id") != self.asset:
            return []
        if event == "best_bid_ask":
            return [self.check(ctx, "venue_bbo", expected=msg)]
        if event == "last_trade_price":
            return [record("trades_v2", ctx, market=self.market, asset_id=self.asset,
                           price=exact(decimal(msg["price"], price=True)), size=exact(decimal(msg.get("size", 0))),
                           side=str(msg.get("side", "")), transaction_hash=str(msg.get("transaction_hash", "")),
                           fee_rate_bps=str(msg.get("fee_rate_bps", "")))]
        if event == "tick_size_change":
            return [self.check(ctx, "tick_size_change", {"old": msg.get("old_tick_size"), "new": msg.get("new_tick_size")})]
        if event == "market_resolved":
            return [self.check(ctx, "market_resolved", msg)]
        return []
