"""ws_ingest.py

Polymarket Market WS -> ClickHouse ingest (two-table mode).

Goal:
- Store the first UP orderbook snapshot per connection in table `orderbook`.
- Store UP price changes as deltas in table `pricechange`.

No analytics/modeling in this script.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlencode
import websockets
from ingest_common import (
    CH_DB, ch_http_command, ch_http_insert_json_each_row, http,
    normalize_messages, now_msk_naive, to_float, to_int, websocket_messages,
)

# -------------------- Env --------------------
WS_URL = os.getenv("POLY_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
ORIGIN = os.getenv("POLY_WS_ORIGIN", "https://polymarket.com")

POLY_TAG_ID = os.getenv("POLY_TAG_ID", "21")
POLY_ACTIVE = os.getenv("POLY_ACTIVE", "true").lower() in ("1", "true", "yes")
POLY_CLOSED = os.getenv("POLY_CLOSED", "false").lower() in ("1", "true", "yes")
POLY_LIMIT = max(1, int(os.getenv("POLY_LIMIT", "200")))
POLY_REFRESH_SEC = int(os.getenv("POLY_REFRESH_SEC", "60"))

POLY_SYMBOLS = [s.strip().upper() for s in os.getenv("POLY_SYMBOLS", "").split(",") if s.strip()]
POLY_TIMEFRAMES = [t.strip().lower() for t in os.getenv("POLY_TIMEFRAMES", "").split(",") if t.strip()]
POLY_MARKET_WHITELIST_IDS = {
    x.strip().lower()
    for x in os.getenv("POLY_MARKET_WHITELIST_IDS", "").split(",")
    if x.strip()
}
POLY_MARKET_EXCLUDE_REGEX = os.getenv("POLY_MARKET_EXCLUDE_REGEX", "").strip()
POLY_REQUIRE_BINARY = os.getenv("POLY_REQUIRE_BINARY", "true").lower() in ("1", "true", "yes")
POLY_REQUIRE_UP_DOWN = os.getenv("POLY_REQUIRE_UP_DOWN", "true").lower() in ("1", "true", "yes")
POLY_TRACK_CURRENT_ONLY = os.getenv("POLY_TRACK_CURRENT_ONLY", "true").lower() in ("1", "true", "yes")
POLY_MAX_MATCHED_MARKETS = int(os.getenv("POLY_MAX_MATCHED_MARKETS", "1"))
POLY_DEBUG_MARKETS = os.getenv("POLY_DEBUG_MARKETS", "true").lower() in ("1", "true", "yes")

ORDERBOOK_TABLE = os.getenv("ORDERBOOK_TABLE", "orderbook")
PRICECHANGE_TABLE = os.getenv("PRICECHANGE_TABLE", "pricechange")
INGEST_HEARTBEAT_TABLE = os.getenv("INGEST_HEARTBEAT_TABLE", "ingest_heartbeat")
INGEST_SERVICE_LOG_TABLE = os.getenv("INGEST_SERVICE_LOG_TABLE", "ingest_service_log")
ORDERBOOK_GRID_MIN = float(os.getenv("ORDERBOOK_GRID_MIN", "0.001"))
ORDERBOOK_GRID_MAX = float(os.getenv("ORDERBOOK_GRID_MAX", "0.999"))
ORDERBOOK_GRID_STEP = float(os.getenv("ORDERBOOK_GRID_STEP", "0.001"))

FLUSH_EVERY_N = int(os.getenv("FLUSH_EVERY_N", "200"))
FLUSH_EVERY_SEC = float(os.getenv("FLUSH_EVERY_SEC", "2"))
DEBUG_RAW = os.getenv("DEBUG_RAW", "false").lower() in ("1", "true", "yes")
INGEST_SERVICE_NAME = os.getenv("INGEST_SERVICE_NAME", "ws_ingest")
HEARTBEAT_ENABLED = os.getenv("HEARTBEAT_ENABLED", "true").lower() in ("1", "true", "yes")
HEARTBEAT_EVERY_SEC = float(os.getenv("HEARTBEAT_EVERY_SEC", "10"))
SERVICE_LOG_ENABLED = os.getenv("SERVICE_LOG_ENABLED", "true").lower() in ("1", "true", "yes")
SERVICE_LOG_FLUSH_SUCCESS = os.getenv("SERVICE_LOG_FLUSH_SUCCESS", "false").lower() in ("1", "true", "yes")
POLY_MARKET_EXCLUDE_RE = None
if POLY_MARKET_EXCLUDE_REGEX:
    try:
        POLY_MARKET_EXCLUDE_RE = re.compile(POLY_MARKET_EXCLUDE_REGEX, flags=re.IGNORECASE)
    except re.error:
        # Keep ingest alive even if regex in env is invalid.
        POLY_MARKET_EXCLUDE_RE = None

if ORDERBOOK_GRID_STEP <= 0:
    raise RuntimeError("ORDERBOOK_GRID_STEP must be > 0")
if ORDERBOOK_GRID_MAX < ORDERBOOK_GRID_MIN:
    raise RuntimeError("ORDERBOOK_GRID_MAX must be >= ORDERBOOK_GRID_MIN")

GRID_TICKS_COUNT = int(round((ORDERBOOK_GRID_MAX - ORDERBOOK_GRID_MIN) / ORDERBOOK_GRID_STEP))
GRID_TICKS: List[int] = list(range(GRID_TICKS_COUNT + 1))

SYMBOL_ALIASES: Dict[str, Tuple[str, ...]] = {
    "BTC": ("BTC", "BITCOIN", "XBT"),
    "ETH": ("ETH", "ETHER", "ETHEREUM"),
    "SOL": ("SOL", "SOLANA"),
    "XRP": ("XRP", "RIPPLE"),
}


# -------------------- Dynamic state from Gamma --------------------
CURRENT_ASSET_IDS: List[str] = []
OUTCOME_MAP: Dict[str, str] = {}  # asset_id -> UP/DOWN


def _pick_first_up_asset_id(asset_ids: List[str], outcome_map: Dict[str, str]) -> str:
    for aid in asset_ids:
        label = str(outcome_map.get(aid, "")).strip().lower()
        if "up" in label or "higher" in label or label == "yes":
            return aid
    return asset_ids[0] if asset_ids else ""


def price_to_tick(price: float) -> Optional[int]:
    if not math.isfinite(price) or price < ORDERBOOK_GRID_MIN - 1e-12 or price > ORDERBOOK_GRID_MAX + 1e-12:
        return None
    tick = int(round((price - ORDERBOOK_GRID_MIN) / ORDERBOOK_GRID_STEP))
    if tick < 0 or tick > GRID_TICKS_COUNT:
        return None
    return tick


def tick_to_price(tick: int) -> float:
    return round(ORDERBOOK_GRID_MIN + tick * ORDERBOOK_GRID_STEP, 6)


def is_up_asset(asset_id: str) -> bool:
    label = str(OUTCOME_MAP.get(str(asset_id), "")).strip().lower()
    if not label:
        return False
    if "up" in label or "higher" in label:
        return True
    return label in ("yes",)


def is_orderbook(msg: Dict[str, Any]) -> bool:
    event_type = str(msg.get("event_type", "")).lower()
    if event_type and event_type != "book":
        return False
    has_core = all(k in msg for k in ("asset_id", "market", "timestamp"))
    if not has_core:
        return False
    return ("bids" in msg and "asks" in msg) or ("buys" in msg and "sells" in msg)


def is_price_change(msg: Dict[str, Any]) -> bool:
    event_type = str(msg.get("event_type", "")).lower()
    if event_type and event_type != "price_change":
        return False
    return (
        "market" in msg
        and "timestamp" in msg
        and "price_changes" in msg
    )


def extract_book_level_sizes(msg: Dict[str, Any]) -> Tuple[Dict[int, float], Dict[int, float]]:
    """Extract bid/ask size maps from a book event, aggregated by grid tick."""
    bids = msg.get("bids") if "bids" in msg else msg.get("buys", [])
    asks = msg.get("asks") if "asks" in msg else msg.get("sells", [])
    if isinstance(bids, str):
        try:
            bids = json.loads(bids)
        except Exception:
            bids = []
    if isinstance(asks, str):
        try:
            asks = json.loads(asks)
        except Exception:
            asks = []

    bid_sizes_by_tick: Dict[int, float] = {}
    ask_sizes_by_tick: Dict[int, float] = {}
    if isinstance(bids, list):
        for lvl in bids:
            if not isinstance(lvl, dict):
                continue
            p = to_float(lvl.get("price"))
            s = to_float(lvl.get("size"))
            tick = price_to_tick(p)
            if tick is None:
                continue
            if not math.isfinite(s) or s <= 0:
                continue
            bid_sizes_by_tick[tick] = bid_sizes_by_tick.get(tick, 0.0) + s

    if isinstance(asks, list):
        for lvl in asks:
            if not isinstance(lvl, dict):
                continue
            p = to_float(lvl.get("price"))
            s = to_float(lvl.get("size"))
            tick = price_to_tick(p)
            if tick is None:
                continue
            if not math.isfinite(s) or s <= 0:
                continue
            ask_sizes_by_tick[tick] = ask_sizes_by_tick.get(tick, 0.0) + s

    return bid_sizes_by_tick, ask_sizes_by_tick


def build_orderbook_rows_from_initial_book(
    msg: Dict[str, Any],
    bid_sizes_by_tick: Dict[int, float],
    ask_sizes_by_tick: Dict[int, float],
) -> List[Tuple]:
    """Build base snapshot rows from first non-empty full book for UP only.

    Output is a dense grid with ORDERBOOK_GRID_STEP.
    Any missing level gets size=0 and inferred side.
    """
    ingest_time = now_msk_naive()
    market = str(msg.get("market", ""))
    asset_id = str(msg.get("asset_id", ""))
    event_ts_ms = to_int(msg.get("timestamp"))
    event_hash = str(msg.get("hash", ""))

    if not is_up_asset(asset_id):
        return []

    if (sum(bid_sizes_by_tick.values()) + sum(ask_sizes_by_tick.values())) <= 0:
        # Ignore empty/partial bootstrap books to avoid all-zero snapshots.
        return []

    best_bid_tick = max(bid_sizes_by_tick.keys()) if bid_sizes_by_tick else None
    best_ask_tick = min(ask_sizes_by_tick.keys()) if ask_sizes_by_tick else None
    if best_bid_tick is not None and best_ask_tick is not None:
        pivot_tick = (best_bid_tick + best_ask_tick) // 2
    elif best_bid_tick is not None:
        pivot_tick = best_bid_tick
    elif best_ask_tick is not None:
        pivot_tick = best_ask_tick
    else:
        pivot_tick = GRID_TICKS_COUNT // 2

    out: List[Tuple] = []
    for tick in GRID_TICKS:
        price = tick_to_price(tick)
        if tick in bid_sizes_by_tick:
            side = "bid"
            size = bid_sizes_by_tick[tick]
        elif tick in ask_sizes_by_tick:
            side = "ask"
            size = ask_sizes_by_tick[tick]
        else:
            side = "bid" if tick <= pivot_tick else "ask"
            size = 0.0

        out.append(
            (
                ingest_time,
                market,
                asset_id,
                event_ts_ms,
                event_hash,
                side,
                price,
                float(size),
            )
        )

    return out


def build_pricechange_rows(
    msg: Dict[str, Any],
    level_state: Dict[str, float],
    ready_snapshot_keys: Set[str],
) -> List[Tuple]:
    """Parse WS price_change and store orderbook size deltas by level.

    We use `price_change` as source transport, but persist delta(size) for
    (market, asset_id, side, price_tick), not trade prints.
    """
    ingest_time = now_msk_naive()
    market = str(msg.get("market", ""))
    event_ts_ms = to_int(msg.get("timestamp"))
    pcs = msg.get("price_changes") or []
    if isinstance(pcs, str):
        try:
            pcs = json.loads(pcs)
        except Exception:
            pcs = []
    if not isinstance(pcs, list):
        return []

    out: List[Tuple] = []
    for pc in pcs:
        if not isinstance(pc, dict):
            continue

        asset_id = str(pc.get("asset_id", ""))
        if not is_up_asset(asset_id):
            continue
        snapshot_key = f"{market}|{asset_id}"
        if snapshot_key not in ready_snapshot_keys:
            # Wait for first book snapshot for this market/asset baseline.
            continue

        side_raw = str(pc.get("side", "")).strip().lower()
        if side_raw == "buy":
            side = "bid"
        elif side_raw == "sell":
            side = "ask"
        elif side_raw in ("bid", "ask"):
            side = side_raw
        else:
            continue

        tick = price_to_tick(to_float(pc.get("price")))
        if tick is None:
            continue

        new_size = to_float(pc.get("size"), default=float("nan"))
        if not math.isfinite(new_size) or new_size < 0:
            continue
        price = tick_to_price(tick)
        event_hash = str(pc.get("hash", "") or msg.get("hash", ""))
        state_key = f"{market}|{asset_id}|{side}|{tick}"
        prev_size = level_state.get(state_key, 0.0)
        delta_size = new_size - prev_size
        if abs(delta_size) <= 1e-12:
            continue

        if abs(new_size) <= 1e-12:
            level_state.pop(state_key, None)
        else:
            level_state[state_key] = new_size

        out.append(
            (
                ingest_time,
                market,
                asset_id,
                event_ts_ms,
                event_hash,
                side,
                price,
                float(delta_size),
            )
        )

    return out


# -------------------- Gamma discovery --------------------
def build_gamma_markets_url(cursor: Optional[str] = None) -> str:
    base = "https://gamma-api.polymarket.com/markets/keyset"
    params = {"tag_id": POLY_TAG_ID, "limit": POLY_LIMIT}
    if cursor:
        params["after_cursor"] = cursor
    if POLY_ACTIVE:
        params["active"] = "true"
    if not POLY_CLOSED:
        params["closed"] = "false"
    return f"{base}?{urlencode(params)}"


def http_get_json(url: str) -> Any:
    resp = http.request(
        "GET",
        url,
        headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
        timeout=30.0,
        retries=False,
    )
    if resp.status >= 300:
        raise RuntimeError(f"HTTP GET failed {resp.status}: {resp.data.decode('utf-8', errors='replace')}")
    return json.loads(resp.data.decode("utf-8"))


def _gamma_payload_to_markets(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [m for m in payload if isinstance(m, dict)]
    if isinstance(payload, dict):
        for k in ("data", "markets", "results"):
            v = payload.get(k)
            if isinstance(v, list):
                return [m for m in v if isinstance(m, dict)]
    return []


def _extract_market_id(m: Dict[str, Any]) -> str:
    for k in ("conditionId", "condition_id", "market", "id"):
        v = m.get(k)
        if isinstance(v, str) and v:
            return v
    return ""


def _extract_clob_token_ids(m: Dict[str, Any]) -> List[str]:
    for k in ("clobTokenIds", "clob_token_ids", "clobTokenIDs"):
        v = m.get(k)
        if isinstance(v, list):
            return [str(x) for x in v if str(x)]
        if isinstance(v, str):
            try:
                arr = json.loads(v)
                if isinstance(arr, list):
                    return [str(x) for x in arr if str(x)]
            except Exception:
                return []
    return []


def _extract_outcomes(m: Dict[str, Any]) -> List[str]:
    v = m.get("outcomes")
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except (TypeError, ValueError):
            return []
    if isinstance(v, list):
        return [str(x) for x in v]
    return []


def _normalize_outcome_label(label: str) -> str:
    s = re.sub(r"[^A-Z0-9]+", "", str(label).upper())
    if s in ("UP", "HIGHER"):
        return "UP"
    if s in ("DOWN", "LOWER"):
        return "DOWN"
    if s in ("YES",):
        return "YES"
    if s in ("NO",):
        return "NO"
    return s


def _market_text_bundle(m: Dict[str, Any]) -> Tuple[str, str]:
    qtn = str(m.get("question") or "")
    ttl = str(m.get("title") or "")
    nm = str(m.get("name") or "")
    slug = str(m.get("slug") or "")
    text = f"{qtn} {ttl} {nm} {slug}"
    return text.upper(), text.lower()


def _parse_gamma_datetime_to_ts(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # Gamma may return ms or sec depending on endpoint/field.
        v = float(value)
        if v > 1e12:
            return v / 1000.0
        if v > 1e9:
            return v
        return None
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    # ISO8601 compatibility for Python fromisoformat
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def _market_end_ts(m: Dict[str, Any]) -> Optional[float]:
    for k in ("endDate", "end_date", "endTime", "end_time", "closedTime", "closed_time"):
        ts = _parse_gamma_datetime_to_ts(m.get(k))
        if ts is not None:
            return ts
    return None


def _market_debug_label(m: Dict[str, Any]) -> str:
    mid = _extract_market_id(m) or str(m.get("id") or "")
    slug = str(m.get("slug") or "")
    end_ts = _market_end_ts(m)
    end_s = "?"
    if end_ts is not None:
        end_s = datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat()
    return f"id={mid} slug={slug} end_utc={end_s}"


def _market_sort_key_for_current(m: Dict[str, Any], now_ts: float) -> Tuple[int, float, str]:
    end_ts = _market_end_ts(m)
    if end_ts is None:
        # Unknown end -> send to the back
        return (2, float("inf"), str(m.get("slug") or m.get("id") or ""))
    # Prefer markets that are not yet expired, then nearest end time.
    if end_ts >= now_ts:
        return (0, end_ts, str(m.get("slug") or m.get("id") or ""))
    return (1, end_ts, str(m.get("slug") or m.get("id") or ""))


def _has_up_down_intent(m: Dict[str, Any], text_l: str) -> bool:
    slug = str(m.get("slug") or "").lower()
    if "up-or-down" in slug:
        return True
    if re.search(r"\bup\s+or\s+down\b", text_l):
        return True

    normalized = {_normalize_outcome_label(x) for x in _extract_outcomes(m)}
    return "UP" in normalized and "DOWN" in normalized


def _is_binary_market(m: Dict[str, Any]) -> bool:
    return len(_extract_clob_token_ids(m)) == 2


def _market_id_keys(m: Dict[str, Any]) -> Set[str]:
    out: Set[str] = set()
    condition_id = str(m.get("conditionId", "")).strip().lower()
    market_id = str(m.get("id", "")).strip().lower()
    if condition_id:
        out.add(condition_id)
    if market_id:
        out.add(market_id)
    return out


def _symbol_aliases(sym: str) -> Tuple[str, ...]:
    sym_u = str(sym or "").strip().upper()
    return SYMBOL_ALIASES.get(sym_u, (sym_u,))


def _token_match(text: str, token: str) -> bool:
    token_u = re.escape(str(token or "").strip().upper())
    if not token_u:
        return False
    return re.search(rf"(^|[^A-Z0-9])\$?{token_u}([^A-Z0-9]|$)", text) is not None


def _symbol_match(title: str, sym: str) -> bool:
    return any(_token_match(title, alias) for alias in _symbol_aliases(sym))


def _infer_market_symbols(m: Dict[str, Any]) -> Set[str]:
    title_blob, _ = _market_text_bundle(m)
    return {sym for sym in POLY_SYMBOLS if _symbol_match(title_blob, sym)}


def _market_selection_debug(markets: List[Dict[str, Any]], *, limit: int = 12) -> Dict[str, Any]:
    symbol_counts: Counter[str] = Counter()
    samples: List[Dict[str, Any]] = []

    for m in markets:
        syms = sorted(_infer_market_symbols(m))
        if syms:
            symbol_counts.update(syms)
        if len(samples) < limit:
            samples.append(
                {
                    "symbols": syms,
                    "label": _market_debug_label(m),
                }
            )

    return {
        "symbol_counts": dict(sorted(symbol_counts.items())),
        "samples": samples,
    }


def _select_current_markets_by_symbol(matched: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    now_ts = datetime.now(timezone.utc).timestamp()
    selected: List[Dict[str, Any]] = []
    selected_ids: Set[str] = set()

    if not POLY_SYMBOLS:
        selected = sorted(matched, key=lambda m: _market_sort_key_for_current(m, now_ts))
    else:
        by_symbol = {symbol: [] for symbol in POLY_SYMBOLS}
        for market in matched:
            for symbol in _infer_market_symbols(market):
                by_symbol[symbol].append(market)
        for sym in POLY_SYMBOLS:
            sym_markets = by_symbol[sym]
            if not sym_markets:
                continue

            best = min(sym_markets, key=lambda m: _market_sort_key_for_current(m, now_ts))
            market_key = _extract_market_id(best) or str(id(best))
            if market_key in selected_ids:
                continue

            selected.append(best)
            selected_ids.add(market_key)

    if POLY_MAX_MATCHED_MARKETS > 0:
        selected = selected[:POLY_MAX_MATCHED_MARKETS]

    return selected


def _timeframe_match(title: str, tf: str) -> bool:
    tf = tf.lower().strip()
    patterns = {
        "15m": r"\b15\s*[- ]?\s*(MIN|MINS|MINUTE|MINUTES)\b",
        "30m": r"\b30\s*[- ]?\s*(MIN|MINS|MINUTE|MINUTES)\b",
        "1h": r"\b(1\s*[- ]?\s*(H|HR|HOUR|HOURS)|60\s*[- ]?\s*(MIN|MINS|MINUTE|MINUTES))\b",
        "4h": r"\b4\s*[- ]?\s*(H|HR|HOUR|HOURS)\b",
    }
    pat = patterns.get(tf)
    if not pat:
        m = re.match(r"^(\d+)(m|h)$", tf)
        if not m:
            return False
        n, unit = m.group(1), m.group(2)
        if unit == "m":
            pat = rf"\b{re.escape(n)}\s*[- ]?\s*(MIN|MINS|MINUTE|MINUTES)\b"
        else:
            pat = rf"\b{re.escape(n)}\s*[- ]?\s*(H|HR|HOUR|HOURS)\b"
    return re.search(pat, title, flags=re.IGNORECASE) is not None


def market_passes_filters(m: Dict[str, Any]) -> bool:
    title_blob, text_l = _market_text_bundle(m)
    slug_l = str(m.get("slug") or "").lower()

    if POLY_MARKET_WHITELIST_IDS:
        if not (_market_id_keys(m) & POLY_MARKET_WHITELIST_IDS):
            return False

    if POLY_MARKET_EXCLUDE_RE and POLY_MARKET_EXCLUDE_RE.search(text_l):
        return False

    if POLY_REQUIRE_BINARY and not _is_binary_market(m):
        return False

    if POLY_REQUIRE_UP_DOWN and not _has_up_down_intent(m, text_l):
        return False

    if POLY_SYMBOLS and not any(_symbol_match(title_blob, s) for s in POLY_SYMBOLS):
        return False

    if POLY_TIMEFRAMES:
        if any(tf in slug_l for tf in POLY_TIMEFRAMES):
            return True
        text_for_tf = text_l
        if not any(_timeframe_match(text_for_tf, tf) for tf in POLY_TIMEFRAMES):
            return False

    return True


def fetch_filtered_markets() -> List[Dict[str, Any]]:
    matched: List[Dict[str, Any]] = []
    cursor = None
    seen_cursors = set()
    while True:
        payload = http_get_json(build_gamma_markets_url(cursor))
        if not isinstance(payload, dict) or not isinstance(payload.get("markets"), list):
            raise RuntimeError("Unexpected Gamma markets/keyset response")
        page = _gamma_payload_to_markets(payload)
        matched.extend(m for m in page if market_passes_filters(m))
        cursor = payload.get("next_cursor")
        if not cursor:
            break
        if cursor in seen_cursors:
            raise RuntimeError("Gamma pagination returned a repeated cursor")
        seen_cursors.add(cursor)

    # If a strict whitelist is set, keep all matched whitelist markets as-is.
    if POLY_MARKET_WHITELIST_IDS:
        return matched

    # Track the currently relevant market per symbol, so BTC/ETH/SOL/XRP means
    # at most four 15m markets instead of every future active 15m market.
    if matched and POLY_TRACK_CURRENT_ONLY:
        matched = _select_current_markets_by_symbol(matched)

    return matched


def build_outcome_map(markets: List[Dict[str, Any]]) -> Tuple[List[str], Dict[str, str]]:
    asset_ids: List[str] = []
    outcome_map: Dict[str, str] = {}
    seen_assets: Set[str] = set()

    for m in markets:
        market_id = _extract_market_id(m)
        toks = _extract_clob_token_ids(m)
        if not market_id or not toks:
            continue

        outcomes = _extract_outcomes(m)
        if outcomes and len(outcomes) >= len(toks):
            labels = outcomes[: len(toks)]
        else:
            labels = ["UP", "DOWN"] if len(toks) == 2 else [f"OUTCOME_{i+1}" for i in range(len(toks))]

        for aid, label in zip(toks, labels):
            if aid not in seen_assets:
                seen_assets.add(aid)
                asset_ids.append(aid)
            outcome_map[aid] = str(label)

    if not asset_ids:
        raise RuntimeError("No markets with clobTokenIds found for selected filters")

    return asset_ids, outcome_map


async def refresh_markets_loop(reload_event: asyncio.Event, runtime_state: Dict[str, Any]) -> None:
    global CURRENT_ASSET_IDS, OUTCOME_MAP
    last_sig = json.dumps({"assets": sorted(set(CURRENT_ASSET_IDS))}, sort_keys=True)

    while True:
        try:
            markets = await asyncio.to_thread(fetch_filtered_markets)
            if not markets:
                print("[REFRESH] no markets matched filters")
                runtime_state["tracked_markets"] = 0
                runtime_state["subscribed_assets"] = 0
                runtime_state["current_market"] = ""
                runtime_state["current_asset_id"] = ""
                runtime_state["current_market_end_ts"] = 0
                runtime_state["status"] = "no_markets"
                runtime_state["last_error"] = ""
                await service_log("WARN", "refresh_no_markets", status="no_markets")
                await asyncio.sleep(max(1.0, POLY_REFRESH_SEC))
                continue

            asset_ids, outcome_map = build_outcome_map(markets)
            runtime_state["tracked_markets"] = len(markets)
            runtime_state["subscribed_assets"] = len(asset_ids)
            runtime_state["current_market"] = _extract_market_id(markets[0])
            runtime_state["current_market_end_ts"] = _market_end_ts(markets[0]) or 0
            runtime_state["current_asset_id"] = _pick_first_up_asset_id(asset_ids, outcome_map)
            runtime_state["last_error"] = ""
            sig = json.dumps({"assets": sorted(set(asset_ids))}, sort_keys=True)

            if sig != last_sig:
                CURRENT_ASSET_IDS = asset_ids
                OUTCOME_MAP = outcome_map
                reload_event.set()
                last_sig = sig
                print(f"[REFRESH] markets={len(markets)} assets={len(asset_ids)}")
                if markets:
                    print(f"[REFRESH] selected { _market_debug_label(markets[0]) }")
                if POLY_DEBUG_MARKETS:
                    dbg = _market_selection_debug(markets)
                    print(f"[REFRESH] symbol_counts={dbg['symbol_counts']}")
                    for sample in dbg["samples"]:
                        print(f"[REFRESH] sample symbols={sample['symbols']} {sample['label']}")
                await service_log(
                    "INFO",
                    "refresh_selection_changed",
                    status=str(runtime_state.get("status") or ""),
                    market=str(runtime_state.get("current_market") or ""),
                    asset_id=str(runtime_state.get("current_asset_id") or ""),
                    details={
                        "markets": len(markets),
                        "assets": len(asset_ids),
                        "market_end_ts": runtime_state.get("current_market_end_ts") or 0,
                        "next_refresh_sec": round(max(1.0, POLY_REFRESH_SEC), 3),
                    },
                )

        except Exception as e:
            print("[REFRESH] error:", repr(e))
            runtime_state["last_error"] = repr(e)
            runtime_state["status"] = "refresh_error"
            await service_log("ERROR", "refresh_error", status="refresh_error", error_text=repr(e))

        await asyncio.sleep(max(1.0, POLY_REFRESH_SEC))


# -------------------- ClickHouse I/O --------------------


async def service_log(
    level: str,
    event: str,
    *,
    status: str = "",
    market: str = "",
    asset_id: str = "",
    event_ts_ms: int = 0,
    details: Union[str, Dict[str, Any], List[Any], None] = None,
    error_text: str = "",
) -> None:
    if not SERVICE_LOG_ENABLED:
        return
    try:
        if isinstance(details, (dict, list)):
            details_text = json.dumps(details, ensure_ascii=False, separators=(",", ":"), default=str)
        elif details is None:
            details_text = ""
        else:
            details_text = str(details)
        row = (
            now_msk_naive(),
            INGEST_SERVICE_NAME,
            str(level).upper(),
            str(event),
            str(status),
            str(market),
            str(asset_id),
            max(0, int(event_ts_ms or 0)),
            details_text,
            str(error_text),
        )
        cols = [
            "ts",
            "service",
            "level",
            "event",
            "status",
            "market",
            "asset_id",
            "event_ts_ms",
            "details",
            "error_text",
        ]
        await asyncio.to_thread(ch_http_insert_json_each_row, f"{CH_DB}.{INGEST_SERVICE_LOG_TABLE}", cols, [row])
    except Exception as e:
        print("[SERVICE_LOG] insert error:", repr(e))


async def heartbeat_write(runtime_state: Dict[str, Any], orderbook_buf_len: int, pricechange_buf_len: int) -> None:
    if not HEARTBEAT_ENABLED:
        return
    try:
        last_event_ts_ms = int(runtime_state.get("last_event_ts_ms") or 0)
        lag_ms = -1
        if last_event_ts_ms > 0:
            lag_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - last_event_ts_ms

        row = (
            now_msk_naive(),
            INGEST_SERVICE_NAME,
            str(runtime_state.get("status") or ""),
            str(runtime_state.get("current_market") or ""),
            str(runtime_state.get("current_asset_id") or ""),
            int(runtime_state.get("subscribed_assets") or 0),
            int(runtime_state.get("tracked_markets") or 0),
            str(runtime_state.get("last_ws_msg_time_msk") or ""),
            last_event_ts_ms,
            int(lag_ms),
            int(orderbook_buf_len),
            int(pricechange_buf_len),
            str(runtime_state.get("last_error") or ""),
        )
        cols = [
            "ts",
            "service",
            "status",
            "current_market",
            "current_asset_id",
            "subscribed_assets",
            "tracked_markets",
            "last_ws_msg_time_msk",
            "last_event_ts_ms",
            "lag_ms",
            "orderbook_buffer_len",
            "pricechange_buffer_len",
            "last_error",
        ]
        await asyncio.to_thread(ch_http_insert_json_each_row, f"{CH_DB}.{INGEST_HEARTBEAT_TABLE}", cols, [row])
    except Exception as e:
        print("[HEARTBEAT] insert error:", repr(e))


def ensure_tables() -> None:
    # ClickHouse 18.x on this server has limited timezone DB support.
    # For stable MSK display we materialize as UTC+3 seconds shift.
    msk_expr = "toDateTime(intDiv(event_ts_ms, 1000) + 10800)"
    for table, sort_key in (
        (ORDERBOOK_TABLE, "market, asset_id, event_ts_ms, side, price, hash"),
        (PRICECHANGE_TABLE, "market, asset_id, event_ts_ms, hash, side, price"),
    ):
        ch_http_command(f"""
CREATE TABLE IF NOT EXISTS {CH_DB}.{table}
(
  ingest_time DateTime,
  market String,
  asset_id String,
  event_ts_ms UInt64,
  event_time_msk DateTime MATERIALIZED {msk_expr},
  hash String,
  side String,
  price Float64,
  size Float64
)
ENGINE = MergeTree
ORDER BY ({sort_key})
""")
    q_heartbeat = f"""
CREATE TABLE IF NOT EXISTS {CH_DB}.{INGEST_HEARTBEAT_TABLE}
(
  ts DateTime,
  service String,
  status String,
  current_market String,
  current_asset_id String,
  subscribed_assets UInt32,
  tracked_markets UInt32,
  last_ws_msg_time_msk String,
  last_event_ts_ms UInt64,
  lag_ms Int64,
  orderbook_buffer_len UInt32,
  pricechange_buffer_len UInt32,
  last_error String
)
ENGINE = MergeTree
ORDER BY (service, ts)
"""
    q_service_log = f"""
CREATE TABLE IF NOT EXISTS {CH_DB}.{INGEST_SERVICE_LOG_TABLE}
(
  ts DateTime,
  service String,
  level String,
  event String,
  status String,
  market String,
  asset_id String,
  event_ts_ms UInt64,
  details String,
  error_text String
)
ENGINE = MergeTree
ORDER BY (service, ts, event)
"""
    if HEARTBEAT_ENABLED:
        ch_http_command(q_heartbeat)
    if SERVICE_LOG_ENABLED:
        ch_http_command(q_service_log)


# -------------------- WS --------------------
async def ws_subscribe(ws) -> None:
    await ws.send(json.dumps({"assets_ids": CURRENT_ASSET_IDS, "type": "market"}))


async def heartbeat_loop(
    runtime_state: Dict[str, Any],
    orderbook_buf: List[Tuple],
    pricechange_buf: List[Tuple],
    lock: asyncio.Lock,
) -> None:
    while True:
        try:
            if HEARTBEAT_ENABLED:
                async with lock:
                    ob_len = len(orderbook_buf)
                    pc_len = len(pricechange_buf)
                await heartbeat_write(runtime_state, ob_len, pc_len)
        except Exception as e:
            print("[HEARTBEAT] loop error:", repr(e))
        await asyncio.sleep(max(1.0, HEARTBEAT_EVERY_SEC))


async def flusher_loop(
    orderbook_buf: List[Tuple],
    pricechange_buf: List[Tuple],
    lock: asyncio.Lock,
    flush_event: asyncio.Event,
    runtime_state: Dict[str, Any],
) -> None:
    while True:
        try:
            await asyncio.wait_for(flush_event.wait(), timeout=FLUSH_EVERY_SEC)
        except asyncio.TimeoutError:
            pass

        async with lock:
            if not orderbook_buf and not pricechange_buf:
                flush_event.clear()
                continue

            ob = orderbook_buf[:]
            pc = pricechange_buf[:]
            orderbook_buf.clear()
            pricechange_buf.clear()
            flush_event.clear()

        cols = ["ingest_time", "market", "asset_id", "event_ts_ms", "hash", "side", "price", "size"]
        for table, event_name, rows, buffer in (
            (ORDERBOOK_TABLE, "orderbook", ob, orderbook_buf),
            (PRICECHANGE_TABLE, "pricechange", pc, pricechange_buf),
        ):
            if not rows:
                continue
            try:
                await asyncio.to_thread(ch_http_insert_json_each_row, f"{CH_DB}.{table}", cols, rows)
                print(f"CH inserted {table}: {len(rows)} rows")
                if SERVICE_LOG_FLUSH_SUCCESS:
                    await service_log(
                        "INFO", f"flush_{event_name}_ok",
                        status=str(runtime_state.get("status") or ""),
                        details={"rows": len(rows)},
                    )
            except Exception as e:
                async with lock:
                    buffer[:0] = rows
                print(f"CH INSERT ERROR {table}:", repr(e))
                runtime_state["last_error"] = repr(e)
                runtime_state["status"] = "ch_insert_error"
                await service_log(
                    "ERROR", f"flush_{event_name}_error", status="ch_insert_error",
                    error_text=repr(e), details={"rows": len(rows)},
                )


async def ingest_forever() -> None:
    orderbook_buf: List[Tuple] = []
    pricechange_buf: List[Tuple] = []

    lock = asyncio.Lock()
    flush_event = asyncio.Event()
    reload_event = asyncio.Event()

    # Each connection starts from a fresh baseline after any missed events.
    initial_snapshot_written: Set[str] = set()
    # Last known absolute size per (market, asset_id, side, price_tick),
    # used to convert price_change stream into delta(size).
    pricechange_level_state: Dict[str, float] = {}
    runtime_state: Dict[str, Any] = {
        "status": "starting",
        "current_market": "",
        "current_asset_id": "",
        "subscribed_assets": 0,
        "tracked_markets": 0,
        "current_market_end_ts": 0,
        "last_ws_msg_time_msk": "",
        "last_event_ts_ms": 0,
        "last_error": "",
    }

    # Initial discovery before first subscribe
    markets = await asyncio.to_thread(fetch_filtered_markets)
    asset_ids, outcome_map = build_outcome_map(markets)
    global CURRENT_ASSET_IDS, OUTCOME_MAP
    CURRENT_ASSET_IDS = asset_ids
    OUTCOME_MAP = outcome_map
    runtime_state["tracked_markets"] = len(markets)
    runtime_state["subscribed_assets"] = len(asset_ids)
    if markets:
        runtime_state["current_market"] = _extract_market_id(markets[0])
        runtime_state["current_market_end_ts"] = _market_end_ts(markets[0]) or 0
    runtime_state["current_asset_id"] = _pick_first_up_asset_id(asset_ids, outcome_map)

    await asyncio.to_thread(ensure_tables)
    await service_log(
        "INFO",
        "startup",
        status="starting",
        market=str(runtime_state.get("current_market") or ""),
        asset_id=str(runtime_state.get("current_asset_id") or ""),
        details={"markets": len(markets), "assets": len(asset_ids)},
    )
    print(f"[INIT] orderbook table ready: {CH_DB}.{ORDERBOOK_TABLE}")
    print(f"[INIT] pricechange table ready: {CH_DB}.{PRICECHANGE_TABLE}")
    print(f"[INIT] heartbeat table ready: {CH_DB}.{INGEST_HEARTBEAT_TABLE}")
    print(f"[INIT] service log table ready: {CH_DB}.{INGEST_SERVICE_LOG_TABLE}")
    print(
        f"[INIT] orderbook grid min={ORDERBOOK_GRID_MIN} max={ORDERBOOK_GRID_MAX} "
        f"step={ORDERBOOK_GRID_STEP} levels={len(GRID_TICKS)}"
    )
    print(f"[INIT] markets={len(markets)} assets={len(asset_ids)}")
    if markets:
        print(f"[INIT] selected { _market_debug_label(markets[0]) }")
    if POLY_DEBUG_MARKETS:
        dbg = _market_selection_debug(markets)
        print(f"[INIT] symbol_counts={dbg['symbol_counts']}")
        for sample in dbg["samples"]:
            print(f"[INIT] sample symbols={sample['symbols']} {sample['label']}")

    background_tasks = [
        asyncio.create_task(refresh_markets_loop(reload_event, runtime_state)),
        asyncio.create_task(flusher_loop(orderbook_buf, pricechange_buf, lock, flush_event, runtime_state)),
        asyncio.create_task(heartbeat_loop(runtime_state, orderbook_buf, pricechange_buf, lock)),
    ]
    try:
        backoff = 1.0
        while True:
            try:
                runtime_state["status"] = "connecting"
                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    open_timeout=15,
                    max_size=10 * 1024 * 1024,
                    origin=ORIGIN,
                ) as ws:
                    print("WS connected")
                    runtime_state["status"] = "connected"
                    runtime_state["last_error"] = ""
                    await service_log(
                        "INFO",
                        "ws_connected",
                        status="connected",
                        market=str(runtime_state.get("current_market") or ""),
                        asset_id=str(runtime_state.get("current_asset_id") or ""),
                        details={"assets_ids": len(CURRENT_ASSET_IDS)},
                    )
                    reload_event.clear()
                    initial_snapshot_written.clear()
                    pricechange_level_state.clear()
                    await ws_subscribe(ws)
                    print("WS subscribed")
                    runtime_state["status"] = "subscribed"
                    runtime_state["subscribed_assets"] = len(CURRENT_ASSET_IDS)
                    await service_log(
                        "INFO",
                        "ws_subscribed",
                        status="subscribed",
                        market=str(runtime_state.get("current_market") or ""),
                        asset_id=str(runtime_state.get("current_asset_id") or ""),
                        details={"assets_ids": len(CURRENT_ASSET_IDS)},
                    )
                    async for message in websocket_messages(ws, ping_every=10.0, stop_event=reload_event):
                        runtime_state["last_ws_msg_time_msk"] = now_msk_naive().strftime("%Y-%m-%d %H:%M:%S")
                        if reload_event.is_set():
                            print("[WS] reload requested; reconnecting...")
                            runtime_state["status"] = "reload_reconnect"
                            await service_log(
                                "INFO",
                                "ws_reload_requested",
                                status="reload_reconnect",
                                market=str(runtime_state.get("current_market") or ""),
                                asset_id=str(runtime_state.get("current_asset_id") or ""),
                            )
                            break

                        if DEBUG_RAW:
                            print("RAW:", message[:400])

                        try:
                            raw = json.loads(message)
                        except Exception:
                            continue

                        for msg in normalize_messages(raw):
                            msg_ts = to_int(msg.get("timestamp"))
                            if msg_ts > 0:
                                runtime_state["last_event_ts_ms"] = msg_ts
                            if is_orderbook(msg):
                                market = str(msg.get("market", ""))
                                asset_id = str(msg.get("asset_id", ""))
                                if not is_up_asset(asset_id):
                                    continue

                                snapshot_key = f"{market}|{asset_id}"
                                if snapshot_key in initial_snapshot_written:
                                    continue

                                bid_sizes_by_tick, ask_sizes_by_tick = extract_book_level_sizes(msg)
                                rows = build_orderbook_rows_from_initial_book(msg, bid_sizes_by_tick, ask_sizes_by_tick)
                                if rows:
                                    initial_snapshot_written.add(snapshot_key)
                                    runtime_state["current_market"] = market or str(runtime_state.get("current_market") or "")
                                    runtime_state["current_asset_id"] = asset_id or str(runtime_state.get("current_asset_id") or "")
                                    # Seed pricechange state with absolute sizes from initial book,
                                    # so subsequent deltas are relative to this baseline.
                                    for tick, sz in bid_sizes_by_tick.items():
                                        if abs(sz) > 1e-12:
                                            key = f"{market}|{asset_id}|bid|{tick}"
                                            pricechange_level_state[key] = float(sz)
                                    for tick, sz in ask_sizes_by_tick.items():
                                        if abs(sz) > 1e-12:
                                            key = f"{market}|{asset_id}|ask|{tick}"
                                            pricechange_level_state[key] = float(sz)
                                    async with lock:
                                        orderbook_buf.extend(rows)
                                        if (len(orderbook_buf) + len(pricechange_buf)) >= FLUSH_EVERY_N:
                                            flush_event.set()
                                    await service_log(
                                        "INFO",
                                        "orderbook_snapshot_written",
                                        status=str(runtime_state.get("status") or ""),
                                        market=market,
                                        asset_id=asset_id,
                                        event_ts_ms=msg_ts,
                                        details={"rows": len(rows), "non_zero_bid": len(bid_sizes_by_tick), "non_zero_ask": len(ask_sizes_by_tick)},
                                    )

                            elif is_price_change(msg):
                                rows = build_pricechange_rows(msg, pricechange_level_state, initial_snapshot_written)
                                if rows:
                                    async with lock:
                                        pricechange_buf.extend(rows)
                                        if (len(orderbook_buf) + len(pricechange_buf)) >= FLUSH_EVERY_N:
                                            flush_event.set()

                    backoff = 1.0
                    runtime_state["status"] = "disconnected"

            except Exception as e:
                print("WS error:", repr(e))
                runtime_state["status"] = "ws_error"
                runtime_state["last_error"] = repr(e)
                await service_log(
                    "ERROR",
                    "ws_error",
                    status="ws_error",
                    market=str(runtime_state.get("current_market") or ""),
                    asset_id=str(runtime_state.get("current_asset_id") or ""),
                    error_text=repr(e),
                    details={"backoff_sec": backoff},
                )
                await asyncio.sleep(backoff)
                runtime_state["status"] = "reconnecting"
                backoff = min(backoff * 2, 30.0)
    finally:
        for task in background_tasks:
            task.cancel()
        await asyncio.gather(*background_tasks, return_exceptions=True)


if __name__ == "__main__":
    from collector_v2 import main
    raise SystemExit(main("orderbooks"))
