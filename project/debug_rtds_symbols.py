"""Debug Polymarket RTDS crypto price symbols.

This script connects to the RTDS websocket and prints the actual
payload.symbol values observed from incoming messages.

Examples:
  python debug_rtds_symbols.py --seconds 30
  python debug_rtds_symbols.py --seconds 30 --symbols eth/usd
  python debug_rtds_symbols.py --seconds 30 --unfiltered --raw
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from collections import Counter
from typing import Any, Dict, Iterable, List, Union

import websockets
from dotenv import load_dotenv


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "password.env"), override=False)
load_dotenv(os.path.join(BASE_DIR, ".env"), override=False)


def split_symbols(raw: str) -> List[str]:
    return list(
        dict.fromkeys(
            s.strip().lower()
            for s in str(raw or "").split(",")
            if s.strip()
        )
    )


def default_symbols() -> List[str]:
    symbols = split_symbols(os.getenv("SPOT_SYMBOLS", ""))
    if symbols:
        return symbols
    return split_symbols(os.getenv("SPOT_SYMBOL", "btc/usd")) or ["btc/usd"]


def normalize_messages(raw: Union[Dict[str, Any], List[Any]]) -> List[Dict[str, Any]]:
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, list):
        out: List[Dict[str, Any]] = []
        for x in raw:
            if isinstance(x, dict):
                out.append(x)
            elif isinstance(x, list):
                out.extend(y for y in x if isinstance(y, dict))
        return out
    return []


def make_subscription(topic: str, symbol: str | None) -> Dict[str, Any]:
    sub: Dict[str, Any] = {
        "topic": topic,
        "type": "*",
    }
    if symbol:
        sub["filters"] = json.dumps({"symbol": symbol}, separators=(",", ":"))
    return sub


async def send_subscriptions(
    ws,
    topic: str,
    symbols: Iterable[str],
    *,
    unfiltered: bool,
    batch: bool,
) -> None:
    if unfiltered:
        subs = [make_subscription(topic, None)]
    else:
        subs = [make_subscription(topic, symbol) for symbol in symbols]

    if batch:
        await ws.send(json.dumps({"action": "subscribe", "subscriptions": subs}, separators=(",", ":")))
        return

    for sub in subs:
        await ws.send(json.dumps({"action": "subscribe", "subscriptions": [sub]}, separators=(",", ":")))


def extract_payload_symbol(msg: Dict[str, Any]) -> str:
    payload = msg.get("payload")
    if isinstance(payload, dict):
        return str(payload.get("symbol") or "").strip().lower()
    return str(msg.get("symbol") or "").strip().lower()


async def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, default=30.0)
    parser.add_argument("--symbols", default=",".join(default_symbols()))
    parser.add_argument("--unfiltered", action="store_true")
    parser.add_argument("--batch", action="store_true", help="Send all subscriptions in one message.")
    parser.add_argument("--raw", action="store_true", help="Print a few raw messages.")
    parser.add_argument("--raw-limit", type=int, default=5)
    args = parser.parse_args()

    url = os.getenv("SPOT_WS_URL", "wss://ws-live-data.polymarket.com")
    topic = os.getenv("SPOT_TOPIC", "crypto_prices_chainlink").strip()
    symbols = split_symbols(args.symbols)

    print(f"url={url}")
    print(f"topic={topic}")
    print(f"mode={'unfiltered' if args.unfiltered else 'filtered'}")
    print(f"batch={args.batch}")
    print(f"symbols={symbols}")

    counts: Counter[str] = Counter()
    raw_seen = 0
    deadline = time.monotonic() + max(1.0, args.seconds)

    async with websockets.connect(
        url,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=5,
        open_timeout=15,
        max_size=10 * 1024 * 1024,
    ) as ws:
        await send_subscriptions(
            ws,
            topic,
            symbols,
            unfiltered=args.unfiltered,
            batch=args.batch,
        )
        print("subscribed")

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                break

            if args.raw and raw_seen < args.raw_limit:
                print("RAW:", str(message)[:1000])
                raw_seen += 1

            try:
                decoded = json.loads(message)
            except Exception:
                continue

            for msg in normalize_messages(decoded):
                symbol = extract_payload_symbol(msg)
                if symbol:
                    if counts[symbol] == 0:
                        print(f"FIRST_SYMBOL: {symbol}")
                    counts[symbol] += 1

    print("summary:")
    if not counts:
        print("  no payload.symbol values observed")
    for symbol, count in counts.most_common():
        print(f"  {symbol}: {count}")


if __name__ == "__main__":
    asyncio.run(run())
