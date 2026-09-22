# %%
# Вспомогательные функции и ячейки Jupyter / VSCode для ClickHouse + Polymarket orderbook.
# Можно копировать ячейки в ноутбук или запускать как файл с ячейками "# %%" в VSCode.

import os
import re
import math
import pandas as pd
import clickhouse_connect
import requests
from dotenv import load_dotenv
from IPython.display import display

load_dotenv(".env", override=True)

DB_NAME = os.getenv("CLICKHOUSE_DB", "polyk")
ORDERBOOK_TABLE = os.getenv("ORDERBOOK_TABLE", "orderbook")
PRICECHANGE_TABLE = os.getenv("PRICECHANGE_TABLE", "pricechange")
SPOT_TABLE = os.getenv("SPOT_TABLE", "crypto_spot")
GAMMA_API_BASE = os.getenv("GAMMA_API_BASE", "https://gamma-api.polymarket.com").rstrip("/")
SECONDS_PER_YEAR = 365.0 * 24.0 * 60.0 * 60.0

c = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST"),
    port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
    username=os.getenv("CLICKHOUSE_USER"),
    password=os.getenv("CLICKHOUSE_PASSWORD"),
    database=DB_NAME,
)


def ch_cmd(sql: str):
    """Выполнить DDL/DML (TRUNCATE/ALTER и т.д.)."""
    return c.command(sql)


def ch_df(sql: str) -> pd.DataFrame:
    """Выполнить SELECT и вернуть pandas DataFrame."""
    res = c.query(sql)
    return pd.DataFrame(res.result_rows, columns=res.column_names)


def _sql_quote(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _depth_price(level_sizes: dict, ordered_prices: list, min_total_size: float):
    total_size = 0.0
    for price in ordered_prices:
        size = float(level_sizes.get(price, 0.0) or 0.0)
        if size <= 0:
            continue
        total_size += size
        if total_size + 1e-12 >= float(min_total_size):
            return float(price), float(total_size)
    return float("nan"), float(total_size)


def _gamma_payload_to_markets(payload):
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("data", "markets", "results"):
            val = payload.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
        return [payload]
    return []


def _safe_int_ts_ms(x):
    if x is None:
        return None
    try:
        n = int(float(x))
        if n > 10_000_000_000:
            return n
        return n * 1000
    except Exception:
        pass
    try:
        ts = pd.to_datetime(x, utc=True)
        if pd.isna(ts):
            return None
        return int(ts.value // 1_000_000)
    except Exception:
        return None


def infer_market_duration_min(market_row: dict, default_minutes: int = 15) -> int:
    rx = re.compile(r"(\d+)\s*[- ]?\s*(min|mins|minute|minutes)\b", re.IGNORECASE)
    for key in ("question", "groupItemTitle", "title", "slug"):
        txt = str(market_row.get(key, "") or "")
        m = rx.search(txt)
        if m:
            return max(1, int(m.group(1)))
    return int(default_minutes)


def fetch_gamma_market_by_condition_id(
    condition_id: str,
    active: bool = None,
    closed: bool = None,
    limit: int = 500,
    max_pages: int = 30,
    timeout_sec: int = 20,
) -> dict:
    target = str(condition_id or "").strip().lower()
    if not target:
        raise ValueError("Пустой condition_id")

    base_url = f"{GAMMA_API_BASE}/markets"
    common = {}
    if active is not None:
        common["active"] = str(bool(active)).lower()
    if closed is not None:
        common["closed"] = str(bool(closed)).lower()

    # Быстрый путь: прямой фильтр по condition_ids.
    params = {"condition_ids": target, "limit": int(limit), "offset": 0, **common}
    r = requests.get(base_url, params=params, timeout=timeout_sec)
    r.raise_for_status()
    for row in _gamma_payload_to_markets(r.json()):
        cid = str(row.get("conditionId") or row.get("condition_id") or "").strip().lower()
        if cid == target:
            return row

    # Резервный путь: постраничный перебор.
    for page in range(max_pages):
        params = {"limit": int(limit), "offset": page * int(limit), **common}
        r = requests.get(base_url, params=params, timeout=timeout_sec)
        r.raise_for_status()
        rows = _gamma_payload_to_markets(r.json())
        if not rows:
            break
        for row in rows:
            cid = str(row.get("conditionId") or row.get("condition_id") or "").strip().lower()
            if cid == target:
                return row
        if len(rows) < int(limit):
            break

    raise ValueError(f"Рынок с conditionId={condition_id} не найден в Gamma API")


def market_start_ts_ms_from_gamma_row(
    market_row: dict,
    duration_min: int = None,
    default_duration_min: int = 15,
) -> int:
    end_ts_ms = _safe_int_ts_ms(
        market_row.get("endDate")
        or market_row.get("end_date")
        or market_row.get("endTime")
        or market_row.get("end_time")
        or market_row.get("closedTime")
        or market_row.get("closed_time")
    )
    if end_ts_ms is None:
        raise ValueError("Не удалось разобрать время окончания рынка из Gamma")

    dur_min = (
        int(duration_min)
        if duration_min is not None
        else infer_market_duration_min(market_row, default_minutes=default_duration_min)
    )
    return int(end_ts_ms - dur_min * 60 * 1000)


def _spot_first_at_or_after(start_ts_ms: int, symbol: str = "btc/usd", spot_table: str = None):
    table = spot_table or SPOT_TABLE
    df = ch_df(
        f"""
SELECT event_ts_ms, event_time_msk, value
FROM {DB_NAME}.{table}
WHERE symbol = {_sql_quote(symbol.lower())}
  AND event_ts_ms >= {int(start_ts_ms)}
ORDER BY event_ts_ms ASC
LIMIT 1
"""
    )
    if df.empty:
        return None
    row = df.iloc[0]
    return {
        "event_ts_ms": int(row["event_ts_ms"]),
        "event_time_msk": row["event_time_msk"],
        "value": float(row["value"]),
    }


def _spot_last_before(start_ts_ms: int, symbol: str = "btc/usd", spot_table: str = None):
    table = spot_table or SPOT_TABLE
    df = ch_df(
        f"""
SELECT event_ts_ms, event_time_msk, value
FROM {DB_NAME}.{table}
WHERE symbol = {_sql_quote(symbol.lower())}
  AND event_ts_ms < {int(start_ts_ms)}
ORDER BY event_ts_ms DESC
LIMIT 1
"""
    )
    if df.empty:
        return None
    row = df.iloc[0]
    return {
        "event_ts_ms": int(row["event_ts_ms"]),
        "event_time_msk": row["event_time_msk"],
        "value": float(row["value"]),
    }


def get_price_to_beat_from_spot(
    condition_id: str,
    duration_min: int = None,
    symbol: str = "btc/usd",
    mode: str = "first_at_or_after_start",
    fallback_to_last_before: bool = True,
    spot_table: str = None,
    gamma_active: bool = None,
    gamma_closed: bool = None,
) -> dict:
    """
    Получить price_to_beat для рынка по conditionId.

    mode:
    - first_at_or_after_start: первый спот-тик на/после старта рынка
    - last_before_start: последний спот-тик до старта рынка
    """
    market_row = fetch_gamma_market_by_condition_id(
        condition_id=condition_id,
        active=gamma_active,
        closed=gamma_closed,
    )
    start_ts_ms = market_start_ts_ms_from_gamma_row(
        market_row=market_row,
        duration_min=duration_min,
    )

    chosen = None
    chosen_mode = mode

    if mode == "last_before_start":
        chosen = _spot_last_before(start_ts_ms, symbol=symbol, spot_table=spot_table)
    else:
        chosen = _spot_first_at_or_after(start_ts_ms, symbol=symbol, spot_table=spot_table)
        if chosen is None and fallback_to_last_before:
            chosen = _spot_last_before(start_ts_ms, symbol=symbol, spot_table=spot_table)
            if chosen is not None:
                chosen_mode = "last_before_start_fallback"

    if chosen is None:
        raise ValueError("Не найдены спот-тики около старта рынка для выбранного символа")

    return {
        "gamma_market_id": market_row.get("id"),
        "conditionId": market_row.get("conditionId") or market_row.get("condition_id"),
        "question": market_row.get("question"),
        "endDate": market_row.get("endDate") or market_row.get("end_date"),
        "market_start_ts_ms": int(start_ts_ms),
        "market_start_time_msk": pd.to_datetime(start_ts_ms, unit="ms", utc=True)
        .tz_convert("Europe/Moscow")
        .tz_localize(None),
        "price_to_beat": float(chosen["value"]),
        "spot_event_ts_ms": int(chosen["event_ts_ms"]),
        "spot_event_time_msk": chosen["event_time_msk"],
        "mode": chosen_mode,
        "symbol": symbol.lower(),
    }


def fetch_spot_ticks_df(
    symbol: str = "btc/usd",
    ts_from_ms: int = None,
    ts_to_ms: int = None,
    spot_table: str = None,
) -> pd.DataFrame:
    table = spot_table or SPOT_TABLE
    where_parts = [f"symbol = {_sql_quote(symbol.lower())}"]
    if ts_from_ms is not None:
        where_parts.append(f"event_ts_ms >= {int(ts_from_ms)}")
    if ts_to_ms is not None:
        where_parts.append(f"event_ts_ms <= {int(ts_to_ms)}")

    where_sql = " AND ".join(where_parts)
    df = ch_df(
        f"""
SELECT
    event_time_msk,
    event_ts_ms,
    value
FROM {DB_NAME}.{table}
WHERE {where_sql}
ORDER BY event_ts_ms ASC
"""
    )
    if df.empty:
        return df
    df = (
        df.sort_values("event_ts_ms")
        .drop_duplicates(subset=["event_ts_ms"], keep="last")
        .reset_index(drop=True)
    )
    return df


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(float(x) / math.sqrt(2.0)))


def _bs_digital_call_prob(
    spot: float,
    strike: float,
    tau_years: float,
    sigma_annual: float,
    r: float = 0.0,
) -> float:
    if any(pd.isna(v) for v in (spot, strike, tau_years, sigma_annual, r)):
        return float("nan")

    spot = float(spot)
    strike = float(strike)
    tau_years = float(tau_years)
    sigma_annual = float(sigma_annual)
    r = float(r)

    if spot <= 0 or strike <= 0:
        return float("nan")
    if tau_years <= 0:
        return 1.0 if spot >= strike else 0.0

    if sigma_annual <= 1e-12:
        forward = spot * math.exp(r * tau_years)
        return 1.0 if forward >= strike else 0.0

    d2 = (
        math.log(spot / strike) + (r - 0.5 * sigma_annual * sigma_annual) * tau_years
    ) / (sigma_annual * math.sqrt(tau_years))
    p = _norm_cdf(d2)
    return min(1.0, max(0.0, float(p)))


def _add_rolling_sigma_annual(
    spot_df: pd.DataFrame,
    lookback_sec: int = 300,
    min_obs: int = 20,
    vol_floor_annual: float = 0.05,
) -> pd.DataFrame:
    if spot_df.empty:
        return spot_df.copy()
    if int(lookback_sec) <= 0:
        raise ValueError("lookback_sec должен быть > 0")
    if int(min_obs) <= 1:
        raise ValueError("min_obs должен быть > 1")
    if float(vol_floor_annual) < 0:
        raise ValueError("vol_floor_annual должен быть >= 0")

    out = spot_df.copy()
    out = out.sort_values("event_ts_ms").reset_index(drop=True)
    out["ts_dt"] = pd.to_datetime(out["event_ts_ms"], unit="ms", utc=True)
    out["log_s"] = out["value"].map(lambda x: math.log(float(x)) if float(x) > 0 else float("nan"))
    out["dt_sec"] = out["event_ts_ms"].diff() / 1000.0
    out["dlog"] = out["log_s"].diff()
    out["inst_var_per_sec"] = float("nan")

    good = (
        out["dt_sec"].notna()
        & (out["dt_sec"] > 0)
        & out["dlog"].notna()
    )
    out.loc[good, "inst_var_per_sec"] = (
        out.loc[good, "dlog"] * out.loc[good, "dlog"]
    ) / out.loc[good, "dt_sec"]

    roll = (
        out.set_index("ts_dt")["inst_var_per_sec"]
        .rolling(f"{int(lookback_sec)}s", min_periods=int(min_obs))
        .mean()
    )
    out["inst_var_per_sec_roll"] = roll.to_numpy()
    out["sigma_annual"] = (out["inst_var_per_sec_roll"] * SECONDS_PER_YEAR) ** 0.5
    out["sigma_annual"] = out["sigma_annual"].clip(lower=float(vol_floor_annual))
    return out


def build_black_scholes_up_df(
    market: str,
    min_total_size: float = 50.0,
    symbol: str = "btc/usd",
    duration_min: int = None,
    vol_lookback_sec: int = 300,
    vol_min_obs: int = 20,
    vol_floor_annual: float = 0.05,
    risk_free_rate: float = 0.0,
    asset_id: str = None,
    snapshot_hash: str = None,
    spot_table: str = None,
    gamma_active: bool = None,
    gamma_closed: bool = None,
) -> pd.DataFrame:
    """
    Теоретическая цена UP-контракта по Black-Scholes (digital call):
      P(UP) = N(d2),
      d2 = [ln(S/K) + (r - 0.5*sigma^2)*tau] / (sigma*sqrt(tau))

    где:
    - S: текущий спот из `crypto_spot`
    - K: price_to_beat (первый спот-тик нового рынка)
    - tau: время до конца рынка в годах
    - sigma: скользящая годовая волатильность по спот-тикам
    """
    if float(min_total_size) <= 0:
        raise ValueError("min_total_size должен быть > 0")

    df_quotes = build_depth_filtered_best_quotes_df(
        market=market,
        min_total_size=min_total_size,
        asset_id=asset_id,
        snapshot_hash=snapshot_hash,
    ).copy()
    if df_quotes.empty:
        return df_quotes

    market_row = fetch_gamma_market_by_condition_id(
        condition_id=market,
        active=gamma_active,
        closed=gamma_closed,
    )
    market_end_ts_ms = _safe_int_ts_ms(
        market_row.get("endDate")
        or market_row.get("end_date")
        or market_row.get("endTime")
        or market_row.get("end_time")
        or market_row.get("closedTime")
        or market_row.get("closed_time")
    )
    if market_end_ts_ms is None:
        raise ValueError("Не удалось разобрать время окончания рынка из Gamma")

    p2b = get_price_to_beat_from_spot(
        condition_id=market,
        duration_min=duration_min,
        symbol=symbol,
        mode="first_at_or_after_start",
        fallback_to_last_before=True,
        spot_table=spot_table,
        gamma_active=gamma_active,
        gamma_closed=gamma_closed,
    )
    strike = float(p2b["price_to_beat"])
    market_start_ts_ms = int(p2b["market_start_ts_ms"])

    spot_from_ms = int(market_start_ts_ms - max(1, int(vol_lookback_sec)) * 1000)
    spot_to_ms = int(
        max(
            market_end_ts_ms,
            int(df_quotes["event_ts_ms"].max()),
        )
    )
    df_spot = fetch_spot_ticks_df(
        symbol=symbol,
        ts_from_ms=spot_from_ms,
        ts_to_ms=spot_to_ms,
        spot_table=spot_table,
    )
    if df_spot.empty:
        raise ValueError("Не найдены спот-тики в выбранном окне рынка")

    df_spot = _add_rolling_sigma_annual(
        df_spot,
        lookback_sec=vol_lookback_sec,
        min_obs=vol_min_obs,
        vol_floor_annual=vol_floor_annual,
    )

    df_q = df_quotes.sort_values("event_ts_ms").reset_index(drop=True)
    df_s = df_spot[["event_ts_ms", "value", "sigma_annual"]].sort_values("event_ts_ms").reset_index(drop=True)
    df = pd.merge_asof(df_q, df_s, on="event_ts_ms", direction="backward")

    df["spot_value"] = df["value"].ffill().bfill()
    df["sigma_annual"] = df["sigma_annual"].fillna(float(vol_floor_annual))
    df["price_to_beat"] = strike
    df["market_start_ts_ms"] = int(market_start_ts_ms)
    df["market_end_ts_ms"] = int(market_end_ts_ms)
    df["time_to_expiry_sec"] = ((df["market_end_ts_ms"] - df["event_ts_ms"]) / 1000.0).clip(lower=0.0)
    df["tau_years"] = df["time_to_expiry_sec"] / SECONDS_PER_YEAR

    df["theoretical_up_bs"] = [
        _bs_digital_call_prob(
            spot=s,
            strike=strike,
            tau_years=t,
            sigma_annual=sig,
            r=risk_free_rate,
        )
        for s, t, sig in zip(df["spot_value"], df["tau_years"], df["sigma_annual"])
    ]

    df["buy_edge_bs"] = df["theoretical_up_bs"] - df["best_ask_n"]
    df["sell_edge_bs"] = df["best_bid_n"] - df["theoretical_up_bs"]
    df["max_cross_side_edge_bs"] = df[["buy_edge_bs", "sell_edge_bs"]].max(axis=1)
    df["inside_spread_bs"] = (
        (df["theoretical_up_bs"] >= df["best_bid_n"])
        & (df["theoretical_up_bs"] <= df["best_ask_n"])
    ).astype(int)

    df["price_to_beat_mode"] = p2b.get("mode")
    df["price_to_beat_spot_event_ts_ms"] = p2b.get("spot_event_ts_ms")
    df["spot_symbol"] = symbol.lower()
    return df


def rank_markets_by_black_scholes_edge(
    markets,
    min_total_size: float = 50.0,
    symbol: str = "btc/usd",
    duration_min: int = None,
    vol_lookback_sec: int = 300,
    vol_min_obs: int = 20,
    vol_floor_annual: float = 0.05,
    risk_free_rate: float = 0.0,
) -> pd.DataFrame:
    rows = []

    for market in markets:
        try:
            df = build_black_scholes_up_df(
                market=market,
                min_total_size=min_total_size,
                symbol=symbol,
                duration_min=duration_min,
                vol_lookback_sec=vol_lookback_sec,
                vol_min_obs=vol_min_obs,
                vol_floor_annual=vol_floor_annual,
                risk_free_rate=risk_free_rate,
            )
            if df.empty:
                rows.append(
                    {
                        "market": market,
                        "rows": 0,
                        "max_cross_side_edge_bs": float("nan"),
                        "p95_cross_side_edge_bs": float("nan"),
                        "inside_spread_rate_bs": float("nan"),
                        "last_theoretical_up_bs": float("nan"),
                        "last_best_bid_n": float("nan"),
                        "last_best_ask_n": float("nan"),
                        "price_to_beat": float("nan"),
                    }
                )
                continue

            last = df.iloc[-1]
            rows.append(
                {
                    "market": market,
                    "rows": int(len(df)),
                    "max_cross_side_edge_bs": float(df["max_cross_side_edge_bs"].max()),
                    "p95_cross_side_edge_bs": float(df["max_cross_side_edge_bs"].quantile(0.95)),
                    "inside_spread_rate_bs": float(df["inside_spread_bs"].mean()),
                    "last_theoretical_up_bs": float(last["theoretical_up_bs"]),
                    "last_best_bid_n": float(last["best_bid_n"]),
                    "last_best_ask_n": float(last["best_ask_n"]),
                    "price_to_beat": float(last["price_to_beat"]),
                }
            )
        except Exception as e:
            rows.append(
                {
                    "market": market,
                    "rows": 0,
                    "max_cross_side_edge_bs": float("nan"),
                    "p95_cross_side_edge_bs": float("nan"),
                    "inside_spread_rate_bs": float("nan"),
                    "last_theoretical_up_bs": float("nan"),
                    "last_best_bid_n": float("nan"),
                    "last_best_ask_n": float("nan"),
                    "price_to_beat": float("nan"),
                    "error": repr(e),
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(
        ["max_cross_side_edge_bs", "p95_cross_side_edge_bs"],
        ascending=[False, False],
        na_position="last",
    ).reset_index(drop=True)


def build_depth_filtered_best_quotes_df(
    market: str,
    min_total_size: float,
    asset_id: str = None,
    snapshot_hash: str = None,
    include_snapshot_point: bool = True,
) -> pd.DataFrame:
    """
    Восстановить стакан UP-стороны рынка и вернуть best bid/ask с depth-фильтром.

    Возвращаемая цена — это предельный исполняемый уровень, где суммарный размер
    впервые достигает `min_total_size`:
    - bid: от best bid, затем 0.001 ниже и т.д.
    - ask: от best ask, затем 0.001 выше и т.д.

    Если для рынка есть несколько snapshot, по умолчанию берется последний.
    Чтобы зафиксировать конкретный snapshot-сегмент, передай `snapshot_hash`.
    """
    if float(min_total_size) <= 0:
        raise ValueError("min_total_size должен быть > 0")

    where_parts = [f"market = {_sql_quote(market)}"]
    if asset_id:
        where_parts.append(f"asset_id = {_sql_quote(asset_id)}")
    if snapshot_hash:
        where_parts.append(f"hash = {_sql_quote(snapshot_hash)}")
    snapshot_where = " AND ".join(where_parts)

    df_snapshot_meta = ch_df(
        f"""
SELECT
    market,
    asset_id,
    hash,
    max(event_ts_ms) AS snapshot_ts_ms,
    max(event_time_msk) AS snapshot_time_msk
FROM {DB_NAME}.{ORDERBOOK_TABLE}
WHERE {snapshot_where}
GROUP BY market, asset_id, hash
ORDER BY snapshot_ts_ms DESC
LIMIT 1
"""
    )
    if df_snapshot_meta.empty:
        raise ValueError("Подходящий snapshot в orderbook не найден")

    snapshot_row = df_snapshot_meta.iloc[0]
    selected_market = str(snapshot_row["market"])
    selected_asset_id = str(snapshot_row["asset_id"])
    selected_hash = str(snapshot_row["hash"])
    snapshot_ts_ms = int(snapshot_row["snapshot_ts_ms"])
    snapshot_time_msk = snapshot_row["snapshot_time_msk"]

    df_snapshot = ch_df(
        f"""
SELECT
    side,
    price,
    size
FROM {DB_NAME}.{ORDERBOOK_TABLE}
WHERE market = {_sql_quote(selected_market)}
  AND asset_id = {_sql_quote(selected_asset_id)}
  AND hash = {_sql_quote(selected_hash)}
ORDER BY price ASC
"""
    )
    if df_snapshot.empty:
        raise ValueError("Строки snapshot не найдены")

    price_grid = sorted(float(x) for x in df_snapshot["price"].tolist())
    bid_prices_desc = list(reversed(price_grid))
    ask_prices_asc = price_grid[:]

    bid_sizes = {price: 0.0 for price in price_grid}
    ask_sizes = {price: 0.0 for price in price_grid}

    for row in df_snapshot.itertuples(index=False):
        price = float(row.price)
        size = float(row.size)
        side = str(row.side)
        if side == "bid":
            bid_sizes[price] = size
        elif side == "ask":
            ask_sizes[price] = size

    records = []

    if include_snapshot_point:
        best_bid_n, cum_bid_size = _depth_price(bid_sizes, bid_prices_desc, min_total_size)
        best_ask_n, cum_ask_size = _depth_price(ask_sizes, ask_prices_asc, min_total_size)
        records.append(
            {
                "event_time_msk": snapshot_time_msk,
                "event_ts_ms": snapshot_ts_ms,
                "market": selected_market,
                "asset_id": selected_asset_id,
                "snapshot_hash": selected_hash,
                "n": float(min_total_size),
                "best_bid_n": best_bid_n,
                "best_ask_n": best_ask_n,
                "cum_bid_size_at_best_bid_n": cum_bid_size,
                "cum_ask_size_at_best_ask_n": cum_ask_size,
                "spread_n": best_ask_n - best_bid_n
                if pd.notna(best_bid_n) and pd.notna(best_ask_n)
                else float("nan"),
            }
        )

    df_pc = ch_df(
        f"""
SELECT
    event_time_msk,
    event_ts_ms,
    side,
    price,
    size
FROM {DB_NAME}.{PRICECHANGE_TABLE}
WHERE market = {_sql_quote(selected_market)}
  AND asset_id = {_sql_quote(selected_asset_id)}
  AND event_ts_ms >= {snapshot_ts_ms}
ORDER BY event_ts_ms ASC, price ASC
"""
    )

    if df_pc.empty:
        return pd.DataFrame(records)

    current_ts = None
    current_time_msk = None

    for row in df_pc.itertuples(index=False):
        row_ts = int(row.event_ts_ms)
        if current_ts is None:
            current_ts = row_ts
            current_time_msk = row.event_time_msk
        elif row_ts != current_ts:
            best_bid_n, cum_bid_size = _depth_price(bid_sizes, bid_prices_desc, min_total_size)
            best_ask_n, cum_ask_size = _depth_price(ask_sizes, ask_prices_asc, min_total_size)
            records.append(
                {
                    "event_time_msk": current_time_msk,
                    "event_ts_ms": current_ts,
                    "market": selected_market,
                    "asset_id": selected_asset_id,
                    "snapshot_hash": selected_hash,
                    "n": float(min_total_size),
                    "best_bid_n": best_bid_n,
                    "best_ask_n": best_ask_n,
                    "cum_bid_size_at_best_bid_n": cum_bid_size,
                    "cum_ask_size_at_best_ask_n": cum_ask_size,
                    "spread_n": best_ask_n - best_bid_n
                    if pd.notna(best_bid_n) and pd.notna(best_ask_n)
                    else float("nan"),
                }
            )
            current_ts = row_ts
            current_time_msk = row.event_time_msk

        price = float(row.price)
        delta_size = float(row.size)
        side = str(row.side)

        if side == "bid":
            new_size = bid_sizes.get(price, 0.0) + delta_size
            bid_sizes[price] = 0.0 if abs(new_size) <= 1e-12 else new_size
        elif side == "ask":
            new_size = ask_sizes.get(price, 0.0) + delta_size
            ask_sizes[price] = 0.0 if abs(new_size) <= 1e-12 else new_size

    if current_ts is not None:
        best_bid_n, cum_bid_size = _depth_price(bid_sizes, bid_prices_desc, min_total_size)
        best_ask_n, cum_ask_size = _depth_price(ask_sizes, ask_prices_asc, min_total_size)
        records.append(
            {
                "event_time_msk": current_time_msk,
                "event_ts_ms": current_ts,
                "market": selected_market,
                "asset_id": selected_asset_id,
                "snapshot_hash": selected_hash,
                "n": float(min_total_size),
                "best_bid_n": best_bid_n,
                "best_ask_n": best_ask_n,
                "cum_bid_size_at_best_bid_n": cum_bid_size,
                "cum_ask_size_at_best_ask_n": cum_ask_size,
                "spread_n": best_ask_n - best_bid_n
                if pd.notna(best_bid_n) and pd.notna(best_ask_n)
                else float("nan"),
            }
        )

    return pd.DataFrame(records)


def build_theoretical_up_price_df(
    market: str,
    min_total_size: float = 50.0,
    ewm_span: int = 50,
    asset_id: str = None,
    snapshot_hash: str = None,
) -> pd.DataFrame:
    """
    Построить практический прокси теоретической цены UP-контракта.

    Механика:
    1) восстанавливаем depth-filtered bid/ask через кумулятивную глубину `min_total_size`
    2) считаем depth-filtered microprice
    3) сглаживаем microprice экспоненциальным средним

    Это прокси на основе микроструктуры стакана, а не spot/vol модель BTC.
    """
    if float(min_total_size) <= 0:
        raise ValueError("min_total_size должен быть > 0")
    if int(ewm_span) <= 1:
        raise ValueError("ewm_span должен быть > 1")

    df = build_depth_filtered_best_quotes_df(
        market=market,
        min_total_size=min_total_size,
        asset_id=asset_id,
        snapshot_hash=snapshot_hash,
    ).copy()

    if df.empty:
        return df

    df = df.sort_values(["event_ts_ms", "event_time_msk"]).reset_index(drop=True)

    df["mid_n"] = (df["best_bid_n"] + df["best_ask_n"]) / 2.0

    denom = df["cum_bid_size_at_best_bid_n"] + df["cum_ask_size_at_best_ask_n"]
    df["microprice_n"] = df["mid_n"]
    good = denom > 0
    df.loc[good, "microprice_n"] = (
        df.loc[good, "best_ask_n"] * df.loc[good, "cum_bid_size_at_best_bid_n"]
        + df.loc[good, "best_bid_n"] * df.loc[good, "cum_ask_size_at_best_ask_n"]
    ) / denom.loc[good]

    df["theoretical_up_raw"] = df["microprice_n"]
    df["theoretical_up"] = (
        df["theoretical_up_raw"]
        .ewm(span=int(ewm_span), adjust=False, min_periods=1)
        .mean()
    )

    df["gap_to_bid_n"] = df["theoretical_up"] - df["best_bid_n"]
    df["gap_to_ask_n"] = df["best_ask_n"] - df["theoretical_up"]
    df["abs_gap_to_mid_n"] = (df["theoretical_up"] - df["mid_n"]).abs()

    # Положительное значение означает, что теоретическая цена вышла за текущую котировку
    # и потенциально дает направленное преимущество.
    df["buy_edge_vs_ask_n"] = df["theoretical_up"] - df["best_ask_n"]
    df["sell_edge_vs_bid_n"] = df["best_bid_n"] - df["theoretical_up"]
    df["max_cross_side_edge_n"] = df[["buy_edge_vs_ask_n", "sell_edge_vs_bid_n"]].max(axis=1)

    return df


def rank_markets_by_theoretical_gap(
    markets,
    min_total_size: float = 50.0,
    ewm_span: int = 50,
) -> pd.DataFrame:
    """
    Ранжировать рынки по силе расхождения котировок
    с теоретическим прокси UP-цены.

    Метрики:
    - max_abs_gap_to_mid_n: максимальный |theoretical_up - mid_n|
    - p95_abs_gap_to_mid_n: 95-й перцентиль этого отклонения
    - max_buy_edge_vs_ask_n: максимальный положительный edge на покупку vs ask
    - max_sell_edge_vs_bid_n: максимальный положительный edge на продажу vs bid
    - max_cross_side_edge_n: максимальный перекос котировки на любой стороне
    """
    rows = []

    for market in markets:
        try:
            df = build_theoretical_up_price_df(
                market=market,
                min_total_size=min_total_size,
                ewm_span=ewm_span,
            )

            if df.empty:
                rows.append(
                    {
                        "market": market,
                        "rows": 0,
                        "max_abs_gap_to_mid_n": float("nan"),
                        "p95_abs_gap_to_mid_n": float("nan"),
                        "max_buy_edge_vs_ask_n": float("nan"),
                        "max_sell_edge_vs_bid_n": float("nan"),
                        "max_cross_side_edge_n": float("nan"),
                        "last_theoretical_up": float("nan"),
                        "last_best_bid_n": float("nan"),
                        "last_best_ask_n": float("nan"),
                    }
                )
                continue

            last = df.iloc[-1]
            rows.append(
                {
                    "market": market,
                    "rows": int(len(df)),
                    "max_abs_gap_to_mid_n": float(df["abs_gap_to_mid_n"].max()),
                    "p95_abs_gap_to_mid_n": float(df["abs_gap_to_mid_n"].quantile(0.95)),
                    "max_buy_edge_vs_ask_n": float(df["buy_edge_vs_ask_n"].max()),
                    "max_sell_edge_vs_bid_n": float(df["sell_edge_vs_bid_n"].max()),
                    "max_cross_side_edge_n": float(df["max_cross_side_edge_n"].max()),
                    "last_theoretical_up": float(last["theoretical_up"]),
                    "last_best_bid_n": float(last["best_bid_n"]),
                    "last_best_ask_n": float(last["best_ask_n"]),
                }
            )
        except Exception as e:
            rows.append(
                {
                    "market": market,
                    "rows": 0,
                    "max_abs_gap_to_mid_n": float("nan"),
                    "p95_abs_gap_to_mid_n": float("nan"),
                    "max_buy_edge_vs_ask_n": float("nan"),
                    "max_sell_edge_vs_bid_n": float("nan"),
                    "max_cross_side_edge_n": float("nan"),
                    "last_theoretical_up": float("nan"),
                    "last_best_bid_n": float("nan"),
                    "last_best_ask_n": float("nan"),
                    "error": repr(e),
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    return out.sort_values(
        ["max_cross_side_edge_n", "p95_abs_gap_to_mid_n"],
        ascending=[False, False],
        na_position="last",
    ).reset_index(drop=True)


pd.set_option("display.max_columns", 50)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", None)

# %%
# Очистить ВСЕ таблицы проекта (запускать только когда ws_ingest.py остановлен)

tables_to_truncate = [
    f"{DB_NAME}.{ORDERBOOK_TABLE}",
    f"{DB_NAME}.{PRICECHANGE_TABLE}",
    f"{DB_NAME}.ingest_heartbeat",
    f"{DB_NAME}.ingest_service_log",
]

for t in tables_to_truncate:
    print("ОЧИСТКА", t)
    c.command(f"TRUNCATE TABLE {t}")

print("Готово")


# %%
# Быстрая проверка здоровья данных
ch_df(
    f"""
SELECT
  countDistinct(tuple(market, asset_id, hash)) AS количество_snapshot,
  count() AS всего_строк_orderbook,
  count() / 999 AS строк_на_snapshot_деление_на_999
FROM {DB_NAME}.{ORDERBOOK_TABLE}
"""
)


# %%
# Полный QC по ВСЕМ рынкам (без ручного market-фильтра)
df_qc_all_markets = ch_df(
    f"""
SELECT
    check_name,
    value_num,
    details
FROM
(
    SELECT 'строк_orderbook' AS check_name, toFloat64(count()) AS value_num, '' AS details
    FROM {DB_NAME}.{ORDERBOOK_TABLE}

    UNION ALL

    SELECT 'строк_pricechange', toFloat64(count()), ''
    FROM {DB_NAME}.{PRICECHANGE_TABLE}

    UNION ALL

    SELECT 'количество_snapshot', toFloat64(countDistinct(concat(market, '|', asset_id, '|', hash))), ''
    FROM {DB_NAME}.{ORDERBOOK_TABLE}

    UNION ALL

    SELECT 'ожидаемых_orderbook_строк_из_snapshot',
           toFloat64(countDistinct(concat(market, '|', asset_id, '|', hash)) * 999),
           'должно быть равно строк_orderbook'
    FROM {DB_NAME}.{ORDERBOOK_TABLE}

    UNION ALL

    SELECT 'плохих_snapshot', toFloat64(count()), 'должно быть 0'
    FROM
    (
        SELECT market, asset_id, hash
        FROM {DB_NAME}.{ORDERBOOK_TABLE}
        GROUP BY market, asset_id, hash
        HAVING count() != 999
           OR sum(if(size > 0, 1, 0)) = 0
           OR sum(size) <= 0
    )

    UNION ALL

    SELECT 'orderbook_отрицательный_size_строк', toFloat64(count()), 'должно быть 0'
    FROM {DB_NAME}.{ORDERBOOK_TABLE}
    WHERE size < 0

    UNION ALL

    SELECT 'orderbook_плохой_диапазон_price_строк', toFloat64(count()), 'должно быть 0'
    FROM {DB_NAME}.{ORDERBOOK_TABLE}
    WHERE price < 0.001 OR price > 0.999

    UNION ALL

    SELECT 'orderbook_плохая_сетка_price_строк', toFloat64(count()), 'должно быть 0'
    FROM {DB_NAME}.{ORDERBOOK_TABLE}
    WHERE abs(price * 1000 - round(price * 1000)) > 0.000000001

    UNION ALL

    SELECT 'orderbook_дубликаты_уровней_строк', toFloat64(count()), 'должно быть 0'
    FROM
    (
        SELECT market, asset_id, hash, side, price
        FROM {DB_NAME}.{ORDERBOOK_TABLE}
        GROUP BY market, asset_id, hash, side, price
        HAVING count() > 1
    )

    UNION ALL

    SELECT 'pricechange_нулевых_delta_строк', toFloat64(count()), 'должно быть 0'
    FROM {DB_NAME}.{PRICECHANGE_TABLE}
    WHERE size = 0

    UNION ALL

    SELECT 'pricechange_плохая_side_строк', toFloat64(count()), 'должно быть 0'
    FROM {DB_NAME}.{PRICECHANGE_TABLE}
    WHERE side NOT IN ('bid', 'ask')

    UNION ALL

    SELECT 'pricechange_плохой_диапазон_price_строк', toFloat64(count()), 'должно быть 0'
    FROM {DB_NAME}.{PRICECHANGE_TABLE}
    WHERE price < 0.001 OR price > 0.999

    UNION ALL

    SELECT 'pricechange_плохая_сетка_price_строк', toFloat64(count()), 'должно быть 0'
    FROM {DB_NAME}.{PRICECHANGE_TABLE}
    WHERE abs(price * 1000 - round(price * 1000)) > 0.000000001

    UNION ALL

    SELECT 'pricechange_дубликаты_строк', toFloat64(count()), 'должно быть 0'
    FROM
    (
        SELECT market, asset_id, event_ts_ms, hash, side, price
        FROM {DB_NAME}.{PRICECHANGE_TABLE}
        GROUP BY market, asset_id, event_ts_ms, hash, side, price
        HAVING count() > 1
    )

    UNION ALL

    SELECT 'pricechange_сиротские_market_asset_пары', toFloat64(count()), 'должно быть 0'
    FROM
    (
        SELECT pc.market, pc.asset_id
        FROM
        (
            SELECT market, asset_id, min(event_ts_ms) AS first_pc_ts
            FROM {DB_NAME}.{PRICECHANGE_TABLE}
            GROUP BY market, asset_id
        ) pc
        LEFT JOIN
        (
            SELECT market, asset_id, min(event_ts_ms) AS first_ob_ts
            FROM {DB_NAME}.{ORDERBOOK_TABLE}
            GROUP BY market, asset_id
        ) ob
        ON pc.market = ob.market AND pc.asset_id = ob.asset_id
        WHERE ob.first_ob_ts IS NULL OR pc.first_pc_ts < ob.first_ob_ts
    )

    UNION ALL

    SELECT 'pricechange_средний_лаг_ms',
           round(avg(toUnixTimestamp(ingest_time) * 1000 - event_ts_ms), 2),
           'информационно'
    FROM {DB_NAME}.{PRICECHANGE_TABLE}

    UNION ALL

    SELECT 'pricechange_макс_лаг_ms',
           toFloat64(max(toUnixTimestamp(ingest_time) * 1000 - event_ts_ms)),
           'информационно'
    FROM {DB_NAME}.{PRICECHANGE_TABLE}

    UNION ALL

    SELECT 'pricechange_лаг_больше_10s_строк', toFloat64(count()), 'должно быть низким'
    FROM {DB_NAME}.{PRICECHANGE_TABLE}
    WHERE (toUnixTimestamp(ingest_time) * 1000 - event_ts_ms) > 10000

    UNION ALL

    SELECT 'service_error_строк', toFloat64(count()), 'должно быть 0'
    FROM {DB_NAME}.ingest_service_log
    WHERE level = 'ERROR' OR error_text != ''
)
ORDER BY check_name
"""
)
print(df_qc_all_markets.to_string(index=False))


# %%
# Список snapshot (метаданные)
df_snapshots = ch_df(
    f"""
SELECT
  market,
  asset_id,
  hash,
  min(event_time_msk) AS snapshot_time_msk,
  max(event_ts_ms) AS snapshot_ts_ms,
  count() AS levels,
  sum(if(size > 0, 1, 0)) AS non_zero_levels,
  sum(size) AS total_size
FROM {DB_NAME}.{ORDERBOOK_TABLE}
GROUP BY market, asset_id, hash
ORDER BY snapshot_ts_ms DESC
"""
)

# Полный вывод (без обрезки длинных id / hash в Jupyter)
with pd.option_context(
    "display.max_columns", None,
    "display.max_colwidth", None,
    "display.width", 1000,
):
    display(df_snapshots)


# %%
# Выбрать snapshot по индексу строки из df_snapshots (0 = самый новый)
snapshot_idx = 0
row = df_snapshots.iloc[snapshot_idx]

market = row["market"]
asset_id = row["asset_id"]
ob_hash = row["hash"]

print("рынок:", market)
print("asset_id:", asset_id)
print("hash:", ob_hash)
print("время_snapshot_msk:", row["snapshot_time_msk"])
print("ненулевых_уровней:", row["non_zero_levels"])


# %%
# Полный выбранный snapshot (999 строк)
df_orderbook = ch_df(
    f"""
SELECT
    event_time_msk,
    ingest_time,
    market,
    asset_id,
    hash,
    side,
    price,
    size,
    event_ts_ms
FROM {DB_NAME}.{ORDERBOOK_TABLE}
WHERE market = 'market_id'

ORDER BY price ASC
"""
)
df_orderbook
# %%
# Только ненулевые уровни в выбранном snapshot (обычно самый полезный вид).
# Примечание: превью Jupyter часто показывает только верх/низ таблицы (где size обычно 0).
df_orderbook_nonzero = ch_df(
    f"""
SELECT
    side,
    price,
    size,
    event_time_msk,
    ingest_time,
    event_ts_ms
FROM {DB_NAME}.{ORDERBOOK_TABLE}
WHERE market = 'market_id'
   AND size > 0
ORDER BY price ASC
"""
)
df_orderbook_nonzero


# %%
# Последние строки pricechange для того же market/asset
# ВАЖНО: pricechange.size — это DELTA размера на уровне (не абсолютный size и не сделка).
# Можно еще выводить hash.
df_pricechange = ch_df(
    f"""
SELECT
    event_time_msk,
    ingest_time,
    market,
    asset_id, 
    side,
    price,
    size,
    event_ts_ms
FROM {DB_NAME}.{PRICECHANGE_TABLE}
WHERE market = 'market_id'
ORDER BY event_ts_ms DESC, price ASC
LIMIT 5000
"""
)
df_pricechange


# %%
# Сводка по pricechange
# В целом опционально.
ch_df(
    f"""
SELECT
    side,
    count() AS rows_cnt,
    sum(abs(size)) AS abs_delta_sum,
    min(event_time_msk) AS min_time_msk,
    max(event_time_msk) AS max_time_msk
FROM {DB_NAME}.{PRICECHANGE_TABLE}
WHERE market = '{market}'
  AND asset_id = '{asset_id}'
GROUP BY side
ORDER BY side
"""
)

# %%
