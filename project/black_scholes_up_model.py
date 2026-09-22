"""
black_scholes_up_model.py

Математическая модель теоретической цены контракта UP
через Black-Scholes для бинарного (digital) call.

Идея:
Контракт UP платит 1, если S(T) >= K, иначе 0.
Тогда при предположениях Black-Scholes теоретическая цена равна вероятности:

    P(UP) = N(d2)

где
    d2 = [ln(S/K) + (r - 0.5*sigma^2)*tau] / (sigma*sqrt(tau))

S      - текущая спот-цена
K      - price_to_beat (страйк)
sigma  - годовая волатильность (например, 0.594)
r      - безрисковая ставка (обычно 0 для коротких горизонтов)
tau    - время до экспирации в годах
N(.)   - CDF стандартного нормального распределения
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Optional

try:
    import pandas as pd
except Exception:  # pragma: no cover - только для сред без pandas
    pd = None


SECONDS_PER_YEAR = 365.0 * 24.0 * 60.0 * 60.0


@dataclass(frozen=True)
class BlackScholesUpParams:
    """
    Параметры модели для UP-контракта.
    """

    strike: float
    sigma_annual: float
    r: float = 0.0


def normal_cdf(x: float) -> float:
    """
    CDF стандартного нормального распределения.
    """
    return 0.5 * (1.0 + math.erf(float(x) / math.sqrt(2.0)))


def digital_call_probability(
    spot: float,
    strike: float,
    tau_years: float,
    sigma_annual: float,
    r: float = 0.0,
) -> float:
    """
    Теоретическая цена/вероятность UP-контракта: P(UP)=N(d2).

    Возвращает число в диапазоне [0, 1].
    """
    spot = float(spot)
    strike = float(strike)
    tau_years = float(tau_years)
    sigma_annual = float(sigma_annual)
    r = float(r)

    if spot <= 0 or strike <= 0:
        return float("nan")

    # На экспирации вероятность превращается в бинарный исход.
    if tau_years <= 0:
        return 1.0 if spot >= strike else 0.0

    # Если sigma ~= 0, используем форвардное правило без шума.
    if sigma_annual <= 1e-12:
        forward = spot * math.exp(r * tau_years)
        return 1.0 if forward >= strike else 0.0

    d2 = (
        math.log(spot / strike) + (r - 0.5 * sigma_annual * sigma_annual) * tau_years
    ) / (sigma_annual * math.sqrt(tau_years))

    p = normal_cdf(d2)
    return min(1.0, max(0.0, float(p)))


def tau_years_from_ms(event_ts_ms: int, market_end_ts_ms: int) -> float:
    """
    Перевод времени до экспирации в годы.
    """
    dt_sec = (int(market_end_ts_ms) - int(event_ts_ms)) / 1000.0
    if dt_sec <= 0:
        return 0.0
    return dt_sec / SECONDS_PER_YEAR


def add_theoretical_up_columns(
    df: "pd.DataFrame",
    *,
    strike: float,
    sigma_annual: float,
    market_end_ts_ms: Optional[int] = None,
    market_end_col: Optional[str] = None,
    event_ts_col: str = "event_ts_ms",
    spot_col: str = "spot_value",
    best_bid_col: str = "best_bid_n",
    best_ask_col: str = "best_ask_n",
    r: float = 0.0,
) -> "pd.DataFrame":
    """
    Добавляет в DataFrame:
    - tau_years
    - theoretical_up_bs
    - buy_edge_bs, sell_edge_bs, max_cross_side_edge_bs
    - inside_spread_bs (0/1)

    Требования:
    - event_ts_col и spot_col обязательны
    - время конца рынка: либо market_end_ts_ms (скаляр), либо market_end_col (построчно)
    """
    if pd is None:
        raise ImportError("Для add_theoretical_up_columns нужен pandas")

    if event_ts_col not in df.columns:
        raise KeyError(f"В DataFrame нет колонки {event_ts_col!r}")
    if spot_col not in df.columns:
        raise KeyError(f"В DataFrame нет колонки {spot_col!r}")
    if market_end_ts_ms is None and market_end_col is None:
        raise ValueError("Нужно передать market_end_ts_ms или market_end_col")
    if market_end_col is not None and market_end_col not in df.columns:
        raise KeyError(f"В DataFrame нет колонки {market_end_col!r}")

    out = df.copy()

    if market_end_col is None:
        end_values = [int(market_end_ts_ms)] * len(out)
    else:
        end_values = out[market_end_col].astype("int64").tolist()

    taus = [
        tau_years_from_ms(int(ts), int(end_ts))
        for ts, end_ts in zip(out[event_ts_col].astype("int64"), end_values)
    ]
    out["tau_years"] = taus

    out["theoretical_up_bs"] = [
        digital_call_probability(
            spot=float(s),
            strike=float(strike),
            tau_years=float(tau),
            sigma_annual=float(sigma_annual),
            r=float(r),
        )
        for s, tau in zip(out[spot_col].astype(float), out["tau_years"].astype(float))
    ]

    if best_bid_col in out.columns and best_ask_col in out.columns:
        out["buy_edge_bs"] = out["theoretical_up_bs"] - out[best_ask_col]
        out["sell_edge_bs"] = out[best_bid_col] - out["theoretical_up_bs"]
        out["max_cross_side_edge_bs"] = out[["buy_edge_bs", "sell_edge_bs"]].max(axis=1)
        out["inside_spread_bs"] = (
            (out["theoretical_up_bs"] >= out[best_bid_col])
            & (out["theoretical_up_bs"] <= out[best_ask_col])
        ).astype(int)

    return out


def deviation_metrics(
    df: "pd.DataFrame",
    *,
    theoretical_col: str = "theoretical_up_bs",
    best_bid_col: str = "best_bid_n",
    best_ask_col: str = "best_ask_n",
) -> Dict[str, float]:
    """
    Считает ключевые метрики отклонения теоретической цены от котировок.
    """
    if pd is None:
        raise ImportError("Для deviation_metrics нужен pandas")

    for col in (theoretical_col, best_bid_col, best_ask_col):
        if col not in df.columns:
            raise KeyError(f"В DataFrame нет колонки {col!r}")

    mid = (df[best_bid_col] + df[best_ask_col]) / 2.0
    err = df[theoretical_col] - mid

    inside_spread = (
        (df[theoretical_col] >= df[best_bid_col])
        & (df[theoretical_col] <= df[best_ask_col])
    )
    buy_edge = df[theoretical_col] - df[best_ask_col]
    sell_edge = df[best_bid_col] - df[theoretical_col]
    cross = pd.concat([buy_edge, sell_edge], axis=1).max(axis=1)

    return {
        "rows": float(len(df)),
        "inside_spread_rate": float(inside_spread.mean()),
        "mae_to_mid": float(err.abs().mean()),
        "rmse_to_mid": float((err.pow(2).mean()) ** 0.5),
        "pct_theory_above_ask": float((df[theoretical_col] > df[best_ask_col]).mean()),
        "pct_theory_below_bid": float((df[theoretical_col] < df[best_bid_col]).mean()),
        "mean_buy_edge": float(buy_edge.mean()),
        "mean_sell_edge": float(sell_edge.mean()),
        "max_cross_side_edge": float(cross.max()),
        "p95_cross_side_edge": float(cross.quantile(0.95)),
    }


__all__ = [
    "SECONDS_PER_YEAR",
    "BlackScholesUpParams",
    "normal_cdf",
    "digital_call_probability",
    "tau_years_from_ms",
    "add_theoretical_up_columns",
    "deviation_metrics",
]

