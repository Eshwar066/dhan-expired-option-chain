"""
Download Dhan WEEKLY expired option OHLC (current + next expiry per trade date).

For a given date (e.g. 2026-01-06) this fetches:
  - expiry_code=1 → current weekly expiry
  - expiry_code=2 → next weekly expiry

Example Jan 6 2026 (Nifty Tuesday weekly):
  current = 2026-01-06, next = 2026-01-13

Layout (same as Monthly / Two-month scripts)::

    Weekly Options data 60 mins/ATM Wise data/{SYMBOL}/{YYYY-MM-DD}/{ATM±n}/{SYMBOL}_{YYYY-MM-DD}_{CALL|PUT}.csv

``YYYY-MM-DD`` in the path is the **weekly expiry calendar date**, not the trade date.

Reference:
  - Expired options data.py (MONTH + expiry_code=1)
  - Two month expired options data.py (MONTH windowed chunks)
"""

from __future__ import annotations

import datetime
import os
import time
from typing import Iterable, List, Tuple

import pandas as pd
from Dhan_Tradehull import Tradehull

client_id = "1000690797"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc2NTkzMjQyLCJpYXQiOjE3NzY1MDY4NDIsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMDAwNjkwNzk3In0.cKS7VdYy8aE6WhxxCdbarpziDCDTjFX1gvaDGs1Ha8sVOsCpT7Qz0U517kX6N8cyrADNykgM7lbllhz5pnfZcQ"
tsl = Tradehull(client_id, access_token)

folder = "Weekly Options data 60 mins"
timeframe = 60
# Nifty weekly = Tuesday (Mon=0 … Sun=6). Sensex / BankNifty may differ.
WEEKLY_WEEKDAY = 1

watchlist = ["NIFTY"]
security_ids = {"NIFTY": 13}

# Inclusive trade-date range. Narrow to one day to smoke-test (Jan 6 → 2 expiries).
start_date = datetime.date(2026, 1, 6)
end_date = datetime.date(2026, 1, 6)

atm_range = [
    "ATM-10",
    "ATM-9",
    "ATM-8",
    "ATM-7",
    "ATM-6",
    "ATM-5",
    "ATM-4",
    "ATM-3",
    "ATM-2",
    "ATM-1",
    "ATM",
    "ATM+1",
    "ATM+2",
    "ATM+3",
    "ATM+4",
    "ATM+5",
    "ATM+6",
    "ATM+7",
    "ATM+8",
    "ATM+9",
    "ATM+10",
]


def current_weekly_expiry(trade_date: datetime.date, weekday: int = WEEKLY_WEEKDAY) -> datetime.date:
    """Weekly expiry on ``weekday`` on or after ``trade_date``."""
    wd = int(weekday) % 7
    days_ahead = (wd - trade_date.weekday()) % 7
    return trade_date + datetime.timedelta(days=days_ahead)


def next_weekly_expiry(trade_date: datetime.date, weekday: int = WEEKLY_WEEKDAY) -> datetime.date:
    """Weekly expiry strictly after the current weekly for ``trade_date``."""
    return current_weekly_expiry(trade_date, weekday=weekday) + datetime.timedelta(days=7)


def iter_weekdays(start: datetime.date, end: datetime.date) -> Iterable[datetime.date]:
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += datetime.timedelta(days=1)


def expiries_for_trade_date(
    trade_date: datetime.date,
) -> List[Tuple[int, datetime.date]]:
    """
    Dhan rollingoption WEEK: expiry_code 1 = near, 2 = next.
    Returns [(1, current_expiry), (2, next_expiry)].
    """
    cur = current_weekly_expiry(trade_date)
    nxt = next_weekly_expiry(trade_date)
    return [(1, cur), (2, nxt)]


def _as_dataframe(data, option_type: str) -> pd.DataFrame:
    if data is None:
        return pd.DataFrame()
    if isinstance(data, pd.DataFrame):
        return data
    if isinstance(data, dict):
        key = "CE" if str(option_type).upper() in ("CALL", "CE") else "PE"
        side = data.get(key)
        if isinstance(side, pd.DataFrame):
            return side
        side = data.get(str(option_type).upper())
        if isinstance(side, pd.DataFrame):
            return side
    return pd.DataFrame()


def _append_csv(path: str, file_name: str, frame: pd.DataFrame) -> None:
    os.makedirs(path, exist_ok=True)
    out = os.path.join(path, file_name)
    if os.path.isfile(out):
        prev = pd.read_csv(out)
        merged = pd.concat([prev, frame], ignore_index=True)
        if "datetime" in merged.columns:
            merged = merged.drop_duplicates(subset=["datetime"], keep="last")
        else:
            merged = merged.drop_duplicates(keep="last")
        merged.to_csv(out, index=False)
    else:
        frame.to_csv(out, index=False)


_demo = datetime.date(2026, 1, 6)
print(
    f"Example {_demo}: current={current_weekly_expiry(_demo)} "
    f"next={next_weekly_expiry(_demo)} (expiry_code 1 and 2)"
)

for name in watchlist:
    security_id = security_ids.get(name)
    if security_id is None:
        raise ValueError(f"Missing security ID for {name}")

    for trade_date in iter_weekdays(start_date, end_date):
        trade_str = trade_date.strftime("%Y-%m-%d")
        for expiry_code, expiry_date in expiries_for_trade_date(trade_date):
            expiry_str = expiry_date.strftime("%Y-%m-%d")

            for rangex in atm_range:
                for right in ["CALL", "PUT"]:
                    try:
                        raw = tsl.get_expired_option_data(
                            exchangeSegment="NSE_FNO",
                            instrument="OPTIDX",
                            fromDate=trade_str,
                            toDate=trade_str,
                            exchange="NSE",
                            interval=timeframe,
                            securityId=security_id,
                            expiry_flag="WEEK",
                            expiry_code=expiry_code,
                            strike=rangex,
                            option_type=right,
                        )
                        data = _as_dataframe(raw, right)
                        if data is None or data.empty:
                            print(
                                f"{name} {trade_str} code={expiry_code} "
                                f"exp={expiry_str} {rangex} {right}: No data"
                            )
                            continue

                        file_name = f"{name}_{expiry_str}_{right}.csv"
                        path = f"{folder}/ATM Wise data/{name}/{expiry_str}/{rangex}"
                        _append_csv(path, file_name, data)
                        print(
                            f"{name} {trade_str} code={expiry_code} "
                            f"exp={expiry_str} {rangex} {right}: OK (+{len(data)} rows)"
                        )
                        time.sleep(0.1)
                    except Exception as e:
                        print(
                            f"{name} {trade_str} code={expiry_code} "
                            f"exp={expiry_str} {rangex} {right}: Error {e}"
                        )
                        continue
