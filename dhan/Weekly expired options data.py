"""
Download Dhan WEEKLY expired option OHLC — 10 calendar days per expiry.

For each weekly expiry (e.g. 2026-01-06) this fetches OHLC from
(expiry - 9 days) through expiry inclusive, using expiry_code 1 or 2
depending on whether that series is near or next week on each trade date.

Layout::

    Weekly Options data 60 mins/ATM Wise data/{SYMBOL}/{YYYY-MM-DD}/{ATM±n}/{SYMBOL}_{YYYY-MM-DD}_{CALL|PUT}.csv

``YYYY-MM-DD`` in the path is the **weekly expiry calendar date**.
"""

from __future__ import annotations

import datetime
import os
import time
from typing import Iterable, List, Optional, Tuple

import pandas as pd
from Dhan_Tradehull import Tradehull

client_id = "1000690797"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzgzODQ2NjQ5LCJpYXQiOjE3ODM3NjAyNDksInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMDAwNjkwNzk3In0.MgT9K8noe0YZX031hEoVD7TzokfVxX1ifAHvwbpEdsKqOD5BKmv1-yK4LBxVr2GCK0h91d41LofupJrBLi93rw"
tsl = Tradehull(client_id, access_token)

folder = "Weekly Options data 60 mins"
timeframe = 60
# Nifty weekly = Tuesday (Mon=0 … Sun=6). Sensex / BankNifty may differ.
WEEKLY_WEEKDAY = 1
# Inclusive calendar days ending on expiry (e.g. Jan 6 → Dec 28 … Jan 6).
DAYS_OF_DATA = 10

watchlist = ["NIFTY"]
security_ids = {"NIFTY": 13}

# Expiries whose calendar date falls in this range (inclusive).
start_date = datetime.date(2026, 1, 1)
end_date = datetime.date(2026, 7, 11)

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


def generate_weekly_expiries(
    start: datetime.date,
    end: datetime.date,
    weekday: int = WEEKLY_WEEKDAY,
) -> List[datetime.date]:
    """All weekly expiry dates with start <= expiry <= end."""
    d = current_weekly_expiry(start, weekday=weekday)
    out: List[datetime.date] = []
    while d <= end:
        out.append(d)
        d += datetime.timedelta(days=7)
    return out


def week_expiry_code(trade_date: datetime.date, target_expiry: datetime.date) -> Optional[int]:
    """
    Dhan WEEK expiry_code relative to ``trade_date``.
    1 = near week, 2 = next week. None if target is not near/next.
    """
    cur = current_weekly_expiry(trade_date)
    if target_expiry < cur:
        return None
    code = ((target_expiry - cur).days // 7) + 1
    if code < 1 or code > 2:
        return None
    return code


def window_for_expiry(expiry_date: datetime.date, days: int = DAYS_OF_DATA) -> Tuple[datetime.date, datetime.date]:
    """Inclusive [from, to] calendar window ending on expiry."""
    return expiry_date - datetime.timedelta(days=days - 1), expiry_date


def iter_code_chunks(
    from_date: datetime.date,
    to_date: datetime.date,
    target_expiry: datetime.date,
) -> Iterable[Tuple[datetime.date, datetime.date, int]]:
    """
    Yield (chunk_start, chunk_end, expiry_code) for consecutive days that share
    the same WEEK expiry_code for ``target_expiry``.
    """
    chunk_start: Optional[datetime.date] = None
    chunk_code: Optional[int] = None
    chunk_end: Optional[datetime.date] = None

    d = from_date
    while d <= to_date:
        code = week_expiry_code(d, target_expiry)
        if code is None:
            if chunk_start is not None and chunk_code is not None and chunk_end is not None:
                yield chunk_start, chunk_end, chunk_code
                chunk_start = chunk_code = chunk_end = None
        elif chunk_start is None:
            chunk_start = chunk_end = d
            chunk_code = code
        elif code == chunk_code:
            chunk_end = d
        else:
            yield chunk_start, chunk_end, chunk_code
            chunk_start = chunk_end = d
            chunk_code = code
        d += datetime.timedelta(days=1)

    if chunk_start is not None and chunk_code is not None and chunk_end is not None:
        yield chunk_start, chunk_end, chunk_code


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


expiries = generate_weekly_expiries(start_date, end_date)
print(
    f"Weekly expiries: {len(expiries)} "
    f"({expiries[0]} … {expiries[-1]}), {DAYS_OF_DATA} days each"
)

for name in watchlist:
    security_id = security_ids.get(name)
    if security_id is None:
        raise ValueError(f"Missing security ID for {name}")

    for expiry_date in expiries:
        win_from, win_to = window_for_expiry(expiry_date)
        expiry_str = expiry_date.strftime("%Y-%m-%d")

        for rangex in atm_range:
            for right in ["CALL", "PUT"]:
                try:
                    chunk_frames = []
                    for chunk_start, chunk_end, expiry_code in iter_code_chunks(
                        win_from, win_to, expiry_date
                    ):
                        raw = tsl.get_expired_option_data(
                            exchangeSegment="NSE_FNO",
                            instrument="OPTIDX",
                            fromDate=chunk_start.strftime("%Y-%m-%d"),
                            toDate=chunk_end.strftime("%Y-%m-%d"),
                            exchange="NSE",
                            interval=timeframe,
                            securityId=security_id,
                            expiry_flag="WEEK",
                            expiry_code=expiry_code,
                            strike=rangex,
                            option_type=right,
                        )
                        data = _as_dataframe(raw, right)
                        if data is not None and not data.empty:
                            chunk_frames.append(data)
                        time.sleep(0.1)

                    if not chunk_frames:
                        print(f"{name} {expiry_str} {rangex} {right}: No data")
                        continue

                    final_data = pd.concat(chunk_frames, ignore_index=True)
                    if "datetime" in final_data.columns:
                        final_data = final_data.drop_duplicates(
                            subset=["datetime"], keep="last"
                        )
                    else:
                        final_data = final_data.drop_duplicates(keep="last")

                    file_name = f"{name}_{expiry_str}_{right}.csv"
                    path = f"{folder}/ATM Wise data/{name}/{expiry_str}/{rangex}"
                    os.makedirs(path, exist_ok=True)
                    final_data.to_csv(f"{path}/{file_name}", index=False)
                    print(
                        f"{name} {expiry_str} {rangex} {right}: "
                        f"OK ({len(final_data)} rows, {win_from}→{win_to})"
                    )
                except Exception as e:
                    print(f"{name} {expiry_str} {rangex} {right}: Error {e}")
                    continue
