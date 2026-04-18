from Dhan_Tradehull import Tradehull
import calendar
import time
import datetime
import os
import pandas as pd

client_id = "1000690797"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc2NTkzMjQyLCJpYXQiOjE3NzY1MDY4NDIsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMDAwNjkwNzk3In0.cKS7VdYy8aE6WhxxCdbarpziDCDTjFX1gvaDGs1Ha8sVOsCpT7Qz0U517kX6N8cyrADNykgM7lbllhz5pnfZcQ"
tsl = Tradehull(client_id, access_token)
folder = "Two month Options data 15 mins"

watchlist = ["NIFTY"]
security_ids = {"NIFTY": 13}
start_year = 2025
end_year = 2025

atm_range = [
    "ATM-10", "ATM-9", "ATM-8", "ATM-7", "ATM-6", "ATM-5", "ATM-4", "ATM-3",
    "ATM-2", "ATM-1", "ATM", "ATM+1", "ATM+2", "ATM+3", "ATM+4", "ATM+5",
    "ATM+6", "ATM+7", "ATM+8", "ATM+9", "ATM+10"
]


def last_thursday(year, month):
    last_day = calendar.monthrange(year, month)[1]
    d = datetime.date(year, month, last_day)
    while d.weekday() != 3:  # Thursday
        d -= datetime.timedelta(days=1)
    return d


def generate_monthly_expiries(from_year, to_year):
    expiries = []
    for yr in range(from_year, to_year + 1):
        for mth in range(1, 13):
            expiries.append(last_thursday(yr, mth))
    return sorted(expiries)


def two_month_window_start_for_expiry(expiry_date):
    """Jan 2025 expiry -> 1 Dec 2024; Feb 2025 expiry -> 1 Jan 2025; etc."""
    if expiry_date.month == 1:
        return datetime.date(expiry_date.year - 1, 12, 1)
    return datetime.date(expiry_date.year, expiry_date.month - 1, 1)


# Match quarterly-style batching: each API call covers at most this many calendar days
# (e.g. Feb expiry: 1–30 Jan, then 31 Jan–expiry).
CHUNK_DAYS = 30


def month_diff(start_day, end_day):
    return (end_day.year - start_day.year) * 12 + (end_day.month - start_day.month)


expiries = generate_monthly_expiries(start_year, end_year)
previous_expiry = None

for name in watchlist:
    security_id = security_ids.get(name)
    if security_id is None:
        raise ValueError(f"Missing security ID for {name}")

    for expiry_date in expiries:
        window_start = two_month_window_start_for_expiry(expiry_date)
        if previous_expiry is None:
            current_start = window_start
        else:
            current_start = max(window_start, previous_expiry + datetime.timedelta(days=1))

        for rangex in atm_range:
            for right in ["CALL", "PUT"]:
                try:
                    chunk_frames = []
                    run_date = current_start

                    while run_date <= expiry_date:
                        run_end = min(
                            run_date + datetime.timedelta(days=CHUNK_DAYS - 1),
                            expiry_date,
                        )
                        expiry_code = month_diff(run_date, expiry_date) + 1
                        expiry_code = max(1, min(3, expiry_code))

                        data = tsl.get_expired_option_data(
                            exchangeSegment="NSE_FNO",
                            instrument="OPTIDX",
                            fromDate=run_date.strftime("%Y-%m-%d"),
                            toDate=run_end.strftime("%Y-%m-%d"),
                            exchange="NSE",
                            interval=15,
                            securityId=security_id,
                            expiry_flag="MONTH",
                            expiry_code=expiry_code,
                            strike=rangex,
                            option_type=right,
                        )
                        if data is not None and not data.empty:
                            chunk_frames.append(data)

                        run_date = run_end + datetime.timedelta(days=1)

                    expiry_str = expiry_date.strftime("%Y-%m-%d")
                    if not chunk_frames:
                        print(f"{name} {rangex} {expiry_str} {right}: No data")
                        continue

                    final_data = pd.concat(chunk_frames, ignore_index=True).drop_duplicates()
                    file_name = f"{name}_{expiry_str}_{right}.csv"
                    path = f"{folder}/ATM Wise data/{name}/{expiry_str}/{rangex}"
                    os.makedirs(path, exist_ok=True)
                    final_data.to_csv(f"{path}/{file_name}", index=False)
                    print(f"{name} {rangex} {expiry_str} {file_name}: Download completed")
                    time.sleep(0.1)
                except Exception as e:
                    print(f"{name} {expiry_date} {right} {rangex}: Error {e}")
                    continue

        previous_expiry = expiry_date
