from Dhan_Tradehull import Tradehull
import calendar
import time
import datetime
import os
import pandas as pd

client_id = "1000690797"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc2MTc2MzMxLCJpYXQiOjE3NzYwODk5MzEsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMDAwNjkwNzk3In0.V2bsKdfP-t6duBE6_FU2NY9y67bNmi0unNzLsKa_ft68CkzsfjOYUxr70_C_uZKYES2gFFTh-d5dSxJpWcY5Sw"
tsl = Tradehull(client_id, access_token)
folder = "Quarterly Options data 15 mins"

watchlist = ["NIFTY"]
security_ids = {"NIFTY": 13}
quarterly_months = [3, 6, 9, 12]
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


def generate_quarterly_expiries(from_year, to_year):
    expiries = []
    for yr in range(from_year, to_year + 1):
        for mth in quarterly_months:
            expiries.append(last_thursday(yr, mth))
    return sorted(expiries)


def quarter_start_for_expiry(expiry_date):
    if expiry_date.month == 3:
        return datetime.date(expiry_date.year, 1, 1)
    if expiry_date.month == 6:
        return datetime.date(expiry_date.year, 4, 1)
    if expiry_date.month == 9:
        return datetime.date(expiry_date.year, 7, 1)
    if expiry_date.month == 12:
        return datetime.date(expiry_date.year, 10, 1)
    raise ValueError(f"Unsupported quarterly month: {expiry_date.month}")


def month_end(day):
    last_day = calendar.monthrange(day.year, day.month)[1]
    return datetime.date(day.year, day.month, last_day)


def month_diff(start_day, end_day):
    return (end_day.year - start_day.year) * 12 + (end_day.month - start_day.month)


expiries = generate_quarterly_expiries(start_year, end_year)
previous_quarter_expiry = None

for name in watchlist:
    security_id = security_ids.get(name)
    if security_id is None:
        raise ValueError(f"Missing security ID for {name}")

    for expiry_date in expiries:
        quarter_start = quarter_start_for_expiry(expiry_date)
        if previous_quarter_expiry is None:
            current_start = quarter_start
        else:
            current_start = max(quarter_start, previous_quarter_expiry + datetime.timedelta(days=1))

        for rangex in atm_range:
            for right in ["CALL", "PUT"]:
                try:
                    chunk_frames = []
                    run_date = current_start

                    while run_date <= expiry_date:
                        run_end = min(month_end(run_date), expiry_date)
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

        previous_quarter_expiry = expiry_date
