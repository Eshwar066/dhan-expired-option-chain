from __future__ import annotations

from pathlib import Path
import datetime as dt
import calendar

import pandas as pd
import streamlit as st


WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
OPTIONS_DATA_ROOT = WORKSPACE_ROOT / "delta" / "BTC-2026"
FUTURES_DATA_ROOT = WORKSPACE_ROOT / "delta" / "F-BTC-2026"


def month_bounds(year: int, month: int) -> tuple[dt.date, dt.date]:
    last_day = calendar.monthrange(year, month)[1]
    return dt.date(year, month, 1), dt.date(year, month, last_day)


@st.cache_data
def discover_monthly_files(root_dir: str) -> list[str]:
    root = Path(root_dir)
    if not root.exists():
        return []
    # Some folders are suffixed with ".csv"; keep only real files.
    files = sorted(p for p in root.glob("**/*.csv") if p.is_file())
    return [str(p) for p in files]


@st.cache_data
def infer_date_bounds(files: tuple[str, ...]) -> tuple[dt.date, dt.date]:
    starts: list[dt.date] = []
    ends: list[dt.date] = []

    for file_name in files:
        stem = Path(file_name).stem
        # Expected stem pattern: BTC_2026-01
        if "_" not in stem:
            continue
        _, month_part = stem.split("_", 1)
        try:
            year_str, month_str = month_part.split("-", 1)
            year = int(year_str)
            month = int(month_str)
            start, end = month_bounds(year, month)
            starts.append(start)
            ends.append(end)
        except (ValueError, TypeError):
            continue

    if not starts:
        today = dt.date.today()
        return today, today
    return min(starts), max(ends)


def parse_product_symbol(chunk: pd.DataFrame) -> pd.DataFrame:
    parsed = chunk["product_symbol"].str.extract(
        r"^(?P<option_type>[CP])-(?P<underlying>[^-]+)-(?P<strike>\d+(?:\.\d+)?)-(?P<expiry_code>\d{6})$"
    )
    chunk = chunk.join(parsed)
    chunk["strike"] = pd.to_numeric(chunk["strike"], errors="coerce")
    chunk["expiry_date"] = pd.to_datetime(chunk["expiry_code"], format="%d%m%y", errors="coerce")
    chunk["trade_date"] = chunk["timestamp"].dt.date
    return chunk.dropna(subset=["option_type", "strike", "expiry_date"])


@st.cache_data
def load_trades_filtered(
    files: tuple[str, ...],
    from_date: dt.date,
    to_date: dt.date,
    time_filter: dt.time | None,
) -> pd.DataFrame:
    start_ts = pd.Timestamp(from_date)
    end_ts = pd.Timestamp(to_date) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)

    frames: list[pd.DataFrame] = []
    selected_hour = None if time_filter is None else time_filter.hour
    selected_minute = None if time_filter is None else time_filter.minute

    for file_path in files:
        file_obj = Path(file_path)
        if not file_obj.is_file():
            continue
        for chunk in pd.read_csv(
            file_path,
            usecols=["product_symbol", "price", "size", "timestamp", "buyer_role"],
            chunksize=300_000,
        ):
            chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], errors="coerce")
            chunk = chunk.dropna(subset=["timestamp"])
            chunk = chunk[(chunk["timestamp"] >= start_ts) & (chunk["timestamp"] <= end_ts)]
            if chunk.empty:
                continue

            if selected_hour is not None and selected_minute is not None:
                # Match minute to emulate "time snapshot" from intraday data.
                chunk = chunk[
                    (chunk["timestamp"].dt.hour == selected_hour)
                    & (chunk["timestamp"].dt.minute == selected_minute)
                ]
                if chunk.empty:
                    continue

            chunk = parse_product_symbol(chunk)
            if chunk.empty:
                continue

            chunk["price"] = pd.to_numeric(chunk["price"], errors="coerce")
            chunk["size"] = pd.to_numeric(chunk["size"], errors="coerce")
            chunk = chunk.dropna(subset=["price", "size"])
            if chunk.empty:
                continue
            frames.append(chunk)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


@st.cache_data
def load_futures_spot(
    files: tuple[str, ...],
    from_date: dt.date,
    to_date: dt.date,
    time_filter: dt.time | None,
) -> pd.DataFrame:
    start_ts = pd.Timestamp(from_date)
    end_ts = pd.Timestamp(to_date) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)

    selected_hour = None if time_filter is None else time_filter.hour
    selected_minute = None if time_filter is None else time_filter.minute
    frames: list[pd.DataFrame] = []

    for file_path in files:
        file_obj = Path(file_path)
        if not file_obj.is_file():
            continue
        for chunk in pd.read_csv(
            file_path,
            usecols=["price", "timestamp"],
            chunksize=300_000,
        ):
            chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], errors="coerce")
            chunk["price"] = pd.to_numeric(chunk["price"], errors="coerce")
            chunk = chunk.dropna(subset=["timestamp", "price"])
            chunk = chunk[(chunk["timestamp"] >= start_ts) & (chunk["timestamp"] <= end_ts)]
            if chunk.empty:
                continue

            if selected_hour is not None and selected_minute is not None:
                chunk = chunk[
                    (chunk["timestamp"].dt.hour == selected_hour)
                    & (chunk["timestamp"].dt.minute == selected_minute)
                ]
                if chunk.empty:
                    continue

            chunk["trade_date"] = chunk["timestamp"].dt.date
            frames.append(chunk[["timestamp", "trade_date", "price"]])

    if not frames:
        return pd.DataFrame(columns=["trade_date", "spot", "spot_time"])

    all_ticks = pd.concat(frames, ignore_index=True).sort_values("timestamp")
    by_date = (
        all_ticks.groupby("trade_date", as_index=False)
        .agg(spot=("price", "last"), spot_time=("timestamp", "last"))
        .sort_values("trade_date")
    )
    return by_date


def build_chain_snapshot(
    trades: pd.DataFrame,
    trades_unfiltered_expiry: pd.DataFrame,
    spot_by_day: pd.DataFrame,
    expiry_date: dt.date,
    selected_strikes: list[float],
) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()

    subset = trades[trades["expiry_date"].dt.date == expiry_date].copy()
    if selected_strikes:
        subset = subset[subset["strike"].isin(selected_strikes)]
    if subset.empty:
        return pd.DataFrame()

    subset = subset.sort_values("timestamp")
    grouped = (
        subset.groupby(["trade_date", "strike", "option_type"], as_index=False)
        .agg(
            last_price=("price", "last"),
            volume=("size", "sum"),
            trades=("size", "count"),
            last_trade_time=("timestamp", "max"),
        )
        .sort_values(["trade_date", "strike", "option_type"])
    )

    call_side = grouped[grouped["option_type"] == "C"].rename(
        columns={
            "last_price": "CE_ltp",
            "volume": "CE_volume",
            "trades": "CE_trades",
            "last_trade_time": "CE_last_trade_time",
        }
    )
    put_side = grouped[grouped["option_type"] == "P"].rename(
        columns={
            "last_price": "PE_ltp",
            "volume": "PE_volume",
            "trades": "PE_trades",
            "last_trade_time": "PE_last_trade_time",
        }
    )

    merged = pd.merge(
        call_side[
            ["trade_date", "strike", "CE_ltp", "CE_volume", "CE_trades", "CE_last_trade_time"]
        ],
        put_side[
            ["trade_date", "strike", "PE_ltp", "PE_volume", "PE_trades", "PE_last_trade_time"]
        ],
        on=["trade_date", "strike"],
        how="outer",
    )

    if not spot_by_day.empty:
        merged = pd.merge(merged, spot_by_day, on="trade_date", how="left")
    else:
        merged["spot"] = pd.NA
        merged["spot_time"] = pd.NaT

    universe = trades_unfiltered_expiry.copy()
    if universe.empty:
        merged["atm_strike"] = pd.NA
    else:
        universe = universe.sort_values(["trade_date", "strike"]).drop_duplicates(
            subset=["trade_date", "strike"]
        )
        atm_rows: list[dict] = []
        for trade_day, day_df in universe.groupby("trade_date"):
            day_spot = merged.loc[merged["trade_date"] == trade_day, "spot"].dropna()
            if day_spot.empty:
                continue
            spot_value = float(day_spot.iloc[0])
            nearest_idx = (day_df["strike"] - spot_value).abs().idxmin()
            atm_strike_value = float(day_df.loc[nearest_idx, "strike"])
            atm_rows.append({"trade_date": trade_day, "atm_strike": atm_strike_value})

        if atm_rows:
            merged = pd.merge(merged, pd.DataFrame(atm_rows), on="trade_date", how="left")
        else:
            merged["atm_strike"] = pd.NA

    merged["CE_moneyness"] = pd.NA
    merged["PE_moneyness"] = pd.NA
    valid = merged["atm_strike"].notna() & merged["strike"].notna()
    merged.loc[valid & (merged["strike"] == merged["atm_strike"]), "CE_moneyness"] = "ATM"
    merged.loc[valid & (merged["strike"] == merged["atm_strike"]), "PE_moneyness"] = "ATM"
    merged.loc[valid & (merged["strike"] < merged["atm_strike"]), "CE_moneyness"] = "ITM"
    merged.loc[valid & (merged["strike"] > merged["atm_strike"]), "CE_moneyness"] = "OTM"
    merged.loc[valid & (merged["strike"] < merged["atm_strike"]), "PE_moneyness"] = "OTM"
    merged.loc[valid & (merged["strike"] > merged["atm_strike"]), "PE_moneyness"] = "ITM"

    merged = merged.sort_values(["trade_date", "strike"]).reset_index(drop=True)
    return merged


def row_color_by_spot_moneyness(row: pd.Series, basis: str) -> list[str]:
    moneyness_col = "CE_moneyness" if basis == "CALL" else "PE_moneyness"
    tag = row.get(moneyness_col)
    if pd.isna(tag):
        return [""] * len(row)
    if tag == "ATM":
        color = "#fff3cd"
    elif tag == "ITM":
        color = "#d4edda"
    else:
        color = "#f8d7da"
    return [f"background-color: {color}"] * len(row)


st.set_page_config(page_title="BTC Historical Option Chain UI", layout="wide")
st.title("BTC Historical Option Chain UI (Delta)")
st.caption("Build chain snapshots from options + futures spot with date range and time.")

if not OPTIONS_DATA_ROOT.exists():
    st.error("Data folder not found: delta/BTC-2026")
    st.stop()
if not FUTURES_DATA_ROOT.exists():
    st.error("Spot folder not found: delta/F-BTC-2026")
    st.stop()

option_csv_files = discover_monthly_files(str(OPTIONS_DATA_ROOT))
if not option_csv_files:
    st.error("No CSV files found under delta/BTC-2026.")
    st.stop()

futures_csv_files = discover_monthly_files(str(FUTURES_DATA_ROOT))
if not futures_csv_files:
    st.error("No CSV files found under delta/F-BTC-2026.")
    st.stop()

min_date_opt, max_date_opt = infer_date_bounds(tuple(option_csv_files))
min_date_fut, max_date_fut = infer_date_bounds(tuple(futures_csv_files))
min_date = max(min_date_opt, min_date_fut)
max_date = min(max_date_opt, max_date_fut)
default_from = min_date
default_to = max_date

filter_col1, filter_col2, filter_col3 = st.columns(3)
with filter_col1:
    from_date = st.date_input("From Date", value=default_from, min_value=min_date, max_value=max_date)
with filter_col2:
    to_date = st.date_input("To Date", value=default_to, min_value=min_date, max_value=max_date)
with filter_col3:
    time_value = st.time_input("Time", value=dt.time(9, 15))

if from_date > to_date:
    st.error("From Date must be before or equal to To Date.")
    st.stop()

with st.spinner("Loading and filtering trades..."):
    trades = load_trades_filtered(tuple(option_csv_files), from_date, to_date, time_value)
    spot_by_day = load_futures_spot(tuple(futures_csv_files), from_date, to_date, time_value)
    all_times_trades = load_trades_filtered(tuple(option_csv_files), from_date, to_date, None)
    all_times_spot = load_futures_spot(tuple(futures_csv_files), from_date, to_date, None)

if trades.empty:
    st.warning("No trades matched the selected date range and time minute.")
    st.stop()

available_expiries = sorted(trades["expiry_date"].dt.date.unique())
selected_expiry = st.selectbox("Expiry", options=available_expiries, index=0)

expiry_trades = trades[trades["expiry_date"].dt.date == selected_expiry]
available_strikes = sorted(expiry_trades["strike"].unique())
selected_strikes = st.multiselect("Strikes", options=available_strikes, default=available_strikes)
moneyness_basis = st.radio(
    "Row colors (ATM / ITM / OTM)",
    options=["CALL", "PUT"],
    horizontal=True,
)

if not selected_strikes:
    st.warning("Select at least one strike.")
    st.stop()

chain_df = build_chain_snapshot(
    trades=trades,
    trades_unfiltered_expiry=expiry_trades,
    spot_by_day=spot_by_day,
    expiry_date=selected_expiry,
    selected_strikes=selected_strikes,
)

if chain_df.empty:
    st.warning("No chain rows for selected expiry/strikes.")
    st.stop()

display_columns = [
    "trade_date",
    # "spot_time",
    "CE_moneyness",
    "spot",
    "CE_ltp",
    "strike",
    "PE_ltp",
    "PE_moneyness",
    # "atm_strike",
    
    # "CE_volume",
    # "CE_trades",
    # "CE_last_trade_time",
    # "PE_last_trade_time",
    # "PE_trades",
    # "PE_volume",
    
]
for col in display_columns:
    if col not in chain_df.columns:
        chain_df[col] = pd.NA

tab1, tab2 = st.tabs(["Chain View", "Strike Specific Data"])

with tab1:
    st.subheader("All Matching Rows")
    st.caption("Legend: **ATM** = yellow · **ITM** = green · **OTM** = red")
    all_styled = chain_df[display_columns].style.apply(
        row_color_by_spot_moneyness, axis=1, basis=moneyness_basis
    )
    st.dataframe(all_styled, use_container_width=True)

    latest_trade_date = chain_df["trade_date"].max()
    latest_snapshot = chain_df[chain_df["trade_date"] == latest_trade_date].copy()
    latest_snapshot = latest_snapshot.sort_values("strike")

    st.subheader("Latest Snapshot In Range")
    st.write(f"Snapshot date: `{latest_trade_date}` at minute `{time_value.strftime('%H:%M')}`")
    spot_series = latest_snapshot["spot"].dropna()
    if not spot_series.empty:
        st.metric("BTC Spot At Selected Time", f"{float(spot_series.iloc[0]):,.2f}")
    else:
        st.info("BTC spot not available for selected filters.")

    latest_styled = latest_snapshot[display_columns].style.apply(
        row_color_by_spot_moneyness, axis=1, basis=moneyness_basis
    )
    st.dataframe(latest_styled, use_container_width=True)

    csv_bytes = chain_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download Filtered Chain (CSV)",
        data=csv_bytes,
        file_name=f"BTC_chain_{selected_expiry}_{from_date}_{to_date}_{time_value.strftime('%H%M')}.csv",
        mime="text/csv",
    )

with tab2:
    st.subheader("Strike Wise Table (All Available Times)")
    st.caption("Choose one strike and side to view all timestamps in the selected range.")
    expiry_all_times = all_times_trades[all_times_trades["expiry_date"].dt.date == selected_expiry].copy()
    if expiry_all_times.empty:
        st.warning("No rows matched these filters for all-time view.")
    else:
        strike_side_col1, strike_side_col2 = st.columns(2)
        with strike_side_col1:
            side_choice = st.radio(
                "Option Side",
                options=["CALL", "PUT"],
                horizontal=True,
                key="strike_side_choice_delta",
            )
        strike_values = sorted(expiry_all_times["strike"].dropna().unique().tolist())
        with strike_side_col2:
            selected_abs_strike = st.selectbox(
                f"Select {side_choice} Strike",
                options=strike_values,
                index=0,
                key="selected_abs_strike_delta",
            )

        strike_subset = expiry_all_times[expiry_all_times["strike"] == float(selected_abs_strike)].copy()
        strike_chain = build_chain_snapshot(
            trades=strike_subset,
            trades_unfiltered_expiry=expiry_all_times,
            spot_by_day=all_times_spot,
            expiry_date=selected_expiry,
            selected_strikes=[float(selected_abs_strike)],
        )
        strike_chain = strike_chain.sort_values("trade_date").reset_index(drop=True)

        metric_prefix = "CE" if side_choice == "CALL" else "PE"
        table_columns = [
            "trade_date",
            "spot_time",
            "spot",
            "atm_strike",
            "strike",
            f"{metric_prefix}_moneyness",
            f"{metric_prefix}_ltp",
            f"{metric_prefix}_volume",
            f"{metric_prefix}_trades",
            f"{metric_prefix}_last_trade_time",
        ]
        for col in table_columns:
            if col not in strike_chain.columns:
                strike_chain[col] = pd.NA

        st.dataframe(strike_chain[table_columns], use_container_width=True)
