from __future__ import annotations

from pathlib import Path
import datetime as dt
import calendar

import pandas as pd
import streamlit as st


WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = WORKSPACE_ROOT / "delta" / "BTC-2026"


def month_bounds(year: int, month: int) -> tuple[dt.date, dt.date]:
    last_day = calendar.monthrange(year, month)[1]
    return dt.date(year, month, 1), dt.date(year, month, last_day)


@st.cache_data
def discover_monthly_files(root_dir: str) -> list[str]:
    root = Path(root_dir)
    if not root.exists():
        return []
    files = sorted(root.glob("**/*.csv"))
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
    time_filter: dt.time,
) -> pd.DataFrame:
    start_ts = pd.Timestamp(from_date)
    end_ts = pd.Timestamp(to_date) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)

    frames: list[pd.DataFrame] = []
    selected_hour = time_filter.hour
    selected_minute = time_filter.minute

    for file_path in files:
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


def build_chain_snapshot(
    trades: pd.DataFrame,
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

    merged = merged.sort_values(["trade_date", "strike"]).reset_index(drop=True)
    return merged


st.set_page_config(page_title="BTC Historical Option Chain UI", layout="wide")
st.title("BTC Historical Option Chain UI (Delta)")
st.caption("Build chain snapshots from trades using date range and minute-level time.")

if not DATA_ROOT.exists():
    st.error("Data folder not found: delta/BTC-2026")
    st.stop()

csv_files = discover_monthly_files(str(DATA_ROOT))
if not csv_files:
    st.error("No CSV files found under delta/BTC-2026.")
    st.stop()

min_date, max_date = infer_date_bounds(tuple(csv_files))
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
    trades = load_trades_filtered(tuple(csv_files), from_date, to_date, time_value)

if trades.empty:
    st.warning("No trades matched the selected date range and time minute.")
    st.stop()

available_expiries = sorted(trades["expiry_date"].dt.date.unique())
selected_expiry = st.selectbox("Expiry", options=available_expiries, index=0)

expiry_trades = trades[trades["expiry_date"].dt.date == selected_expiry]
available_strikes = sorted(expiry_trades["strike"].unique())
selected_strikes = st.multiselect("Strikes", options=available_strikes, default=available_strikes)

if not selected_strikes:
    st.warning("Select at least one strike.")
    st.stop()

chain_df = build_chain_snapshot(
    trades=trades,
    expiry_date=selected_expiry,
    selected_strikes=selected_strikes,
)

if chain_df.empty:
    st.warning("No chain rows for selected expiry/strikes.")
    st.stop()

st.subheader("All Matching Rows")
st.dataframe(chain_df, use_container_width=True)

latest_trade_date = chain_df["trade_date"].max()
latest_snapshot = chain_df[chain_df["trade_date"] == latest_trade_date].copy()

st.subheader("Latest Snapshot In Range")
st.write(f"Snapshot date: `{latest_trade_date}` at minute `{time_value.strftime('%H:%M')}`")
st.dataframe(latest_snapshot, use_container_width=True)

csv_bytes = chain_df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download Filtered Chain (CSV)",
    data=csv_bytes,
    file_name=f"BTC_chain_{selected_expiry}_{from_date}_{to_date}_{time_value.strftime('%H%M')}.csv",
    mime="text/csv",
)
