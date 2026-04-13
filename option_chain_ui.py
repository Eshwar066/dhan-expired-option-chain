from pathlib import Path
import datetime as dt

import pandas as pd
import streamlit as st


WORKSPACE_ROOT = Path(__file__).resolve().parent


def parse_atm_label(label: str) -> int:
    if label == "ATM":
        return 0
    if label.startswith("ATM+"):
        return int(label[4:])
    if label.startswith("ATM-"):
        return -int(label[4:])
    return 999


@st.cache_data
def discover_data_roots() -> list[Path]:
    roots = []
    for path in WORKSPACE_ROOT.glob("*Options data 15 mins"):
        atm_wise = path / "ATM Wise data"
        if atm_wise.exists():
            roots.append(atm_wise)
    return sorted(roots)


@st.cache_data
def list_symbols(root: str) -> list[str]:
    root_path = Path(root)
    if not root_path.exists():
        return []
    return sorted([p.name for p in root_path.iterdir() if p.is_dir()])


@st.cache_data
def list_expiries(root: str, symbol: str) -> list[str]:
    exp_dir = Path(root) / symbol
    if not exp_dir.exists():
        return []
    expiries = [p.name for p in exp_dir.iterdir() if p.is_dir()]
    return sorted(expiries)


@st.cache_data
def list_strikes(root: str, symbol: str, expiry: str) -> list[str]:
    strike_dir = Path(root) / symbol / expiry
    if not strike_dir.exists():
        return []
    labels = [p.name for p in strike_dir.iterdir() if p.is_dir()]
    return sorted(labels, key=parse_atm_label)


@st.cache_data
def load_leg_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    if "datetime" not in df.columns:
        return pd.DataFrame()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])
    return df


def build_chain_data(
    root: str,
    symbol: str,
    expiry: str,
    strikes: list[str],
    from_date: dt.date,
    to_date: dt.date,
    time_filter: dt.time,
) -> pd.DataFrame:
    frames = []
    root_path = Path(root)
    start_ts = pd.Timestamp(from_date)
    end_ts = pd.Timestamp(to_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    for strike_label in strikes:
        strike_path = root_path / symbol / expiry / strike_label
        ce_file = strike_path / f"{symbol}_{expiry}_CALL.csv"
        pe_file = strike_path / f"{symbol}_{expiry}_PUT.csv"

        ce = load_leg_data(str(ce_file)) if ce_file.exists() else pd.DataFrame()
        pe = load_leg_data(str(pe_file)) if pe_file.exists() else pd.DataFrame()

        if not ce.empty:
            ce = ce[(ce["datetime"] >= start_ts) & (ce["datetime"] <= end_ts)]
            ce = ce[ce["datetime"].dt.time == time_filter]
            ce = ce.rename(
                columns={
                    "open": "CE_open",
                    "high": "CE_high",
                    "low": "CE_low",
                    "close": "CE_close",
                    "volume": "CE_volume",
                    "iv": "CE_iv",
                    "oi": "CE_oi",
                    "spot": "CE_spot",
                    "strike": "CE_strike",
                }
            )

        if not pe.empty:
            pe = pe[(pe["datetime"] >= start_ts) & (pe["datetime"] <= end_ts)]
            pe = pe[pe["datetime"].dt.time == time_filter]
            pe = pe.rename(
                columns={
                    "open": "PE_open",
                    "high": "PE_high",
                    "low": "PE_low",
                    "close": "PE_close",
                    "volume": "PE_volume",
                    "iv": "PE_iv",
                    "oi": "PE_oi",
                    "spot": "PE_spot",
                    "strike": "PE_strike",
                }
            )

        if ce.empty and pe.empty:
            continue

        if ce.empty:
            merged = pe.copy()
        elif pe.empty:
            merged = ce.copy()
        else:
            merged = pd.merge(ce, pe, on="datetime", how="outer")

        merged["symbol"] = symbol
        merged["expiry"] = expiry
        merged["atm_label"] = strike_label
        merged["atm_distance"] = parse_atm_label(strike_label)
        frames.append(merged)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result = result.sort_values(["datetime", "atm_distance"]).reset_index(drop=True)
    return result


st.set_page_config(page_title="Historical Option Chain UI", layout="wide")
st.title("Historical Option Chain UI")
st.caption("Filter by date range, expiry and time to view CE/PE side-by-side.")

roots = discover_data_roots()
if not roots:
    st.error("No data folders found. Expected folders like 'Monthly Options data 15 mins/ATM Wise data'.")
    st.stop()

root_display = [str(p.relative_to(WORKSPACE_ROOT)) for p in roots]
root_choice = st.selectbox("Data Source", options=root_display, index=0)
selected_root = str((WORKSPACE_ROOT / root_choice).resolve())

symbols = list_symbols(selected_root)
if not symbols:
    st.error("No symbols found in selected data source.")
    st.stop()

col1, col2, col3 = st.columns(3)
with col1:
    symbol = st.selectbox("Symbol", options=symbols, index=0)

expiries = list_expiries(selected_root, symbol)
if not expiries:
    st.error("No expiries found for selected symbol.")
    st.stop()

with col2:
    expiry = st.selectbox("Expiry", options=expiries, index=max(0, len(expiries) - 1))

strikes = list_strikes(selected_root, symbol, expiry)
if not strikes:
    st.error("No strike folders found for selected expiry.")
    st.stop()

with col3:
    selected_strikes = st.multiselect("ATM Labels", options=strikes, default=strikes)

filter_col1, filter_col2, filter_col3 = st.columns(3)
today = dt.date.today()
default_from = today - dt.timedelta(days=7)
with filter_col1:
    from_date = st.date_input("From Date", value=default_from)
with filter_col2:
    to_date = st.date_input("To Date", value=today)
with filter_col3:
    time_value = st.time_input("Time", value=dt.time(9, 15))

if from_date > to_date:
    st.error("From Date must be before or equal to To Date.")
    st.stop()

if not selected_strikes:
    st.warning("Select at least one ATM label.")
    st.stop()

chain_df = build_chain_data(
    root=selected_root,
    symbol=symbol,
    expiry=expiry,
    strikes=selected_strikes,
    from_date=from_date,
    to_date=to_date,
    time_filter=time_value,
)

if chain_df.empty:
    st.warning("No rows matched these filters.")
    st.stop()

chain_df["spot"] = chain_df.get("CE_spot").combine_first(chain_df.get("PE_spot"))

display_columns = [
    # "datetime",
    "atm_label",
    # "spot",
    # "CE_open",
    # "CE_high",
    # "CE_low",
    "CE_volume",
    "CE_iv",
    "CE_oi",
    "CE_close",
    "CE_strike",
    "PE_strike",
    "PE_close",
    # "PE_open",
    # "PE_high",
    # "PE_low",
    "PE_volume",
    "PE_iv",
    "PE_oi",
    
]

for col in display_columns:
    if col not in chain_df.columns:
        chain_df[col] = pd.NA

st.subheader("All Matching Rows")
st.dataframe(chain_df[display_columns], use_container_width=True, hide_index=True)

st.subheader("Latest Snapshot In Range")
latest_ts = chain_df["datetime"].max()
latest_snapshot = chain_df[chain_df["datetime"] == latest_ts].copy()
latest_snapshot = latest_snapshot.sort_values("atm_distance")
st.write(f"Snapshot time: `{latest_ts}`")

spot_series = latest_snapshot["spot"].dropna()
if not spot_series.empty:
    nifty_spot = float(spot_series.median())
    st.metric("NIFTY Spot At Selected Time", f"{nifty_spot:.2f}")
else:
    st.info("NIFTY spot not available for selected filters.")

st.dataframe(latest_snapshot[display_columns], use_container_width=True, hide_index=True)

csv_bytes = chain_df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download Filtered Data (CSV)",
    data=csv_bytes,
    file_name=f"{symbol}_{expiry}_{from_date}_{to_date}_{time_value}.csv",
    mime="text/csv",
)
