import streamlit as st
import polars as pl
from datetime import datetime


# Function to process data for a given time range
def process_data(
    df: pl.DataFrame, start_time: datetime, end_time: datetime
) -> pl.DataFrame:
    return (
        df.filter(
            (pl.col("EntryTime") >= start_time) & (pl.col("EntryTime") <= end_time)
        )
        .with_columns(
            [
                pl.col("EntryTime").dt.to_string("%A").alias("Day"),
                pl.col("EntryTime").dt.to_string("%I:%M %p").alias("Time"),
                (
                    pl.col("ProfitLossAfterSlippage") * 100 - pl.col("CommissionFees")
                ).alias("PnL"),
                (
                    (pl.col("ProfitLossAfterSlippage") * 100 - pl.col("CommissionFees"))
                    / pl.col("Premium")
                ).alias("PCR"),
            ]
        )
        .group_by(["Day", "Time"])
        .agg(pl.col("PnL").mean(), pl.col("PCR").mean())
    )


# Main Streamlit app
st.title("Walk-Forward Testing for Trading Strategy")

# Load data
df = pl.scan_csv("data/MEIC-BYOB-2.5-1x-50W.csv")
df = df.with_columns(
    pl.col("EntryTime")
    .str.strptime(pl.Datetime, format="%m/%d/%Y %I:%M:%S %p")
    .alias("EntryTime"),
)

# Get date range of the data
min_date = df.select(pl.col("EntryTime").min()).collect().item().date()
max_date = df.select(pl.col("EntryTime").max()).collect().item().date()

# User inputs
st.sidebar.header("Test Parameters")

# Date inputs for lookback period
st.sidebar.subheader("Lookback Period")
lookback_start = st.sidebar.date_input(
    "Start date", min_date, min_value=min_date, max_value=max_date
)
lookback_end = st.sidebar.date_input(
    "End date",
    min_date + (max_date - min_date) // 2,
    min_value=lookback_start,
    max_value=max_date,
)

# Date inputs for forward testing period
st.sidebar.subheader("Forward Testing Period")
forward_start = st.sidebar.date_input(
    "Start date", lookback_end, min_value=lookback_end, max_value=max_date
)
forward_end = st.sidebar.date_input(
    "End date", max_date, min_value=forward_start, max_value=max_date
)

sort_by = st.sidebar.selectbox("Optimize for", ("PnL", "PCR"))
top_n = st.sidebar.slider("Number of top time slots to consider", 1, 20, 5)

# Convert date objects to datetime
lookback_start = datetime.combine(lookback_start, datetime.min.time())
lookback_end = datetime.combine(lookback_end, datetime.max.time())
forward_start = datetime.combine(forward_start, datetime.min.time())
forward_end = datetime.combine(forward_end, datetime.max.time())

# Process lookback data
lookback_data = process_data(df, lookback_start, lookback_end)
lookback_data = lookback_data.sort(sort_by, descending=True).limit(top_n)

st.header("Lookback Period Analysis")
st.write(f"From {lookback_start.date()} to {lookback_end.date()}")
st.dataframe(lookback_data.collect(), use_container_width=True)

# Get top performing day-time combinations
top_combinations = lookback_data.select(["Day", "Time"]).collect().to_dicts()

# Process forward testing data
forward_data = (
    process_data(df, forward_start, forward_end)
    .filter(pl.struct(["Day", "Time"]).is_in(top_combinations))
    .sort(sort_by, descending=True)
)

st.header("Forward Testing Results")
st.write(f"From {forward_start.date()} to {forward_end.date()}")
st.dataframe(forward_data.collect(), use_container_width=True)

# Calculate overall performance
overall_performance = forward_data.select(pl.col(["PnL", "PCR"]).mean()).collect()
st.header("Overall Performance")
st.write(f"Average PnL: {overall_performance['PnL'][0]:.2f}")
st.write(f"Average PCR: {overall_performance['PCR'][0]:.2f}")
