import streamlit as st
import polars as pl
from datetime import datetime


def main(csv: bytes):
    # Load and validate data
    df = pl.read_csv(csv)
    required_columns = {
        "EntryTime",
        "Premium",
        "ProfitLossAfterSlippage",
        "CommissionFees",
    }
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        st.error(f"Missing columns: {missing_columns}")
        return

    # Pre-process data
    df = (
        df.with_columns(
            pl.col("EntryTime")
            .str.strptime(pl.Datetime, format="%m/%d/%Y %I:%M:%S %p")
            .alias("EntryTime"),
        )
        .with_columns(
            pl.col("EntryTime").dt.to_string("%Y").alias("Year"),
            pl.col("EntryTime").dt.to_string("%B").alias("Month"),
            pl.col("EntryTime").dt.week().alias("Week"),
            pl.col("EntryTime").dt.to_string("%A").alias("Day"),
            pl.col("EntryTime").dt.to_string("%I:%M %p").alias("Time"),
            (pl.col("ProfitLossAfterSlippage") * 100 - pl.col("CommissionFees")).alias(
                "PnL"
            ),
        )
        .with_columns((pl.col("PnL") / pl.col("Premium")).alias("PCR"))
    )

    # Get date range of the data
    min_date = df.select(pl.col("EntryTime").min()).item().date()
    max_date = df.select(pl.col("EntryTime").max()).item().date()

    # Date inputs for lookback period
    st.sidebar.subheader("Lookback Period")
    lookback_start = st.sidebar.date_input(
        "Start date",
        min_date,
        min_value=min_date,
        max_value=max_date,
        key="lookback_start",
    )
    lookback_end = st.sidebar.date_input(
        "End date",
        max(lookback_start, lookback_start + (max_date - lookback_start) // 2),
        min_value=lookback_start,
        max_value=max_date,
        key="lookback_end",
    )

    # Calculation parameters
    st.sidebar.subheader("Calculation Parameters")
    sort_by = st.sidebar.selectbox("Optimize for", ("PnL", "PCR"))
    agg_by = st.sidebar.selectbox("Aggregate by", ("Month", "Week"))
    top_agg_n = st.sidebar.number_input(
        f"Number of top time slots for each {agg_by.lower()}", 1, 1000, 5
    )
    top_n = st.sidebar.number_input("Number of top time slots to consider", 1, 1000, 5)

    # Convert date objects to datetime
    lookback_start = datetime.combine(lookback_start, datetime.min.time())
    lookback_end = datetime.combine(lookback_end, datetime.max.time())

    # Process and display lookback data (calculate PnL and PCR)
    lookback_data = (
        df.filter(
            pl.col("EntryTime") >= lookback_start, pl.col("EntryTime") <= lookback_end
        )
        .group_by("Year", agg_by, "Time")
        .agg(pl.col("PnL").mean(), pl.col("PCR").mean())
        .sort(sort_by, descending=True)
        .group_by("Year", agg_by)
        .agg(
            pl.col("Time").limit(top_agg_n),
            pl.col("PnL").limit(top_agg_n),
            pl.col("PCR").limit(top_agg_n),
        )
        .explode("Time", "PnL", "PCR")
        .group_by("Time")
        .agg(pl.col("PnL").mean(), pl.col("PCR").mean())
        .sort(sort_by, descending=True)
        .limit(top_n)
    )

    st.header("Lookback Period Analysis")
    st.write(f"From {lookback_start.date()} to {lookback_end.date()}")
    st.dataframe(lookback_data, use_container_width=True)


if __name__ == "__main__":
    # Main app title
    st.title("Walk-Forward Testing for Trading Strategy")

    # Get file and run app
    file = st.file_uploader("Upload your CSV file", type=["csv"])
    if file is not None:
        main(file.getvalue())
