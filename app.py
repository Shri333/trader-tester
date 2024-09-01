import streamlit as st
import polars as pl
from datetime import datetime
import matplotlib.pyplot as plt


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

    # User inputs
    st.sidebar.header("Test Parameters")

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

    # Date inputs for forward testing period
    st.sidebar.subheader("Forward Testing Period")
    forward_start = st.sidebar.date_input(
        "Start date",
        lookback_end,
        min_value=lookback_end,
        max_value=max_date,
        key="forward_start",
    )
    forward_end = st.sidebar.date_input(
        "End date",
        max_date,
        min_value=forward_start,
        max_value=max_date,
        key="forward_end",
    )

    # Sort and limit parameters
    sort_by = st.sidebar.selectbox("Optimize for", ("PnL", "PCR"))
    top_n = st.sidebar.slider("Number of top time slots to consider", 1, 20, 5)

    # Convert date objects to datetime
    lookback_start = datetime.combine(lookback_start, datetime.min.time())
    lookback_end = datetime.combine(lookback_end, datetime.max.time())
    forward_start = datetime.combine(forward_start, datetime.min.time())
    forward_end = datetime.combine(forward_end, datetime.max.time())

    # Process and display lookback data (calculate PnL and PCR)
    lookback_data = (
        df.filter(
            pl.col("EntryTime") >= lookback_start, pl.col("EntryTime") <= lookback_end
        )
        .group_by("Day", "Time")
        .agg(pl.col("PnL").mean(), pl.col("PCR").mean())
        .sort(sort_by, descending=True)
        .limit(top_n)
    )

    st.header("Lookback Period Analysis")
    st.write(f"From {lookback_start.date()} to {lookback_end.date()}")
    st.dataframe(lookback_data, use_container_width=True)

    # Get top performing day-time combinations
    top_combinations = lookback_data.select("Day", "Time").to_dicts()

    # Process forward testing data (calculate running PnL)
    forward_data = (
        df.filter(
            pl.col("EntryTime") >= forward_start,
            pl.col("EntryTime") <= forward_end,
            pl.struct(["Day", "Time"]).is_in(top_combinations),
        )
        .sort("EntryTime")
        .select(pl.col("EntryTime"), pl.col("PnL").cum_sum())
    )

    # Plot running PnL
    st.header("Forward Period Running PnL")
    st.write(f"From {forward_start.date()} to {forward_end.date()}")

    fig, ax = plt.subplots()
    ax.plot(
        forward_data.select("EntryTime"),
        forward_data.select("PnL"),
        label="Running PnL",
        color="blue",
        linestyle="-",
    )
    ax.set_xlabel("EntryTime")
    ax.set_ylabel("Running PnL")
    ax.legend()
    plt.xticks(rotation=45)
    st.pyplot(fig)


if __name__ == "__main__":
    # Main app title
    st.title("Walk-Forward Testing for Trading Strategy")

    # Get file and run app
    file = st.file_uploader("Upload your CSV file", type=["csv"])
    if file is not None:
        main(file.getvalue())
