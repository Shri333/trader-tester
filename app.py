import streamlit as st
import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt


class App:
    def __init__(self, csv: bytes):
        self.df = pl.read_csv(csv)

    def run(self):
        if not self._validate_data():
            return
        self._preprocess_data()
        self._get_lookback_parameters()
        self._get_calc_parameters()
        self._calc_forward()

    def _validate_data(self):
        missing_cols = {
            "EntryTime",
            "Premium",
            "ProfitLossAfterSlippage",
            "CommissionFees",
        } - set(self.df.columns)
        if missing_cols:
            st.error(f"Missing columns: {missing_cols}")
            return False
        return True

    def _preprocess_data(self):
        self.df = self.df.with_columns(
            pl.col("EntryTime")
            .str.strptime(pl.Datetime, format="%m/%d/%Y %I:%M:%S %p")
            .alias("EntryTime"),
            (pl.col("ProfitLossAfterSlippage") * 100 - pl.col("CommissionFees")).alias(
                "PnL"
            ),
            (
                (pl.col("ProfitLossAfterSlippage") * 100 - pl.col("CommissionFees"))
                / pl.col("Premium")
            ).alias("PCR"),
        )
        self.df = self.df.with_columns(
            pl.col("EntryTime").dt.year().alias("Year"),
            pl.col("EntryTime").dt.month().alias("Month"),
            pl.col("EntryTime").dt.week().alias("Week"),
            pl.col("EntryTime").dt.to_string("%I:%M %p").alias("Time"),
        )

    def _get_lookback_parameters(self):
        # Get date range
        self.min_datetime = self.df.select(pl.col("EntryTime").min()).item()
        self.max_datetime = self.df.select(pl.col("EntryTime").max()).item()

        # Date inputs for lookback period
        st.sidebar.subheader("Lookback Period")
        lookback_start = st.sidebar.date_input(
            "Start date",
            self.min_datetime,
            min_value=self.min_datetime,
            max_value=self.max_datetime - relativedelta(months=2),
            key="lookback_start",
        )
        lookback_start = datetime.combine(lookback_start, datetime.min.time())
        lookback_months = st.sidebar.number_input(
            "Number of months",
            value=1,
            min_value=1,
            max_value=((self.max_datetime - lookback_start).days // 30) - 1,
            key="lookback_months",
        )
        lookback_end = lookback_start + relativedelta(months=lookback_months)
        lookback_end = datetime.combine(lookback_end, datetime.max.time())
        self.lookback_start, self.lookback_end = lookback_start, lookback_end

    def _get_calc_parameters(self):
        st.sidebar.subheader("Calculation Parameters")
        self.sort_by = st.sidebar.selectbox("Optimize for", ("PnL", "PCR"))
        self.agg_by = st.sidebar.selectbox("Aggregate by", ("Month", "Week"))
        self.top_agg_n = st.sidebar.number_input(
            f"Number of top time slots for each {self.agg_by.lower()}", 1, 1000, 5
        )
        self.top_n = st.sidebar.number_input(
            "Number of top time slots to consider", 1, 1000, 5
        )

    def _calc_forward(self):
        # Display lookback data for lookback period
        lookback_data = self._get_lookback_data(self.lookback_start, self.lookback_end)
        st.header("Lookback Analysis")
        st.write(f"From {self.lookback_start.date()} to {self.lookback_end.date()}")
        st.dataframe(lookback_data, use_container_width=True)

        # Extend lookback period month by month and calculate PnL
        x, anchored, unanchored = [], [], []
        unanchored_start = self.lookback_start
        forward_end = self.lookback_end + relativedelta(months=1)
        while forward_end <= self.max_datetime:
            # Get current month
            forward_start = forward_end - relativedelta(months=1)
            x.append(forward_start)

            # Calculate monthly anchored
            lookback_data = self._get_lookback_data(self.lookback_start, forward_start)
            pnl = self._calc_forward_pnl(lookback_data, forward_start, forward_end)
            anchored.append(pnl + anchored[-1] if anchored else pnl)

            # Calculate monthly unanchored
            lookback_data = self._get_lookback_data(unanchored_start, forward_start)
            pnl = self._calc_forward_pnl(lookback_data, forward_start, forward_end)
            unanchored.append(pnl + unanchored[-1] if unanchored else pnl)

            unanchored_start += relativedelta(months=1)
            forward_end += relativedelta(months=1)

        # Display data as line graph
        st.header("Forward Analysis")
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(x, anchored, label="Anchored")
        ax.plot(x, unanchored, label="Unanchored")
        ax.set_xlabel("Date")
        ax.set_ylabel("PnL")
        ax.set_title("Running PnL")
        ax.legend()
        plt.xticks(rotation=45)
        st.pyplot(fig)

    def _get_lookback_data(self, start, end):
        return (
            self.df.filter(
                pl.col("EntryTime") >= start,
                pl.col("EntryTime") <= end,
            )
            .group_by("Year", self.agg_by, "Time")
            .agg(pl.col("PnL").mean(), pl.col("PCR").mean())
            .sort(self.sort_by, descending=True)
            .group_by("Year", self.agg_by)
            .agg(
                pl.col("Time").limit(self.top_agg_n),
                pl.col("PnL").limit(self.top_agg_n),
                pl.col("PCR").limit(self.top_agg_n),
            )
            .explode("Time", "PnL", "PCR")
            .group_by("Time")
            .agg(pl.col("PnL").mean(), pl.col("PCR").mean())
            .sort(self.sort_by, descending=True)
            .limit(self.top_n)
        )

    def _calc_forward_pnl(self, lookback_data, start, end):
        top_times = set(lookback_data["Time"])
        return (
            self.df.filter(
                pl.col("EntryTime") > start,
                pl.col("EntryTime") <= end,
                pl.col("Time").is_in(top_times),
            )
            .select("PnL")
            .sum()
            .item()
        )


if __name__ == "__main__":
    # Main app title
    st.title("Walk-Forward Testing for Trading Strategy")

    # Get file and run app
    file = st.file_uploader("Upload your CSV file", type=["csv"])
    if file is not None:
        app = App(file.getvalue())
        app.run()
