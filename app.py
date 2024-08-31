import streamlit as st
import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta

df = pl.scan_csv("data/MEIC-BYOB-2.5-1x-50W.csv")
df = df.with_columns(
    [
        pl.col("EntryTime")
        .str.strptime(pl.Datetime, format="%m/%d/%Y %I:%M:%S %p")
        .alias("EntryTime"),
        (pl.col("ProfitLossAfterSlippage") * 100 - pl.col("CommissionFees")).alias("PnL"),
    ]
)
df = df.sort(["PnL", "EntryTime"], descending=True)

limit = st.slider("number of rows", 1, 10)
months = st.slider("number of months to look back", 1, 48)
start_time = datetime.now() - relativedelta(months=months)

st.write(df.filter(pl.col("EntryTime") >= start_time).limit(limit).collect())
