import yfinance as yf
import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import os

# ============================================================
# CONFIG
# ============================================================
BATCH_SIZE = 50
EARLIEST_DATE = datetime(2020, 1, 1)
CHECKPOINT_FILE = "last_loaded.txt"

# Starting reference date (latest historical point)
INITIAL_START_DATE = datetime(2026, 2, 6)

# ============================================================
# SQL CONNECTION
# ============================================================
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=.\\SQLEXPRESS02;"
    "DATABASE=INVESTMENTS;"
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# ============================================================
# MANUAL TICKER LIST
# ============================================================
tickers = [
"AAPL","MSFT","AMZN","NVDA","GOOGL","GOOG","META","BRK.B","TSLA","UNH",
"JNJ","JPM","V","XOM","PG","MA","HD","LLY","CVX","MRK","ABBV","PEP","KO",
"AVGO","COST","BAC","TMO","WMT","CSCO","MCD","ABT","CRM","ACN","DHR","LIN",
"CMCSA","ORCL","ADBE","NKE","TXN","NEE","PM","AMD","NFLX","BMY","UPS","RTX",
"QCOM","HON","T","AMGN","LOW","INTC","IBM","INTU","SPGI","CAT","AMAT","GE",
"PLD","BA","GS","BLK","SYK","ISRG","MDT","ADP","ELV","GILD","DE","VRTX",
"DIS","MMC","CI","CB","C","REGN","SLB","PGR","SO","ZTS","DUK","LRCX","BDX",
"ITW","BSX","FI","ETN","AON","MO","COP","PANW","KLAC","CME","NOC","SNPS",
"WM","CDNS","CL","EOG","APD","MCO","TGT","FDX","SHW","HUM","PXD","USB"
]

# Convert Yahoo special symbols (BRK.B → BRK-B)
tickers = [t.replace(".", "-") for t in tickers]

print(f"Total tickers: {len(tickers)}")

# ============================================================
# DETERMINE MONTH TO LOAD
# ============================================================
if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, "r") as f:
        start_date = datetime.strptime(f.read().strip(), "%Y-%m-%d")
else:
    # First run → start from INITIAL_START_DATE month
    start_date = datetime(INITIAL_START_DATE.year, INITIAL_START_DATE.month, 1)

# Calculate end of that month
next_month = (start_date.replace(day=28) + timedelta(days=4)).replace(day=1)
end_date = next_month

# Stop condition
if start_date < EARLIEST_DATE:
    print("✅ Historical load complete")
    conn.close()
    exit()

print(f"Loading data: {start_date.date()} → {end_date.date()}")

# ============================================================
# BATCH PROCESS
# ============================================================
for i in range(0, len(tickers), BATCH_SIZE):

    batch = tickers[i:i+BATCH_SIZE]
    print(f"Processing batch {i//BATCH_SIZE + 1}")

    data = yf.download(
        batch,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False
    )

    for ticker in batch:

        if ticker not in data:
            continue

        df = data[ticker]

        if df.empty:
            continue

        df = df.reset_index()

        for _, row in df.iterrows():

            open_p  = None if pd.isna(row["Open"]) else round(float(row["Open"]), 2)
            close_p = None if pd.isna(row["Close"]) else round(float(row["Close"]), 2)
            high_p  = None if pd.isna(row["High"]) else round(float(row["High"]), 2)
            low_p   = None if pd.isna(row["Low"]) else round(float(row["Low"]), 2)
            volume  = None if pd.isna(row["Volume"]) else int(row["Volume"])

            cursor.execute("""
                INSERT INTO dbo.Stocks_History
                (ticker, date, open_price, close_price, high, low, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ticker,
            row["Date"].date(),
            open_p,
            close_p,
            high_p,
            low_p,
            volume
            )

# ============================================================
# COMMIT DATA
# ============================================================
conn.commit()

# ============================================================
# SAVE NEXT MONTH CHECKPOINT
# ============================================================
prev_month = start_date - timedelta(days=1)
prev_month = datetime(prev_month.year, prev_month.month, 1)

with open(CHECKPOINT_FILE, "w") as f:
    f.write(prev_month.strftime("%Y-%m-%d"))

conn.close()

print("✅ One month historical data loaded into dbo.Stocks_History")
