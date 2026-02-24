import pandas as pd
import pyodbc

# -----------------------------
# 1️⃣ Read Excel File
# -----------------------------
file_path = r"C:\Users\user\Desktop\option_spreads_data.xlsx"
df = pd.read_excel(file_path)

# -----------------------------
# 2️⃣ Clean / Convert Data Types
# -----------------------------

# Convert date columns properly
df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
df['expiration_date'] = pd.to_datetime(df['expiration_date'], errors='coerce')
df['status_change_date'] = pd.to_datetime(df['status_change_date'], errors='coerce')

# Convert numeric columns safely
numeric_columns = [
    'ticker_price', 'option_price', 'strike_price_lower',
    'strike_price_upper', 'option_quantity',
    'contract_amount', 'coll_amount',
    'rate_of_return', 'num_of_days',
    'status_change_price', 'cost_of_contract',
    'cost_of_close'
]

for col in numeric_columns:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# -----------------------------
# 3️⃣ Connect to SQL Server
# -----------------------------
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost\\SQLEXPRESS02;'
    'DATABASE=Investments;'
    'Trusted_Connection=yes;'
)

cursor = conn.cursor()
cursor.fast_executemany = True  # Faster inserts

# -----------------------------
# 4️⃣ Insert Data Properly
# -----------------------------
insert_query = """
INSERT INTO dbo.option_spreads (
    id,
    date,
    ticker,
    ticker_price,
    option_type,
    tran_type,
    option_price,
    strike_price_lower,
    strike_price_upper,
    option_quantity,
    contract_amount,
    coll_amount,
    rate_of_return,
    expiration_date,
    num_of_days,
    status,
    status_change_date,
    status_change_price,
    cost_of_contract,
    cost_of_close
)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

data_to_insert = []

for _, row in df.iterrows():
    data_to_insert.append((
        str(row['id']) if pd.notna(row['id']) else None,
        row['trade_date'].date() if pd.notna(row['trade_date']) else None,
        row['ticker'],
        row['ticker_price'],
        row['option_type'],
        row['tran_type'],
        row['option_price'],
        row['strike_price_lower'],
        row['strike_price_upper'],
        row['option_quantity'],
        row['contract_amount'],
        row['coll_amount'],
        row['rate_of_return'],
        row['expiration_date'].date() if pd.notna(row['expiration_date']) else None,
        int(row['num_of_days']) if pd.notna(row['num_of_days']) else None,
        row['status'],
        row['status_change_date'].date() if pd.notna(row['status_change_date']) else None,
        row['status_change_price'],
        row['cost_of_contract'],
        row['cost_of_close']
    ))

cursor.executemany(insert_query, data_to_insert)

# -----------------------------
# 5️⃣ Commit & Close
# -----------------------------
conn.commit()
cursor.close()
conn.close()

print("✅ Data inserted successfully into dbo.option_spreads!")
