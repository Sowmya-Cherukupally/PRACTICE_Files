import streamlit as st
import pyodbc
from datetime import date

# ===============================
# SQL Server Connection
# ===============================
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=.\\SQLEXPRESS02;"
    "DATABASE=INVESTMENTS;"
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# ===============================
# Title
# ===============================
st.title("Options Contract Entry")

# ===============================
# Form
# ===============================
with st.form("contract_form"):

    # ---- Row 1 ----
    col1, col2, col3 = st.columns(3)

    contract_date = col1.date_input(
        "Date",
        value=date.today()
    )

    ticker = col2.text_input("Ticker")

    contract_type = col3.selectbox(
        "Contract Type",
        ["Credit", "Debit"],
        index=0
    )

    st.divider()

    # ---- Row 2 ----
    col4, col5, col6 = st.columns(3)

    option_type = col4.selectbox(
        "Option Type",
        ["PUT", "CALL"],
        index=0
    )

    contract_price = col5.number_input(
        "Contract Price",
        min_value=0.0,
        format="%.2f"
    )

    # Upper + Lower stacked
    upper_limit = col6.number_input(
        "Upper Limit",
        min_value=0.0,
        format="%.2f"
    )

    lower_limit = col6.number_input(
        "Lower Limit",
        min_value=0.0,
        format="%.2f"
    )

    st.divider()

    col_submit, col_cancel = st.columns(2)

    submit = col_submit.form_submit_button("Submit")
    cancel = col_cancel.form_submit_button("Cancel")

# ===============================
# Submit Logic
# ===============================
if submit:

    if ticker.strip() == "":
        st.error("Ticker cannot be empty")

    else:
        cursor.execute("""
            INSERT INTO contracts (
                contract_date,
                ticker,
                contract_type,
                option_type,
                contract_price,
                upper_limit,
                lower_limit
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        contract_date,
        ticker.upper(),
        contract_type,
        option_type,
        contract_price,
        upper_limit,
        lower_limit
        )

        conn.commit()

        st.success("Contract saved successfully!")

# ===============================
# Cancel
# ===============================
if cancel:
    st.warning("Form cleared")

# ===============================
# Display Saved Data
# ===============================
st.subheader("Saved Contracts")

cursor.execute("SELECT TOP 50 * FROM contracts ORDER BY id DESC")
rows = cursor.fetchall()

if rows:
    data = [list(row) for row in rows]
    st.dataframe(data)
else:
    st.info("No records found")