from __future__ import annotations

import streamlit as st
import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime
import time


# =========================================================
# CONFIG — PUT YOUR FILE PATH HERE
# =========================================================

FILE_PATH = r"C:\Users\user\Downloads\Book 8.xlsx"


# =========================================================
# Custom Exceptions
# =========================================================

class DataFileError(Exception):
    pass


class ColumnMissingError(Exception):
    pass


class NoFutureExpiryError(Exception):
    pass


# =========================================================
# Data Processing Class
# =========================================================

class FutureExpiryProcessor:

    REQUIRED_COLUMNS = ["Date", "Expiry Date", "Ticker"]

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.df = None

    # -----------------------------
    # Load Data
    # -----------------------------
    def load_data(self):

        if not self.file_path.exists():
            raise DataFileError("Data file not found")

        self.df = pd.read_excel(self.file_path)

    # -----------------------------
    # Validate Columns
    # -----------------------------
    def validate_columns(self):

        missing = [
            col for col in self.REQUIRED_COLUMNS
            if col not in self.df.columns
        ]

        if missing:
            raise ColumnMissingError(
                f"Missing columns: {missing}"
            )

    # -----------------------------
    # Convert Dates
    # -----------------------------
    def preprocess(self):

        self.df["Date"] = pd.to_datetime(
            self.df["Date"], errors="coerce"
        )

        self.df["Expiry Date"] = pd.to_datetime(
            self.df["Expiry Date"], errors="coerce"
        )

    # -----------------------------
    # Filter Future Expiry Rows
    # -----------------------------
    def get_future_expiry(self):

        today = pd.Timestamp(datetime.today().date())

        future_df = self.df[
            self.df["Expiry Date"] > today
        ].copy()

        if future_df.empty:
            raise NoFutureExpiryError(
                "No tickers with future expiry dates"
            )

        return future_df


# =========================================================
# Price Fetcher
# =========================================================

class PriceFetcher:

    @staticmethod
    def fetch_prices(tickers):

        prices = []

        for ticker in tickers:

            try:
                stock = yf.Ticker(ticker)
                price = stock.fast_info.last_price

                prices.append(
                    round(price, 2) if price else None
                )

            except Exception:
                prices.append(None)

        return prices


# =========================================================
# Dashboard UI
# =========================================================

class FutureExpiryDashboard:

    def __init__(self):

        st.set_page_config(
            page_title="Future Expiry Dashboard",
            layout="wide"
        )

        st.title("📈 Future Expiry — Live Prices")

    # -----------------------------
    # Display Table
    # -----------------------------
    def show_table(self, future_df):

        tickers = future_df["Ticker"].dropna().unique()

        # Fetch live prices (array)
        prices = PriceFetcher.fetch_prices(tickers)

        price_map = dict(zip(tickers, prices))

        result_df = future_df[
            ["Date", "Expiry Date", "Ticker"]
        ].drop_duplicates()

        result_df["Current Price"] = (
            result_df["Ticker"].map(price_map)
        )

        st.dataframe(
            result_df,
            width="stretch"
        )


# =========================================================
# MAIN
# =========================================================

def main():

    dashboard = FutureExpiryDashboard()

    try:

        processor = FutureExpiryProcessor(
            Path(FILE_PATH)
        )

        processor.load_data()
        processor.validate_columns()
        processor.preprocess()

        future_df = processor.get_future_expiry()

        dashboard.show_table(future_df)

        # ---------------------------------------------
        # Auto refresh every 30 seconds
        # ---------------------------------------------
        time.sleep(30)
        st.rerun()

    except DataFileError as e:
        st.error(f"File Error: {e}")

    except ColumnMissingError as e:
        st.error(f"Column Error: {e}")

    except NoFutureExpiryError as e:
        st.warning(str(e))

    except Exception as e:
        st.error(f"Unexpected error: {e}")


# =========================================================

if __name__ == "__main__":
    main()
