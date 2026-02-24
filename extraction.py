from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import yfinance as yf


# ================================================================
# Logging Configuration
# ================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("OptionTradeEnrichment")


# ================================================================
# Yahoo Finance Service Layer
# ================================================================
class YahooFinanceService:

    # ------------------------------------------------------------
    # Fetch stock price near trade date
    # ------------------------------------------------------------
    @staticmethod
    def fetch_stock_price(
        ticker: str, trade_date: pd.Timestamp
    ) -> Optional[float]:

        try:
            start = trade_date - timedelta(days=3)
            end = trade_date + timedelta(days=3)

            data = yf.download(
                ticker,
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                progress=False,
                auto_adjust=True,
                threads=False,
            )

            if data.empty:
                logger.warning("No price data for %s near %s", ticker, trade_date)
                return None

            data.index = pd.to_datetime(data.index)

            # Find nearest trading day
            deltas = (data.index - trade_date).to_series().abs()
            closest_idx = deltas.argmin()

            close_value = data.iloc[closest_idx]["Close"]

            # Handle Series case
            if isinstance(close_value, pd.Series):
                close_value = close_value.iloc[0]

            return float(close_value)

        except Exception as exc:
            logger.exception("Failed to fetch price for %s: %s", ticker, exc)
            return None

    # ------------------------------------------------------------
    # Fetch nearest option expiration date
    # ------------------------------------------------------------
    @staticmethod
    def fetch_expiration_date(
        ticker: str, trade_date: pd.Timestamp
    ) -> Optional[pd.Timestamp]:

        try:
            tk = yf.Ticker(ticker)
            expirations = tk.options

            if not expirations:
                logger.warning("No option expirations available for %s", ticker)
                return None

            expiry_dates = pd.to_datetime(expirations)

            future_expiry = expiry_dates[expiry_dates >= trade_date]

            if len(future_expiry) == 0:
                return None

            return future_expiry.min()

        except Exception as exc:
            logger.exception("Failed to fetch expiration for %s: %s", ticker, exc)
            return None


# ================================================================
# Main Processor
# ================================================================
class OptionTradeProcessor:
    """
    Reads Excel → Detects NULL values → Enriches using Yahoo Finance
    """

    REQUIRED_COLUMNS = {
        "Date": "trade_date",
        "Sticker": "ticker",
        "Expiry Date": "expiration_date",
        "Stock Price": "price",
    }

    def __init__(self, file_path: str | Path):
        self.file_path = Path(file_path)
        self.df: Optional[pd.DataFrame] = None
        self.yf_service = YahooFinanceService()

    # ------------------------------------------------------------
    # Load Excel
    # ------------------------------------------------------------
    def load_excel(self) -> None:

        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")

        logger.info("Loading Excel file: %s", self.file_path)

        df = pd.read_excel(self.file_path)

        missing_cols = set(self.REQUIRED_COLUMNS.keys()) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        df = df.rename(columns=self.REQUIRED_COLUMNS)

        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df["expiration_date"] = pd.to_datetime(
            df["expiration_date"], errors="coerce"
        )

        self.df = df

        logger.info("Loaded %d rows", len(df))

    # ------------------------------------------------------------
    # Fill missing expiration date
    # ------------------------------------------------------------
    def _fill_expiration(self, row: pd.Series) -> Optional[pd.Timestamp]:

        if pd.notna(row["expiration_date"]):
            return row["expiration_date"]

        ticker = str(row["ticker"]).strip()
        trade_date = row["trade_date"]

        logger.info("Fetching expiration date for %s", ticker)

        return self.yf_service.fetch_expiration_date(ticker, trade_date)

    # ------------------------------------------------------------
    # Fill missing stock price
    # ------------------------------------------------------------
    def _fill_price(self, row: pd.Series) -> Optional[float]:

        if pd.notna(row["price"]):
            return float(row["price"])

        ticker = str(row["ticker"]).strip()
        trade_date = row["trade_date"]

        logger.info("Fetching price for %s on %s", ticker, trade_date.date())

        return self.yf_service.fetch_stock_price(ticker, trade_date)

    # ------------------------------------------------------------
    # Enrich dataset
    # ------------------------------------------------------------
    def enrich_data(self) -> None:

        if self.df is None:
            raise RuntimeError("Data not loaded")

        logger.info("Filling missing expiration dates...")
        self.df["final_expiration_date"] = self.df.apply(
            self._fill_expiration, axis=1
        )

        logger.info("Filling missing stock prices...")
        self.df["final_trade_price"] = self.df.apply(
            self._fill_price, axis=1
        )

    # ------------------------------------------------------------
    # Get processed result
    # ------------------------------------------------------------
    def get_result(self) -> pd.DataFrame:

        if self.df is None:
            raise RuntimeError("No processed data")

        return self.df[
            [
                "trade_date",
                "ticker",
                "final_expiration_date",
                "final_trade_price",
            ]
        ].copy()

    # ------------------------------------------------------------
    # Save output
    # ------------------------------------------------------------
    def save_output(self, output_path: str | Path) -> None:

        result = self.get_result()
        result.to_excel(output_path, index=False)

        logger.info("Output saved to %s", output_path)

    # ------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------
    def run(self) -> pd.DataFrame:

        self.load_excel()
        self.enrich_data()
        return self.get_result()


# ================================================================
# Entry Point
# ================================================================
def main():

    input_file = r"C:\Users\user\Downloads\OptionEntries.xlsx"

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = rf"C:\Users\user\Downloads\processed_option_data_{timestamp}.xlsx"

    processor = OptionTradeProcessor(input_file)

    result_df = processor.run()

    print(result_df.head())

    processor.save_output(output_file)


if __name__ == "__main__":
    main()
