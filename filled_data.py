from __future__ import annotations

import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import yfinance as yf


# ==========================================================
# Logging
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("StockDataFiller")


# ==========================================================
# Yahoo Finance Service
# ==========================================================
class YahooFinanceService:
    LOOKBACK_DAYS = 7

    @staticmethod
    def _clean_columns(data: pd.DataFrame) -> pd.DataFrame:
        """Flatten MultiIndex columns if present."""
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        return data

    # ------------------------------------------------------
    @staticmethod
    def download_data(ticker: str, target_date: date) -> pd.DataFrame:
        try:
            start_date = target_date - timedelta(days=YahooFinanceService.LOOKBACK_DAYS)

            data = yf.download(
                ticker,
                start=start_date,
                end=target_date + timedelta(days=1),
                progress=False,
                auto_adjust=False,
            )

            if data.empty:
                logger.warning(f"No data for {ticker}")
                return pd.DataFrame()

            data = YahooFinanceService._clean_columns(data)

            return data

        except Exception as e:
            logger.error(f"Download failed for {ticker}: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------
    @staticmethod
    def get_avg_price(ticker: str, target_date: date) -> Optional[float]:
        try:
            data = YahooFinanceService.download_data(ticker, target_date)

            if data.empty:
                return None

            row = data.iloc[-1]

            avg_price = (
                float(row["Open"])
                + float(row["High"])
                + float(row["Low"])
                + float(row["Close"])
            ) / 4

            return avg_price

        except Exception as e:
            logger.error(f"Avg price failed for {ticker}: {e}")
            return None

    # ------------------------------------------------------
    @staticmethod
    def get_close_price(ticker: str, target_date: date) -> Optional[float]:
        try:
            data = YahooFinanceService.download_data(ticker, target_date)

            if data.empty:
                return None

            close_price = float(data["Close"].iloc[-1])
            return close_price

        except Exception as e:
            logger.error(f"Close price failed for {ticker}: {e}")
            return None


# ==========================================================
# Excel Processor
# ==========================================================
class StockDataProcessor:

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.today = datetime.today().date()
        self.df = pd.DataFrame()

    # ------------------------------------------------------
    def load_data(self):

        try:
            print("Reading Excel file...")

            self.df = pd.read_excel(self.file_path)

            print(f"Rows loaded: {len(self.df)}")

        except Exception as e:
            print("Error loading Excel:", e)
            raise


    # ------------------------------------------------------
    def fill_missing_values(self):

        try:
            print("Starting data processing...")

            for i, row in self.df.iterrows():

                ticker = str(row["Sticker"]).strip()
                print(f"Processing row {i} — {ticker}")

                trade_date = pd.to_datetime(row["Date"]).date()
                expiry_date = pd.to_datetime(row["Expiry Date"]).date()

                if pd.isna(row["Stock Price"]):
                    avg_price = YahooFinanceService.get_avg_price(
                    ticker, trade_date
                    )

                    if avg_price is not None:
                        self.df.at[i, "Stock Price"] = avg_price
                        print(f"✔ Stock Price filled for {ticker}")

                if pd.isna(row["SP_End"]) and expiry_date < self.today:
                    close_price = YahooFinanceService.get_close_price(
                    ticker, expiry_date
                    )

                    if close_price is not None:
                        self.df.at[i, "SP_End"] = close_price
                        print(f"✔ SP_End filled for {ticker}")

            print("Processing finished.")

        except Exception as e:
            logger.error("Error during processing: %s", e)
            raise


            # ---- SP_End ----
            if pd.isna(row["SP_End"]) and expiry_date < self.today:

                close_price = YahooFinanceService.get_close_price(
                    ticker, expiry_date
                    )

                if close_price is not None:
                    self.df.at[i, "SP_End"] = close_price
                    logger.info(f"Filled SP_End for {ticker}")

    # ------------------------------------------------------
    def save_output(self, output_path: Path):

        try:
            print(f"Saving file to: {output_path}")

        # Round values
            self.df["Stock Price"] = self.df["Stock Price"].round(2)
            self.df["SP_End"] = self.df["SP_End"].round(2)

            self.df.to_excel(output_path, index=False)

            print("File saved successfully.")

        except PermissionError:
            print("❌ File is open. Please close Excel and retry.")
            raise

        except Exception as e:
            print("Error saving file:", e)
            raise

    



# ==========================================================
# MAIN
# ==========================================================
def main():

    print("===== PROGRAM STARTED =====")

    try:
        input_file = Path(r"C:\OptionEntries (1).xlsx")

        print(f"Input file path: {input_file}")

        if not input_file.exists():
            raise FileNotFoundError("Input Excel file not found!")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        output_file = Path(
            fr"C:\Users\user\Downloads\{timestamp}.xlsx"
        )

        print("Creating processor object...")

        processor = StockDataProcessor(input_file)

        print("Loading Excel data...")
        processor.load_data()
        print("Excel loaded successfully.")

        print("Filling missing values from Yahoo Finance...")
        processor.fill_missing_values()
        print("Data enrichment completed.")

        print("Saving output file...")
        processor.save_output(output_file)

        print("===== SUCCESS =====")
        print(f"Output saved to: {output_file}")

    except FileNotFoundError as e:
        print("❌ FILE ERROR:", e)

    except PermissionError as e:
        print("❌ PERMISSION ERROR:", e)

    except Exception as e:
        print("❌ UNEXPECTED ERROR:", e)

    finally:
        print("===== PROGRAM FINISHED =====")

if __name__ == "__main__":
    main()

