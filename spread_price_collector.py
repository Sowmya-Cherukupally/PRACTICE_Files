from __future__ import annotations

import logging
import time
from datetime import datetime, time as dt_time
from typing import List

import pyodbc
import pytz
import schedule
import yfinance as yf


# ============================================================
# CONFIGURATION — CHANGE THESE
# ============================================================

class Config:
    SERVER = ".\\SQLEXPRESS02"      
    DATABASE = "INVESTMENTS"
    DRIVER = "ODBC Driver 17 for SQL Server"
    TRUSTED_CONNECTION = "yes"

    SPREADS_TABLE = "dbo.option_spreads"
    TARGET_TABLE = "dbo.Spread_Prices"


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("SpreadPriceCollector")


# ============================================================
# Database Manager
# ============================================================

class DatabaseManager:

    def __init__(self):
        self.connection_string = (
            f"DRIVER={{{Config.DRIVER}}};"
            f"SERVER={Config.SERVER};"
            f"DATABASE={Config.DATABASE};"
            f"Trusted_Connection={Config.TRUSTED_CONNECTION};"
        )

    def get_connection(self):
        return pyodbc.connect(self.connection_string)

    # --------------------------------------------------------

    def get_active_tickers(self) -> List[str]:
        query = f"""
            SELECT DISTINCT Ticker
            FROM {Config.SPREADS_TABLE}
            WHERE ExpiryDate >= CAST(GETDATE() AS DATE)
        """

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

            tickers = [row[0] for row in rows]
            logger.info("Active tickers found: %s", len(tickers))

            return tickers

    # --------------------------------------------------------

    def insert_price(self, ticker: str, price: float):

        query = f"""
            INSERT INTO {Config.TARGET_TABLE}
            (Ticker, Date_Time, Price)
            VALUES (?, ?, ?)
        """

        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                query,
                ticker,
                datetime.now(),
                round(price, 4),
            )

            conn.commit()


# ============================================================
# Market Schedule (EST)
# ============================================================

class MarketSchedule:

    EST = pytz.timezone("US/Eastern")

    START_TIME = dt_time(8, 24)
    END_TIME = dt_time(16, 1)

    @classmethod
    def is_market_window(cls) -> bool:

        now_est = datetime.now(cls.EST)

        # Skip weekends
        if now_est.weekday() >= 5:
            return False

        return cls.START_TIME <= now_est.time() <= cls.END_TIME


# ============================================================
# Price Fetcher
# ============================================================

class PriceFetcher:

    @staticmethod
    def get_price(ticker: str) -> float | None:

        try:
            data = yf.Ticker(ticker)
            price = data.fast_info.last_price

            return float(price) if price else None

        except Exception:
            logger.warning("Price fetch failed for %s", ticker)
            return None


# ============================================================
# Main Collector
# ============================================================

class SpreadPriceCollector:

    def __init__(self):
        self.db = DatabaseManager()

    def run(self):

        logger.info("Running price collection cycle")

        tickers = self.db.get_active_tickers()

        if not tickers:
            logger.warning("No active tickers found")
            return

        for ticker in tickers:

            price = PriceFetcher.get_price(ticker)

            if price is None:
                continue

            self.db.insert_price(ticker, price)

            logger.info("Inserted %s → %.4f", ticker, price)


# ============================================================
# Scheduler
# ============================================================

def main():

    collector = SpreadPriceCollector()

    # Run every 15 minutes
    schedule.every(15).minutes.do(collector.run)

    logger.info("Scheduler started — EST Market Window Mode")

    while True:

        if MarketSchedule.is_market_window():
            schedule.run_pending()
            time.sleep(1)

        else:
            # Outside market window
            time.sleep(60)


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    main()