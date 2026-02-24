import logging
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine


# ======================================================
# Logging
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("StocksHistoryLoader")


# ======================================================
# ETL Class
# ======================================================
class StocksHistoryETL:

    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.engine = self._create_engine()

    def _create_engine(self):
        conn_str = (
            f"mssql+pyodbc://{self.server}/{self.database}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
        )
        return create_engine(conn_str)

    # --------------------------------------------------
    # Get tickers
    # --------------------------------------------------
    def get_tickers(self):
        query = "SELECT tickerSymbol FROM dbo.TickerMaster"
        df = pd.read_sql(query, self.engine)

        tickers = df["tickerSymbol"].dropna().unique().tolist()

        logger.info(f"Fetched {len(tickers)} tickers")
        return tickers

    # --------------------------------------------------
    # Download history (FIXED)
    # --------------------------------------------------
    def download_history(self, ticker, start_date, end_date):

        try:
            logger.info(f"Downloading data for {ticker}")

            data = yf.download(
                ticker,
                start=start_date,
                end=end_date,
                progress=False,
                auto_adjust=False,
                group_by="column"
            )

            if data.empty:
                logger.warning(f"No data for {ticker}")
                return None

            # 🔥 Flatten columns if MultiIndex
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)

            data = data.reset_index()

            df = pd.DataFrame({
                "ticker": ticker,
                "date": pd.to_datetime(data["Date"]).dt.date,
                "open_price": data["Open"].round(2),
                "close_price": data["Close"].round(2),
                "high": data["High"].round(2),
                "low": data["Low"].round(2),
                "volume": data["Volume"]
            })

            return df

        except Exception as e:
            logger.error(f"Download failed for {ticker}: {e}")
            return None

    # --------------------------------------------------
    # Load to SQL Server
    # --------------------------------------------------
    def load_to_sql(self, df):

        df.to_sql(
            "Stocks_History",
            self.engine,
            schema="dbo",
            if_exists="append",
            index=False
        )

        logger.info(f"Inserted {len(df)} rows")

    # --------------------------------------------------
    # Run ETL
    # --------------------------------------------------
    def run(self, start_date, end_date):

        tickers = self.get_tickers()
        total_rows = 0

        for ticker in tickers:

            df = self.download_history(ticker, start_date, end_date)

            if df is not None:
                self.load_to_sql(df)
                total_rows += len(df)

        logger.info(f"ETL completed. Total rows inserted: {total_rows}")


# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":

    SERVER = r".\SQLEXPRESS02"   
    DATABASE = "Investments"
    TABLE = "Stocks_History"
    SCHEMA = "dbo"

    START_DATE = "2026-02-09"
    END_DATE = "2026-02-13"

    etl = StocksHistoryETL(SERVER, DATABASE)
    etl.run(START_DATE, END_DATE)
