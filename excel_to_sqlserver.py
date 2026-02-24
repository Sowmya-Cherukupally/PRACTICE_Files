from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


# ================================================================
# Logging Configuration
# ================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("ExcelToSQLServerETL")


# ================================================================
# Configuration Model
# ================================================================
class DBConfig:
    def __init__(
        self,
        server: str,
        database: str,
        table: str,
        schema: str = "dbo",
    ):
        self.server = server
        self.database = database
        self.table = table
        self.schema = schema


# ================================================================
# SQL Server Loader
# ================================================================
class SQLServerLoader:
    """
    Handles connection and data loading into SQL Server.
    """

    def __init__(self, config: DBConfig):
        self.config = config
        self.engine: Engine = self._create_engine()

    def _create_engine(self) -> Engine:
        logger.info("Creating SQL Server connection engine")

        connection_string = (
            f"mssql+pyodbc://{self.config.server}/{self.config.database}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
            "&trusted_connection=yes"
        )

        return create_engine(connection_string)

    def load_dataframe(self, df: pd.DataFrame) -> None:
        """
        Upload DataFrame to SQL Server table.
        """

        logger.info(
            "Loading %d rows into %s.%s",
            len(df),
            self.config.schema,
            self.config.table,
        )

        df.to_sql(
            self.config.table,
            self.engine,
            schema=self.config.schema,
            if_exists="append",   # append to existing table
            index=False,
        )

        logger.info("Data successfully loaded into SQL Server")


# ================================================================
# Excel Extractor
# ================================================================
class ExcelExtractor:
    """
    Reads Excel file into DataFrame with validation.
    """

    def __init__(self, file_path: str | Path):
        self.file_path = Path(file_path)

    def extract(self) -> pd.DataFrame:

        if not self.file_path.exists():
            raise FileNotFoundError(f"Excel file not found: {self.file_path}")

        logger.info("Reading Excel file: %s", self.file_path)

        df = pd.read_excel(self.file_path)

        if df.empty:
            raise ValueError("Excel file contains no data")

        logger.info("Extracted %d rows", len(df))

        return df


# ================================================================
# Main ETL Pipeline
# ================================================================
class ExcelToSQLServerETL:
    """
    Orchestrates extraction and loading.
    """

    def __init__(self, excel_path: str, db_config: DBConfig):
        self.extractor = ExcelExtractor(excel_path)
        self.loader = SQLServerLoader(db_config)

    def run(self) -> None:

        logger.info("Starting ETL pipeline")

        df = self.extractor.extract()

        self.loader.load_dataframe(df)

        logger.info("ETL pipeline completed successfully")


# ================================================================
# Entry Point
# ================================================================
def main():

    excel_file = r"C:\Users\user\Downloads\SP500_Data 4.xlsx"

    db_config = DBConfig(
        server = r".\SQLEXPRESS02",    
        database="INVESTMENTS", 
        table = "TickerMaster",
        schema = "dbo"

    )

    etl = ExcelToSQLServerETL(excel_file, db_config)

    etl.run()


if __name__ == "__main__":
    main()
