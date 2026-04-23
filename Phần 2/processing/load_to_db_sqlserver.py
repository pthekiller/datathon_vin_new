"""Load final warehouse CSV outputs into SQL Server.

This loader is aligned with the final Transform.py output structure:
    Clean_Data/datamart/
        dim/
        fact/
        mart/

Main features
-------------
1. Creates the target SQL Server database if it does not exist.
2. Executes the SQL Server DDL file before loading data.
3. Loads all warehouse tables in dependency-safe order.
4. Normalizes date / integer / float / boolean columns from CSV.
5. Uses truncate-then-load semantics for repeatable ETL runs.
6. Temporarily disables all foreign keys in the target schema while reloading.

Recommended packages
--------------------
    pip install pandas sqlalchemy pyodbc
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL

LOAD_ORDER: List[Tuple[str, str]] = [
    ("dim", "dim_date"),
    ("dim", "dim_geography"),
    ("dim", "dim_product"),
    ("dim", "dim_customer"),
    ("dim", "dim_promotion"),
    ("dim", "dim_return_reason"),
    ("dim", "dim_traffic_source"),
    ("fact", "fact_sales_item"),
    ("fact", "fact_orders"),
    ("fact", "fact_returns"),
    ("fact", "fact_shipments"),
    ("fact", "fact_reviews"),
    ("fact", "fact_inventory_snapshot"),
    ("fact", "fact_sales_series"),
    ("fact", "fact_web_traffic"),
    ("mart", "mart_sales_overview"),
    ("mart", "mart_customer_insight"),
    ("mart", "mart_product_performance"),
    ("mart", "mart_operations"),
    ("mart", "mart_promotion_effectiveness"),
    ("mart", "mart_return_diagnostics"),
    ("mart", "mart_shipping_service"),
    ("mart", "mart_inventory_risk"),
    ("mart", "mart_sales_forecast_ready"),
]

DATE_COLUMNS: Dict[str, List[str]] = {
    "dim_date": ["full_date"],
    "dim_customer": ["signup_date"],
    "dim_promotion": ["start_date", "end_date"],
    "fact_sales_item": ["order_date"],
    "fact_orders": ["order_date"],
    "fact_returns": ["return_date"],
    "fact_shipments": ["ship_date", "delivery_date"],
    "fact_reviews": ["review_date"],
    "fact_inventory_snapshot": ["snapshot_date"],
    "fact_sales_series": ["date"],
    "fact_web_traffic": ["date"],
    "mart_sales_forecast_ready": ["date"],
}

BOOLEAN_COLUMNS: Dict[str, List[str]] = {
    "dim_date": ["is_weekend"],
}

INT_COLUMNS: Dict[str, List[str]] = {
    "dim_date": ["date_key", "day", "month", "quarter", "year", "week_of_year", "day_of_week"],
    "dim_geography": ["zip"],
    "dim_product": ["product_id"],
    "dim_customer": ["customer_id", "zip"],
    "dim_return_reason": ["return_reason_key"],
    "dim_traffic_source": ["traffic_source_key"],
    "fact_sales_item": ["order_id", "product_id", "customer_id", "zip", "date_key", "month_key", "year", "month", "quarter", "quantity"],
    "fact_orders": ["order_id", "customer_id", "zip", "date_key", "month_key", "year", "month", "quarter", "distinct_products", "total_quantity"],
    "fact_returns": ["order_id", "product_id", "return_date_key", "return_reason_key", "return_month_key", "return_year", "return_month", "return_quarter", "return_quantity"],
    "fact_shipments": ["order_id", "ship_date_key", "delivery_date_key", "ship_month_key", "delivery_month_key", "delivery_days"],
    "fact_reviews": ["order_id", "product_id", "customer_id", "review_date_key", "review_month_key", "rating"],
    "fact_inventory_snapshot": ["snapshot_date_key", "product_id", "month_key", "year", "month", "quarter", "stock_on_hand", "units_received", "units_sold", "stockout_days", "stockout_flag", "overstock_flag", "reorder_flag"],
    "fact_sales_series": ["date_key", "month_key", "year", "month", "quarter"],
    "fact_web_traffic": ["date_key", "traffic_source_key", "month_key", "year", "month", "quarter", "sessions", "unique_visitors", "page_views"],
    "mart_sales_overview": ["month_key", "year", "month", "quarter", "orders", "units_sold"],
    "mart_customer_insight": ["month_key", "year", "month", "quarter", "active_customers", "new_customers", "orders"],
    "mart_product_performance": ["month_key", "year", "month", "quarter", "product_id", "orders", "units_sold", "return_qty", "review_count"],
    "mart_operations": ["month_key", "year", "month", "quarter", "orders", "returns"],
    "mart_promotion_effectiveness": ["month_key", "year", "month", "quarter", "orders", "units_sold"],
    "mart_return_diagnostics": ["month_key", "return_reason_key", "return_count", "return_qty"],
    "mart_shipping_service": ["month_key", "shipments"],
    "mart_inventory_risk": ["month_key", "year", "month", "quarter", "product_id", "stock_on_hand", "units_received", "units_sold", "stockout_days", "stockout_flag", "overstock_flag", "reorder_flag"],
    "mart_sales_forecast_ready": ["date_key", "month_key", "year", "month", "quarter"],
}

FLOAT_COLUMNS: Dict[str, List[str]] = {
    "dim_product": ["price", "cogs"],
    "dim_promotion": ["discount_value", "min_order_value"],
    "fact_sales_item": ["unit_price", "discount_amount", "list_price", "cogs", "payment_value", "gross_sales", "net_sales", "line_cogs", "line_profit"],
    "fact_orders": ["gross_sales", "discount_amount", "net_sales", "order_cogs", "order_profit", "payment_value"],
    "fact_returns": ["refund_amount"],
    "fact_shipments": ["shipping_fee"],
    "fact_inventory_snapshot": ["days_of_supply", "fill_rate", "sell_through_rate"],
    "fact_sales_series": ["revenue", "cogs", "profit"],
    "fact_web_traffic": ["bounce_rate", "avg_session_duration_sec", "conversion_rate"],
    "mart_sales_overview": ["gross_sales", "discount_amount", "net_sales", "cogs", "profit", "aov"],
    "mart_customer_insight": ["revenue", "revenue_per_customer", "orders_per_customer", "aov"],
    "mart_product_performance": ["revenue", "profit", "cogs", "discount_amount", "refund_amount", "avg_rating"],
    "mart_operations": ["revenue", "avg_delivery_days", "shipping_fee", "refund_amount", "avg_rating"],
    "mart_promotion_effectiveness": ["discount_amount", "revenue", "profit"],
    "mart_return_diagnostics": ["refund_amount", "avg_rating"],
    "mart_shipping_service": ["shipping_fee", "avg_delivery_days"],
    "mart_inventory_risk": ["days_of_supply", "fill_rate", "sell_through_rate"],
    "mart_sales_forecast_ready": ["revenue", "cogs", "profit", "lag_1", "lag_7", "lag_30", "rolling_mean_7", "rolling_mean_30", "rolling_std_30", "revenue_growth_7d", "profit_margin"],
}

STRING_DTYPE_TABLES: Dict[str, Dict[str, str]] = {
    "dim_promotion": {"promo_id": "string"},
    "fact_sales_item": {"promo_id": "string", "promo_id_2": "string"},
    "mart_promotion_effectiveness": {"promo_id": "string"},
}

IDENTITY_TABLES = {"fact_sales_item"}


class DataWarehouseLoader:
    """Load warehouse CSV outputs into SQL Server."""

    def __init__(
        self,
        server: str,
        database: str,
        username: str | None,
        password: str | None,
        driver: str,
        schema: str,
        datamart_root: Path,
        ddl_path: Path,
        chunksize: int = 5000,
        trust_server_certificate: bool = True,
    ) -> None:
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.schema = schema
        self.datamart_root = datamart_root
        self.ddl_path = ddl_path
        self.chunksize = chunksize
        self.trust_server_certificate = trust_server_certificate

        self.master_engine = self._build_engine("master", autocommit=True)
        self.engine = self._build_engine(self.database, autocommit=False)

    def _build_engine(self, database_name: str, autocommit: bool) -> Engine:
        query = {"driver": self.driver, "TrustServerCertificate": "yes" if self.trust_server_certificate else "no"}

        if self.username and self.password:
            url = URL.create(
                "mssql+pyodbc",
                username=self.username,
                password=self.password,
                host=self.server,
                database=database_name,
                query=query,
            )
        else:
            query["trusted_connection"] = "yes"
            url = URL.create(
                "mssql+pyodbc",
                host=self.server,
                database=database_name,
                query=query,
            )

        if autocommit:
            return create_engine(url, future=True, isolation_level="AUTOCOMMIT", fast_executemany=True)
        return create_engine(url, future=True, fast_executemany=True)

    def _csv_path(self, group: str, table_name: str) -> Path:
        return self.datamart_root / group / f"{table_name}.csv"

    def validate_input_paths(self) -> None:
        if not self.datamart_root.exists():
            raise FileNotFoundError(f"Datamart root not found: {self.datamart_root}")
        if not self.ddl_path.exists():
            raise FileNotFoundError(f"DDL file not found: {self.ddl_path}")

        missing: List[str] = []
        for group, table_name in LOAD_ORDER:
            csv_path = self._csv_path(group, table_name)
            if not csv_path.exists():
                missing.append(str(csv_path))

        if missing:
            raise FileNotFoundError("Missing CSV files:\n- " + "\n- ".join(missing))

    def ensure_database_exists(self) -> None:
        safe_database = self.database.replace("]", "]]" )
        create_sql = text(
            f"""
            IF DB_ID(:db_name) IS NULL
            BEGIN
                EXEC('CREATE DATABASE [{safe_database}]');
            END;
            """
        )
        with self.master_engine.connect() as conn:
            conn.execute(create_sql, {"db_name": self.database})

    @staticmethod
    def _split_batches(raw_sql: str) -> List[str]:
        return [batch.strip() for batch in re.split(r"^\s*GO\s*$", raw_sql, flags=re.MULTILINE | re.IGNORECASE) if batch.strip()]

    def run_ddl(self) -> None:
        raw_sql = self.ddl_path.read_text(encoding="utf-8")
        batches = self._split_batches(raw_sql)
        with self.engine.begin() as conn:
            for batch in batches:
                conn.exec_driver_sql(batch)

    def disable_schema_constraints(self) -> None:
        sql = text(
            """
            SELECT s.name AS schema_name, t.name AS table_name
            FROM sys.tables t
            INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = :schema_name
            ORDER BY t.name;
            """
        )
        with self.engine.begin() as conn:
            rows = conn.execute(sql, {"schema_name": self.schema}).fetchall()
            for row in rows:
                conn.exec_driver_sql(
                    f"ALTER TABLE [{row.schema_name}].[{row.table_name}] NOCHECK CONSTRAINT ALL"
                )

    def enable_schema_constraints(self) -> None:
        sql = text(
            """
            SELECT s.name AS schema_name, t.name AS table_name
            FROM sys.tables t
            INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = :schema_name
            ORDER BY t.name;
            """
        )
        with self.engine.begin() as conn:
            rows = conn.execute(sql, {"schema_name": self.schema}).fetchall()
            for row in rows:
                conn.exec_driver_sql(
                    f"ALTER TABLE [{row.schema_name}].[{row.table_name}] WITH CHECK CHECK CONSTRAINT ALL"
                )

    def clear_all_tables(self) -> None:
        delete_order = list(reversed(LOAD_ORDER))
        self.disable_schema_constraints()
        try:
            with self.engine.begin() as conn:
                for _, table_name in delete_order:
                    conn.exec_driver_sql(f"DELETE FROM [{self.schema}].[{table_name}]")
                    if table_name in IDENTITY_TABLES:
                        conn.exec_driver_sql(f"DBCC CHECKIDENT ('[{self.schema}].[{table_name}]', RESEED, 0)")
        finally:
            self.enable_schema_constraints()

    @staticmethod
    def _coerce_boolean(series: pd.Series) -> pd.Series:
        mapped = series.map(
            {
                True: 1,
                False: 0,
                "True": 1,
                "False": 0,
                "true": 1,
                "false": 0,
                1: 1,
                0: 0,
                "1": 1,
                "0": 0,
            }
        )
        return mapped.astype("Int64")

    def _normalize_dataframe(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        for col in DATE_COLUMNS.get(table_name, []):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        for col in BOOLEAN_COLUMNS.get(table_name, []):
            if col in df.columns:
                df[col] = self._coerce_boolean(df[col])

        for col in INT_COLUMNS.get(table_name, []):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        for col in FLOAT_COLUMNS.get(table_name, []):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        for col in df.columns:
            if df[col].dtype == "object" or str(df[col].dtype).startswith("string"):
                df[col] = df[col].replace({pd.NA: None, "<NA>": None})
                df[col] = df[col].where(df[col].notna(), None)

        df = df.where(pd.notnull(df), None)
        return df

    def _load_table(self, table_name: str, group: str) -> int:
        csv_path = self._csv_path(group, table_name)
        read_kwargs = {"low_memory": False}
        if table_name in STRING_DTYPE_TABLES:
            read_kwargs["dtype"] = STRING_DTYPE_TABLES[table_name]

        df = pd.read_csv(csv_path, **read_kwargs)
        df = self._normalize_dataframe(df, table_name)

        if table_name == "fact_sales_item" and "sales_item_key" in df.columns:
            df = df.drop(columns=["sales_item_key"])

        if df.empty:
            print(f"[SKIP] {table_name}: file exists but has 0 rows")
            return 0

        df.to_sql(
            name=table_name,
            con=self.engine,
            schema=self.schema,
            if_exists="append",
            index=False,
            method=None,
            chunksize=self.chunksize,
        )
        print(f"[OK]   {table_name:<28} rows={len(df):>8,} source={csv_path}")
        return len(df)

    def load_all(self) -> None:
        self.validate_input_paths()
        self.ensure_database_exists()
        self.run_ddl()
        self.clear_all_tables()

        total_rows = 0
        for group, table_name in LOAD_ORDER:
            total_rows += self._load_table(table_name, group)

        print("-" * 88)
        print(f"Loaded {len(LOAD_ORDER)} tables into [{self.database}].[{self.schema}] with total rows = {total_rows:,}")
        print("Done.")


def parse_args() -> argparse.Namespace:
    current_dir = Path(__file__).resolve().parent
    project_root = current_dir.parent.parent

    parser = argparse.ArgumentParser(description="Load warehouse CSV outputs into SQL Server.")
    parser.add_argument("--server", default=r"localhost\SQLEXPRESS", help="SQL Server host or host\\instance.")
    parser.add_argument("--database", default="datawarehouse_final", help="Target SQL Server database.")
    parser.add_argument("--username", default=None, help="SQL Server username. Omit for Windows Authentication.")
    parser.add_argument("--password", default=None, help="SQL Server password. Omit for Windows Authentication.")
    parser.add_argument("--driver", default="ODBC Driver 17 for SQL Server", help="Installed ODBC driver name.")
    parser.add_argument("--schema", default="datamart", help="Target schema name.")
    parser.add_argument(
        "--datamart-root",
        type=Path,
        default=project_root / "output" / "Clean_Data" / "datamart",
        help="Folder containing dim/fact/mart CSV outputs.",
    )
    parser.add_argument(
        "--ddl-path",
        type=Path,
        default=project_root / "Phần 2" / "processing" / "create_db_sqlserver.sql",
        help="Path to the SQL Server DDL file.",
    )
    parser.add_argument("--chunksize", type=int, default=5000, help="Batch size for inserts.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    loader = DataWarehouseLoader(
        server=args.server,
        database=args.database,
        username=args.username,
        password=args.password,
        driver=args.driver,
        schema=args.schema,
        datamart_root=args.datamart_root,
        ddl_path=args.ddl_path,
        chunksize=args.chunksize,
    )
    loader.load_all()


if __name__ == "__main__":
    main()
