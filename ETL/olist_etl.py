import os
from pathlib import Path

import kagglehub
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# -----------------------------
# 0) Env + DB connection
# -----------------------------
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)

# -----------------------------
# 1) Download Olist dataset
# -----------------------------
olist_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")

# -----------------------------
# 2) Load required CSVs into raw schema (pandas creates tables)
# -----------------------------
with engine.begin() as conn:
    conn.execute(text("DROP SCHEMA IF EXISTS raw CASCADE;"))
    conn.execute(text("CREATE SCHEMA raw;"))

tables = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "products": "olist_products_dataset.csv",
    "product_category_translation": "product_category_name_translation.csv",
    "payments": "olist_order_payments_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
}

for table, filename in tables.items():
    df = pd.read_csv(f"{olist_path}/{filename}")
    df.to_sql(table, engine, schema="raw", if_exists="replace", index=False)
    print(f"Loaded raw.{table}")


# -----------------------------
# 3) Run DW DDL + DW load scripts
# -----------------------------


def run_sql(path: str) -> None:
    sql_text = Path(path).read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql_text))
    print(f"Executed: {path}")

run_sql("ETL/sql/dw_DDL.sql")
run_sql("ETL/sql/dw_load.sql")

print("DW build complete.")
