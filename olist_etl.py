import kagglehub
import pandas as pd
from sqlalchemy import create_engine, text
import os
import seaborn as sb

# -----------------------------
# 0. LOAD ENVIRONMENT VARIABLES 
# -----------------------------
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent / ".env"
print("Looking for .env at:", env_path)

loaded = load_dotenv(dotenv_path=env_path)
print("Loaded .env:", loaded)

# -----------------------------
# 1. DOWNLOAD DATASETS
# -----------------------------
olist_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
funnel_path = kagglehub.dataset_download("olistbr/marketing-funnel-olist")

print("Funnel path:", funnel_path)
print("Files in funnel dataset:")
print(os.listdir(funnel_path))

# -----------------------------
# 2. READ MAIN OLIST CSV FILES
# -----------------------------
customers = pd.read_csv(f"{olist_path}/olist_customers_dataset.csv")
orders = pd.read_csv(f"{olist_path}/olist_orders_dataset.csv")
order_items = pd.read_csv(f"{olist_path}/olist_order_items_dataset.csv")
products = pd.read_csv(f"{olist_path}/olist_products_dataset.csv")
payments = pd.read_csv(f"{olist_path}/olist_order_payments_dataset.csv")
sellers = pd.read_csv(f"{olist_path}/olist_sellers_dataset.csv")
geolocation = pd.read_csv(f"{olist_path}/olist_geolocation_dataset.csv")
reviews = pd.read_csv(f"{olist_path}/olist_order_reviews_dataset.csv")

# -----------------------------
# 3. READ MARKETING FUNNEL CSV FILES
# -----------------------------
leads = pd.read_csv(f"{funnel_path}/olist_marketing_qualified_leads_dataset.csv")
closed_deals = pd.read_csv(f"{funnel_path}/olist_closed_deals_dataset.csv")

# -----------------------------
# 4. CONNECT TO POSTGRESQL
# -----------------------------

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)

# -----------------------------
# 5. LOAD ALL TABLES INTO POSTGRES (raw schema)
# -----------------------------
tables = {
    "customers": customers,
    "orders": orders,
    "order_items": order_items,
    "products": products,
    "payments": payments,
    "sellers": sellers,
    "geolocation": geolocation,
    "reviews": reviews,
    "leads": leads,
    "closed_deals": closed_deals,
}

for name, df in tables.items():
    df.to_sql(name, engine, schema="raw", if_exists="replace", index=False)
    print(f"Loaded table: raw.{name}")

print("All raw tables loaded successfully.")

# ============================================================
# Helper function: run a .sql file
# ============================================================
def run_sql_file(engine, path):
    """Read and execute an entire .sql file as one execution block."""
    with engine.begin() as conn:
        with open(path, "r", encoding="utf-8") as f:
            sql_text = f.read()
        conn.execute(text(sql_text))
    print(f"Executed SQL file: {path}")

# -----------------------------
# 6. RUN DDL TO CREATE CORE SCHEMA
# -----------------------------
ddl_path = os.path.join("sql", "1_ddl_core.sql")
run_sql_file(engine, ddl_path)
print("Core schema (core.* tables, indexes, views) created successfully.")

# -----------------------------
# 7. LOAD RAW DATA INTO CORE SCHEMA
# -----------------------------
load_path = os.path.join("sql", "2_load_core.sql")
run_sql_file(engine, load_path)
print("Core tables populated from raw.")

# -----------------------------
# 8. CREATE ANALYTICS VIEWS (core.v_*)
# -----------------------------
analytics_views_path = os.path.join("sql", "3_analytics_views.sql")
run_sql_file(engine, analytics_views_path)
print("Analytics views (core.v_*) created successfully.")

# -----------------------------
# 9. CREATE ANALYTICS STAR SCHEMA TABLES (analytics.dim_*, analytics.fact_*)
# -----------------------------
star_schema_path = os.path.join("sql", "4_analytics_star_schema.sql")
run_sql_file(engine, star_schema_path)
print("Analytics star schema (analytics.* tables) created successfully.")

# -----------------------------
# 10. LOAD ANALYTICS TABLES FROM CORE (fills dim_*, fact_*)
# -----------------------------
analytics_load_path = os.path.join("sql", "5_analytics_load.sql")
run_sql_file(engine, analytics_load_path)
print("Analytics dimension and fact tables populated from core.")
