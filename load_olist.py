import kagglehub
import pandas as pd
from sqlalchemy import create_engine
import os

# -----------------------------
# 1. DOWNLOAD DATASETS
# -----------------------------
olist_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
funnel_path = kagglehub.dataset_download("olistbr/marketing-funnel-olist")

# Debug print
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
engine = create_engine("postgresql://postgres:Postgres28@localhost:5432/olist_db")

# -----------------------------
# 5. LOAD ALL TABLES INTO POSTGRES
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
    print(f"Loaded table: {name}")

print("All tables loaded successfully.")

