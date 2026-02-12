import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

query = """
SELECT *
FROM dw.fact_order_item
"""

df = pd.read_sql(query, engine)

print(df.shape)
df.head()

df.info()
df.describe()

df["revenue"] = df["item_price"] + df["freight_value"]

total_revenue = df["revenue"].sum()
print("Total Revenue:", total_revenue)

late_rate = df["late_delivery_flag"].mean()
print("Late Delivery Rate:", late_rate)

df[df["order_status"] == "delivered"]["delivery_days"].mean()

query = """
SELECT f.*, d.year
FROM dw.fact_order_item f
JOIN dw.dim_date d
  ON f.purchase_date_key = d.date_key
"""

df = pd.read_sql(query, engine)

df["revenue"] = df["item_price"] + df["freight_value"]

df.groupby("year")["revenue"].sum()

query = """
SELECT f.item_price,
       f.freight_value,
       p.product_category_name_english
FROM dw.fact_order_item f
JOIN dw.dim_product p
  ON f.product_key = p.product_key
"""

df = pd.read_sql(query, engine)

df["revenue"] = df["item_price"] + df["freight_value"]

df.groupby("product_category_name_english")["revenue"] \
  .sum() \
  .sort_values(ascending=False) \
  .head(10)
