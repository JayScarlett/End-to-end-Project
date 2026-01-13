import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

def main():
    print("RUNNING:", os.path.abspath(__file__), flush=True)

    # Load environment variables for secure database access
    load_dotenv()

    # Create database connection to analytics layer
    engine = create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    print("ENGINE OK", flush=True)

    # Image output directory (relative to this script)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    img_dir = os.path.join(script_dir, "EDA_images")
    os.makedirs(img_dir, exist_ok=True)
    print("Saving images to:", img_dir, flush=True)

    # Load only what we need for this phase (facts)
    print("Loading fact_orders...", flush=True)
    fact_orders = pd.read_sql("SELECT * FROM analytics.fact_orders", engine)
    print("Loaded fact_orders:", fact_orders.shape, flush=True)

    print("Loading fact_daily_sales...", flush=True)
    fact_daily_sales = pd.read_sql("SELECT * FROM analytics.fact_daily_sales", engine)
    print("Loaded fact_daily_sales:", fact_daily_sales.shape, flush=True)

    # Save order value distribution (raw)
    plt.figure()
    fact_orders["total_payment_value"].hist(bins=50)
    plt.title("Total Payment Value Distribution")
    plt.xlabel("total_payment_value")
    plt.ylabel("count")
    plt.tight_layout()
    plt.savefig(os.path.join(img_dir, "order_value_distribution_raw.png"))
    plt.close()
    print("Saved: order_value_distribution_raw.png", flush=True)

    # Time-series setup (fact_daily_sales)
    fact_daily_sales["date_key"] = pd.to_datetime(fact_daily_sales["date_key"])
    fact_daily_sales = fact_daily_sales.sort_values("date_key")

    # Daily trends
    plt.figure()
    plt.plot(fact_daily_sales["date_key"], fact_daily_sales["total_payment_value"])
    plt.title("Daily Revenue (Total Payment Value)")
    plt.xlabel("date")
    plt.ylabel("total_payment_value")
    plt.tight_layout()
    plt.savefig(os.path.join(img_dir, "daily_revenue_trend.png"))
    plt.close()
    print("Saved: daily_revenue_trend.png", flush=True)

    plt.figure()
    plt.plot(fact_daily_sales["date_key"], fact_daily_sales["total_orders"])
    plt.title("Daily Order Volume")
    plt.xlabel("date")
    plt.ylabel("total_orders")
    plt.tight_layout()
    plt.savefig(os.path.join(img_dir, "daily_order_volume_trend.png"))
    plt.close()
    print("Saved: daily_order_volume_trend.png", flush=True)

    # Monthly rollup
    monthly = (
        fact_daily_sales
        .set_index("date_key")
        .resample("MS")
        .agg(
            total_payment_value=("total_payment_value", "sum"),
            total_orders=("total_orders", "sum"),
            avg_order_value=("avg_order_value", "mean"),
        )
        .reset_index()
    )

    plt.figure()
    plt.plot(monthly["date_key"], monthly["total_payment_value"])
    plt.title("Monthly Revenue (Total Payment Value)")
    plt.xlabel("month")
    plt.ylabel("total_payment_value")
    plt.tight_layout()
    plt.savefig(os.path.join(img_dir, "monthly_revenue_trend.png"))
    plt.close()
    print("Saved: monthly_revenue_trend.png", flush=True)

    plt.figure()
    plt.plot(monthly["date_key"], monthly["total_orders"])
    plt.title("Monthly Order Volume")
    plt.xlabel("month")
    plt.ylabel("total_orders")
    plt.tight_layout()
    plt.savefig(os.path.join(img_dir, "monthly_order_volume_trend.png"))
    plt.close()
    print("Saved: monthly_order_volume_trend.png", flush=True)

    # Prove what is on disk
    print("\nFiles currently in EDA_images:", flush=True)
    for f in sorted(os.listdir(img_dir)):
        print(" -", f, flush=True)

    # Snapshot for narrative support
    print("\nMonthly snapshot (first 6 rows):\n", monthly.head(6), flush=True)
    print("\nMonthly snapshot (last 6 rows):\n", monthly.tail(6), flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", repr(e), flush=True)
        raise

