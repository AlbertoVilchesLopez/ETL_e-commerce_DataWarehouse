# scripts/02_load_postgres.py
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

BASE = Path(__file__).resolve().parents[1]
load_dotenv(BASE / ".env")

PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASSWORD", "postgres")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "olist_dw")

def get_engine():
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}?sslmode=disable"

    return create_engine(url)

def create_schema(engine):
    with engine.begin() as conn:
        schema_sql = Path(BASE / "sql" / "warehouse_schema.sql").read_text()
        conn.execute(text(schema_sql))

def load_table(engine, df: pd.DataFrame, schema: str, table_name: str, if_exists='replace'):
    df.to_sql(table_name, engine, schema=schema, index=False, if_exists=if_exists, method='multi', chunksize=1000)

def load_all():
    transformed_dir = BASE / "data" / "transformed"
    orders = pd.read_parquet(transformed_dir / "orders.parquet")
    items = pd.read_parquet(transformed_dir / "order_items.parquet")
    products = pd.read_parquet(transformed_dir / "products.parquet")

    engine = get_engine()
    print("Creando schema y tablas si no existen...")
    create_schema(engine)

    print("Cargando tabla orders...")
    load_table(engine, orders, schema="olist_dw", table_name="orders", if_exists='replace')
    print("Cargando tabla order_items...")
    load_table(engine, items, schema="olist_dw", table_name="order_items", if_exists='replace')
    print("Cargando tabla products...")
    load_table(engine, products, schema="olist_dw", table_name="products", if_exists='replace')

    # Precompute summaries
    with engine.begin() as conn:
        # Product summary
        conn.execute(text("""
            INSERT INTO olist_dw.product_sales_summary (product_id, units_sold, total_revenue)
            SELECT product_id, count(*)::bigint AS units_sold, sum(price)::numeric AS total_revenue
            FROM olist_dw.order_items
            GROUP BY product_id
            ON CONFLICT (product_id) DO UPDATE
            SET units_sold = EXCLUDED.units_sold, total_revenue = EXCLUDED.total_revenue;
        """))
        # Daily summary
        conn.execute(text("""
            INSERT INTO olist_dw.daily_orders_summary (order_date, orders_count, total_revenue)
            SELECT (order_purchase_timestamp::date) AS order_date,
                   count(DISTINCT order_id)::bigint AS orders_count,
                   sum(total_order_value)::numeric AS total_revenue
            FROM olist_dw.orders
            GROUP BY order_date
            ON CONFLICT (order_date) DO UPDATE
            SET orders_count = EXCLUDED.orders_count, total_revenue = EXCLUDED.total_revenue;
        """))

    print("Carga completa.")

if __name__ == "__main__":
    load_all()
