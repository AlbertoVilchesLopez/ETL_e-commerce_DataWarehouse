# scripts/01_extract_transform.py
import os
import pandas as pd
from pathlib import Path
from helpers import read_csv_safe, parse_dates, basic_clean

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "transformed"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def load_raw():
    orders = read_csv_safe(DATA_DIR / "olist_orders_dataset.csv")
    items = read_csv_safe(DATA_DIR / "olist_items_dataset.csv")
    products = read_csv_safe(DATA_DIR / "olist_products_dataset.csv")
    return orders, items, products

def transform(orders, items, products):
    # Basic cleaning
    orders = basic_clean(orders)
    items = basic_clean(items)
    products = basic_clean(products)

    # Parse date columns in orders
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    orders = parse_dates(orders, date_cols)

    # Convert numeric columns in items
    for col in ["price", "freight_value"]:
        if col in items.columns:
            items[col] = pd.to_numeric(items[col], errors='coerce')

    # Calculate total order value by grouping items
    order_values = items.groupby("order_id", as_index=False).agg(
        total_order_value=pd.NamedAgg(column="price", aggfunc="sum")
    )

    # Merge order total into orders
    orders = orders.merge(order_values, on="order_id", how="left")
    orders["total_order_value"] = orders["total_order_value"].fillna(0.0)

    # Standardize product columns (ej: product_category_name may contener NaN)
    if "product_category_name" in products.columns:
        products["product_category_name"] = products["product_category_name"].fillna("unknown")

    # Join items with products for enriched order_items
    items_enriched = items.merge(
        products[["product_id", "product_category_name"]],
        on="product_id",
        how="left"
    )

    return orders, items_enriched, products

def save_transformed(orders, items, products):
    orders.to_parquet(OUTPUT_DIR / "orders.parquet", index=False)
    items.to_parquet(OUTPUT_DIR / "order_items.parquet", index=False)
    products.to_parquet(OUTPUT_DIR / "products.parquet", index=False)

def main():
    print("Cargando CSVs...")
    orders, items, products = load_raw()
    print("Transformando...")
    orders_t, items_t, products_t = transform(orders, items, products)
    print("Guardando transformados en /data/transformed ...")
    save_transformed(orders_t, items_t, products_t)
    # También imprimimos unas agregaciones rápidas
    top_products = items_t.groupby("product_id").agg(units_sold=("order_id","count"), total_revenue=("price","sum")).reset_index()
    print("Top 5 productos por unidades vendidas:")
    print(top_products.sort_values("units_sold", ascending=False).head(5))

if __name__ == "__main__":
    main()
