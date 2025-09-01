# analysis/analysis_notebook.py
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
transformed_dir = BASE / "data" / "transformed"

orders = pd.read_parquet(transformed_dir / "orders.parquet")
items = pd.read_parquet(transformed_dir / "order_items.parquet")
products = pd.read_parquet(transformed_dir / "products.parquet")

# 1) Productos más vendidos
top_products = items.groupby("product_id").agg(
    units_sold=("order_id","count"),
    total_revenue=("price","sum")
).reset_index().sort_values("units_sold", ascending=False)

top5 = top_products.head(10)
plt.figure(figsize=(10,6))
plt.barh(top5["product_id"].astype(str)[::-1], top5["units_sold"][::-1])
plt.xlabel("Unidades vendidas")
plt.title("Top 10 productos por unidades vendidas")
plt.tight_layout()
plt.savefig(BASE / "analysis" / "top10_products.png")
plt.close()

# 2) Pedidos por día de la semana
orders['weekday'] = orders['order_purchase_timestamp'].dt.day_name()
orders_by_weekday = orders.groupby('weekday').size().reindex(
    ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
)

plt.figure(figsize=(8,5))
orders_by_weekday.plot(kind='bar')
plt.ylabel("Número de pedidos")
plt.title("Pedidos por día de la semana")
plt.tight_layout()
plt.savefig(BASE / "analysis" / "orders_by_weekday.png")
plt.close()

print("Gráficos guardados en analysis/")
