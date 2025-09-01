# ETL
## Pipeline ETL para análisis de e-commerce
´´´
etl-olist/
├─ data/                       # coloca aquí los CSV (olist_orders_dataset.csv, olist_items_dataset.csv, olist_products_dataset.csv)
├─ scripts/
│  ├─ 01_extract_transform.py
│  ├─ 02_load_postgres.py
│  └─ helpers.py
├─ airflow/
│  └─ dags/
│     └─ etl_olist_dag.py
├─ sql/
│  └─ warehouse_schema.sql
├─ analysis/
│  └─ analysis_notebook.py   # script o notebook para análisis y visualizaciones
├─ requirements.txt
├─ docker-compose.yml        # (opcional) Postgres + Airflow skeleton
└─ README.md
´´´



Resumen rápido:

Coloca los CSVs en data/.

Crea un entorno virtual e instala dependencias:

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt


Configura .env con credenciales de Postgres.

Levanta Postgres (localmente o con docker-compose).

Ejecuta ETL localmente:

python scripts/01_extract_transform.py
python scripts/02_load_postgres.py


(Opcional) Ejecuta Airflow y activa el DAG etl_olist_pipeline.

Ejecuta análisis:

python analysis/analysis_notebook.py



Los gráficos se guardarán en analysis/.





