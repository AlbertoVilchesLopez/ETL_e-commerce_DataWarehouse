# ETL
## Pipeline ETL para análisis de e-commerce con Data Warehouse



## Estructura del Proyecto

```
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
```





# Resumen rápido:

Este proyecto consiste en diseñar y ejecutar un pipeline ETL (Extract, Transform, Load) aplicado a un dataset de e-commerce (Olist Store de Kaggle). El objetivo principal es extraer información de pedidos, productos y clientes, transformarla mediante procesos de limpieza y unificación de datos, y cargarla en un sistema de almacenamiento estructurado, como PostgreSQL. De esta manera se construye una base sólida para realizar análisis posteriores que permitan responder preguntas clave de negocio, como identificar los productos más vendidos o los días con mayor volumen de pedidos.

Para garantizar un entorno reproducible y estable, se utiliza Docker para desplegar PostgreSQL de forma aislada y portable, evitando problemas de configuración en diferentes sistemas. El pipeline se desarrolla en Python con librerías como Pandas y SQLAlchemy, que permiten manejar grandes volúmenes de datos y conectarse a la base de datos de manera sencilla. Además, se contempla la posibilidad de orquestar el flujo de trabajo con Apache Airflow, lo que facilita la automatización y la programación de tareas ETL periódicas.

En la fase final, los datos transformados y centralizados en PostgreSQL sirven como base para generar análisis y visualizaciones que aportan valor a la toma de decisiones en e-commerce. Esto convierte al proyecto en un ejercicio completo de ingeniería de datos, abarcando desde la ingestión de datos crudos hasta la preparación de información lista para insights de negocio.










