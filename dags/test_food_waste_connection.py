
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def test_food_waste_connection():
    hook = PostgresHook(postgres_conn_id="food_waste_postgres")

    rows = hook.get_records("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
        AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name;
    """)

    for row in rows:
        print(row)

with DAG(
    dag_id="food_waste_connection_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["food_waste", "database"],
) as dag:

    test_connection = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_food_waste_connection,
    )
