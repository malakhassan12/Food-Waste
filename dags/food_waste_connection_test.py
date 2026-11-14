
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def test_food_waste_connection():
    hook = PostgresHook(
        postgres_conn_id="food_waste_postgres"
    )

    result = hook.get_first("""
        SELECT
            current_database(),
            COUNT(*)
        FROM food_waste.observation;
    """)

    print(f"Database: {result[0]}")
    print(f"Observation records: {result[1]}")


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
