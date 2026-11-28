
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ============================================================
# Configuration
# ============================================================

PROJECT = "/opt/airflow/project"

POSTGRES_CONN_ID = "food_waste_postgres"
ANALYTICS_SCHEMA = "analytics"


# ============================================================
# Helper Functions
# ============================================================

def check_postgres():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.get_first("SELECT 1;")
    print("PostgreSQL connection successful.")


def verify_tables(prefix, layer_name):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    result = hook.get_first(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
          AND table_name LIKE %s;
        """,
        parameters=(ANALYTICS_SCHEMA, f"{prefix}%"),
    )

    count = result[0]

    if count == 0:
        raise ValueError(
            f"No {layer_name} tables found in "
            f"schema '{ANALYTICS_SCHEMA}'."
        )

    print(
        f"Verified {count} {layer_name} tables "
        f"in schema '{ANALYTICS_SCHEMA}'."
    )


def verify_silver_tables():
    verify_tables("silver_", "Silver")


def verify_gold_tables():
    verify_tables("gold_", "Gold")


def verify_dashboard_tables():
    verify_tables("dashboard_", "Dashboard")


def final_verification():
    print(
        "All Silver, Gold, and Dashboard table "
        "existence checks passed."
    )


# ============================================================
# DAG: Consumer + Verification Pipeline
# ============================================================

with DAG(
    dag_id="food_waste_consumer_pipeline",
    description="Consume Kafka output topics and verify PostgreSQL analytics tables.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["food_waste", "consumer", "postgres", "verification"],
) as dag:

    # 1. Check PostgreSQL
    check_connection = PythonOperator(
        task_id="check_postgres_connection",
        python_callable=check_postgres,
    )

    # 2. Kafka Consumer
    # Assumes the consumer is a batch script that exits after processing.
    kafka_consumer = BashOperator(
        task_id="run_kafka_consumer",
        bash_command=(
            f"cd {PROJECT} && "
            "python kafka/kafka_consumer.py"
        ),
    )

    # 3. Verify Silver tables
    verify_silver = PythonOperator(
        task_id="verify_silver_tables",
        python_callable=verify_silver_tables,
    )

    # 4. Verify Gold tables
    verify_gold = PythonOperator(
        task_id="verify_gold_tables",
        python_callable=verify_gold_tables,
    )

    # 5. Verify Dashboard tables
    verify_dashboard = PythonOperator(
        task_id="verify_dashboard_tables",
        python_callable=verify_dashboard_tables,
    )

    # 6. Final verification
    final_check = PythonOperator(
        task_id="final_verification",
        python_callable=final_verification,
    )

    # ========================================================
    # Task Dependencies
    # ========================================================

    check_connection >> kafka_consumer

    kafka_consumer >> [
        verify_silver,
        verify_gold,
        verify_dashboard,
    ] >> final_check
