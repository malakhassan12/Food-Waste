
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


def verify_gold_tables():
    hook = PostgresHook(postgres_conn_id="food_waste_postgres")

    result = hook.get_first("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name LIKE 'gold_%';
    """)

    count = result[0]

    if count == 0:
        raise ValueError("No Gold tables found in PostgreSQL.")

    print(f"Verified {count} Gold tables in PostgreSQL.")


with DAG(
    dag_id="food_waste_batch_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["food_waste", "batch", "spark"],
) as dag:

    check_postgres = PythonOperator(
        task_id="check_postgres_connection",
        python_callable=lambda: PostgresHook(
            postgres_conn_id="food_waste_postgres"
        ).get_first("SELECT 1;"),
    )

    run_spark_batch = BashOperator(
        task_id="run_spark_batch",
        bash_command="""
        docker exec spark-master sh -lc '
        mkdir -p /tmp/.ivy2 &&
        WRITE_TO_POSTGRES=1 /opt/spark/bin/spark-submit \
          --conf spark.jars.ivy=/tmp/.ivy2 \
          --packages org.postgresql:postgresql:42.7.5 \
          --master spark://spark-master:7077 \
          /opt/spark/jobs/food_waste_processing_main.py
        '
        """,
    )

    verify_gold = PythonOperator(
        task_id="verify_gold_tables",
        python_callable=verify_gold_tables,
    )

    check_postgres >> run_spark_batch >> verify_gold
