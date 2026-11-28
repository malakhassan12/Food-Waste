
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ============================================================
# Configuration
# ============================================================

PROJECT = "/opt/airflow/project"
NOTEBOOKS = f"{PROJECT}/notebooks"

POSTGRES_CONN_ID = "food_waste_postgres"


# ============================================================
# Helper Functions
# ============================================================

def check_postgres():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.get_first("SELECT 1;")
    print("PostgreSQL connection successful.")


def run_notebook(filename):
    return f"""
    set -e
    mkdir -p /tmp/airflow_notebooks
    cd {NOTEBOOKS}

    jupyter nbconvert \
      --to notebook \
      --execute \
      --ExecutePreprocessor.timeout=-1 \
      --output-dir=/tmp/airflow_notebooks \
      {filename}
    """


# ============================================================
# DAG: Producer + Spark Pipeline
# ============================================================

with DAG(
    dag_id="food_waste_producer_pipeline",
    description="Prepare database, publish raw data to Kafka, and run Spark processing.",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["food_waste", "producer", "spark"],
      schedule="@once",
) as dag:

    # 1. Check PostgreSQL
    check_connection = PythonOperator(
        task_id="check_postgres_connection",
        python_callable=check_postgres,
    )

    # 2. Initial notebooks
    installations = BashOperator(
        task_id="run_00_installations",
        bash_command=run_notebook("00_installations.ipynb"),
    )

    data_exploration = BashOperator(
        task_id="run_01_data_exploration",
        bash_command=run_notebook("01_data_exploration.ipynb"),
    )

    data_cleaning = BashOperator(
        task_id="run_02_data_cleaning",
        bash_command=run_notebook("02_data_cleaning.ipynb"),
    )

    # 3. Database setup
    setup_database = BashOperator(
        task_id="setup_database",
        bash_command=(
            f"cd {PROJECT} && "
            "python db/config/setup_database.py"
        ),
    )

    database_exploration = BashOperator(
        task_id="run_03_database_exploration",
        bash_command=run_notebook("03_database_exploration.ipynb"),
    )

    sqlalchemy_crud = BashOperator(
        task_id="run_04_sqlalchemy_crud",
        bash_command=run_notebook("04_sqlalchemy_crud.ipynb"),
    )

    # 4. Kafka Producer
    kafka_producer = BashOperator(
        task_id="run_kafka_producer",
        bash_command=(
            f"cd {PROJECT} && "
            "python kafka/kafka_producer.py"
        ),
    )

    # 5. Notebook 05: Lookup tables and SQL analysis
    food_waste_analysis = BashOperator(
        task_id="run_05_food_waste_analysis",
        bash_command=run_notebook("05_food_waste_analysis.ipynb"),
    )

    # 6. Spark: Kafka RAW -> Silver / Gold / Dashboard Kafka topics
    run_spark = BashOperator(
        task_id="run_spark_batch",
        bash_command="""
        docker exec spark-master sh -lc '
        set -e
        mkdir -p /tmp/.ivy2

        WRITE_TO_KAFKA=1 WRITE_TO_POSTGRES=0 \
        /opt/spark/bin/spark-submit \
          --conf spark.jars.ivy=/tmp/.ivy2 \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.postgresql:postgresql:42.7.5 \
          --master spark://spark-master:7077 \
          /opt/spark/jobs/food_waste_processing_main.py
        '
        """,
    )

    # 7. Trigger Consumer DAG after successful Spark execution
    trigger_consumer = TriggerDagRunOperator(
        task_id="trigger_consumer_pipeline",
        trigger_dag_id="food_waste_consumer_pipeline",
        wait_for_completion=False,
    )

    # ========================================================
    # Task Dependencies
    # ========================================================

    (
        check_connection
        >> installations
        >> data_exploration
        >> data_cleaning
        >> setup_database
        >> database_exploration
        >> sqlalchemy_crud
        >> kafka_producer
        >> food_waste_analysis
        >> run_spark
        >> trigger_consumer
    )
