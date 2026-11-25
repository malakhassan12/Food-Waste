
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


PROJECT = "/opt/airflow/project"
NOTEBOOKS = f"{PROJECT}/notebooks"


def check_postgres():
    hook = PostgresHook(postgres_conn_id="food_waste_postgres")
    hook.get_first("SELECT 1;")


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


with DAG(
    dag_id="food_waste_batch_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["food_waste", "batch", "spark"],
) as dag:

    check_connection = PythonOperator(
        task_id="check_postgres_connection",
        python_callable=check_postgres,
    )

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

    database_exploration = BashOperator(
        task_id="run_03_database_exploration",
        bash_command=run_notebook("03_database_exploration.ipynb"),
    )

    sqlalchemy_crud = BashOperator(
        task_id="run_04_sqlalchemy_crud",
        bash_command=run_notebook("04_sqlalchemy_crud.ipynb"),
    )

    setup_database = BashOperator(
        task_id="setup_database",
        bash_command=(
            f"cd {PROJECT} && "
            "python db/config/setup_database.py"
        ),
    )

    food_waste_analysis = BashOperator(
        task_id="run_05_food_waste_analysis",
        bash_command=run_notebook("05_food_waste_analysis.ipynb"),
    )

    run_spark = BashOperator(
        task_id="run_spark_batch",
        bash_command="""
        docker exec spark-master sh -lc '
        set -e
        mkdir -p /tmp/.ivy2
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

    (
    check_connection
    >> installations
    >> data_exploration
    >> data_cleaning
    >> setup_database
    >> database_exploration
    >> sqlalchemy_crud
    >> food_waste_analysis
    >> run_spark
    >> verify_gold
)
