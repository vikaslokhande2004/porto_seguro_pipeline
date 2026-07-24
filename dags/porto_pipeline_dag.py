from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/mnt/d/porto_seguro_pipeline"


with DAG(
    dag_id="porto_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["porto", "insurance", "data-pipeline"],
) as dag:

    # ------------------------------------------
    # Spark Bronze → Silver
    # ------------------------------------------
    task_spark_silver = BashOperator(
        task_id="spark_silver",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m src.transform.spark_porto"
        ),
    )

    # ------------------------------------------
    # dbt Run + Test
    # ------------------------------------------
    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {PROJECT_DIR}/sql/dbt && "
            "dbt run && "
            "dbt test"
        ),
    )

    # ------------------------------------------
    # Pipeline dependency
    # ------------------------------------------
    task_spark_silver >> task_dbt_test