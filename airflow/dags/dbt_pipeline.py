from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt"
DBT_CMD = f"cd {DBT_DIR} && dbt"
DBT_FLAGS = f"--profiles-dir {DBT_DIR} --target dev"


def on_failure_callback(context):
    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    execution_date = context["execution_date"]
    log_msg = (
        f"[FAILURE] DAG={dag_id} | Task={task_id} | "
        f"ExecutionDate={execution_date}\n"
    )
    log_path = "/opt/airflow/logs/dbt_failure.log"
    with open(log_path, "a") as f:
        f.write(log_msg)
    print(log_msg)


default_args = {
    "owner": "marwan",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="dbt_pipeline",
    default_args=default_args,
    description="Daily dbt pipeline: seed → test sources → run stage → test stage → run dev → test dev",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "dataops"],
) as dag:

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT_CMD} seed {DBT_FLAGS}",
    )

    dbt_test_sources = BashOperator(
        task_id="dbt_test_sources",
        bash_command=f'{DBT_CMD} test --select "source:*" {DBT_FLAGS}',
    )

    dbt_run_stage = BashOperator(
        task_id="dbt_run_stage",
        bash_command=f"{DBT_CMD} run --select stage {DBT_FLAGS}",
    )

    dbt_test_stage = BashOperator(
        task_id="dbt_test_stage",
        bash_command=f"{DBT_CMD} test --select stage {DBT_FLAGS}",
    )

    dbt_run_dev = BashOperator(
        task_id="dbt_run_dev",
        bash_command=f"{DBT_CMD} run --select dev {DBT_FLAGS}",
    )

    dbt_test_dev = BashOperator(
        task_id="dbt_test_dev",
        bash_command=f"{DBT_CMD} test --select dev {DBT_FLAGS}",
    )

    dbt_seed >> dbt_test_sources >> dbt_run_stage >> dbt_test_stage >> dbt_run_dev >> dbt_test_dev
