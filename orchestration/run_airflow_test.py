from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

from utils.config.config_loader import get_project_root_path, get_config

# from utils.config.config_loader import get_config, get_project_root_path

with DAG(
    "airflow_test",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=60),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="Airflow Test DAG",
    # schedule=timedelta(days=30),
    # start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    WORKING_DIR = get_project_root_path() / get_config()["orchestration_path"]

    """ Configure Tasks """

    """ Test """

    # test_task_dev = BashOperator(
    #     task_id="run_test",
    #     bash_command=f"echo 'Working dir: {WORKING_DIR}' && ls -la {WORKING_DIR}/sbatch/",
    #     execution_timeout=timedelta(hours=1),
    # )

    # test_task_prod = BashOperator(
    #     task_id="run_test",
    #     bash_command=f"echo 'Working dir: {WORKING_DIR}' && ls -la {WORKING_DIR}/sbatch/ && which sbatch",
    #     execution_timeout=timedelta(hours=1),
    # )

    # Add this task to your DAG
    env_test = BashOperator(
        task_id="test_environment",
        bash_command=f"""
        echo "Current directory: $(pwd)" &&
        echo "Working dir: {WORKING_DIR}" &&
        cd {WORKING_DIR}/.. &&
        echo "After cd: $(pwd)" &&
        source venv/bin/activate &&
        echo "Python version: $(python --version)" &&
        echo "Python path: $(which python)" &&
        python -c "print('Python is working')" &&
        python src/elt/extraction/run_extractor.py --source arxiv --query_id 0;
        """,
        execution_timeout=timedelta(minutes=5),
    )
