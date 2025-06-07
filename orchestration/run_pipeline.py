import textwrap
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.providers.standard.operators.bash import BashOperator

from utils.config.config_loader import get_config, get_project_root_path

with DAG(
    "dh_pipeline",
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
    description="DAG of the entire pipeline",
    schedule=timedelta(days=30),
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    WORKING_DIR = get_project_root_path() / get_config()["orchestration_path"]

    """ Configure Tasks """

    """ Arxiv """

    extraction_arxiv_q0 = BashOperator(
        task_id="run_extractor_arxiv_q0",
        bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_extractor.sh arxiv 0",
        execution_timeout=timedelta(days=6),
    ).doc_md = "Extraction runner for arxiv query_id=0"

    loader_arxiv_q0 = BashOperator(
        task_id="run_loader_arxiv_q0",
        bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_loader.sh arxiv 0",
        execution_timeout=timedelta(days=6),
    ).doc_md = "Loading runner for arxiv query_id=0"

    """ CoreAc """

    extraction_coreac_q0 = BashOperator(
        task_id="run_extractor_coreac_q0",
        bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_extractor.sh coreac 0",
        execution_timeout=timedelta(days=6),
    ).doc_md = "Extraction runner for coreac query_id=0"

    loader_coreac_q0 = BashOperator(
        task_id="run_loader_coreac_q0",
        bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_loader.sh coreac 0",
        execution_timeout=timedelta(days=6),
    ).doc_md = "Loading runner for coreac query_id=0"

    """ Task Dependencies """

    # Loaders depend on their extractors
    extraction_arxiv_q0 >> loader_arxiv_q0
    extraction_coreac_q0 >> loader_coreac_q0
    
    # Transformations wait for all loaders and run sequentially
    # [loader_arxiv_q0, loader_coreac_q0] >> transformation_arxiv
    # transformation_arxiv >> transformation_coreac
