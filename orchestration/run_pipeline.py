from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

from utils.config.config_loader import get_project_root_path, get_config

with DAG(
    "dh_pipeline",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "retries": 10,
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
    schedule=timedelta(days=10),
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    """

    IMPORTANT

    While the pipeline holds the true database, we can only use one database instance at a time.

    """

    WORKING_DIR = get_project_root_path() / get_config()["orchestration_path"]

    """ Configure Tasks """

    """ Arxiv """

    # extraction_arxiv_q0 = BashOperator(
    #     task_id="run_extractor_arxiv_q0",
    #     bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_extractor.sh arxiv 0",
    #     execution_timeout=timedelta(days=3),
    # )
    # extraction_arxiv_q0.doc_md = "Extraction runner for arxiv query_id=0"
    #
    # loader_arxiv_q0 = BashOperator(
    #     task_id="run_loader_arxiv_q0",
    #     bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_loader.sh arxiv 0",
    #     execution_timeout=timedelta(days=3),
    # )
    # loader_arxiv_q0.doc_md = "Loading runner for arxiv query_id=0"

    """ Cordis """

    # extraction_cordis_q0 = BashOperator(
    #     task_id="run_extractor_cordis_q0",
    #     bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_extractor.sh cordis 0",
    #     execution_timeout=timedelta(days=3),
    # )
    # extraction_cordis_q0.doc_md = "Extraction runner for cordis query_id=0"

    loader_cordis_q0 = BashOperator(
        task_id="run_loader_cordis_q0",
        bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_loader.sh cordis 0",
        execution_timeout=timedelta(days=3),
    )
    loader_cordis_q0.doc_md = "Loading runner for cordis query_id=0"

    """ CoreAc """

    # extraction_coreac_q0 = BashOperator(
    #     task_id="run_extractor_coreac_q0",
    #     bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_extractor.sh coreac 0",
    #     execution_timeout=timedelta(days=3),
    # )
    # extraction_coreac_q0.doc_md = "Extraction runner for coreac query_id=0"
    
    # loader_coreac_q0 = BashOperator(
    #     task_id="run_loader_coreac_q0",
    #     bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_loader.sh coreac 0",
    #     execution_timeout=timedelta(days=3),
    # )
    # loader_coreac_q0.doc_md = "Loading runner for coreac query_id=0"

    """ OpenAIRE """

    # extraction_openaire_q0 = BashOperator(
    #     task_id="run_extractor_openaire_q0",
    #     bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_extractor.sh openaire 0",
    #     execution_timeout=timedelta(days=3),
    # )
    # extraction_openaire_q0.doc_md = "Extraction runner for openaire query_id=0"

    # loader_openaire_q0 = BashOperator(
    #     task_id="run_loader_openaire_q0",
    #     bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_loader.sh openaire 0",
    #     execution_timeout=timedelta(days=3),
    # )
    # loader_openaire_q0.doc_md = "Loading runner for openaire query_id=0"

    # ---

    # extraction_openaire_q1 = BashOperator(
    #     task_id="run_extractor_openaire_q1",
    #     bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_extractor.sh openaire 1",
    #     execution_timeout=timedelta(days=3),
    # )
    # extraction_openaire_q1.doc_md = "Extraction runner for openaire query_id=1"

    # loader_openaire_q1 = BashOperator(
    #     task_id="run_loader_openaire_q1",
    #     bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_loader.sh openaire 1",
    #     execution_timeout=timedelta(days=3),
    # )
    # loader_openaire_q1.doc_md = "Loading runner for openaire query_id=1"

    # ---

    # extraction_openaire_q2 = BashOperator(
    #     task_id="run_extractor_openaire_q2",
    #     bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_extractor.sh openaire 2",
    #     execution_timeout=timedelta(days=3),
    # )
    # extraction_openaire_q2.doc_md = "Extraction runner for openaire query_id=2"

    # loader_openaire_q2 = BashOperator(
    #     task_id="run_loader_openaire_q2",
    #     bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_loader.sh openaire 2",
    #     execution_timeout=timedelta(days=3),
    # )
    # loader_openaire_q2.doc_md = "Loading runner for openaire query_id=2"

    # ---

    # extraction_openaire_q3 = BashOperator(
    #     task_id="run_extractor_openaire_q3",
    #     bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_extractor.sh openaire 3",
    #     execution_timeout=timedelta(days=3),
    # )
    # extraction_openaire_q3.doc_md = "Extraction runner for openaire query_id=3"

    # loader_openaire_q3 = BashOperator(
    #     task_id="run_loader_openaire_q3",
    #     bash_command=f"sbatch --wait {WORKING_DIR}/sbatch/sbatch_loader.sh openaire 3",
    #     execution_timeout=timedelta(days=3),
    # )
    # loader_openaire_q3.doc_md = "Loading runner for openaire query_id=3"

    """ Task Dependencies """

    # Loaders depend on their extractors
    # extraction_arxiv_q0 >> loader_arxiv_q0
    # extraction_coreac_q0 >> loader_coreac_q0

    # Transformations wait for all loaders and run sequentially
    # [loader_arxiv_q0, loader_coreac_q0] >> transformation_arxiv
    # transformation_arxiv >> transformation_coreac

    (
        # extraction_openaire_q0
        # >> extraction_openaire_q1
        # >> extraction_openaire_q2
        # >> extraction_openaire_q3
        # >> extraction_cordis_q0
        # loader_openaire_q0
        # >> loader_openaire_q1
        # >> loader_openaire_q2
        # >> loader_openaire_q3
        # >> 
        loader_cordis_q0
    )
