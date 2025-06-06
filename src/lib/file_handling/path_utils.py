import os
from pathlib import Path

from utils.config.config_loader import (
    get_query_config,
    get_config,
    get_project_root_path,
)


def get_source_data_path(source_name: str, query_id: int | None) -> Path:
    """
    Returns the path to the raw source data.
    Source data is saved under /data/pile/{source_name}-{query_id}
    In dev mode source data is saved in project dir.
    In prod mode source data is saved on another place on disk, see config.
    """
    assert get_query_config().get(source_name), f"Faulty source name: {source_name}"
    # test query_id in query_config if not None

    config = get_config()
    path = (
        Path(config["data_path"]) / f"{source_name}-query_id-{query_id}"
        if query_id  # for data without queries, e.g. meta_heritage
        else Path(config["data_path"]) / source_name
    )

    if os.getenv("ENV") == "dev":
        return get_project_root_path() / path
    return path
