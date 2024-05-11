import logging
from os import path

import pendulum
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow_config_management.config_manager import ConfigManager

log = logging.getLogger(__name__)

config = ConfigManager(
    path.join(path.abspath(path.dirname(__file__)), "settings", "config.yml")
)

default_args = {
    "owner": config.read("dag.owner"),
    "start_date": pendulum.datetime(2014, 1, 1, tz="Europe/Amsterdam"),
    "retries": 3,
}

with DAG(
    dag_id="process_reviews_and_metadata",
    default_args=default_args,
    schedule_interval="0 * * * *",  # run every hour on the dot
    catchup=True,
) as dag:
    common = {
        "namespace": config.read("cluster_namespace"),
        "image": "489198589229.dkr.ecr.eu-north-1.amazonaws.com/takeaway-challenge:live",
        "image_pull_policy": "Always",
        "env_vars": {
            "S3_ACCESS_KEY_ID": config.read("vault_secret_s3_key"),
            "S3_ACCESS_KEY_SECRET": config.read("vault_secret_s3_secret"),
            "API_BEARER_TOKEN": config.read("vault_secret_bearer_token"),
        },
        "labels": {
            "app": "airflow-community",
            "component": "operator",
            "tribe": config.read("tribe"),
            "squad": config.read("squad"),
        },
        "in_cluster": True,
        "get_logs": True,
    }

    # create dummmy task for the run start
    start_operator = DummyOperator(task_id="start_task")

    # and then actual tasks
    process_reviews_data = KubernetesPodOperator(
        **common,
        name="process_reviews_data",
        arguments=[
            "python",
            "src/entrypoint.py",
            "--task",
            "process_raw_reviews_data_with_timestamps",
            "--start_timestamp",
            "{{ data_interval_start }}",
            "--end_timestamp",
            "{{ data_interval_end }}",
        ],
        task_id="process_reviews_data"
    )

    process_metadata = KubernetesPodOperator(
        **common,
        name="process_metadata",
        arguments=[
            "python",
            "src/entrypoint.py",
            "--task",
            "process_raw_metadata_with_timestamps",
            "--start_timestamp",
            "{{ data_interval_start }}",
            "--end_timestamp",
            "{{ data_interval_end }}",
        ],
        task_id="process_metadata"
    )

    start_operator >> [process_reviews_data, process_metadata]
