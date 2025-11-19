import yaml
import json
import os

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from pathlib import Path
from kubernetes.client import models as k8s

DAG_DIR = Path(__file__).parent
CONFIG_DIR = "configs"
SOURCES_FILE_NAME = "zvirt_retro_config.yaml"
SOURCE_CONFIG_FILE_PATH = DAG_DIR / CONFIG_DIR / SOURCES_FILE_NAME


default_args = {
    'retries': 0,
}


with DAG(
        'retro_test_1',
        default_args=default_args,
        description='Забор ретро данных из Victoria Metrics для Zvirt',
        schedule_interval=None,
        catchup=False,
        start_date=days_ago(7),
        max_active_tasks=1,
        max_active_runs=1,
        tags=['victoria_metrics', 'etl', 'retro'],
    ) as dag:

    start_task = DummyOperator(task_id="start")
    finish_task = DummyOperator(task_id="finish")

    if SOURCE_CONFIG_FILE_PATH.exists():
        with open(SOURCE_CONFIG_FILE_PATH, "r") as config_file:
            sources_config = yaml.safe_load(config_file)


    source_tasks = []
    for source_name, source_config in sources_config.items():
        with TaskGroup(group_id=f"exctract_{source_name}_data") as source_task_group:

            pyspark_task = KubernetesPodOperator(
                    task_id='pyspark_task',
                    name='airflow-spark-job',
                    image=os.getenv("SPARK_IMAGE"),
                    image_pull_secrets=[
                        k8s.V1LocalObjectReference("cloud-registry")
                    ],
                    cmds=["spark-submit"],
                    arguments=[
                        "--master", "local[*]",
                        "--name", "test-spark-job",
                        "/opt/spark/scripts/zvirt_retro.py"
                    ],
                    env_vars={
                        'EXECUTION_DATE': '{{ ds }}',
                        'source': source_name,
                        'metrics': json.dumps(source_config['metrics']),
                        'jobs': json.dumps(source_config['jobs']),
                        'step': source_config['step'],
                        'hours': str(source_config['hours']),
                        'start_date': "2025-11-15",
                        'end_date': "2025-11-22",
                    },
                    env_from=[
                        k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource("airflow-dag-clickhouse-connection")),
                        k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource("airflow-dag-victoriametrics-connection"))
                    ],
                    container_resources=k8s.V1ResourceRequirements(
                        requests={"cpu": "200m", "memory": "1024Mi"},
                        limits={"cpu": "2000m", "memory": "2048Mi"}
                    ),
                    is_delete_operator_pod=True,
                    hostnetwork=False,
                    startup_timeout_seconds=300,
                    get_logs=True,
                )
            source_tasks.append(source_task_group)

    start_task >> source_tasks >> finish_task
