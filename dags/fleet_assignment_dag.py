from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from docker.types import Mount

# DAG
import os
host_data = os.environ.get("DATA_HOST_PATH")


with DAG(
    dag_id="fleet_assignment_pipeline_v3",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_fleet_job = DockerOperator(
    task_id="run_fleet_job",
    image="fleet-optimization-batch-fleet_job:latest",
    force_pull = False,
    api_version="auto",
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    mount_tmp_dir=False,

    mounts=[
        Mount(
            source=host_data,
            target="/app/data",
            type="bind"
        )
    ],
    environment={
         "DATA_PATH": "/app/data",
    },
    
    )   