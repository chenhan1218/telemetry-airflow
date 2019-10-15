from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from operators.gcp_container_operator import GKEPodOperator


default_args = {
    "owner": "elin@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2018, 11, 26),
    "email": ["elin@mozilla.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=30),
}


with DAG("taipei_etl", default_args=default_args, schedule_interval=None) as dag:

    gcp_conn_id = "google_cloud_derived_datasets"
    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    bq = GKEPodOperator(
        task_id="list-dataset",
        gcp_conn_id=gcp_conn_id,
        project_id=connection.project_id,
        location="us-central1-a",
        cluster_name="bq-load-gke-1",
        name="tapei-etl-1",
        namespace="default",
        image="mozilla/bigquery-etl",
        arguments=["bq", "ls", "-d"],
    )
    auth = GKEPodOperator(
        task_id="test-auth",
        gcp_conn_id=gcp_conn_id,
        project_id=connection.project_id,
        location="us-central1-a",
        cluster_name="bq-load-gke-1",
        name="tapei-etl-1",
        namespace="default",
        image="mozilla/bigquery-etl",
        cmds=["python"],
        arguments=[
            "-c",
            'from google.cloud import bigquery;client=bigquery.Client();print(client.project);dataset = client.get_dataset("mango_dev");print(dataset.dataset_id)',
        ],
    )
    gcloud = GKEPodOperator(
        task_id="gcloud",
        gcp_conn_id=gcp_conn_id,
        project_id=connection.project_id,
        location="us-central1-a",
        cluster_name="bq-load-gke-1",
        name="tapei-etl-1",
        namespace="default",
        image="mozilla/bigquery-etl",
        cmds=["bash"],
        arguments=["-c", "gcloud config list; gcloud auth list"],
    )
    pod = GKEPodOperator(
        task_id="pod_op",
        gcp_conn_id=gcp_conn_id,
        project_id=connection.project_id,
        location="us-central1-a",
        cluster_name="bq-load-gke-1",
        name="tapei-etl-1",
        namespace="default",
        image="mozilla/bigquery-etl",
        cmds=["echo"],
        arguments=["{{ ds }}"],
    )
