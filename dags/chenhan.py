from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.kubernetes.secret import Secret
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    "owner": "chenhan.hsiao.tw@gmail.com",
    "depends_on_past": False,
    "start_date": datetime(2017, 6, 1),
    "email": ["chenhan.hsiao.tw@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


def bigquery_etl_query(
    destination_table,
    dataset_id,
    parameters=(),
    arguments=("--use_legacy_sql=false", "--replace", "--max_rows=0"),
    project_id="testwithbilling",
    sql_file_path=None,
    gcp_conn_id="google_cloud_derived_datasets",
    gke_location="us-central1-a",
    gke_cluster_name="bq-load-gke-1",
    gke_namespace="default",
    docker_image="mozilla/bigquery-etl:latest",
    image_pull_policy="Always",
    date_partition_parameter="submission_date",
    **kwargs
):
    kwargs["task_id"] = kwargs.get("task_id", destination_table)
    sql_file_path = sql_file_path or "/app/sql/{}/{}/query.sql".format(
        dataset_id, destination_table
    )
    if destination_table is not None and date_partition_parameter is not None:
        destination_table = destination_table + "\${{ds_nodash}}"
        parameters += (date_partition_parameter + ":DATE:{{ds}}",)

    arguments = (
        ["query"]
        + (["--destination_table=" + destination_table] if destination_table else [])
        + ["--dataset_id=" + dataset_id]
        + (["--project_id=" + project_id] if project_id else [])
        + ["--parameter=" + parameter for parameter in parameters]
        + list(arguments)
        + ["<", sql_file_path]
    )
    return BashOperator(bash_command=(" ".join(["bq"] + arguments)), **kwargs)


with DAG(
    "chenhan", catchup=True, default_args=default_args, schedule_interval="30 23 * * *"
) as dag:

    gcp_conn_id = "google_cloud_chenhan"
    dataset_id = "mango_staging"
    docker_image = "chenhan/bigquery-etl:mango"

    tables = [
        "mango_cohort_retained_users",
        "mango_events",
        "mango_core",
        "mango_core_normalized",
        "mango_feature_active_user_count",
        "mango_feature_cohort_date",
        "mango_feature_roi",
        "mango_user_rfe_28d",
        "mango_user_rfe_daily_partial",
        "mango_user_rfe_daily_session",
        "mango_events_unnested",
        "mango_events_feature_mapping",
    ]
    no_partition_tables = [
        "mango_channel_mapping",
    ]
    dummy_tables = [
        #
        "mango_cohort_user_occurrence",
        "google_rps",
        "mango_user_channels",
        "mango_user_feature_occurrence",
    ]

    tasks = [
        bigquery_etl_query(
            gcp_conn_id=gcp_conn_id,
            docker_image=docker_image,
            destination_table=t,
            dataset_id=dataset_id,
            dag=dag,
        )
        for t in tables
    ]
    no_partition_tasks = [
        bigquery_etl_query(
            gcp_conn_id=gcp_conn_id,
            docker_image=docker_image,
            destination_table=t,
            dataset_id=dataset_id,
            date_partition_parameter=None,
            dag=dag,
        )
        for t in no_partition_tables
    ]
    dummy = [DummyOperator(task_id=t, dag=dag,) for t in dummy_tables]

