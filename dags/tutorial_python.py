from datetime import datetime, timedelta

from pprint import pprint
import pendulum

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.bash_operator import BashOperator


default_args = {"owner": "airflow"}

def f1(tzinfo: datetime.tzinfo = None, **context):
    if tzinfo is not None:
        print(("local time:", tzinfo.convert(context["execution_date"])))
    for key in context:
        print((key, context[key]))


def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return "Whatever you return gets printed in the logs"


def f2():
    print("nothing")


DAG_NAME = "tutorial_python"
with DAG(
    DAG_NAME,
    default_args=default_args,
    start_date=datetime(2018, 11, 1),
    schedule_interval="@daily",
) as dag:
    run_this = PythonOperator(
        task_id="print_the_context",
        provide_context=True,
        python_callable=print_context,
        dag=dag,
    )

