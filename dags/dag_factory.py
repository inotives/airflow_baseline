from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.utils import dates
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_TRIGGER_RULE = "none_failed"
DEFAULT_RETRIES = 1

def create_dag(name, schedule=None, descr=None, args=None):
    default_args = {
        "owner": "inotives",
        "start_date": dates.days_ago(2) , # datetime(year=2024, month=1, day=1),
        # If task fails, retry once after waiting for 2 min
        "retries": 1,
        "retry_delay": timedelta(minutes=2)
    }
    args = args if args else default_args

    return DAG(dag_id=name, default_args=args, schedule_interval=schedule, description=descr)


def add_bash_task(
    dag, name, command,
    trigger_rule=DEFAULT_TRIGGER_RULE, retries=DEFAULT_RETRIES):

    return BashOperator(
        task_id=name,
        bash_command=command,
        trigger_rule=trigger_rule,
        retries=retries,
        dag=dag
    )

def add_python_task(
    dag, name, function, kwargs=None, context=True,
    trigger_rule=DEFAULT_TRIGGER_RULE, retries=DEFAULT_RETRIES):

    return PythonOperator(
        task_id=name,
        op_args=kwargs,
        trigger_rule=trigger_rule,
        python_callable=function,
        provide_context=context,
        retries=retries,
        dag=dag
    )

