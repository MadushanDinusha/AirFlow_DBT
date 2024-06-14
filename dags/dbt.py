from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry':False,
    'retries':0
}


with DAG(
    'dbt_run',
    default_args=default_args,
    description = '',
    schedule_interval =None,
    start_date = days_ago(1),
    tags= [ 'dbt' ]
) as dag:

    dbt_seed = BashOperator(task_id='bdt_seed',
    bash_command='cd /opt/airflow/dbt && dbt seed')