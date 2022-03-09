import datetime
import os
from dotenv import load_dotenv

from airflow import models, DAG
from airflow.operators import bash_operator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.trigger_rule import TriggerRule

from dags_utils.slack import *

load_dotenv()
bearer_token = os.getenv('BEARER_TOKEN')
header = {"Authorization": "Bearer {}".format(bearer_token)}

default_dag_args = {
        'owner' : 'Raj', 
        'start_date': datetime.datetime(2022,3,9),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay' : datetime.timedelta(minutes=5),
        'on_failure_callback': alert_slack_channel
}

dag = DAG(
        dag_id = 'Twitter_ETL_pipeline',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args
)

Is_twitter_api_available = HttpSensor(
        task_id='is_twitter_api_available',
        http_conn_id='twitter_user_api',
        method='GET',
        endpoint='2/tweets/search/all',
        request_params={"query":["#ChargeNow"]},
        headers={"Authorization": "Bearer {}".format(bearer_token)},
        dag=dag
)

run_extraction = bash_operator.BashOperator(
        task_id='extract_tweets',
        bash_command='python3 /home/prudhvirajstark/Documents/Repos/DCS_Data_engineer_task/Extract.py',
        dag=dag
)

run_transformation = bash_operator.BashOperator(
        task_id='transform_tweets',
        bash_command='python3 /home/prudhvirajstark/Documents/Repos/DCS_Data_engineer_task/Transform.py',
        dag=dag
)

slack_end = PythonOperator(
            task_id='Notify_Slack_DAG_success',
            python_callable=slack_dag_end,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            dag=dag
)

Is_twitter_api_available >> run_extraction >> run_transformation  >> slack_end

