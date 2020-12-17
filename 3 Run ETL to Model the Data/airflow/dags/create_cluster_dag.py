from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import CreateRedshiftClusterOperator


default_args = {
    'owner': 'DavidH',
    'start_date': datetime(2020, 11, 18),
    'depends_on_past': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email': ['email@spfy.com'],
    'retries': 3
}

dag = DAG('Create_cluster_task',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@yearly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_create_redshift_cluster = CreateRedshiftClusterOperator(
                     task_id="Stage_create_cluster",
                     conn_id="redshift",
                     aws_credentials="aws_credentials",
                     dag=dag
)

stage_wait_redshift_cluster_creation = BashOperator(
    task_id="Stage_wait_cluster_creation",
    bash_command='sleep 300',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_create_redshift_cluster >>stage_wait_redshift_cluster_creation >> end_operator