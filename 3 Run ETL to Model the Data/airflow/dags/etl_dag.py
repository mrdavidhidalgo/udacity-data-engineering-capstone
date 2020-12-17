from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import (StageToRedshiftOperator, 
                                DataQualityOperator)

from airflow.operators.postgres_operator import PostgresOperator

import sql_tables


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

dag = DAG('Load_accidents_data_task',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

drop_tables_task = PostgresOperator(
    task_id='drop_tables',
    postgres_conn_id="redshift",
    sql=sql_tables.DROP_TABLES_IF_EXISTS,
    dag=dag
)

create_table_stage_accidents_task = PostgresOperator(
    task_id='create_stage_accidents_table',
    postgres_conn_id="redshift",
    sql=sql_tables.CREATE_TABLE_STAGING_ACCIDENTS,
    dag=dag
)

create_table_stage_cities_task = PostgresOperator(
    task_id='create_stage_cities_table',
    postgres_conn_id="redshift",
    sql=sql_tables.CREATE_TABLE_STAGING_CITIES,
    dag=dag
)

stage_accidents_to_redshift_task = StageToRedshiftOperator(
    task_id="Stage_accidents",
    conn_id="redshift",
    aws_credentials="aws_credentials",
    table="staging_accidents",
    s3_bucket='davidhidalgo-udacity',
    s3_key="accidents.csv",
    dag=dag
)

stage_cities_to_redshift_task = StageToRedshiftOperator(
    task_id='Stage_cities',
    conn_id="redshift",
    aws_credentials="aws_credentials",
    table="staging_cities",
    s3_bucket="davidhidalgo-udacity",
    s3_key="cities.csv",
    dag=dag
)

create_table_dim_cities_task = PostgresOperator(
    task_id='create_dim_cities_table',
    postgres_conn_id="redshift",
    sql=sql_tables.CREATE_TABLE_DIM_CITIES,
    dag=dag
)

create_table_dim_date_hour_task = PostgresOperator(
    task_id='create_dim_date_hour_table',
    postgres_conn_id="redshift",
    sql=sql_tables.CREATE_TABLE_DIM_DATE_HOURS,
    dag=dag
)

create_table_fact_accidents_task = PostgresOperator(
    task_id='create_fact_accidents_table',
    postgres_conn_id="redshift",
    sql=sql_tables.CREATE_TABLE_FACT_ACCIDENTS,
    dag=dag
)

load_dim_cities_task = PostgresOperator(
    task_id='load_dim_cities_table',
    postgres_conn_id="redshift",
    sql=sql_tables.INSERT_DIM_CITIES,
    dag=dag
)

load_dim_date_hour_task = PostgresOperator(
    task_id='load_dim_date_hour_table',
    postgres_conn_id="redshift",
    sql=sql_tables.INSERT_DIM_DATE_HOUR,
    dag=dag
)

load_fact_accidents_task = PostgresOperator(
    task_id='load_fact_accidents_table',
    postgres_conn_id="redshift",
    sql=sql_tables.INSERT_FACT_ACCIDNETS,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    conn_id="redshift",
    dq_checks=[{"table":"fact_accidents" , "expected_result":3000000}, {"table":"dim_date_hour" , "expected_result":365}, {"table":"dim_cities" , "expected_result":10000}],
    dag=dag
)

tables_create_task =  DummyOperator(task_id='Tables_created',  dag=dag)

staging_loaded = DummyOperator(task_id='Staging_area_loaded',  dag=dag)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> drop_tables_task 
drop_tables_task >> [create_table_stage_accidents_task, create_table_stage_cities_task, create_table_dim_date_hour_task, create_table_dim_cities_task] >> create_table_fact_accidents_task >> tables_create_task

tables_create_task >> [stage_accidents_to_redshift_task,stage_cities_to_redshift_task] >> staging_loaded

staging_loaded >> [load_dim_cities_task, load_dim_date_hour_task] >> load_fact_accidents_task

load_fact_accidents_task  >> run_quality_checks >> end_operator