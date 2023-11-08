import pendulum

from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
#import create_tables.sql

default_args = {
    'owner': 'Sparkify',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}


@dag(
    default_args=default_args,
    description='Create tables on RedShift with Airflow',
    start_date=pendulum.now()
)
def create_tables():
    start_operator = DummyOperator(task_id='Begin_Execution')

    create_redshift_tables = PostgresOperator(
        task_id='Create_tables',
        postgres_conn_id='redshift',
        sql='create_tables.sql'
    )

    end_operator = DummyOperator(task_id='End_Execution')

    start_operator >> create_redshift_tables >> end_operator


create_tables_dag = create_tables()




