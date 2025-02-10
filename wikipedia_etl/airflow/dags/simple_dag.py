from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'lshunak',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define a simple function to be executed by the PythonOperator
def print_hello():
    print("Hello, Airflow!")

# Define the DAG
with DAG(
    'simple_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Define the tasks
    start = DummyOperator(
        task_id='start'
    )

    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello
    )

    end = DummyOperator(
        task_id='end'
    )

    # Set the task dependencies
    start >> hello_task >> end