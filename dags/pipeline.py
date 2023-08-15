from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_world():
    print("Hello, World!")

dag = DAG(
    'my_first_dag',
    description='Mon premier DAG avec Airflow',
    schedule_interval='0 0 * * *',
    start_date=datetime(2023, 8, 15),
    catchup=False
)

task_hello = PythonOperator(
    task_id='hello_task',
    python_callable=hello_world,
    dag=dag
)