from datetime import datetime
from airflow import DAG
from airflow.operators.papermill_operator import PapermillOperator
from airflow.models import Variable

path = Variable.get("DATA_PATH")

date_now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
with DAG(
    'ETL_news_dag',
    description='Pipeline,etl,news',
    schedule_interval='3 * * *',
    start_date=datetime(2023, 8, 15),
    catchup=False
) as dag:
    news_etl_task = PapermillOperator(
        task_id='news_task_id',
        input_nb=f"{path}extractNewsApi.ipynb",
        output_nb=f"{path}newsOutput-{date_now}.ipynb",
        dag=dag
    )
    news_etl_task