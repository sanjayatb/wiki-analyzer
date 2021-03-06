from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'description': 'Wikipedia assistant',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 28),
    'email': ['arstbandara@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'Wiki_Analyzer',
    default_args=default_args,
    schedule_interval=timedelta(days=20))

def validate_connections():
    print("Connections validation code goes here")

validate_connectivity = PythonOperator(
    task_id='validate_connectivity',
    provide_context=True,
    python_callable=validate_connections,
    dag=dag,
)

extract_wiki_data = BashOperator(
    task_id='extract_wiki_data',
    bash_command='docker run --network=host -v $(pwd):/job godatadriven/pyspark --name "Wiki Extractor" --master "local[1]"  --conf "spark.ui.showConsoleProgress=True"  --jars /job/libs/*  --conf spark.cassandra.connection.host=localhost  /job/code/spark_reader_server.py',
    dag=dag,
)

validate_connectivity >> extract_wiki_data