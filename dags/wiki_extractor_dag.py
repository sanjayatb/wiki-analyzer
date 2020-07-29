from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
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
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'Wiki Analyzer',
    default_args=default_args,
    schedule_interval=timedelta(days=20))

reader = DockerOperator(
    task_id='extract_wiki_data',
    image='godatadriven/pyspark',
    api_version='auto',
    auto_remove=True,
    command='godatadriven/pyspark --name "Wiki Extractor" --master "local[1]"  --conf "spark.ui.showConsoleProgress=True"  --jars /job/libs/*  --conf spark.cassandra.connection.host=localhost  /job/code/reader_v3.py',
    volumes=['root/wiki/:/job'],
    docker_url="unix://var/run/docker.sock",
    network_mode="host",
    dag=dag
)

reader