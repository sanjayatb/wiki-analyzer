# wiki-analyzer
Simple English Wikipedia content analyze using pyspark, cassandra, airflow

### Requirements
* [spark-xml](https://github.com/databricks/spark-xml)
* [
spark-cassandra-connector](https://github.com/datastax/spark-cassandra-connector)
 
### Usage

    docker-compose -f docker-compose-wiki-analyzer.yml up -d
    
If you want to use Ad hoc query use **category_api.py** rest api

If you want to use sql queries use **sql_api.py**

### Results
Airflow DAG

pyspark Dataframes

Cassandra Tables

Rest API