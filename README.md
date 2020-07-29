# wiki-analyzer
Simple English Wikipedia content analyze using **pyspark, cassandra, airflow, flask** and deployed **docker** container in **AWS**

### Requirements
* pyspark
* cassandra
* Docker
* Flask
* [spark-xml](https://github.com/databricks/spark-xml)
* [spark-cassandra-connector](https://github.com/datastax/spark-cassandra-connector)
 
### Usage

    docker-compose -f docker-compose-wiki-analyzer.yml up -d
    
If you want to use Ad hoc query use **category_api.py** rest api

If you want to use sql queries use **sql_api.py**

### Results
1) Airflow scheduler
![Airflow DAG](https://github.com/sanjayatb/wiki-analyzer/blob/master/notes/AirflowWikiDag.JPG)

2) py spark analyzer
![pyspark Dataframes](https://github.com/sanjayatb/wiki-analyzer/blob/master/notes/SparkDataFrames.JPG)

3) Cassandra database
![Cassandra Tables](https://github.com/sanjayatb/wiki-analyzer/blob/master/notes/Cassandra_tables.JPG)

4) Flask Rest API
![Rest API](https://github.com/sanjayatb/wiki-analyzer/blob/master/notes/rest_api.JPG)

5) AWS Deployment
![AWS](https://github.com/sanjayatb/wiki-analyzer/blob/master/notes/AWS_EC2.JPG)
