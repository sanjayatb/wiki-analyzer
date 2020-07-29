# wiki-analyzer
Simple English Wikipedia content analyze using pyspark, cassandra, airflow

### Required libs
spark-cassandra-connector-2.4.0-s_2.11.jar

spark-xml_2.12-0.9.0.jar

### Usage

    docker-compose -f docker-compose-wiki-analyzer.yml up -d
    
If you want to use Ad hoc query use **category_api.py** rest api

If you want to use sql quries use **sql_api.py**