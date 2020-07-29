from pyspark.sql import SparkSession

appName = "Wikipedia Data Reader"
master = "local"

spark = SparkSession.builder \
   .appName(appName) \
   .master(master)\
   .getOrCreate()

spark.conf.set("spark.cassandra.connection.host", "localhost")


def execute(sql):
    df = spark.sql(sql)
    return df


execute("select * from wiki.pages")


