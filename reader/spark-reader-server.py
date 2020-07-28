from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark import SparkFiles
from pyspark.sql.functions import udf,col,to_timestamp,expr,max as max_,unix_timestamp
import re

APP_NAME = "Wikipedia extractor"
MASTER = "local"

URL="https://dumps.wikimedia.org/simplewiki/latest/simplewiki-latest-pages-articles-multistream.xml.bz2"
PATH_WIKI_XML = '/job/data/'
FILENAME_WIKI = 'simplewiki-latest-pages-articles-multistream.xml.bz2'
TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

spark = SparkSession.builder \
   .appName(APP_NAME) \
   .master(MASTER) \
   .getOrCreate()

spark.sparkContext.addFile(URL)

schema = StructType([
   StructField('id', IntegerType(), False),
   StructField('title', StringType(), False),
   # StructField('redirect', StringType(), True)
   StructField('revision', StructType([
        StructField('timestamp', StringType(), False),
        StructField('text', StringType(), False)
   ]), False)
])

df = spark\
   .read\
   .format("xml") \
   .option("rowTag","page")\
   .option("attributePrefix","title")\
   .option("mode","DROPMALFORMED")\
   .load(SparkFiles.get(FILENAME_WIKI), schema=schema)

df.printSchema()

def extract_cat(text):
    all_groups = re.findall(r'(\[\[Category:)(\w+)(\]\])', text)
    words = [ group[1] for group in all_groups]
    return words

def extract_links(text):
    all_groups = re.findall(r'(\[\[)(\w+)(\]\])', text)
    words = [ group[1] for group in all_groups]
    return words

extract_cat_udf = udf(extract_cat, ArrayType(StringType()))
extract_links_udf = udf(extract_links, ArrayType(StringType()))

##Select sample for analyze
# df = df.limit(1000)

df = df.withColumn('categories',extract_cat_udf('revision.text'))\
    .withColumn('page_links',extract_links_udf('revision.text'))\
    .withColumn('last_modify_date',to_timestamp('revision.timestamp',TIME_FORMAT))

df_pages = df.select('id','title','last_modify_date','categories','page_links')
# df_pages.show()

df_categories = df.selectExpr('id','title','explode(categories) as category','last_modify_date')
df_links = df.selectExpr('id','title','explode(page_links) as link','last_modify_date')

# df_links.show()

joined_df = df_links.alias("dl")\
    .join(df_pages.alias("pg"), col('dl.link') == col('pg.title'))\
    .selectExpr('dl.*','pg.last_modify_date as link_modify_date')\
    .filter(col('link_modify_date') > col('dl.last_modify_date'))\
    .withColumn('outdate_time',(unix_timestamp(col('link_modify_date'),TIME_FORMAT) - unix_timestamp(col('dl.last_modify_date'),TIME_FORMAT)))\
    .groupBy('title')\
    .agg(max_(col('outdate_time')).alias('max_outdate_time'))

joined_df.printSchema()
# joined_df.show()

cat_out_dated = df_categories.alias('ct')\
    .join(joined_df.alias('jd'),expr('ct.title = jd.title'))\
    .select('ct.category','ct.title','jd.max_outdate_time')

# cat_out_dated.sort('ct.category').show()

cat_out_dated_grouped = cat_out_dated.groupBy('ct.category')\
    .agg(max_(col('max_outdate_time')).alias('max_outdate_time_for_cat'))

# final_outdate_df = cat_out_dated.alias('cl')\
#     .join(cat_out_dated_grouped.alias('gp'),expr('cl.category=gp.category & cl.max_outdate_time=gp.max_outdate_time_for_cat'))
#
# final_outdate_df.sort('cl.category').show()

df_links.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="pages", keyspace="wiki")\
    .save()

cat_out_dated_grouped.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="categories", keyspace="wiki")\
    .save()