from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import *
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.LongType.html !

spark = SparkSession.builder.getOrCreate()



fields = [
    StructField('user_id', StringType(), True),
    StructField('session_id', StringType(), True),
    StructField('event_type', StringType(), True),
    StructField('event_page', StringType(), True),
    StructField('timestamp', LongType(), True)
]

# spark.sql.types.StructType aa
schema = StructType(fields=fields)



# DF
df = spark.read.csv('/user/gutevova/lsml/sga/clickstream.csv', schema=schema, sep='\t', header=True)

df = df.where("event_type != 'event'") \
       .withColumn("id", concat(col("user_id"), lit('_'), col("session_id"))) \
       .select("id", "event_type", "event_page", "timestamp")

# https://sparkbyexamples.com/spark/using-groupby-on-dataframe/#agg

first_err = df.filter(col('event_type').contains('error')) \
                .groupBy("id") \
                .agg(min("timestamp").alias("first_err_timestamp"))

# do not forget sort desc  and top 30
paths = df.join(first_err, ["id"], "left") \
           .filter("timestamp < first_err_timestamp OR first_err_timestamp is NULL") \
           .select("id", "event_page", "timestamp") \
           .groupBy("id").agg(collect_list(struct("timestamp", "event_page")).alias("pages")) \
           .select("id", sort_array("pages").alias("pages")) \
           .select(concat_ws("-", "pages.event_page").alias("path")) \
           .groupBy("path").count().orderBy('count', ascending=False).limit(30) \
           .collect()


# print(path
for path, count in paths:
    print(path, count, sep='\t')
