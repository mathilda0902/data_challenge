from pyspark import SparkContext, SQLContext
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.functions import udf, struct, date_format, countDistinct
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, StringType, FloatType, DoubleType
import json

text_file = sc.textFile('s3://emr-spark-notebook-data-challenge/file1.txt.gz')
# text_file.take(10)
rdd = text_file.map(lambda r: json.loads(r))
# rdd.take(2)
json_schema = StructType([StructField('batch_id', StringType(), nullable=False), 
                    StructField('vendor_id', StringType(), nullable=False),
                    StructField('product_id', StringType(), nullable=False),
                    StructField('lab_id', StringType(), nullable=False),
                    StructField('state', StringType(), nullable=False),
                    StructField('tested_at', StringType(), nullable=False),
                    StructField('expires_at', StringType(), nullable=False),
                    StructField('thc', DoubleType(), nullable=False),
                    StructField('thca', DoubleType(), nullable=False),
                    StructField('cbd', DoubleType(), nullable=False),
                    StructField('cbda', DoubleType(), nullable=False)])
df = spark.createDataFrame(data=rdd,schema=json_schema)
df.printSchema()
df.show(2)

new_df = df.select('tested_at',date_format('tested_at', 'E').alias('tested_at_day_of_week'),'batch_id','vendor_id','product_id','lab_id','state','expires_at','thc','thca','cbd','cbda')
new_df.show(2)

df1 = new_df.limit(100000)
df1.repartition(4).write.partitionBy("tested_at").parquet("s3a://data-challenge-lab-tests/",mode="append")

batch_list = df1.select("batch_id").rdd.flatMap(lambda x: x).collect()
remaining_df = new_df.filter(new_df.batch_id.isin(batch_list) == False)
df2 = remaining_df.limit(100000)
df2.repartition(4).write.partitionBy("tested_at").parquet("s3a://data-challenge-lab-tests/",mode="append")

def trunc_update(sub_df, batch_list):
    batch_list.extend(sub_df.select("batch_id").rdd.flatMap(lambda x: x).collect())
    remaining_df = new_df.filter(new_df.batch_id.isin(batch_list) == False)
    sub_df = remaining_df.limit(100000)
    sub_df.repartition(4).write.partitionBy("tested_at").parquet("s3a://data-challenge-lab-tests/",mode="append")
    batch_df = spark.createDataFrame(batch_list, StringType())
    batch_df.write.csv('"s3a://emr-spark-notebook-data-challenge/batch.csv',mode="overwrite")
    return sub_df, batch_list


batch_download_df = spark.read.csv("s3a://emr-spark-notebook-data-challenge/batch.csv")

sub_df, batch_list = trunc_update(df2, batch_list)
sub_df, batch_list = trunc_update(sub_df, batch_list)