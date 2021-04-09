from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, StringType, FloatType
import json


def parse_json(array_str):
    json_obj = json.loads(array_str)
    return json_obj

def construct_json_array():
    row_array = []
    for row in text_file.take(10):
        row_array.append(parse_json(row))

def construct_schema():
    json_schema = StructType([StructField('batch_id', StringType(), nullable=False), 
                    StructField('vendor_id', StringType(), nullable=False),
                    StructField('product_id', StringType(), nullable=False),
                    StructField('lab_id', StringType(), nullable=False),
                    StructField('state', StringType(), nullable=False),
                    StructField('tested_at', StringType(), nullable=False),
                    StructField('expires_at', StringType(), nullable=False),
                    StructField('thc', FloatType(), nullable=False),
                    StructField('thca', FloatType(), nullable=False),
                    StructField('cbd', FloatType(), nullable=False),
                    StructField('cbda', FloatType(), nullable=False)])
    return json_schema

text_file = sc.textFile('s3://emr-spark-notebook-data-challenge/file1.txt.gz')

df = spark.createDataFrame(data=row_array,schema=json_schema)
df2 = df.withColumn("tested_at_date", F.col("tested_at").cast("date"))
df2 = df2.withColumn("expires_at_date", F.col("expires_at").cast("date"))