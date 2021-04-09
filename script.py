from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, StringType, DoubleType
import json

def construct_json_rdd(text_file):
    rdd = text_file.map(lambda r: json.loads(r))
    return rdd

def construct_schema():
    schema = StructType([StructField('batch_id', StringType(), nullable=False), 
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
    return schema

def main():
    sc = SparkContext()
    text_file = sc.textFile('s3://emr-spark-notebook-data-challenge/file1.txt.gz')
    data_rdd = construct_json_rdd(text_file)
    schema = construct_schema()
    df = sc.createDataFrame(data=data_rdd,schema=schema)
    # n = 12 
    # spark_df = df2.repartition(n)
    spark_df = df.limit(1000)
    spark_df.write.partitionBy("tested_at").parquet("s3a://data-challenge-lab-tests/",mode="append")


if __name__ == '__main__':
    main()