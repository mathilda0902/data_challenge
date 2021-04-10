from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json

def create_spark_session():
    """Create spark session.
    """
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    return spark

def main(file_path):
    text_file = sc.textFile(file_path)
    rdd = text_file.map(lambda r: json.loads(r))
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
    new_df = df.select('tested_at',date_format('tested_at', 'E').alias('tested_at_day_of_week'),'batch_id','vendor_id','product_id', \
                'lab_id','state','expires_at','thc','thca','cbd','cbda')
    new_df.repartition(12).write.partitionBy("tested_at").parquet("s3a://data-challenge-lab-tests/",mode="append")


if __name__ == '__main__':
    file_path_pattern = f's3://emr-spark-notebook-data-challenge/file{i}.txt.gz'
    spark = create_spark_session()
    sc = SparkContext(appName="data-challenge")
    for i in range(8):
        file_path = file_path_pattern
        main(file_path)