import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def setSparkSession():  
    spark = SparkSession \
        .builder \
        .appName("test") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


def create_scheams():
    schema1 = StructType([
        StructField("id",IntegerType()), \
        StructField("first_name", StringType()), \
        StructField("last_name", StringType()), \
        StructField("email", StringType()), \
        StructField("country", StringType()), \
    ])

    schema2 = StructType([
        StructField("id",IntegerType()), \
        StructField("btc_a", StringType()), \
        StructField("cc_t", StringType()), \
        StructField("cc_n", StringType()), \
    ])

    return schema1, schema2

def read_file(path, spark, schema):
    return spark.read.schema(schema).csv(path)

schema1, schema2 = create_scheams()
spark = setSparkSession()
df1 = read_file("dataset_one.csv", spark, schema1)
df1.show()