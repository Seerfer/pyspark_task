import logging
from typing import Union, TypedDict

from pyspark.sql import SparkSession, DataFrame 
import pyspark.sql.functions as F
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

def read_file(path: str, spark: SparkSession, schema: Union[StructType, None] = None) -> DataFrame:
    if schema is not None:
        return spark.read.schema(schema).option("header", "true").csv(path)
    else: 
       return spark.read.option("header", "true").csv(path)


def filter_df_equal(df: DataFrame, column_value: TypedDict) -> DataFrame:
    for col, values in column_value.items():
        df=df.filter(F.col(f"{col}").isin(values))
    return df


if __name__ == "__main__":
    schema1, schema2 = create_scheams()
    spark = setSparkSession()
    df1 = read_file("dataset_one.csv", spark)
    filter_values ={"country": "Netherlands"}
    df1 = filter_df_equal(df1, filter_values)
    df1.show()