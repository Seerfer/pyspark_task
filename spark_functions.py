import logging
from typing import Union, TypedDict, List

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def setSparkSession(appname: str):
    spark = (
        SparkSession.builder.appName(appname)
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
    )
    return spark


def create_scheams():
    schema1 = StructType(
        [
            StructField("id", IntegerType()),
            StructField("first_name", StringType()),
            StructField("last_name", StringType()),
            StructField("email", StringType()),
            StructField("country", StringType()),
        ]
    )

    schema2 = StructType(
        [
            StructField("id", IntegerType()),
            StructField("btc_a", StringType()),
            StructField("cc_t", StringType()),
            StructField("cc_n", StringType()),
        ]
    )
    return schema1, schema2


def read_file(
    path: str, spark: SparkSession, schema: Union[StructType, None] = None
) -> DataFrame:
    if schema is not None:
        return spark.read.schema(schema).option("header", "true").csv(path)
    else:
        logging.warning(f'There is no schema provided for {path} file. Provide schema for better reading performence')
        return spark.read.option("header", "true").csv(path)


def write_df_to_file(
    df: DataFrame, filename: str = "output", path: str = "client_data"
):
    """Function that write given dataframe to csv file

       Important note: Commented line should work but it s not working on my local machine due to some spark/hadoop problems.
       Alternatively I used pandas dataframe to write this to csv

    Args:
        df (DataFrame): Input dataframe
        filename (str, optional): Name of csv file. Defaults to "output".
        path (str, optional): Destination path to save given file(path shoulkd be without "/" at the end). Defaults to "client_data".
    """
    #df.write.option("header",True).format("csv").save(f"{path}/{filename}.csv")
    logging.info(f'Writing file dataframe as: "{path}/{filename}.csv"')
    df.toPandas().to_csv(f"{path}/{filename}.csv", index=False)


def inner_join(df1: DataFrame, df2: DataFrame, key: str) -> DataFrame:
    """Function that perform inner join on two spark dataframes

    Args:
        df1 (DataFrame): Datframe 1
        df2 (DataFrame): Dataframe 2
        key (str): Join key

    Returns:
        DataFrame: output dataframe
    """
    logging.info(f'Performing join on: {key} == {key}')
    return df1.join(df2, key)


def drop_columns(df: DataFrame, columns: List) -> DataFrame:
    """Droping selected columns from given dataframe

    Args:
        df (DataFrame): Input dataframe
        columns (List): List of columns to drop

    Returns:
        DataFrame: Processed dataframe
    """
    logging.info(f'Dropping columns: {columns}')
    return df.drop(*columns)


def filter_df_equal(df: DataFrame, column_value: TypedDict) -> DataFrame:
    """Function that filter dataframe columns with given values

    Args:
        df (DataFrame): Input dataframe that we want to filter
        column_value (TypedDict): Python dictionary where keys are name of columns to filter and values are values used to filter given column
                                  Values can be given on list of strings (OR condition will be applied between values) or singel string

    Returns:
        DataFrame: Processed dataframe
    """
    for col, values in column_value.items():
        logging.info(f'Adding "{col} == {values}" filter condition')
        df = df.filter(F.col(f"{col}").isin(values))
    return df


def rename_columns(df: DataFrame, column_value: TypedDict) -> DataFrame:
    """Function that rename mulitple column in one dataframe

    Args:
        df (DataFrame): Input dataframe
        column_value (TypedDict): Python dictionary with format {"existing column": "new name"}

    Returns:
        DataFrame: Processed dataframe
    """
    for col, values in column_value.items():
        logging.info(f'Column renaming {col} -> {values}')
        df = df.withColumnRenamed(f"{col}", f"{values}")
    return df