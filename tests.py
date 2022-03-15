import pytest
from chispa import *
from pyspark.sql import SparkSession
from main import filter_df_equal, rename_columns

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local") \
      .appName("chispa") \
      .getOrCreate()


def test_filter_df_equal(spark):
    source_data = [
        ("Jan", "Kowalski"),
        ("Maciej", "Nowak"),
        ("Jan", "Nowak")
    ]
    expected_date = [
        ("Maciej", "Nowak"),
        ("Jan", "Nowak")
    ]
    column_names = ["first_name", "second_name"]
    df_source = spark.createDataFrame(source_data, column_names)
    df_expected = spark.createDataFrame(expected_date, column_names)
    filter_dict = {"second_name": "Nowak"}
    assert_df_equality(filter_df_equal(df_source, filter_dict), df_expected, ignore_column_order=True)

def test_rename_columns(spark):
    data = [
        ("test1", "test2"),
        ("test3", "test4")
    ]
    input_col_names = ["col1", "col2"]
    expected_col_names = ["renamed1", "renamed2"]
    df_source = spark.createDataFrame(data, input_col_names)
    df_expected = spark.createDataFrame(data, expected_col_names)   
    column_rename = {"col1":"renamed1", "col2":"renamed2"}
    assert_df_equality(rename_columns(df_source, column_rename), df_expected, ignore_column_order=True)