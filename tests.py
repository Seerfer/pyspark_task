import pytest
from chispa import *
from pyspark.sql import SparkSession
from main import filter_df_equal

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

    df_source = spark.createDataFrame(source_data, ["first_name", "second_name"])
    df_expected = spark.createDataFrame(expected_date, ["first_name", "second_name"])
    filter_dict = {"second_name": "Nowak"}
    assert_df_equality(filter_df_equal(df_source, filter_dict), df_expected, ignore_column_order=True)

