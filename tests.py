import pytest
from chispa import *
from pyspark.sql import SparkSession
from main import filter_df_equal, rename_columns, drop_columns, inner_join


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("chispa").getOrCreate()


def test_filter_df_equal(spark):
    source_data = [("Jan", "Kowalski"), ("Maciej", "Nowak"), ("Jan", "Nowak")]
    expected_date = [("Maciej", "Nowak"), ("Jan", "Nowak")]
    column_names = ["first_name", "second_name"]
    df_source = spark.createDataFrame(source_data, column_names)
    df_expected = spark.createDataFrame(expected_date, column_names)
    filter_dict = {"second_name": "Nowak"}
    assert_df_equality(
        filter_df_equal(df_source, filter_dict), df_expected, ignore_column_order=True
    )


def test_rename_columns(spark):
    data = [("test1", "test2"), ("test3", "test4")]
    input_col_names = ["col1", "col2"]
    expected_col_names = ["renamed1", "renamed2"]
    df_source = spark.createDataFrame(data, input_col_names)
    df_expected = spark.createDataFrame(data, expected_col_names)
    column_rename = {"col1": "renamed1", "col2": "renamed2"}
    assert_df_equality(
        rename_columns(df_source, column_rename), df_expected, ignore_column_order=True
    )


def test_drop_columns(spark):
    input_data = [
        ("test11", "test12", "test13", "test14"),
        (("test21", "test22", "test23", "test24")),
    ]
    input_col = ["col1", "col2", "col3", "col4"]
    expected_data = [("test11", "test12"), ("test21", "test22")]
    expectedt_col = ["col1", "col2"]
    col_to_drop = ["col3", "col4"]
    df_input = spark.createDataFrame(input_data, input_col)
    df_expected = spark.createDataFrame(expected_data, expectedt_col)

    assert_df_equality(
        drop_columns(df_input, col_to_drop), df_expected, ignore_column_order=True
    )


def test_inner_join(spark):
    data1 = [("1", "John", "Coffey"), ("2", "Paul", "Edgecombe")]
    col1 = ["id", "first_name", "last_name"]
    data2 = [("1", "12345678"), ("2", "87654321")]
    col2 = ["id", "phone_number"]

    expected_data = [
        ("1", "John", "Coffey", "12345678"),
        ("2", "Paul", "Edgecombe", "87654321"),
    ]
    expected_col = ["id", "first_name", "last_name", "phone_number"]

    df1 = spark.createDataFrame(data1, col1)
    df2 = spark.createDataFrame(data2, col2)
    df_expected = spark.createDataFrame(expected_data, expected_col)
    assert_df_equality(
        inner_join(df1, df2, "id"), df_expected, ignore_column_order=True
    )
