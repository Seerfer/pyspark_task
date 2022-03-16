from spark_functions import *


def main(dataset1_path: str, dataset2_path: str, *args):
    logging.basicConfig(level=logging.INFO)
    spark = setSparkSession("spark_app")
    df1 = read_file(dataset1_path, spark)
    df2 = read_file(dataset2_path, spark)
    df1 = drop_columns(df1, ["first_name", "last_name"])
    df1 = filter_df_equal(df1, {"country": list(args)})
    df = inner_join(df1, df2, "id")
    rename_dict = {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type",
    }
    df = rename_columns(df, rename_dict)
    write_df_to_file(df)


if __name__ == "__main__":
    main("dataset_one.csv", "dataset_two.csv", "United Kingdom", "Netherlands")
