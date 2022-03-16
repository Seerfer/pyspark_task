import argparse

from spark_functions import *


def _country_name_process(name: str) -> str:
    """Function made to add space in country names that contains two words splited by upper case
    Example: UnitedKingdom -> United Kingdom

    Args:
        name (str): string to proces

    Returns:
        str: proccesed string
    """
    prevletter = None
    output = ""
    for letter in name:
        if letter.isupper():
            if prevletter == "lower":
                output += " "
            output += letter
        else:
            prevletter = "lower"
            output += letter
    return output


def main(dataset1_path: str, dataset2_path: str, filter_countries):
    logging.basicConfig(level=logging.INFO)
    schema1, schema2 = create_scheams()
    spark = setSparkSession("spark_app")
    df1 = read_file(dataset1_path, spark, schema1)
    df2 = read_file(dataset2_path, spark, schema2)
    df1 = drop_columns(df1, ["first_name", "last_name"])
    df2 = drop_columns(df2, ["cc_n"])
    df1 = filter_df_equal(df1, {"country": filter_countries})
    df = inner_join(df1, df2, "id")
    rename_dict = {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type",
    }
    df = rename_columns(df, rename_dict)
    write_df_to_file(df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--d1",
        "--dataset1",
        type=str,
        help="Provide path to datset 1",
        default="dataset_one.csv",
    )
    parser.add_argument(
        "--d2",
        "--dataset2",
        type=str,
        help="Provide path to dataset 2",
        default="dataset_two.csv",
    )
    parser.add_argument("--countries", help="Countries to fillter", nargs="*")
    args = vars(parser.parse_args())
    countries = [_country_name_process(name) for name in args["countries"]]
    main(args["d1"], args["d2"], countries)
