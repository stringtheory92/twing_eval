import csv
import pandas as pd
import ast
import json


def load_csv(filename):
    """
    Load data from a CSV file and return it as a pandas DataFrame.
    """
    return pd.read_csv(filename)


def extract_tables(row):
    """
    Extract unique tables from the "tables" column along with their aliases.
    """
    tables = ast.literal_eval(row["tables"])
    table_names = []
    aliases = []
    # print("tables: ", tables)
    # print("aliases: ", aliases)
    for table in tables:
        parts = table.split(" as ")
        table_names.append(parts[0])
        if len(parts) > 1:
            aliases.append(parts[1])
        else:
            aliases.append(None)
    return pd.Series({"table": table_names, "aliases": [aliases]})


def replace_table_aliases(row):
    """
    Replace table aliases with full table names in specified columns.
    """
    tables = ast.literal_eval(row["tables"])

    table_name = None
    table_alias = None

    for table in tables:

        parts = table.split(" as ")
        if len(parts) == 2:
            table_name = parts[0]
            table_alias = parts[1]
        # elif len(parts) > 2:
        # print(f"Unexpected format in 'tables' column: {table}")
        # else:
        # print(f"No alias found for {table}")

        if table_name is not None and table_alias is not None:

            columns_to_replace = [
                "select_columns",
                "join_columns",
                "where_columns",
                "agg_columns",
                "update_columns",
                "insert_columns",
            ]

            for col in columns_to_replace:
                if isinstance(row[col], str):
                    if pd.notna(row[col]):  # Check if the value is not NaN
                        replaced_columns = []
                        col_list = json.loads(row[col])
                        for column in col_list:
                            if column.startswith(table_alias + "."):
                                column = column.replace(
                                    table_alias + ".", table_name + "."
                                )
                            replaced_columns.append(column)

                        row[col] = replaced_columns
                elif isinstance(row[col], list):
                    replaced_columns = []
                    for column in row[col]:
                        if column.startswith(table_alias + "."):
                            column = column.replace(table_alias + ".", table_name + ".")
                        replaced_columns.append(column)

                    row[col] = replaced_columns

    return row


def main():

    csv_filename = "query_analysis_data_v2.csv"
    data = load_csv(csv_filename)

    data = data.apply(replace_table_aliases, axis=1)

    data.to_csv("refined.csv", index=False)

    return data


if __name__ == "__main__":
    main()
